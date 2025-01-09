# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 15:33:29 2018

@author: carlos.santanna.ext
"""

### Import libraries ###
from __future__ import print_function, division
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
import sys
from datetime import datetime, timedelta
import config_database as dbconf
import config_aws as awsconf


if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    # Get current date in GMT-0 from the system to execute the process
    dateToProcess = datetime.now() - timedelta(days = 1)
    
    # Configure the dates for the filter to be used in the query
    fromDateGmt0 = dateToProcess.strftime("%Y-%m-%d") + ' 00:00:00'
    toDateGmt0 = dateToProcess.strftime("%Y-%m-%d") + ' 23:59:59'
    
    inicioDia = dateToProcess.strftime("%Y-%m-%d") + ' 00:00:00'
    fimDia = dateToProcess.strftime("%Y-%m-%d") + ' 23:59:59'
    
    #Get position date short to filter the query
    fromDate = datetime.now() - timedelta(days = 2)
    toDate = datetime.now()
    
    fromDateShort = fromDate.strftime("%Y-%m-%d")
    toDateShort = toDate.strftime("%Y-%m-%d")
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    hiveSql = "SELECT VEICULO AS ID_VEICULO, \
                      DATA_POSICAO_GMT0, \
                      ODOMETRO \
               FROM  POSICOES_CRCU \
               WHERE DATA_POSICAO_SHORT >= '{0}' \
                AND  DATA_POSICAO_SHORT <= '{1}' \
                AND  DATA_POSICAO_GMT0 >= '{2}' \
                AND  DATA_POSICAO_GMT0 <= '{3}'".format(fromDateShort, toDateShort, fromDateGmt0, toDateGmt0)
    
    vehicles = sqlContext.sql(hiveSql)
    
    redshiftSql = "SELECT ID_VEICULO AS ID_VEICULO_ODOMETRO, \
                          ODOMETRO_ACUMULADO, \
                          ODOMETRO_ULTIMA_POSICAO, \
                          DATA_INICIO, \
                          DATA_PROCESSAMENTO \
                   FROM  ODOMETRO_CARRETA"
    
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    
    vehiclesOdometer = sqlContext.read \
    		.format("com.databricks.spark.redshift") \
    		.option("url", redshiftURL) \
    		.option("tempdir", s3TempDir) \
    		.option("query", redshiftSql) \
    		.load()
    
    vehiclesJoin = vehicles.join(vehiclesOdometer, vehicles.ID_VEICULO == vehiclesOdometer.id_veiculo_odometro, "leftouter")
    
    adjustedVehicles = vehiclesJoin.fillna({'odometro_acumulado':0}).fillna({'odometro_ultima_posicao':0})
    
    vehiclesCastDate = adjustedVehicles.select("ID_VEICULO", F.col("DATA_POSICAO_GMT0").cast("timestamp").alias("DATA_POSICAO"), "ODOMETRO", "ODOMETRO_ACUMULADO", "ODOMETRO_ULTIMA_POSICAO")
    
    vehiclesPartitioned = vehiclesCastDate.repartition(5, "ID_VEICULO")
    
    vehiclesRdd = vehiclesPartitioned.rdd
    
    vehiclesRddMapped = vehiclesRdd.map(lambda x: (x['ID_VEICULO'], (x['DATA_POSICAO'], x['ODOMETRO'], x['ODOMETRO_ACUMULADO'], x['ODOMETRO_ULTIMA_POSICAO'])))
    
    vehiclesRddGrouped = vehiclesRddMapped.groupByKey().mapValues(lambda vs: sorted(vs, key=lambda x: x[0:1]))
    
    def ajustaOdometro(group):
        key, values = group[0], group[1]
        
        isFirstRow = 1
        odometroAnterior = 0
        distanciaAcumulada = 0
        lista = []
        
        for i in range(len(values)):
            value = values[i]
            
            dataPosicao = value[0]
            odometro = int(value[1])
            odometroAcumuladoPassado = int(value[2])
            odometroUltimaPosicao = int(value[3])
            
            if(isFirstRow == 1):
               if(odometro > 0):
                  odometroAnterior = odometroUltimaPosicao
                  
               else:
                  odometroAnterior = 0
               
               isFirstRow = 0
            
            distancia = int(odometro) - int(odometroAnterior)
            
            if(distancia < 0):
               if(odometro == 0):
                  distancia = 0
               else:
                  distancia = odometro
            
            distanciaAcumulada = distanciaAcumulada + distancia
            
            odometroAnterior = odometro
            
            odometroRow = {}
            odometroRow['ID_VEICULO'] = key
            odometroRow['DATA_POSICAO_GMT0'] = str(dataPosicao)
            odometroRow['ODOMETRO'] = odometro
            odometroRow['ODOMETRO_ACUMULADO_PASSADO'] = odometroAcumuladoPassado
            odometroRow['DISTANCIA_ACUMULADA'] = distanciaAcumulada
            
            if(i == len(values) - 1):
              odometroRow['ODOMETRO_ULTIMA_POSICAO_NEW'] = odometro
              
            else:
              odometroRow['ODOMETRO_ULTIMA_POSICAO_NEW'] = 0
            
            lista.append(odometroRow)
            
        return lista
    
    processedRdd = vehiclesRddGrouped.flatMap(ajustaOdometro)
    
    if(processedRdd.isEmpty() == False):
       
       vehiclesDf = processedRdd.toDF()
       
       vehiclesCumulativeOdometer = vehiclesDf.withColumn('ODOMETRO_ACUMULADO_NEW', vehiclesDf['DISTANCIA_ACUMULADA'] + vehiclesDf['ODOMETRO_ACUMULADO_PASSADO'])
       
       finalOdometerVehicles = vehiclesCumulativeOdometer.select("ID_VEICULO", "DATA_POSICAO_GMT0", F.col("ODOMETRO_ACUMULADO_NEW").alias("ODOMETRO"))
       
       finalOdometerVehicles.write.mode("overwrite").saveAsTable("stageodometro")
       
       dateProcess = datetime.now()
       dateProcessString = dateProcess.strftime("%Y-%m-%d %H:%M:%S")
       
       agregadoOdometroCarreta = vehiclesCumulativeOdometer.groupBy("ID_VEICULO").agg(F.max("ODOMETRO_ACUMULADO_NEW").alias("ODOMETRO_ACUMULADO"), F.max("ODOMETRO_ULTIMA_POSICAO_NEW").alias("ODOMETRO_ULTIMA_POSICAO"))
       
       ajusteVeiculosRedshift = vehiclesOdometer.select("ID_VEICULO_ODOMETRO", "DATA_INICIO")
       
       joinOdometroCarreta = agregadoOdometroCarreta.join(ajusteVeiculosRedshift, agregadoOdometroCarreta.ID_VEICULO == ajusteVeiculosRedshift.ID_VEICULO_ODOMETRO, "leftouter").drop("ID_VEICULO_ODOMETRO")
       
       ajustedOdometroCarreta = joinOdometroCarreta.fillna({'data_inicio':dateProcessString}).withColumn('DATA_PROCESSAMENTO', F.lit(dateProcessString))
       
       odometroCarreta = ajustedOdometroCarreta.select("ID_VEICULO", "DATA_INICIO", "DATA_PROCESSAMENTO", "ODOMETRO_ACUMULADO", "ODOMETRO_ULTIMA_POSICAO")
       
       ajusteVeiculosRedshift = vehiclesOdometer.select("ID_VEICULO_ODOMETRO", "DATA_INICIO", "DATA_PROCESSAMENTO", "ODOMETRO_ACUMULADO", "ODOMETRO_ULTIMA_POSICAO")
       
       odometroCarreta.createOrReplaceTempView("odometroCarreta")
       ajusteVeiculosRedshift.createOrReplaceTempView("odometroRedshift")
       
       sql = "SELECT * \
              FROM odometroRedshift or \
              WHERE NOT EXISTS (SELECT ID_VEICULO \
                                FROM odometroCarreta o \
                                WHERE o.id_veiculo = or.id_veiculo_odometro)"
       
       odometroCarretaDiff = sqlContext.sql(sql)
       
       unionAllOdometroCarreta = odometroCarreta.unionAll(odometroCarretaDiff)
       
       unionAllOdometroCarreta.write \
           .format("com.databricks.spark.redshift") \
           .option("url", redshiftURL) \
           .option("tempdir", s3TempDir) \
           .option("dbtable", "ODOMETRO_CARRETA") \
           .mode("overwrite") \
           .save()
    
    
    
    
    
    
    
    
    
    