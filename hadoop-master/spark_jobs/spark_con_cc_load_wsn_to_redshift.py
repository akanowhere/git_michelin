# -*- coding: utf-8 -*-
##############################################################################
#### spark_con_cc_load_positions_to_redshift.py                           ####
#### Job para Carga das posições do hive para o redshift                  ####
#### -------------------------------------------------------------------- ####
#### Versões                                                              ####
#### 1 - Fabio Schultz - 25-01-2018                                       ####
####     Criação do Processo                                              ####
##############################################################################


### Import libraries 
# ####

from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from pyspark.sql.functions import udf, lit, col, max, when
from pyspark.sql.types import *
import sys
import logging
import config_database as dbconf
import config_aws as awsconf




if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    #Get the root logger
    logger = logging.getLogger()
    
    #Have to set the root logger level, it defaults to logging.WARNING
    logger.setLevel(logging.NOTSET)
    #Config log to show msgs in stdout
    logging_handler_out = logging.StreamHandler(sys.stdout)
    logging_handler_out.setLevel(logging.ERROR)
    logger.addHandler(logging_handler_out)

    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)

    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    dateProcessBegin = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
    dateProcessEnd = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
    
    dateProcessShortBegin = (dateProcessBegin - timedelta(days=5)).strftime("%Y-%m-%d")
    dateProcessShortEnd = (dateProcessEnd + timedelta(days=1)).strftime("%Y-%m-%d")
    
    #dateBegin = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S") - timedelta(days = 5)
    #dateEnd = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S") + timedelta(days = 1)
    #dateProcessShortBegin = dateBegin.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    #dateProcessShortEnd = dateEnd.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    
    #dateProcessShortBegin = dateProcessBegin.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    #dateProcessShortEnd = dateProcessEnd.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    
    ### Select all positions from hive###
    ### all CRCU positions have 0 to satelital ###

    wsnSql = """select veiculo as id_veiculo, 
              data_posicao,
       plw.payload_wsn.content.dos as wsn_dos,
       plw.payload_wsn.content.tes as wsn_tes,
       plw.payload_wsn.content.vid as wsn_vid
from posicoes_crcu
lateral view explode(payload_wsn) plw as payload_wsn 
WHERE DATA_POSICAO_SHORT >= '{0}' \
                 AND  DATA_POSICAO_SHORT <= '{1}' \
                 AND  DATA_INSERT > '{2}' \
                 AND  DATA_INSERT <= '{3}'""".format(dateProcessShortBegin, dateProcessShortEnd, dateProcessBegin, dateProcessEnd)

    dfWsn = sqlContext.sql(wsnSql)

    def getDos(wsn_dos):
        dos = []
        for sensor in wsn_dos:
           if sensor.status <> 0:
                dosSensor = {}
                dosSensor['Rank'] = str(sensor["index"])
                dosSensor['State'] = 'OPEN' if sensor.status == 1  else 'CLOSED' 
                dos.append(dosSensor) 
        return str(dos)
  
    def getTes(wsn_tes):
        tes = []
        for sensor in wsn_tes:
            if sensor.temperature > -50:
                tesSensor = {}
                tesSensor['Rank'] = str(sensor["index"])
                tesSensor['Temperature'] = str(sensor["temperature"])
                tes.append(tesSensor)  
        return str(tes)

    def getVid(wsn_vid):
        vid = []
        for sensor in wsn_vid:
            if sensor.status == "active": 
                vidSensor = {}
                vidSensor['Rank'] = str(sensor["index"]) 
                vidSensor['Identifier'] = str(sensor["id"])
                vidSensor['Label'] = str(sensor["tag"]) 
                vid.append(vidSensor)
        return str(vid)

    getDosUDF = udf(getDos,StringType())

    getTesUDF = udf(getTes,StringType())

    getVidUDF = udf(getVid,StringType())


    newDfWsn = dfWsn.withColumn('dos', getDosUDF('wsn_dos')) \
            .withColumn('tes', getTesUDF('wsn_tes')) \
            .withColumn('vid', getVidUDF('wsn_vid'))
    
    wsn = newDfWsn.select('id_veiculo','data_posicao','dos','tes','vid') 
      
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL

    ### Make a connection to Redshift and write the data into the table 'FT_CC_POSICOES_WSN' ###
    wsn.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "FT_CC_POSICOES_WSN") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()

    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()