# -*- coding: utf-8 -*-
##############################################################################
#### spark_con_cc_rep_temp_pres.py                                        ####
#### Script de carga de informações de pressão e temperatura              ####
#### -------------------------------------------------------------------- ####
#### Versões                                                              ####
#### 1 - Carlos Eduardo - 09-02-2018                                      ####
####     Criação do Processo                                              ####
#### 2 - Thomas Lima - 01-04-2019                                         ####
####     Inclusão da posição do pneu na carga dos dados                   ####
##############################################################################

### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys
import config_database as dbconf
import config_aws as awsconf
from pyspark.sql.functions import udf


### FUNCTION TO RETURN THE TIRE POSITION ###
def getTyrePosition(devicePosition, tire):
    devicePosition1 = {
                1: '0x00', 2: '0x01', 3: '0x02', 4: '0x03',  5: '0x10', 6: '0x11',
                7: '0x12', 8: '0x13', 9: '0x20', 10: '0x21', 11: '0x22', 12: '0x23'
            }
    devicePosition2 = {
                1: '0x40', 2: '0x41', 3: '0x42', 4: '0x43',  5: '0x50',  6: '0x51',
                7: '0x52', 8: '0x53', 9: '0x60', 10: '0x61', 11: '0x62', 12: '0x63'
            }
    devicePosition3 = {
                1: '0x80', 2: '0x81', 3: '0x82', 4: '0x83',  5: '0x90',  6: '0x91',
                7: '0x92', 8: '0x93', 9: '0xA0', 10: '0xA1', 11: '0xA2', 12: '0xA3'
            }
    if devicePosition == 1:
        return devicePosition1.get(tire+1, None)
    if devicePosition == 2:
        return devicePosition2.get(tire+1, None)
    if devicePosition == 3:
        return devicePosition3.get(tire+1, None)
    return None

getTyrePositionUDF = udf(getTyrePosition)

if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ### Create Stage table name dynamic using parameter
    stg_table_name = 'STG_GMT_'+sys.argv[3].replace('/','_').replace(' ','_')
    
    hiveSql = """SELECT 
                    d.data_posicao_short, 
                    d.data_posicao_gmt0, 
                    d.veiculo, 
                    d.cliente, 
                    d.identifier AS id_pneu, 
                    d.pressure AS pressao, 
                    d.temperature AS temperatura, 
                    d.index as index_pneu,
                    d.device_position,
                    d.odometro, 
                    d.flag_new 
                FROM 
                    (
                        SELECT b.cliente, 
                            b.veiculo, 
                            b.data_posicao_gmt0, 
                            b.data_posicao_short, 
                            b.odometro, 
                            b.device_position,
                            c.sensor.identifier, 
                            c.sensor.pressure, 
                            c.sensor.temperature, 
                            c.sensor.index, 
                            b.flag_new 
                        FROM 
                            (
                                SELECT 
                                    p.cliente, 
                                    p.veiculo,  
                                    p.data_posicao_gmt0, 
                                    p.data_posicao_short, 
                                    p.odometro, 
                                    a.payload_tpm.content.sensors as sensors,
                                    a.payload_tpm.content.device_position,
                                    p.flag_new 
                                FROM 
                                    {0} p 
                                    LATERAL VIEW explode(payload_tpm) a AS payload_tpm 
                                WHERE 
                                    a.payload_tpm.content.device_serial > 0  
                                    and a.payload_tpm.content.device_serial is not null 
                            ) b 
                            LATERAL VIEW explode(b.sensors) c AS sensor 
                        WHERE 
                            c.sensor.status = 'activated' 
                            AND  c.sensor.com = 2 
                            AND  c.sensor.identifier > 0 
                            AND  c.sensor.identifier < 4294967293 
                            AND c.sensor.identifier IS NOT NULL
                    ) d""".format(stg_table_name)
    
    veiculos = sqlContext.sql(hiveSql)
	
    ### Query to get the correct aixs and position of the tire
    sql = "(SELECT distinct RFSID_VEICULO as veiculoobgfinan \
            FROM manutencao.cliente_obg_financeira \
            WHERE RFSTAG_FUNCIONALIDADE = 'SAS_WEB_SEG_TPMS') obrigacao"
	
    veiculosObrigacao = sqlContext.read.format("jdbc"). \
		 option("url", dbconf.DOMINIO). \
		 option("driver", "org.postgresql.Driver"). \
		 option("dbtable", sql). \
		 load()
    veiculosTires = veiculos.join(veiculosObrigacao, veiculos.veiculo == veiculosObrigacao.veiculoobgfinan)
	
    ### Create a temporary view of the current Spark dataframe ###
    veiculosTires.createOrReplaceTempView("veiculosTires")
	
    # Prepare dataframe for detailed report to store in the Redshift database
    sqlTempView = """SELECT 
                        data_posicao_gmt0 as DAT_POSICAO, 
						id_pneu AS ID_PNEU, 
						veiculo AS ID_VEICULO, 
						cliente as ID_CLIENTE, 
						pressao AS PRESSAO_PNEU, 
						temperatura AS TEMPERATURA_PNEU, 
                        index_pneu, 
                        device_position, 
                        flag_new 
					FROM 
                        veiculosTires"""
    redshiftReport = sqlContext.sql(sqlTempView)
	
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    
    ## Create Detailed Report
    detailedReport = redshiftReport.filter("flag_new = 1").drop("flag_new")
	
    ## Add posição do pneu
    detailedReportFinal = detailedReport.withColumn('POSICAO_PNEU', getTyrePositionUDF('device_position', 'index_pneu'))
    
    ## Select the columns and save on database 
    detailedReportFinal.select('DAT_POSICAO','ID_PNEU','ID_VEICULO','ID_CLIENTE','PRESSAO_PNEU','TEMPERATURA_PNEU','POSICAO_PNEU').write \
		.format("com.databricks.spark.redshift") \
		.option("url", redshiftURL) \
		.option("tempdir", s3TempDir) \
		.option("dbtable", "FT_CC_TEMP_PRES_PNEU_DETALHADO") \
		.mode("append") \
		.save()
	
    ## create an agregate stage table with the max temp and minimal pressuere
    ## we use nullif to get the min and max because We have the ignore the 0 values, but if the information has only 0 values we need to show it
    ## temp is storaged in Celcius degrees
    ## pressuere is storaged in Pascal
    finalReport = sqlContext.sql("SELECT data_posicao_short AS DAT_REFERENCIA, \
										 veiculo AS ID_VEICULO, \
										 cliente AS ID_CLIENTE, \
										 COALESCE(min(NULLIF(pressao,0)),0) AS PRESSAO_MIN, \
										  COALESCE(max(NULLIF(temperatura,0)),0) AS TEMPERATURA_MAX, \
										 (max(odometro) - min(odometro)) / 10 AS DISTANCIA\
								  FROM   veiculosTires \
								  GROUP BY data_posicao_short, \
										   veiculo, \
										   cliente")
	
    ## Persist stage table in hive
    finalReport.write.mode("overwrite").saveAsTable("stageprestemp")
	
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()