# -*- coding: utf-8 -*-
"""
spark_con_cc_load_positions_to_redshift.py
Job para Carga das posições do hive para o redshift

Created on 25-01-2018 - @author: Fabio Schultz
Updated on 18-02-2019 - @author: thomas.lima.ext (add novos campos: dat_recebido, altitude, direcao_veiculo, causa)

"""

### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
import sys
import logging
import config_database as dbconf
import config_aws as awsconf


if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    # Get the root logger
    logger = logging.getLogger()
    # Have to set the root logger level, it defaults to logging.WARNING
    logger.setLevel(logging.NOTSET)
    # Config log to show msgs in stdout
    logging_handler_out = logging.StreamHandler(sys.stdout)
    logging_handler_out.setLevel(logging.ERROR)
    logger.addHandler(logging_handler_out)
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    ## Date process
    dateProcessBegin = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
    dateProcessEnd = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
    dateProcessShortBegin = (dateProcessBegin - timedelta(days=5)).strftime("%Y-%m-%d")
    dateProcessShortEnd = (dateProcessEnd + timedelta(days=1)).strftime("%Y-%m-%d")
    ### Select all positions from hive ###
    ### all CRCU positions have 0 to satelital ###
    hiveSql = "SELECT   \
						POSICOES_CRCU.DATA_POSICAO_GMT0 AS DAT_POSICAO, \
						POSICOES_CRCU.VEICULO AS ID_VEICULO, \
						POSICOES_CRCU.VELOCIDADE AS VELOCIDADE, \
						POSICOES_CRCU.IGNICAO AS IGNICAO, \
						POSICOES_CRCU.BATERIAEXT AS ESTADO_BATERIA_EXT, \
						POSICOES_CRCU.LATITUDE AS LATITUDE, \
						POSICOES_CRCU.LONGITUDE AS LONGITUDE, \
						POSICOES_CRCU.HORIMETRO AS HORIMETRO, \
						POSICOES_CRCU.ODOMETRO AS ODOMETRO, \
						POSICOES_CRCU.BLOCKVEHICLE AS BLOQUEIO, \
						0 AS SATELITAL,  \
						POSICOES_CRCU.POS_MEMORIA AS POS_MEMORIA, \
						POSICOES_CRCU.GPS_VALIDO AS GPS_VALIDO,  \
						CASE WHEN POSICOES_CRCU.BATERIAEXT = 1 THEN POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.VEHICLE_VOLTAGE \
							 WHEN POSICOES_CRCU.BATERIAEXT = 0 THEN POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.BATTERY_VOLTAGE \
						END AS VCC_ALIM, \
						POSICOES_CRCU.BATERIAINT AS ESTADO_BATERIA_INT, \
						CASE WHEN POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.BATTERY_CHARGE_LEVEL = 0 THEN -1 \
								WHEN POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.BATTERY_CHARGE_LEVEL > 5 THEN NULL \
								ELSE (POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.BATTERY_CHARGE_LEVEL - 1) * 25 END AS PERC_BAT_CALC, \
						EBS.PAYLOAD_EBS.CONTENT.AXLE_LOAD_SUM_MEAN AS PESO, \
                        POSICOES_CRCU.PAYLOAD_DIAG.HEADER.MESSAGE_ARRIVAL_DATE.DATE_TIME AS DAT_RECEBIDO, \
                        POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.GNSS_HEADING AS DIRECAO_VEICULO, \
                        POSICOES_CRCU.PAYLOAD_DIAG.CONTENT.GNSS_ALTITUDE AS ALTITUDE, \
                        POSICOES_CRCU.PAYLOAD_DIAG.HEADER.PAYLOAD_CAUSE AS CAUSA \
                FROM POSICOES_CRCU AS POSICOES_CRCU  \
                LATERAL VIEW OUTER EXPLODE(POSICOES_CRCU.PAYLOAD_EBS) EBS AS PAYLOAD_EBS \
                WHERE DATA_POSICAO_SHORT >= '{0}' \
                    AND  DATA_POSICAO_SHORT <= '{1}' \
                    AND  DATA_INSERT > '{2}' \
                    AND  DATA_INSERT <= '{3}'".format(dateProcessShortBegin, dateProcessShortEnd, dateProcessBegin, dateProcessEnd)
    positions = sqlContext.sql(hiveSql)
    positionsDistinct = positions.distinct()
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    ### Make a connection to Redshift and write the data into the table 'FT_CC_POSICOES_DETALHADAS' ###
    positionsDistinct.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "FT_CC_POSICOES_DETALHADAS") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()