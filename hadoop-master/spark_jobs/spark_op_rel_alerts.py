# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 10:59:18 2019

@author: carlos.santanna.ext
"""

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, lit, col, max, when
from pyspark.sql import Row
from pyspark.sql.types import *
import psycopg2
import json
import sys
import time
from datetime import datetime, timedelta
### O import abaixo não funciona qunado executado diretamente no spark   ###
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf

#### GLOBAL VARIABLES ####
errorMsg = ''

### INÍCIO DA EXECUÇÃO DO PROGRAMA ###
if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### NSF - Como é a chamada do programa, i.é, quais os parâmetros de chada que estão sendo passados? ###
    ### get args ####
    correlationId = sys.argv[3]
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Query Enterprise details ###
    opDbURLEnterprise = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    
    ### Query vehicles ids
    queryGetReportDetails = """(SELECT rpspartneroid partner_id,
                                       rpsdatetimefrom AS date_from,
                                       rpsdatetimeto AS date_to,
                                       rpsfullrequest
                                FROM report.report_partner_solicitation
                                WHERE rpsoid = {0}) as A""".format(correlationId)
    
    dfReportDetails = sqlContext.read.format("jdbc"). \
         option("url", opDbURLEnterprise). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetReportDetails). \
         load()
    
    resultSolicitationTuple = dfReportDetails.select("*").collect()[0]
    
    #### GET JSON WITH ALL FILTERS REQUESTED ####
    filtersStr = resultSolicitationTuple["rpsfullrequest"]
    #filters = addFilter(filtersStr)
    
    ### CREATE DATA FILTERS ###
    dateFrom = str(resultSolicitationTuple["date_from"])
    
    dateTo = ''
    if resultSolicitationTuple["date_to"] is not None:
       dateTo = str(resultSolicitationTuple["date_to"])
       
    else:
       dateTo = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    partnerId = str(resultSolicitationTuple["partner_id"])
    
    queryGetAlerts = """(SELECT raisedalertdate,
                                alert_json
                        FROM public.ft_electrum_alerts_detailed
                        WHERE raisedalertdate > '{0}'
                         AND  raisedalertdate <= '{1}'
                         AND id_partner = {2}) as a""".format(dateFrom, dateTo, partnerId)
    
    opDbURLAlerts = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.RDS_AWS_HOST, dbconf.RDS_AWS_DATABASE, dbconf.RDS_AWS_USERNAME, dbconf.RDS_AWS_PASSWORD)
    
    dfAlerts = sqlContext.read.format("jdbc"). \
         option("url", opDbURLAlerts). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetAlerts). \
         load()
    
    lastObjectDate = str(dfAlerts.select("raisedalertdate").agg(max("raisedalertdate").alias("raisedalertdate")).collect()[0]['raisedalertdate'])
    
    if lastObjectDate == None:
        lastObjectDate = dateFrom
        
    dfAlertsJson = sqlContext.read.json(dfAlerts.rdd.map(lambda r: r['alert_json']))
    
    ### CREATE FILE NAME ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    fileName = 'ALERTS_' + str(correlationId) + "_" + timeStr
    bucketUrl = awsconf.AWS_S3_BUCKET_MSOL
    hdfsPath  = "%s/%s" % ( genconf.PATH_ELECTRUM_HDFS, correlationId )
    publicUrlS3 = 'https://s3-eu-west-1.amazonaws.com/'
    
    print(" Salvando os dados em %s - com nome %s" % ( hdfsPath, fileName ) )
    
    dfAlertsJson.select("ROName", "EvtDateTime", "vehicleExternalIdList", "DriverId", 
                        "GPSLatitude", "GPSLongitude", "AlertUserLabel", "AlertType", 
                        "NotificationList", "AlertLevel", "ThresholdLabel", "ThresholdValue", 
                        "ThresholdUnit", "ThresholdOperator", "RaisedValueList", "WSNVIDList") \
        .coalesce( 10 ) \
        .write \
        .format('com.databricks.spark.xml') \
        .options(rowTag='alert') \
        .save(hdfsPath) #bucketUrl+fileName)
    
    ### config python to update report status ###
    conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_HOST, dbconf.OP_PASSWORD))
    cur = conn.cursor()
    
    sql = "update report.report_partner_solicitation \
              set rpsreportfailuremessage = '{0}', \
                  rpsreportfullpath = '{1}', \
                  rpsfilename = '{2}', \
                  rpslastinfotimestamp = '{3}' \
            where rpsoid = {4}"\
          .format(errorMsg, publicUrlS3+bucketUrl[5:], fileName, lastObjectDate, correlationId)
    
    cur.execute(sql)
    conn.commit()
    
    cur.close()
    conn.close()
    
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
