# -*- coding: utf-8 -*-
"""
Updated on 2019-12-18 16:25
@author: thomas.lima.ext
@description: Alteração da query queryGetIdVehicles para buscar das novas tabelas 
              vehicle.vehicle_technical_solution e vehicle.vehicle_product_list
"""
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, lit, col, max
from pyspark.sql.types import *
import psycopg2
import ast
import json
import sys
import time
from datetime import datetime, timedelta
import config_database as dbconf
### O import abaixo não funciona qunado executado diretamente no spark   ###
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf


### GLOBAL TYPES DEFINITION ####
WSNVIDElement = StructType([
		StructField("WSNVIDElement", ArrayType(
			StructType([
				StructField("WSNVIDPosition", StringType()),
				StructField("WSNVIDId", StringType()),
				StructField("WSNVIDTag", StringType()),
				StructField("WSNVIDBatteryStatus", StringType())
			])
		))
	])
#### GLOBAL VARIABLES ####
errorMsg = ''

### Generate row with array of WSNData ###
def getWSNData(payloadWSN):
    element = {}
    array = []
    obj = {}
    try:
        for line in payloadWSN[0]["content"]["vid"]:
            if line["id"] != None and line["id"] != 0:
                obj = {}
                obj["WSNVIDPosition"] =  str(1 + line["index"])
                obj["WSNVIDId"] = str(line["id"])
                obj["WSNVIDTag"] = str(line["tag"])
                obj["WSNVIDBatteryStatus"] = str(line["battery_status"])
                array.append(obj)
    except Exception:  
        array = None
    element["WSNVIDElement"] = array
    return WSNVIDElement.toInternal(element)


### FUNCTION TO CREATE THE FILTER LINES AND RETURN AN ARRAY WITH ALL FILTERS
def addFilter(jsonFilter):
    sql = []
    global errorMsg
    try:
        filters = ast.literal_eval(jsonFilter)
        for tipoDict in filters["scope"]:
            for typeFilter in tipoDict:
                if (validateTypeFilter(typeFilter) != 'ERROR'):
                    sqlAux = ' '
                    sqlAux = sqlAux + str(typeFilter) + ' IN '
                    values = str(tipoDict.get(typeFilter)).split(',')
                    strIn = "("
                    if len(values) > 0:
                        for word in values:
                            strIn = strIn + "'"+word+"',"
                        sqlAux = strIn[:-1]+")"
                        sql.append(sqlAux)                                   
                else:
                    errorMsg = errorMsg + 'Filter type {0} is invalid, it was ignored.\n'.format(typeFilter) 
    except Exception:
        errorMsg = errorMsg + 'Error when we try to format filters maybe we ignore some filter.\n' 
    return sql

if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### get args ####
    correlationId = sys.argv[3]
    
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure UDF Function ###
    getWSNDataUDF = udf(getWSNData, WSNVIDElement)
    
    ### SQL
    sql = "(select * from report.report_partner_solicitation where rpsoid = {0}) as reportSolicitation".format(correlationId)
    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
  
    ### Execute SQL
    reportSolicitation = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", sql). \
         load()

    resultSolicitationTuple = reportSolicitation.select("*").collect()[0]
    
    partnerId = resultSolicitationTuple["rpspartneroid"]
    
    #### GET JSON WITH ALL FILTERS REQUESTED ####
    filtersStr = resultSolicitationTuple["rpsfullrequest"]
    #filters = addFilter(filtersStr)
    
    ### CREATE DATA FILTERS ###
    dateFrom = str(resultSolicitationTuple["rpsdatetimefrom"])
    
    dateTo = ''
    if resultSolicitationTuple["rpsdatetimeto"] is not None:
       dateTo = str(resultSolicitationTuple["rpsdatetimeto"])
       
    else:
       dateTo = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    queryGetDomain = "(select array_agg(d.domoid) as domains from config.domain_type dt \
                        inner join config.domain d on dt.domtypoid = d.domtypoid \
                        inner join config.partner_interface_domain pid on pid.domoid = d.domoid \
                        inner join config.partner_interface as pi on pi.parintoid = pid.parintoid \
                       where dt.domtypname = 'VEHICLE' \
                        and pi.paroid ={0}) as tableGetDomain".format(partnerId)
    
    tableGetDomain = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetDomain). \
         load()
    ### Get domain in arrays
   
    dom = tableGetDomain.select("domains").collect()[0]["domains"]
    ### transform array in a string separeted by comma
   
    dom = ",".join(str(x) for x in dom)

    queryGetIdVehicles = """
    (select 
        vts.vtsinfoveioid, 
        coalesce(coalesce((select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'EMC2' and vei1.veiextveioid = v.veioid limit 1), 
        (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'TYC' and vei1.veiextveioid = v.veioid limit 1)), 
        (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextveioid = v.veioid limit 1)) as TyreCheckID, 
        v.veiplate||'/'||v.veifleetnumber as VID, 
        case when matmodname ilike '%TCU%' then 'LDL_CRCU2_' else 'LDL_CRCU1_' end ||m.matserialoid as ROName 
    from 
        vehicle.vehicle v 
        inner join vehicle.vehicle_technical_solution vts on v.veioid = vts.vtsveioid 
        INNER JOIN vehicle.vehicle_product_list vpl ON vpl.vplvtsoid = vts.vtsoid
        INNER JOIN "subscription".product prd ON prd.prdoid = vpl.vplprdoid AND prd.prdclassification = 'EQ'
        inner join vehicle.equipment_status es on es.eqsoid = vpl.vpleqsoid 
        inner join material.material m on vpl.vplmatoid = m.matoid 
        inner join material.material_model mo on m.matmodoid = mo.matmodoid
    where 
        v.veidomoid in ({0}) 
        and es.eqstag in ('RT_ENT_EQUIP_STATUS_INSTALLED')) as tableGetIdVehicles""".format(dom)

    
    tableGetIdVehicles = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetIdVehicles). \
         load()
         
    
    ### GET ACCUMULATED ODOMETER ###
    redshiftQuery = "select id_veiculo, (odometro_acumulado * 100) as VDist from odometro_carreta"
    dfOdometer = sqlContext.read \
            .format("com.databricks.spark.redshift") \
            .option("url", dbconf.REDSHIFT_DW_SASCAR_URL) \
            .option("tempdir", awsconf.REDSHIFT_S3_TEMP_OUTPUT) \
            .option("query", redshiftQuery) \
            .load()
    
    
    dateFromShort = datetime.strptime(dateFrom, "%Y-%m-%d %H:%M:%S") - timedelta(days = 1)
    dateToShort = datetime.strptime(dateTo, "%Y-%m-%d %H:%M:%S") + timedelta(days = 1)
    
    dateFromShort = dateFromShort.strftime("%Y-%m-%d")
    dateToShort = dateToShort.strftime("%Y-%m-%d")
    
    ### define hive query ### 
    hiveSql = """select 
                    from_unixtime(unix_timestamp(data_posicao_gmt0 , 'yyyy-MM-dd hh:mm:ss.SSS'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as EvtDateTime, 
                    data_posicao_gmt0, 
                    veiculo as veiculo, 
			          '' as RORelease, 
                    latitude as GPSLatitude, 
                    longitude as GPSLongitude, 
                    '' as GPSFixQuality, 
                    from_unixtime(unix_timestamp(data_posicao_gmt0 , 'yyyy-MM-dd hh:mm:ss.SSS'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as GPSDateTime, 
                    odometro as GPSTripDist, 
                    case when payload_diag.content.power_source = 'true' then 1 else 0 end PowerSource, 
                    payload_wsn 
                 from posicoes_crcu
                 where data_posicao_short >= '{0}'
                  and  data_posicao_short <= '{1}'
                  and  data_posicao_gmt0 > '{2}'
                  and  data_posicao_gmt0 <= '{3}'
                 order by data_posicao_gmt0""".format(dateFromShort, dateToShort, dateFrom, dateTo)
    
    ### EXECUTE REPORT QUERY ###
    dfHive = sqlContext.sql(hiveSql)
    
    ## IGNORE DATA FROM WDL ##
    # pegando a data de hoje para filtrar a tabela stg_last_position
    fromDate = datetime.now()
    fromDateShort = fromDate.strftime("%Y-%m-%d")
    
    sql = '(SELECT RONAME as RONAME_LAST_POSITION, \
                  DATE_POSITION \
           FROM REPORT.WLD_LAST_POSITION) as a'
    
    lastDateVehicles = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", sql). \
         load()
    
    ## filtra DF com veiculos do parceiro   
    dfPar = dfHive.alias('a').join(tableGetIdVehicles.alias('b'),col('a.veiculo') == col('b.vtsinfoveioid')).select("*")
    
    ## join com o odometro
    df = dfPar.alias('a').join(dfOdometer.alias('b'), col('a.veiculo') == col('b.id_veiculo'), 'left_outer').select("*")
    
    ### join with wdlData and DataFrame  ####
    dfVeiAux = df.join(lastDateVehicles, (lastDateVehicles.roname_last_position == df.roname) & (df.data_posicao_gmt0 <= lastDateVehicles.date_position ), 'left_outer').select("*")
    
    ### filter WDL data  ####
    dfVei = dfVeiAux.filter('date_position is null')
    
    ### Get the last processed position date ###
    lastObjectDate = dfVei.select("data_posicao_gmt0").agg(max("data_posicao_gmt0").alias("data_posicao_gmt0")).collect()[0]['data_posicao_gmt0']
    
    if lastObjectDate == None:
        lastObjectDate = dateFrom
    
    ### CREATE A COLUMN WITH ARRAY WSNVIDList ####
    dfReport = dfVei.withColumn('WSNVIDList', getWSNDataUDF('payload_wsn'))
    
    # eliminando possíveis duplicidades
    dfReport = dfReport.distinct()
    
    ### CREATE FILE NAME ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    #fileName = 'INDEX_'+str(correlationId)+"_"+timeStr+".xml"
    fileName = 'INDEX_'+str(correlationId)+"_"+timeStr
    bucketUrl = awsconf.AWS_S3_BUCKET_MSOL
    hdfsPath  = "%s/%s" % ( genconf.PATH_ELECTRUM_HDFS, correlationId )
    publicUrlS3 = 'https://s3-eu-west-1.amazonaws.com/'
    
    print(" Salvando os dados em %s - com nome %s" % ( hdfsPath, fileName ) )
    
    ### GENERATE REPORT IN S3 ### 
    dfReport.select("ROName", "EvtDateTime", "TyreCheckID", "VID", 
                     "RORelease", dfReport.GPSLatitude.cast('float').alias('GPSLatitude'), dfReport.GPSLongitude.cast('float').alias('GPSLongitude'),  
                     "GPSFixQuality", "GPSDateTime", "VDist",  
                     "GPSTripDist", "PowerSource", "WSNVIDList") \
        .coalesce( 10 ) \
        .write \
        .mode("overwrite") \
        .format('com.databricks.spark.xml') \
        .options(charset='UTF-8', rowTag='trailerIndex')\
        .save(hdfsPath) #+fileName)
    
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
