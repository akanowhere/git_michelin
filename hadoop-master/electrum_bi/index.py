# -*- coding: utf-8 -*-

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
#import os
from datetime import datetime, timedelta
#import config_database as dbconf
### O import abaixo não funciona qunado executado diretamente no spark   ###
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf

conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_HOST, dbconf.OP_PASSWORD))
REPORT = 'INDEX'

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

def RunSQL(conn, sql ):
    """ Executa e faz commit """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except Exception as e:
        print("*** ERRO: Falha ao conectar na base de dados: %s" % str(e) )
        #os.exit( 1 )

def executeIndex():
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    #correlationId = 999999
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    # string de conexão
    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    
    ### Configure UDF Function ###
    getWSNDataUDF = udf(getWSNData, WSNVIDElement)
    
    ### get args ####
    #cusoid = sys.argv[1]
    #dateFrom = sys.argv[2]
    #dateTo = sys.argv[3]
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Query get dateFrom and DateTo for Monitoring Report
    queryGetDate = """(
        SELECT 
            e.ebistartdate as start_date, 
            e.ebienddate as end_date 
        FROM public.electrum_bi e WHERE e.ebitypereport = '{0}'
            and e.ebistartdate <= now() - interval '6 day'
            and e.ebienddate < now()
    ) as getDate""".format(REPORT)
    
    tableGetDate = sqlContext.read.format("jdbc"). \
            option("url", opDbURL). \
            option("driver", "org.postgresql.Driver"). \
            option("dbtable", queryGetDate). \
            load()
    
    resultGetDate = tableGetDate.select("*").collect()

    if not (resultGetDate):
         print("Nothing to process. The date could´nt be greater than the rule of D-6 on log table.")
         sc.stop()
         quit()
         
    queryGetIdVehicles = """(
    WITH electrum_vehicles AS (
    select 
        vts.VTSINFOVEIOID AS vehicle_id
    from 
        VEHICLE.VEHICLE_TECHNICAL_SOLUTION vts  
        INNER JOIN VEHICLE.VEHICLE_PRODUCT_LIST vpl ON vpl.VPLVTSOID = vts.VTSOID 
        INNER JOIN "subscription".PRODUCT p ON p.PRDOID = vpl.VPLPRDOID AND p.PRDEXTERNALID ='MS3680SW'
    )
    select 
        vts.vtsinfoveioid, 
        coalesce(coalesce((select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'EMC2' and vei1.veiextveioid = v.veioid limit 1), 
        (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'TYC' and vei1.veiextveioid = v.veioid limit 1)), 
        (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextveioid = v.veioid limit 1)) as TyreCheckID, 
        v.veiplate||'/'||v.veifleetnumber as VID, 
        case when matmodname ilike '%TCU%' then 'LDL_CRCU2_' else 'LDL_CRCU1_' end ||m.matserialoid as ROName 
    from 
        vehicle.vehicle v 
        join vehicle.vehicle_technical_solution vts on v.veioid = vts.vtsveioid 
        JOIN vehicle.vehicle_product_list vpl ON vpl.vplvtsoid = vts.vtsoid
        JOIN "subscription".product prd ON prd.prdoid = vpl.vplprdoid AND prd.prdclassification = 'EQ'
        join vehicle.equipment_status es on es.eqsoid = vpl.vpleqsoid 
        join material.material m on vpl.vplmatoid = m.matoid 
        join material.material_model mo on m.matmodoid = mo.matmodoid
        join customer.customer_vehicle cv on CVVEIOID = vts.VTSINFOVEIOID
        JOIN electrum_vehicles ev on ev.vehicle_id = vts.VTSINFOVEIOID
    where 
        es.eqstag in ('RT_ENT_EQUIP_STATUS_INSTALLED')) as tableGetIdVehicles"""
    
    tableGetIdVehicles = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetIdVehicles). \
         load()
         
    ### CREATE DATA FILTERS ###
    dateFrom = str(resultGetDate[0][0])
    dateTo = str(resultGetDate[0][1])
    
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
    
    ### Query get customers 
    queryCustomers = """(
        select 
            cv.cvcusoid as cusoid
        from VEHICLE.VEHICLE_TECHNICAL_SOLUTION vts  
        JOIN VEHICLE.VEHICLE_PRODUCT_LIST vpl ON vpl.VPLVTSOID = vts.VTSOID 
        JOIN "subscription".PRODUCT p ON p.PRDOID = vpl.VPLPRDOID AND p.PRDEXTERNALID ='MS3680SW'
        JOIN customer.customer_vehicle cv on cv.cvveioid = vts.vtsinfoveioid 
        group by cv.cvcusoid
    ) as customers"""
    
    tableCustomers = sqlContext.read.format("jdbc"). \
            option("url", opDbURL). \
            option("driver", "org.postgresql.Driver"). \
            option("dbtable", queryCustomers). \
            load()
    
    resultCustomers = tableCustomers.select("*").collect()
    
    ### Variable to store the list of the customers to filter for electrum-bi
    customerList = []
    
    for row in resultCustomers:
        customerList.append(row['cusoid'])
    
    customerListString = ",".join(str(x) for x in customerList)
    
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
                    and  data_posicao_gmt0 >= '{2}'
                    and  data_posicao_gmt0 <= '{3}'
                    and cliente in ({4})
                    order by data_posicao_gmt0""".format(dateFromShort, dateToShort, dateFrom, dateTo, customerListString)
    
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
    
    # query para pegar informacoes de identificacao do veiculo (VID) 
    query_dfWSNVid = """
    select
        veiculo,
        DATA_POSICAO_GMT0,
        vid.INDEX AS wsn_vid_position,
        vid.id AS wsn_vid_id,
        vid.tag AS wsn_vid_tag,
        vid.battery_status AS wsn_vid_battery_status
    from
        posicoes_crcu
        LATERAL VIEW OUTER explode(payload_wsn) a AS wsn
        LATERAL VIEW OUTER explode(a.wsn.content.vid) b AS vid
        where data_posicao_short >= '{0}'
                    and  data_posicao_short <= '{1}'
                    and  data_posicao_gmt0 >= '{2}'
                    and  data_posicao_gmt0 <= '{3}'
                    and cliente in ({4})
        order by data_posicao_gmt0""".format(dateFromShort, dateToShort, dateFrom, dateTo, customerListString)
        
    dfHiveWSNVID = sqlContext.sql(query_dfWSNVid)
    
    # eliminando possíveis duplicidades
    dfHiveWSNVID = dfHiveWSNVID.distinct()
    
    ### CREATE FILE NAME ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    fileName = 'INDEX_'+timeStr
    fileNameVid = 'INDEX_'+"_VID_"+timeStr
    bucketUrl = awsconf.AWS_S3_BUCKET_MSOL+"electrum-bi/"
    #hdfsPath  = "%s/%s" % ( genconf.PATH_ELECTRUM_HDFS, correlationId )
    publicUrlS3 = 'https://s3-eu-west-1.amazonaws.com/'
    
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    
    ### Make a connection to Redshift and write the data into the table 'stage.INDEX' ###
    dfReport.select(
        "veiculo", 
        "ROName", 
        "EvtDateTime", 
        "TyreCheckID", 
        "VID",
        "RORelease", 
        dfReport.GPSLatitude.cast('float').alias('GPSLatitude'), 
        dfReport.GPSLongitude.cast('float').alias('GPSLongitude'),
        "GPSFixQuality", 
        "GPSDateTime", 
        "VDist",
        "GPSTripDist", 
        "PowerSource"
        ).write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "stage.INDEX") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()
    
    """
    dfReport.select(
        "veiculo", 
        "ROName", 
        "EvtDateTime", 
        "TyreCheckID", 
        "VID",
        "RORelease", 
        dfReport.GPSLatitude.cast('float').alias('GPSLatitude'), 
        dfReport.GPSLongitude.cast('float').alias('GPSLongitude'),
        "GPSFixQuality", 
        "GPSDateTime", 
        "VDist",
        "GPSTripDist", 
        "PowerSource"
        ).coalesce( 1 ) \
        .write \
        .format('csv') \
        .options(charset='UTF-8', header='true') \
        .mode("overwrite") \
        .save(bucketUrl+fileName)
    """
    
    ### Make a connection to Redshift and write the data into the table 'stage.INDEX_VID' ###
    dfHiveWSNVID.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "stage.INDEX_VID") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()
    
    """
    # grava o csv do WSNVID
    dfHiveWSNVID.coalesce( 1 ) \
        .write \
        .format('csv') \
        .options(charset='UTF-8', header='true') \
        .mode("overwrite") \
        .save(bucketUrl+fileNameVid)
    """

    ## Atualiza controle de execucao electrum
    query_update_report = " \
        insert into public.electrum_bi_history (ebihebioid, ebihebitypereport, ebihebistartdate, ebihebienddate, ebihebiinterval, ebihstatus, ebihupdatedate) \
        select ebioid, ebitypereport, ebistartdate, ebienddate, ebiinterval,'Processado' as status, now() as executeDate from public.electrum_bi where ebitypereport = '{0}'; \
        \
        update public.electrum_bi \
        set \
            ebistartdate = ebienddate + interval '1 second', \
            ebienddate = ebienddate + ebiinterval, \
            ebistatus = 'A processar', \
            ebiupdatedate = now(), \
            ebimensagem = null \
        where ebitypereport = '{1}' \
    ".format(REPORT,REPORT)
    
    RunSQL(conn,query_update_report)
    
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()

if __name__ == "__main__":    
    # Executa o report
    try:
        executeIndex()
    except Exception as e:
        mensagem = str.replace(str(e),'\'','')
        query_error_report = " \
        update public.electrum_bi \
        set \
            ebistatus = 'Erro', \
            ebimensagem = '{0}', \
            ebiupdatedate = now() \
        where ebitypereport = '{1}'; \
        insert into public.electrum_bi_history (ebihebioid, ebihebitypereport, ebihebistartdate, ebihebienddate, ebihebiinterval, ebihstatus, ebihmensagem,ebihupdatedate) \
        select ebioid, ebitypereport, ebistartdate, ebienddate, ebiinterval, ebistatus, ebimensagem, ebiupdatedate from public.electrum_bi where ebitypereport = '{2}'; \
        ".format(mensagem,REPORT,REPORT)
        
        try:
            RunSQL(conn,query_error_report)
        except Exception as err:
            print("Erro ao rodar query.", err)
