# -*- coding: utf-8 -*-
"""
Created on Sat Sep 29 10:27:44 2018
@author: carlos.santanna.ext

Updated on 2019-03-12 11:20
@author: thomas.lima.ext
@description: Inclusão das informações de deslocamento na OS

Updated on 2019-12-18 16:25
@author: thomas.lima.ext
@description: Alteração da query queryGetIdVehicles para buscar das novas tabelas 
              vehicle.vehicle_technical_solution e vehicle.vehicle_product_list
"""

from __future__ import print_function, division
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from datetime import datetime, timedelta
import time
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf
import ast
import sys
import psycopg2
import json


#### GLOBAL VARIABLES ####
errorMsg = ''

### GLOBAL TYPES DEFINITION ####
schema = StructType([
        StructField("identifierIntervention", LongType(), True),
        StructField("vehicleExternalIdList", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True)
            ])
        ), True),
        StructField("ROName", StringType(), True),
        StructField("identifierInstallationCompany", LongType(), True),
        StructField("installationCompanyName", StringType(), True),
        StructField("externalInstallationCompanyId", StringType(), True),
        StructField("interventionStatusList", ArrayType(
            StructType([
                StructField("status", IntegerType(), True),
                StructField("updateDateTime", StringType(), True)
            ])
        ), True),
        StructField("identifierInterventionType", IntegerType(), True),
        StructField("installationComponents", ArrayType(
            StructType([
                StructField("componentType", IntegerType(), True),
                StructField("componentAction", IntegerType(), True),
                StructField("componentQuantity", IntegerType(), True)
            ])
        ), True),
        StructField("customerExternalId", StringType(), True),
        StructField("onFieldInformation", ArrayType(
            StructType([
                StructField("key", StringType(), True),
                StructField("value", StringType(), True)
            ])
        ), True)
    ])


### FUNCTION TO CREATE THE FILTER LINES AND RETURN AN ARRAY WITH ALL FILTERS
def addFilter(jsonFilter):
    sql = []
    global errorMsg
    try:
        filters = ast.literal_eval(jsonFilter)
        
        if len(filters) > 0:
            
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


def getVehicleExternalId(vehicleId, cursor):
    
    sqlVehicleExternal = "select veiextid as id, \
                                 veiextidname as Name \
                          from vehicle.vehicle_external_ids \
                          where veiextveioid = {0}".format(vehicleId)
    
    cursor.execute(sqlVehicleExternal)
    
    ### execute sql ###
    rows = cursor.fetchall()
    
    vehicleIdList = []
    
    for row in rows:
        vehicleIdName = {}
        vehicleIdName['id'] = row[0]
        vehicleIdName['name'] = row[1]
        
        vehicleIdList.append(vehicleIdName)
    
    return vehicleIdList


def getInterventionHistoryStatus(interventionId, cursor):
    
    sqlVehicleExternal = "select ist.itsoid as statusId, \
                                 to_char(ish.ihlupdatedate, 'yyyy-mm-ddThh24:mi:ss.MSZ') as updateDateTime \
                          from intervention.intervention i \
                          inner join intervention.intervention_status_history ish on ish.ihlintoid = i.intoid \
                          inner join intervention.intervention_status ist on ist.itsoid = ish.ihlstatusoid \
                          where i.intoid = {0} \
                          order by ish.ihlupdatedate asc".format(interventionId)
    
    cursor.execute(sqlVehicleExternal)
    
    ### execute sql ###
    rows = cursor.fetchall()
    
    interventionHistoryList = []
    
    for row in rows:
        interventionStatus = {}
        interventionStatus['status'] = row[0]
        interventionStatus['updateDateTime'] = row[1]
        
        interventionHistoryList.append(interventionStatus)
        
    return interventionHistoryList


def getInterventionComponents(interventionId, cursor):
    # get all intervention items but CANCELLED
    sqlVehicleExternal = "select pc.prcoid as componentType, \
                                 ia.iiaoid as componentAction, \
                                 count(*) as componentQuantity \
                          from intervention.intervention i \
                          inner join intervention.intervention_item ii on ii.itiintoid = i.intoid \
                          inner join subscription.product p on ii.itiproductoid = p.prdoid \
                          inner join subscription.product_category pc on pc.prcoid = p.prdprcoid \
                          inner join intervention.intervention_item_action ia on ia.iiaoid = ii.itiiiaoid \
                          where i.intoid = {0} \
                           and ii.itiiisoid <> 3 \
                          group by pc.prcoid, \
                                   ia.iiaoid".format(interventionId)
    
    cursor.execute(sqlVehicleExternal)
    
    ### execute sql ###
    rows = cursor.fetchall()
    
    interventionComponentsList = []
    
    for row in rows:
        interventionComponents = {}
        interventionComponents['componentType'] = row[0]
        interventionComponents['componentAction'] = row[1]
        interventionComponents['componentQuantity'] = row[2]
        
        interventionComponentsList.append(interventionComponents)
    
    
    if (len(interventionComponentsList) > 0):
        return interventionComponentsList
    
    else:
        return None


def getInterventionComments(interventionId, cursor):
    
    sqlVehicleExternal = "select ic.icmcomment \
                          from intervention.intervention i \
                          inner join intervention.intervention_comments ic on ic.icmintoid = i.intoid \
                          where i.intoid = {0}".format(interventionId)
    
    cursor.execute(sqlVehicleExternal)
    
    ### execute sql ###
    rows = cursor.fetchall()
    
    interventionCommentsList = []
    
    for row in rows:
        interventionComments = {}
        interventionComments['key'] = 'COMMENT'
        interventionComments['value'] = unicode(row[0], "utf-8")
        
        interventionCommentsList.append(interventionComments)
    
    
    if (len(interventionCommentsList) > 0):
        return interventionCommentsList
    
    else:
        return None
      
def processInterventionReport(intervention, cur):
    
    interventionReport = {}
    interventionReport['identifierIntervention'] = intervention['identifierintervention']
    interventionReport['vehicleExternalIdList'] = getVehicleExternalId(intervention['intvehicle'], cur)
    interventionReport['ROName'] = intervention['roname']
    interventionReport['identifierInstallationCompany'] = intervention['identifierinstallationcompany']
    interventionReport['installationCompanyName'] = intervention['installationcompanyname']
    interventionReport['externalInstallationCompanyId'] = intervention['externalinstallationcompanyid']
    interventionReport['interventionStatusList'] = getInterventionHistoryStatus(intervention['identifierintervention'], cur)
    interventionReport['identifierInterventionType'] = intervention['identifierinterventiontype']
    interventionReport['installationComponents'] = getInterventionComponents(intervention['identifierintervention'], cur)
    interventionReport['customerExternalId'] = intervention['customerexternalid']
    
    onFieldInformation = None
    # TRAVEL FEE (INFORMAÇÃO DE DESLOCAMENTO)
    travelFee = None
    if intervention['intmileage_value'] is not None and intervention['intmileage_unit'] is not None:
        travelFee = [
            {"key":"MILEAGE_VALUE", "value": intervention['intmileage_value']},
            {"key":"MILEAGE_UNIT", "value": intervention['intmileage_unit']}
        ]
    # COMMENTS
    comments = getInterventionComments(intervention['identifierintervention'], cur)
    # if there data to travelFee AND comments... merge
    if travelFee is not None and comments is not None:
        onFieldInformation = travelFee + comments
    else:
        # if there only travelFee data...
        if travelFee is not None:
            onFieldInformation = travelFee
        else:
            # if there only comments data...
            onFieldInformation = comments
            
    interventionReport['onFieldInformation'] = onFieldInformation
    
    return interventionReport, intervention['lastobjectdate']
    
    
if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ### Get correlation id from args ####
    correlationId = sys.argv[3]
    
    ### Query report solicitation ###
    sql = "(select * from report.report_partner_solicitation where rpsoid = {0}) as reportSolicitation".format(correlationId)
    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    
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
    dateFrom = resultSolicitationTuple["rpsdatetimefrom"]
    dateTo = resultSolicitationTuple["rpsdatetimeto"]
    
    if(dateTo == None):
        # Get current date in GMT-0 from the system to execute the process
        dateToProcess = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        dateTo = dateToProcess
    
    ### Get Domain ###
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
    
    
    ### Query vehicles ids
    queryGetIdVehicles = """
                            (select 
                                vts.vtsinfoveioid, 
                                coalesce(coalesce((select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'EMC2' and vei1.veiextveioid = v.veioid limit 1), 
                                (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'TYC' and vei1.veiextveioid = v.veioid limit 1)), 
                                (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextveioid = v.veioid limit 1)) as TyreCheckID, 
                                v.veiplate||'/'||v.veifleetnumber as VID, 
                                coalesce(case when matmodname ilike '%TCU%' then 'LDL_CRCU2_' else 'LDL_CRCU1_' end || m.matserialoid, '') as ROName, 
                                v.veioid 
                            from 
                                vehicle.vehicle v 
                                inner join vehicle.vehicle_technical_solution vts on v.veioid = vts.vtsveioid 
                                INNER JOIN vehicle.vehicle_product_list vpl ON vpl.vplvtsoid = vts.vtsoid
                                INNER JOIN "subscription".product prd ON prd.prdoid = vpl.vplprdoid AND prd.prdclassification = 'EQ'
                                inner join vehicle.equipment_status es on es.eqsoid = vpl.vpleqsoid 
                                left join material.material m on vpl.vplmatoid = m.matoid 
                                left join material.material_model mo on m.matmodoid = mo.matmodoid
                            where 
                                v.veidomoid in ({0})) as tableGetIdVehicles""".format(dom)
                            
    
    
    tableGetIdVehicles = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetIdVehicles). \
         load()
    
    ### Query to get intervention main data ###
    sqlIntervention = "(select i.intoid as identifierIntervention, \
                              ve.veiextid as vehicleExternalId, \
                              ve.veiextidname as vehicleExternalName, \
                              coalesce(i.intserviceprovider, 0) as identifierInstallationCompany, \
                              coalesce(sp.sepname, '') as installationCompanyName, \
                              coalesce(sp.sepexternalid, '') as externalInstallationCompanyId, \
                              it.itpoid as identifierInterventionType, \
                              c.cusexternalid as customerExternalId, \
                              i.intvehicle, \
                              i.intmileage_value, \
                              i.intmileage_unit, \
                              to_char(max(ish.ihlupdatedate), 'yyyy-mm-ddThh24:mi:ss.MSZ') as lastObjectDate \
                       from intervention.intervention i \
                       inner join vehicle.vehicle v on i.intvehicle = v.veioid \
                       inner join vehicle.vehicle_external_ids ve on ve.veiextveioid = v.veioid  \
                       left join service_provider.service_provider sp on i.intserviceprovider = sp.sepoid \
                       inner join intervention.intervention_type it on it.itpoid = i.intitpoid \
                       inner join customer.customer c on c.cusoid = v.veicusoid \
                       inner join intervention.intervention_status_history ish on ish.ihlintoid = i.intoid \
                       where ish.ihlupdatedate > to_timestamp('{0}.999', 'yyyy-mm-dd hh24:mi:ss.MSZ') \
                        and  ish.ihlupdatedate <= to_timestamp('{1}.999', 'yyyy-mm-dd hh24:mi:ss.MSZ') \
                       group by i.intoid, \
                                ve.veiextid, \
                                ve.veiextidname, \
                                i.intserviceprovider, \
                                sp.sepname, \
                                it.itpoid, \
                                c.cusexternalid, \
                                sp.sepexternalid , \
                                i.intvehicle \
                       order by lastObjectDate asc) as a".format(str(dateFrom), str(dateTo))
    
    dfInterventions = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", sqlIntervention). \
         load()
    
    interventiosVehiclesIds = dfInterventions.join(tableGetIdVehicles, dfInterventions.intvehicle == tableGetIdVehicles.veioid)
    
    ### Redistribute the dataframe according to the column desire into 'n' partitions ###
    interventionsPartitioned = interventiosVehiclesIds.repartition(4, "identifierintervention")
    
    interventionsResultList = []
    lastObjectDate = ''
    
    ### Open enterprise bd conection ###
    conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_HOST, dbconf.OP_PASSWORD))
    
    ### open cursor ###
    cur = conn.cursor()
    
    for row in interventionsPartitioned.rdd.collect():
        intervention, lastDate = processInterventionReport(row, cur)
        interventionsResultList.append(intervention)
        if((lastObjectDate == '') or (lastObjectDate == None) or (lastDate > lastObjectDate)):
            lastObjectDate = lastDate
    if((lastObjectDate == '') or (lastObjectDate == None)):
        lastObjectDate = dateTo
    
    interventionsReportRdd = sc.parallelize(interventionsResultList)
    interventionsReport = sqlContext.createDataFrame(interventionsReportRdd, schema)
    
    ### CREATE FILE NAME ###
    ### File names are : MessageType+_+CorrelationId+_+TimeStamp+.json.gzip ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    #fileName = 'INTERVENTION_'+str(correlationId)+"_"+timeStr+".json"
    fileName = 'INTERVENTION_'+str(correlationId)+"_"+timeStr
    bucketUrl = awsconf.AWS_S3_BUCKET_MSOL
    hdfsPath  = "%s/%s" % ( genconf.PATH_ELECTRUM_HDFS, correlationId )
    publicUrlS3 = 'https://s3-eu-west-1.amazonaws.com/'
    
    print(" Salvando os dados em %s - com nome %s" % ( hdfsPath, fileName ) )
    
    interventionsReport.select("identifierIntervention", "vehicleExternalIdList", "ROName", "identifierInstallationCompany", \
                               "installationCompanyName", "externalInstallationCompanyId", "interventionStatusList", \
                               "identifierInterventionType", "installationComponents", "customerExternalId", "onFieldInformation") \
        .coalesce( 10 ) \
        .write \
        .mode("overwrite") \
        .format('json') \
        .save(hdfsPath)

    sql = "update report.report_partner_solicitation \
              set rpsreportfailuremessage = '{0}', \
                  rpsreportfullpath = '{1}', \
                  rpsfilename = '{2}', \
                  rpslastinfotimestamp = '{3}' \
            where rpsoid = {4}"\
          .format(errorMsg, publicUrlS3 + bucketUrl[5:], fileName, str(lastObjectDate), correlationId)
          
    cur.execute(sql)
    conn.commit()
    
    cur.close()
    conn.close()
    
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()
