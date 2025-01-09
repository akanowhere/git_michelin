# -*- coding: utf-8 -*-
"""
Updated on 2019-12-18 16:25
@author: thomas.lima.ext
@description: Alteração da query queryGetIdVehicles para buscar das novas tabelas 
              vehicle.vehicle_technical_solution e vehicle.vehicle_product_list
"""
from pyspark import SparkContext
from pyspark.sql import HiveContext, Row, Window
from pyspark.sql.functions import udf, lit, col, sum, max, min, round, when, count, avg, first, last, lag
import pyspark.sql.functions as func
from pyspark.sql.types import *
import psycopg2
import ast
import json
import sys
import time
from datetime import datetime, timedelta
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf


### GLOBAL TYPES DEFINITION ####
TPMSRcu = StructType([
		StructField("TPMSRcu", ArrayType(
			StructType([
				StructField("RcuId", StringType()),
				StructField("TireTempMaxThreshold", StringType())
			])
		))
	])


TPMSStatus = StructType([
		StructField("TPMSStatus", ArrayType(
			StructType([
				StructField("TireLocation", StringType()),
				StructField("WusId", StringType()),
              StructField("TireId", StringType()),
              StructField("FirstTirePressure", StringType()),
              StructField("MinTirePressure", StringType()),
              StructField("MaxTirePressure", StringType()),
              StructField("FirstTireTemperature", StringType()),
              StructField("MinTireTemperature", StringType()),
              StructField("MaxTireTemperature", StringType()),
              StructField("LastTirePressure", StringType()),
              StructField("LastTireTemperature", StringType()),
              StructField("WusComLoss", StringType()),
              StructField("WusComState", StringType()),
              StructField("WusConfigError", StringType()),
              StructField("WusLowBattery", StringType()),
              StructField("TireNominalPressure", StringType()),
              StructField("TireLowPressureThreshold1", StringType()),
              StructField("TireLowPressureThreshold2", StringType())
			])
		))
	])


### payload_wsn.content.vid
WSNVIDElement = StructType([
		StructField("WSNVIDElement", ArrayType(
			StructType([
				StructField("WSNVIDPosition", StringType()),
				StructField("WSNVIDId", StringType()),
				StructField("WSNVIDTag", StringType()),
				StructField("WSNVIDLastBatteryStatus", StringType())
			])
		))
	])

### WSNTesList
WSNTesElement = StructType([
		StructField("WSNTesElement", ArrayType(
			StructType([
				StructField("WSNTesFirstTemperature", StringType()),
				StructField("WSNTesMinTemperature", StringType()),
				StructField("WSNTesMaxTemperature", StringType())
			])
		))
	])


### Definindo a estrutura do arquivo
documentBody = StructType([StructField("ROName", StringType()),
                           StructField("EvtDateTime", StringType()),
                           StructField("EventDateTimeFirstIndex", StringType()),
                           StructField("VID", StringType()),
                           StructField("TyreCheckID", StringType()),
                           StructField("RORelease", StringType()),
                           StructField("VEvtID", StringType()),
                           StructField("GPSStartLatitude", StringType()),
                           StructField("GPSEndLatitude", StringType()),
                           StructField("GPSStartLongitude", StringType()),
                           StructField("GPSEndLongitude", StringType()),
                           StructField("GPSStartDateTime", StringType()),
                           StructField("GPSEndDateTime", StringType()),
                           StructField("GPSVehDist", StringType()),
                           StructField("GPSStartVehDist", StringType()),
                           StructField("GPSStopVehDist", StringType()),
                           StructField("TPMSStatusList", TPMSStatus),
                           StructField("TPMSRcuList", TPMSRcu),
                           StructField("NumberOfPowerOn", StringType()),
                           StructField("PowerOffDuration", StringType()),
                           StructField("PowerOffDist", StringType()),
                           StructField("EBSLoadAverage", StringType()),
                           StructField("EBSLoadedState", StringType()),
                           StructField("VEmptyLoad", StringType()),
                           StructField("EBSBrakeCount", StringType()),
                           StructField("EBSCanConnectionState", StringType()),
                           StructField("EBSWarningYellowCount", StringType()),
                           StructField("EBSWarningRedCount", StringType()),
                           StructField("EBSABSCount", StringType()),
                           StructField("PowerBatteryStartVoltage", StringType()),
                           StructField("PowerBatteryStopVoltage", StringType()),
                           StructField("WSNTesStatusList", WSNTesElement),
                           StructField("WSNVIDStatusList", WSNVIDElement),
                           StructField("EBS11ebsVdcActiveTowedCounter", StringType()),
                           StructField("EBS12ropSystemCounter", StringType()),
                           StructField("EBS12ycSystemCounter", StringType()),
                           StructField("EBS21ebsVdcActiveTowedCounter", StringType()),
                           StructField("PowerSource", StringType()),
                           StructField("PowerChargeLevelFirst", StringType()),
                           StructField("PowerChargeLevelLast", StringType()),
                           StructField("PositCumulGradient", StringType()),
                           StructField("NegatCumulGradient", StringType()),
                        ])

#### GLOBAL VARIABLES ####
errorMsg = ''

### To Hex ###
intToHex = lambda x: hex(x)[2:].zfill(2)

## Creating RORelease field according to the rules
def getRORelease(software_version):
    # check if there is a valid value
    if software_version != '' and software_version != None:
        if int(software_version) > 0:
            # converting to hex
            svHex = intToHex(int(software_version))
            # The first number (6 digits) represents the type of device, and the last two figures (2 digits) is the version of the firmware.
            device_type = svHex[:-2]
            fv = svHex[-2:]
            # Xxxxxx/Y.Z – xxxxxx is the version of the program on the device. Y.Z is the version.
            firmware_version = fv[0]+"."+fv[1]
            rorelease = device_type+"/"+firmware_version
        else:
            rorelease = None
    else:
        rorelease = None
    
    return rorelease



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


### GET PAYLOAD_TPM DETAILS ###
def generateStatusReport(group):
    pattitionKey, groupValues = group[0], group[1]
    
    viagem = {}
    element_sensores = {}
    element_serial = {}
    element_wsn_tes = {}
    element_wsn_vid = {}
    array_sensores = []
    array_serial = []
    array_wsn_tes = []
    array_wsn_vid = []
    device_serial = []
    
    # Variabels to control the processed fields
    checkSensor = []
    checkWsnTes = []
    checkWsnVid = []
    
    tripRows = []
    
    tripRoname = 0
    tripTyreCheckId = ''
    tripVid = ''
    tripEvtdatetime = ''
    tripEventDatetimeFirstIndex = ''
    tripGpsStartLatitude = 0.0
    tripGpsEndLatitude = 0.0
    tripGpsStartLongitude = 0.0
    tripGpsEndLongitude = 0.0
    tripGpsStartDatetime = ''
    tripGpsEndDatetime = ''
    tripGpsVehdist = 0
    tripGpsStartVehdist = 0
    tripGpsStopVehdist = 0
    
    try:
        
        for line in groupValues:
            
            # Get report fields for the current group row
            tripRoname = line[0]
            tripTyreCheckId = line[1]
            tripVid = line[2]
            tripEvtdatetime = line[3]
            tripEventDatetimeFirstIndex = line[4]
            tripGpsStartLatitude = line[5]
            tripGpsEndLatitude = line[6]
            tripGpsStartLongitude = line[7]
            tripGpsEndLongitude = line[8]
            tripGpsStartDatetime = line[9]
            tripGpsEndDatetime = line[10]
            tripGpsVehdist = line[11]
            tripGpsStartVehdist = line[12]
            tripGpsStopVehdist = line[13]
            currentDiagPowerState = line[14]
            currentEBS11ebsVdcActiveTowedCounter = line[15]
            currentEBS12ropSystemCounter = line[16]
            currentEBS12ycSystemCounter = line[17]
            currentEBS21ebsVdcActiveTowedCounter = line[18]
            currentEBSABSCount = line[19]
            currentEBSLoadAverage = line[20]
            currentServiceBrakeCounter = line[21]
            currentPowerBatteryStartVoltage = line[22]
            currentPowerBatteryStopVoltage = line[23]
            currentEBSCanConnectionState = line[24]
            currentEBSWarningYellowCount = line[25]
            currentEBSWarningRedCount = line[26]
            currentDeviceSerial = line[27]
            currentDevicePosition = line[28]
            currentSensorIdentifier = line[29]
            currentSensorIndex = line[30]
            currentHighTemperatureThr = line[31]
            currentLowPressureAlertThr = line[32]
            currentLowPressureWarningThr = line[33]
            currentTireNominalPressure = line[34]
            currentFirstTirePressure = line[35]
            currentLastTirePressure = line[36]
            currentMaxTirePressure = line[37]
            currentMinTirePressure = line[38]
            currentFirstTireTemperature = line[39]
            currentLastTireTemperature = line[40]
            currentMaxTireTemperature = line[41]
            currentMinTireTemperature = line[42]
            currentWusLowBattery = line[43]
            currentWusComState = line[44]
            currentSensorConf = line[45]
            currentWsnIndex = line[46]
            currentFirstWsnTemperature = line[47]
            currentMaxWsnTtemperature = line[48]
            currentMinWsnTemperature = line[49]
            currentWsnVidPosition = line[50]
            currentWsnVidId = line[51]
            currentWsnVidTag = line[52]
            currentWsnVidLastBatteryStatus = line[53]
            currentPowerChargeLevelFirst = line[54]
            currentPowerChargeLevelLast = line[55]
            currentPowerSource = line[56]
            currentPositCumulGradient = line[57]
            currentNegatCumulGradient = line[58]
            tripRORelease = line[59]
            
            
            if ((currentSensorIdentifier not in checkSensor) and (currentSensorIdentifier is not None)):
                obj_sensores = {}
                obj_sensores["TireLocation"]              = getTyrePosition(currentDevicePosition, currentSensorIndex)
                obj_sensores["WusId"]                     = intToHex(currentSensorIdentifier)
                obj_sensores["FirstTirePressure"]         = str(currentFirstTirePressure)
                obj_sensores["MinTirePressure"]           = str(currentMinTirePressure)
                obj_sensores["MaxTirePressure"]           = str(currentMaxTirePressure)
                obj_sensores["LastTirePressure"]          = str(currentLastTirePressure)
                obj_sensores["FirstTireTemperature"]      = int(currentFirstTireTemperature)
                obj_sensores["MinTireTemperature"]        = int(currentMinTireTemperature)
                obj_sensores["MaxTireTemperature"]        = int(currentMaxTireTemperature)
                obj_sensores["LastTireTemperature"]       = int(currentLastTireTemperature)
                obj_sensores["WusComLoss"]                = None
                obj_sensores["WusComState"]               = int(currentWusComState)
                obj_sensores["WusConfigError"]            = int(currentSensorConf) if currentSensorConf == 1 else 0
                #obj_sensores["WusLowBattery"]             = int(currentWusLowBattery) if currentWusLowBattery == 2 else None
                obj_sensores["WusLowBattery"]             = int(currentWusLowBattery)
                obj_sensores["TireNominalPressure"]       = str(currentTireNominalPressure)
                obj_sensores["TireLowPressureThreshold1"] = str(currentLowPressureWarningThr)
                obj_sensores["TireLowPressureThreshold2"] = str(currentLowPressureAlertThr)
                
                checkSensor.append(currentSensorIdentifier)
            
                array_sensores.append(obj_sensores)
            
            if ((currentDeviceSerial != None) and (currentDeviceSerial not in device_serial)):
                obj_serial = {}
                obj_serial["RcuId"]                 = intToHex(currentDeviceSerial) if currentDeviceSerial != None else ""
                obj_serial["TireTempMaxThreshold"]  = str(currentHighTemperatureThr) if currentHighTemperatureThr != None else ""
                array_serial.append(obj_serial)
                device_serial.append(currentDeviceSerial)
            
            if(currentWsnIndex not in checkWsnTes):
                obj_wsn_tes = {}
                obj_wsn_tes["WSNTesFirstTemperature"]   = str(currentFirstWsnTemperature)
                obj_wsn_tes["WSNTesMinTemperature"]     = str(currentMinWsnTemperature)
                obj_wsn_tes["WSNTesMaxTemperature"]     = str(currentMaxWsnTtemperature)
                array_wsn_tes.append(obj_wsn_tes)
                
                checkWsnTes.append(currentWsnIndex)
            
            if ((currentWsnVidId != None) and (currentWsnVidId != 0) and (currentWsnVidId not in checkWsnVid)):
                obj_wsn_vid = {}
                obj_wsn_vid["WSNVIDPosition"]           = str(1 + currentWsnVidPosition)
                obj_wsn_vid["WSNVIDId"]                 = intToHex(currentWsnVidId)
                obj_wsn_vid["WSNVIDTag"]                = str(currentWsnVidTag)
                obj_wsn_vid["WSNVIDLastBatteryStatus"]  = str(currentWsnVidLastBatteryStatus)
                array_wsn_vid.append(obj_wsn_vid)
                
                checkWsnVid.append(currentWsnVidId)
            
    except Exception:
        
        array_serial = []
        array_sensores = []
        array_wsn_tes = []
        array_wsn_vid = []
        
    element_sensores["TPMSStatus"]      = array_sensores if len(array_sensores) > 0 else None
    element_serial['TPMSRcu']           = array_serial if len(array_serial) > 0 else None
    element_wsn_tes["WSNTesElement"]    = array_wsn_tes if len(array_wsn_tes) > 0 else None
    element_wsn_vid["WSNVIDElement"]    = array_wsn_vid if len(array_wsn_vid) > 0 else None
    
    ## por se tratarem da mesma informação para todos os sensores... as informações não precisam estar em uma lista
    viagem['NumberOfPowerOn']               = str(currentDiagPowerState)  if currentDiagPowerState != None else None
    viagem['EBS11ebsVdcActiveTowedCounter'] = int(currentEBS11ebsVdcActiveTowedCounter) if currentEBS11ebsVdcActiveTowedCounter != None else None
    viagem['EBS12ropSystemCounter']         = int(currentEBS12ropSystemCounter) if currentEBS12ropSystemCounter != None else None
    viagem['EBS12ycSystemCounter']          = int(currentEBS12ycSystemCounter) if currentEBS12ycSystemCounter != None else None
    viagem['EBS21ebsVdcActiveTowedCounter'] = int(currentEBS21ebsVdcActiveTowedCounter) if currentEBS21ebsVdcActiveTowedCounter != None else None
    viagem['EBSABSCount']                   = int(currentEBSABSCount) if currentEBSABSCount != None else None
    viagem['EBSLoadAverage']                = int(currentEBSLoadAverage) if currentEBSLoadAverage != None else None
    viagem['EBSBrakeCount']                 = str(currentServiceBrakeCounter) if currentServiceBrakeCounter != None else None
    viagem['EBSCanConnectionState']         = str(currentEBSCanConnectionState) if currentEBSCanConnectionState != None else None
    viagem['PowerBatteryStartVoltage']      = str(currentPowerBatteryStartVoltage) if currentPowerBatteryStartVoltage != None else None
    viagem['PowerBatteryStopVoltage']       = str(currentPowerBatteryStopVoltage) if currentPowerBatteryStopVoltage != None else None
    viagem['EBSWarningYellowCount']         = str(currentEBSWarningYellowCount) if currentEBSWarningYellowCount != None else None
    viagem['EBSWarningRedCount']            = str(currentEBSWarningRedCount) if currentEBSWarningRedCount != None else None
    
    
    # preenchendo os dados da viagem
    viagem['ROName']                    = str(tripRoname) if tripRoname != None else ""
    viagem['EvtDateTime']               = str(tripEvtdatetime) if tripEvtdatetime != None else ""
    viagem['EventDateTimeFirstIndex']   = str(tripEventDatetimeFirstIndex) if tripEventDatetimeFirstIndex != None else ""
    viagem['TyreCheckID']               = str(tripTyreCheckId) if tripTyreCheckId != None else ""
    viagem['VID']                       = str(tripVid.encode('latin1')) if tripVid != None else ""
    viagem['RORelease']                 = str(tripRORelease) if tripRORelease != None else ""
    viagem['VEvtID']                    = None
    viagem['GPSStartLatitude']          = str(tripGpsStartLatitude)
    viagem['GPSEndLatitude']            = str(tripGpsEndLatitude)
    viagem['GPSStartLongitude']         = str(tripGpsStartLongitude)
    viagem['GPSEndLongitude']           = str(tripGpsEndLongitude)
    viagem['GPSStartDateTime']          = str(tripGpsStartDatetime)
    viagem['GPSEndDateTime']            = str(tripGpsEndDatetime)
    viagem['GPSVehDist']                = str(tripGpsVehdist)
    viagem['GPSStartVehDist']           = str(tripGpsStartVehdist)
    viagem['GPSStopVehDist']            = str(tripGpsStopVehdist)
    viagem['TPMSStatusList']            = TPMSStatus.toInternal(element_sensores) if element_sensores["TPMSStatus"] != None else None
    viagem['TPMSRcuList']               = TPMSRcu.toInternal(element_serial) if element_serial["TPMSRcu"] != None else None
    viagem['WSNTesStatusList']          = WSNTesElement.toInternal(element_wsn_tes) if element_wsn_tes["WSNTesElement"] != None else None
    viagem['WSNVIDStatusList']          = WSNVIDElement.toInternal(element_wsn_vid) if element_wsn_vid["WSNVIDElement"] != None else None
    viagem['EBSLoadedState']            = None
    viagem['VEmptyLoad']                = None
    viagem['PowerSource']               = str(currentPowerSource) if currentPowerSource != None else None
    viagem['PowerChargeLevelFirst']     = str(currentPowerChargeLevelFirst) if currentPowerChargeLevelFirst != None else None
    viagem['PowerChargeLevelLast']      = str(currentPowerChargeLevelLast) if currentPowerChargeLevelLast != None else None
    viagem['PositCumulGradient']        = str(currentPositCumulGradient) if currentPositCumulGradient != None else None
    viagem['NegatCumulGradient']        = str(currentNegatCumulGradient) if currentNegatCumulGradient != None else None
    
    tripRows.append(viagem)
    
    return tripRows


def calculatePositiveGradiente (currentAltitude, previousAltitude):
    
    if ((currentAltitude is not None) and (previousAltitude is not None)):
       diffAltitude = currentAltitude - previousAltitude
       
       if(diffAltitude > 0):
          return int(diffAltitude)
          
       else:
          return int(0)
       
    else:
        return int(0)
       

def calculateNegativeGradiente (currentAltitude, previousAltitude):
    
    if ((currentAltitude is not None) and (previousAltitude is not None)):
       diffAltitude = currentAltitude - previousAltitude
       
       if(diffAltitude < 0):
          return int(diffAltitude * (-1))
          
       else:
          return int(0)
       
    else:
        return int(0)


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
    
    ### Define the functions to be used in the dataframe ###
    calculatePositiveGradienteUdf = udf(calculatePositiveGradiente, IntegerType())
    calculateNegativeGradienteUdf = udf(calculateNegativeGradiente, IntegerType())
    getROReleaseUDF = udf(getRORelease, StringType())
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Query Enterprise details ###
    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    
    
    ### Query vehicles ids
    queryGetIdVehicles = """(SELECT
        rpspartneroid AS partner_id,
        d.domoid AS partner_domain,
        rpsdatetimefrom AS date_from,
        rpsdatetimeto AS date_to,
        rpsfullrequest,
        vts.vtsinfoveioid AS veiculo_id,
        coalesce(
            coalesce(
                (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'EMC2' and vei1.veiextveioid = v.veioid limit 1),
            (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextidname = 'TYC' and vei1.veiextveioid = v.veioid limit 1)
            ),
            (select vei1.veiextid from vehicle.vehicle_external_ids vei1 where vei1.veiextveioid = v.veioid limit 1)
        ) as tyrecheckid,
        v.veiplate||'/'||v.veifleetnumber as vid,
        case when matmodname ilike '%TCU%' then 'LDL_CRCU2_' else 'LDL_CRCU1_' end ||m.matserialoid as roname
    FROM
        config.domain_type dt
        INNER JOIN config."domain" d ON dt.domtypoid = d.domtypoid
        INNER JOIN config.partner_interface_domain pid ON pid.domoid = d.domoid
        INNER JOIN config.partner_interface AS pi ON pi.parintoid = pid.parintoid
        INNER JOIN report.report_partner_solicitation rps ON rps.rpspartneroid = pi.paroid
        INNER JOIN vehicle.vehicle v ON v.veidomoid = d.domoid
        INNER JOIN vehicle.vehicle_technical_solution vts ON v.veioid = vts.vtsveioid
        INNER JOIN vehicle.vehicle_product_list vpl ON vpl.vplvtsoid = vts.vtsoid
        INNER JOIN "subscription".product prd ON prd.prdoid = vpl.vplprdoid AND prd.prdclassification = 'EQ'
        INNER JOIN vehicle.equipment_status es ON es.eqsoid = vpl.vpleqsoid
        INNER JOIN material.material m ON vpl.vplmatoid = m.matoid
        INNER JOIN material.material_model mo ON m.matmodoid = mo.matmodoid
    WHERE
        rpsoid = {0}
        AND dt.domtypname = 'VEHICLE') as A""".format(correlationId)
        
    tableGetIdVehicles = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetIdVehicles). \
         load()
    
    resultSolicitationTuple = tableGetIdVehicles.select("*").collect()
    
    #### GET JSON WITH ALL FILTERS REQUESTED ####
    filtersStr = resultSolicitationTuple[0]["rpsfullrequest"]
    #filters = addFilter(filtersStr)
    
    ### CREATE DATA FILTERS ###
    dateFrom = str(resultSolicitationTuple[0]["date_from"])
    
    dateTo = ''
    if resultSolicitationTuple[0]["date_to"] is not None:
       dateTo = str(resultSolicitationTuple[0]["date_to"])
       
    else:
       dateTo = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    ### Variable to store the list of the vehicle to filter the trips
    vehicleList = []
    
    for row in resultSolicitationTuple:
        vehicleList.append(row['veiculo_id'])
    
    vehicleListString = ",".join(str(x) for x in vehicleList)
    
    ### GET CONSOLIDATED DATA FROM TRIP REPORT ###
    redshiftQuery = """SELECT  
                vehicle, 
                to_char(dt_begin_trip, 'yyyy-MM-dd' ) as data_posicao_short_begin, 
                to_char(dt_end_trip, 'yyyy-MM-dd' ) as data_posicao_short_end, 
                dt_begin_trip,  
                dt_end_trip, 
                vehicle || '_' || dt_begin_trip::varchar as partition_key,
                to_char(dt_end_trip, 'yyyy-MM-ddThh24:mi:ss.MSZ' ) as EvtDateTime, 
                to_char(dt_begin_trip, 'yyyy-MM-ddThh24:mi:ss.MSZ' ) as EventDateTimeFirstIndex, 
                begin_trip_latitude as GPSStartLatitude, 
                end_trip_latitude as GPSEndLatitude, 
                begin_trip_longitude as GPSStartLongitude, 
                end_trip_longitude as GPSEndLongitude, 
                to_char(dt_begin_trip, 'yyyy-MM-ddThh24:mi:ss.MSZ' ) as GPSStartDateTime, 
                to_char(dt_end_trip, 'yyyy-MM-ddThh24:mi:ss.MSZ' ) as GPSEndDateTime, 
                total_gnss_distance AS GPSVehDist, 
                begin_gnss_distance AS GPSStartVehDist, 
                end_gnss_distance AS GPSStopVehDist,
                software_version as sv
                FROM public.ft_trip_report_agg
                WHERE vehicle IN ({0})
                 AND ft_trip_report_agg.dt_begin_trip > '{1}' 
                 AND ft_trip_report_agg.dt_begin_trip <= '{2}' """.format(vehicleListString, dateFrom, dateTo)
    
    try:
        tripTypeFilter = ''
        filterJson = json.loads(filtersStr)
        tripTypeFilterList = filterJson['scopeParams'][0]['TRIP_TYPE']
        stringFilter = " AND ft_trip_report_agg.trip_mode IN ('{0}')".format(tripTypeFilterList)
        redshiftQuery = redshiftQuery + stringFilter
        
    except:
       tripTypeFilter = ''
    
    dfTripReport = sqlContext.read \
            .format("com.databricks.spark.redshift") \
            .option("url", dbconf.REDSHIFT_DW_SASCAR_URL) \
            .option("tempdir", awsconf.REDSHIFT_S3_TEMP_OUTPUT) \
            .option("query", redshiftQuery) \
            .load()
    
    ## filtra DF com veiculos do parceiro
    dfPar = dfTripReport.alias('a').join(tableGetIdVehicles.alias('b'),col('a.vehicle') == col('veiculo_id')).select("*")
    
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
    
    ### join with wdlData and DataFrame  ####
    dfVeiAux = dfPar.join(lastDateVehicles, (lastDateVehicles.roname_last_position == dfPar.roname) & (dfPar.dt_begin_trip <= lastDateVehicles.date_position ), 'left_outer').select("*")

    ###########################################################################    
    ## SOLUÇÃO PALIATIVA PARA PEGAR O SOFTWARE VERSION CORRETO DO VEÍCULO
    dfVeiSV = dfVeiAux.select(col("vehicle").alias("veiculo_with_sv"), col("sv").alias('software_version')).filter("sv != '0'").distinct()
    # incluíndo o software version no df
    dfVei = dfVeiAux.alias('a').join(dfVeiSV.alias('b'),col('a.vehicle') == col('b.veiculo_with_sv'), 'left_outer').select("*")
    ###########################################################################
    ## transformando o software_version no RORelease
    dfVeiOk = dfVei.withColumn('RORelease', getROReleaseUDF('software_version'))
    
    
    ### filter WDL data  ####
    finalVehicleDataFrame = dfVeiOk.filter('date_position is null')
    
    ### Get the last processed position date ###
    lastObjectDate = finalVehicleDataFrame.select("dt_begin_trip").agg(max("dt_begin_trip").alias("dt_begin_trip")).collect()[0]['dt_begin_trip']
    
    if lastObjectDate == None:
       lastObjectDate = dateFrom
       
    ### Query to get the details data regarding the trips in the range date ###
    redshiftQuery = """SELECT  VEHICLE AS VEHICLE_DETAILED,
                               DT_BEGIN_TRIP AS DT_BEGIN_TRIP_DETAILED,
                               DT_END_TRIP AS DT_END_TRIP_DETAILED,
                               DATE_POSITION AS DATE_POSITION_DETAILED,
                               ALTITUDE,
                               DIAG_BATTERY_VOLTAGE,
                               BATTERY_CHARGE_LEVEL,
                               CAN_ACTIVITY, 
                               POWER_SOURCE AS DIAG_POWER_STATE,
                               COALESCE(ABS_ACTIVE_TOWING_COUNTER, 0) AS ABS_ACTIVE_TOWING_COUNTER,
                               COALESCE(YC_SYSTEM_COUNTER, 0) AS YC_SYSTEM_COUNTER,
                               COALESCE(ROP_SYSTEM_COUNTER, 0) AS EBS_ROP_SYSTEM_COUNTER,
                               COALESCE(VDC_ACTIVE_TOWING_COUNTER, 0) AS VDC_ACTIVE_TOWING_COUNTER,
                               NULLIF(EBS_AXLE_LOAD_SUM_MAX, 0) EBS_AXLE_LOAD_SUM_MAX,
                               NULLIF(EBS_AXLE_LOAD_SUM_MEAN, 0) EBS_AXLE_LOAD_SUM_MEAN,
                               NULLIF(EBS_AXLE_LOAD_SUM_MIN, 0) EBS_AXLE_LOAD_SUM_MIN,
                               COALESCE(SERVICE_BRAKE_COUNTER, 0) AS SERVICE_BRAKE_COUNTER,
                               COALESCE(AMBER_WARNING_SIGNAL_COUNTER, 0) AS AMBER_WARNING_SIGNAL_COUNTER,
                               COALESCE(RED_WARNING_SIGNAL_COUNTER, 0) AS RED_WARNING_SIGNAL_COUNTER,
                               DEVICE_SERIAL,
                               DEVICE_POSITION,
                               HIGH_TEMPERATURE_THR,
                               LOW_PRESSURE_ALERT_THR,
                               LOW_PRESSURE_WARNING_THR,
                               NOMINAL_PRESSURE,
                               SENSOR_IDENTIFIER,
                               SENSOR_INDEX,
                               SENSOR_PRESSURE,
                               SENSOR_TEMPERATURE,
                               SENSOR_ALERT_BATTERY,
                               SENSOR_COM,
                               SENSOR_CONF,
                               SENSOR_STATUS,
                               WSN_TEMPERATURE,
                               WSN_INDEX,
                               WSN_TES_BATTERY_STATUS,
                               WSNVIDPOSITION,
                               WSNVIDID,
                               WSNVIDTAG,
                               WSN_VID_BATTERY_STATUS
                        FROM FT_TRIP_REPORT_DETAILED
                        WHERE VEHICLE IN ({0})
                         AND  DT_BEGIN_TRIP >= '{1}'
                         AND  DT_BEGIN_TRIP <= '{2}'""".format(vehicleListString, dateFrom, dateTo)
                        
    vehicleTrip = sqlContext.read \
            .format("com.databricks.spark.redshift") \
            .option("url", dbconf.REDSHIFT_DW_SASCAR_URL) \
            .option("tempdir", awsconf.REDSHIFT_S3_TEMP_OUTPUT) \
            .option("query", redshiftQuery) \
            .load()
    
    tripsToProcess = vehicleTrip.join(finalVehicleDataFrame, (vehicleTrip.vehicle_detailed == finalVehicleDataFrame.vehicle) & (finalVehicleDataFrame.dt_begin_trip == vehicleTrip.dt_begin_trip_detailed))
    #tripsToProcess = vehicleTrip.join(dfTripReport, (vehicleTrip.vehicle_detailed == dfTripReport.vehicle) & (dfTripReport.dt_begin_trip == vehicleTrip.dt_begin_trip_detailed))
    
    # Get TRIP/EBS/DIAG DATA
    # For the BATTERY_CHARGE_LEVEL value it was needed subtract '1', in this case the range for this field it will be from -1 to 4, where '-1' means no battery at all, '0' means baterry with no power and '5'means battery full charged
    tripEbsDiagDataframe = tripsToProcess.select(col("vehicle").alias("vehicle_ebs_diag"), "DATE_POSITION_DETAILED", \
                                col("dt_begin_trip").alias("dt_begin_ebs_diag"), \
                                col("DIAG_BATTERY_VOLTAGE").cast("integer").alias("DIAG_BATTERY_VOLTAGE"), \
                                col("CAN_ACTIVITY").cast("integer").alias("CAN_ACTIVITY"), \
                                col("DIAG_POWER_STATE").cast("integer").alias("DIAG_POWER_STATE"), \
                                col("ABS_ACTIVE_TOWING_COUNTER").cast("integer").alias("ABS_ACTIVE_TOWING_COUNTER"), \
                                col("YC_SYSTEM_COUNTER").cast("integer").alias("YC_SYSTEM_COUNTER"), \
                                col("EBS_ROP_SYSTEM_COUNTER").cast("integer").alias("EBS_ROP_SYSTEM_COUNTER"), \
                                col("VDC_ACTIVE_TOWING_COUNTER").cast("integer").alias("VDC_ACTIVE_TOWING_COUNTER"), \
                                col("EBS_AXLE_LOAD_SUM_MAX").cast("integer").alias("EBS_AXLE_LOAD_SUM_MAX"), \
                                col("EBS_AXLE_LOAD_SUM_MEAN").cast("integer").alias("EBS_AXLE_LOAD_SUM_MEAN"), \
                                col("EBS_AXLE_LOAD_SUM_MIN").cast("integer").alias("EBS_AXLE_LOAD_SUM_MIN"), \
                                col("SERVICE_BRAKE_COUNTER").cast("integer").alias("SERVICE_BRAKE_COUNTER"),\
                                col("AMBER_WARNING_SIGNAL_COUNTER").cast("integer").alias("AMBER_WARNING_SIGNAL_COUNTER"), \
                                col("RED_WARNING_SIGNAL_COUNTER").cast("integer").alias("RED_WARNING_SIGNAL_COUNTER"), \
                                col("ALTITUDE").cast("integer").alias("ALTITUDE"), \
                                (col("BATTERY_CHARGE_LEVEL") - 1).cast("integer").alias("BATTERY_CHARGE_LEVEL")).distinct()

    tripEbsDiagCalculated = tripEbsDiagDataframe.select("vehicle_ebs_diag", "dt_begin_ebs_diag", "DATE_POSITION_DETAILED", \
                                                        when(col("CAN_ACTIVITY") == 2, 2).otherwise(0).alias("CAN_ACTIVITY"), \
                                                        when(col("DIAG_POWER_STATE") == 0, 1).otherwise(0).alias("DIAG_POWER_STATE"), \
                                                        col("DIAG_POWER_STATE").alias("POWER_SOURCE"), \
                                                        round(col("ABS_ACTIVE_TOWING_COUNTER")/2, 0).alias("ABS_ACTIVE_TOWING_COUNTER"), \
                                                        round(col("YC_SYSTEM_COUNTER")/2, 0).alias("YC_SYSTEM_COUNTER"), \
                                                        round(col("EBS_ROP_SYSTEM_COUNTER")/2, 0).alias("ROP_SYSTEM_COUNTER"), \
                                                        round(col("VDC_ACTIVE_TOWING_COUNTER")/2, 0).alias("VDC_ACTIVE_TOWING_COUNTER"), \
                                                        "EBS_AXLE_LOAD_SUM_MAX", "EBS_AXLE_LOAD_SUM_MEAN", \
                                                        when((col("EBS_AXLE_LOAD_SUM_MEAN") >= 0) & (col("EBS_AXLE_LOAD_SUM_MEAN") <= 50000), 1).otherwise(0).alias("EBS_AXLE_LOAD_SUM_MEAN_OK"), \
                                                        "EBS_AXLE_LOAD_SUM_MIN", "DIAG_BATTERY_VOLTAGE", \
                                                        round(col("SERVICE_BRAKE_COUNTER")/2, 0).alias("SERVICE_BRAKE_COUNTER"), \
                                                        round(col("AMBER_WARNING_SIGNAL_COUNTER")/2, 0).alias("AMBER_WARNING_SIGNAL_COUNTER"), \
                                                        round(col("RED_WARNING_SIGNAL_COUNTER")/2, 0).alias("RED_WARNING_SIGNAL_COUNTER"), \
                                                        "ALTITUDE", \
                                                        when(col("BATTERY_CHARGE_LEVEL") >= 0, col("BATTERY_CHARGE_LEVEL") * 25).otherwise(0).alias("BATTERY_CHARGE_LEVEL"))
    
    # Window function for analytical process over the whole partition (in this case the trip itself)
    tripEbsDiagWindow = Window.partitionBy(tripEbsDiagCalculated["vehicle_ebs_diag"],\
                                           tripEbsDiagCalculated["dt_begin_ebs_diag"])\
                              .orderBy(tripEbsDiagCalculated["DATE_POSITION_DETAILED"].asc())\
                              .rowsBetween(-sys.maxsize, sys.maxsize)
    
    # Window function for analytical process over a single row
    tripEbsDiagWindowLag = Window.partitionBy(tripEbsDiagCalculated["vehicle_ebs_diag"],\
                                              tripEbsDiagCalculated["dt_begin_ebs_diag"])\
                                 .orderBy(tripEbsDiagCalculated["DATE_POSITION_DETAILED"].asc())\
                                 .rowsBetween(-1, -1)
    
    # Nulls will NOT be computed for the analytical functions below
    firstPowerChargeLevel = first(tripEbsDiagCalculated['BATTERY_CHARGE_LEVEL'], ignorenulls=True).over(tripEbsDiagWindow)
    lastPowerChargeLevel = last(tripEbsDiagCalculated['BATTERY_CHARGE_LEVEL'], ignorenulls=True).over(tripEbsDiagWindow)
    firstTripPowerSource = first(tripEbsDiagCalculated['POWER_SOURCE'], ignorenulls=True).over(tripEbsDiagWindow)
    firstBatteryVoltage = first(tripEbsDiagCalculated['DIAG_BATTERY_VOLTAGE'], ignorenulls=True).over(tripEbsDiagWindow)
    lastBatteryVoltage = last(tripEbsDiagCalculated['DIAG_BATTERY_VOLTAGE'], ignorenulls=True).over(tripEbsDiagWindow)
    
    # Nulls will be computed for the analytical functions below
    previousLatitude = lag(tripEbsDiagCalculated['ALTITUDE']).over(tripEbsDiagWindowLag)
    
    tripEbsDiagTransformed = tripEbsDiagCalculated.withColumn('FIRST_POWER_CHARGE_LEVEL', firstPowerChargeLevel)\
                                                  .withColumn('LAST_POWER_CHARGE_LEVEL', lastPowerChargeLevel)\
                                                  .withColumn('FIRST_POWER_SOURCE', firstTripPowerSource)\
                                                  .withColumn('FIRST_DIAG_BATTERY_VOLTAGE', firstBatteryVoltage)\
                                                  .withColumn('LAST_DIAG_BATTERY_VOLTAGE', lastBatteryVoltage)\
                                                  .withColumn('PREVIOUS_ALTITUDE', previousLatitude)
    
    
    tripEbsDiagCumulativeGradient = tripEbsDiagTransformed.withColumn('POSITIVE_GRADIENT', calculatePositiveGradienteUdf('ALTITUDE', 'PREVIOUS_ALTITUDE'))\
                                                          .withColumn('NEGATIVE_GRADIENT', calculateNegativeGradienteUdf('ALTITUDE', 'PREVIOUS_ALTITUDE'))
    
    
    tripEbsDiagTemp = tripEbsDiagCumulativeGradient.groupBy("vehicle_ebs_diag", \
                                                            "dt_begin_ebs_diag")\
                                                   .agg(sum("DIAG_POWER_STATE").alias("DIAG_POWER_STATE"), \
                                                        sum("VDC_ACTIVE_TOWING_COUNTER").alias("EBS11ebsVdcActiveTowedCounter"), \
                                                        sum("VDC_ACTIVE_TOWING_COUNTER").alias("EBS21ebsVdcActiveTowedCounter"), \
                                                        sum("ROP_SYSTEM_COUNTER").alias("EBS12ropSystemCounter"), \
                                                        sum("YC_SYSTEM_COUNTER").alias("EBS12ycSystemCounter"), \
                                                        sum("ABS_ACTIVE_TOWING_COUNTER").alias("EBSABSCount"), \
                                                        when(sum("EBS_AXLE_LOAD_SUM_MEAN_OK")/count("EBS_AXLE_LOAD_SUM_MEAN") >= 0.4, avg("EBS_AXLE_LOAD_SUM_MEAN")).otherwise(0).alias("EBSLoadAverage"), \
                                                        sum("SERVICE_BRAKE_COUNTER").alias("SERVICE_BRAKE_COUNTER"), \
                                                        max("FIRST_DIAG_BATTERY_VOLTAGE").alias("PowerBatteryStartVoltage"), \
                                                        max("LAST_DIAG_BATTERY_VOLTAGE").alias("PowerBatteryStopVoltage"), \
                                                        when(max("CAN_ACTIVITY") == 2, True).otherwise(False).alias("EBSCanConnectionState"), \
                                                        sum("AMBER_WARNING_SIGNAL_COUNTER").alias("EBSWarningYellowCount"), \
                                                        sum("RED_WARNING_SIGNAL_COUNTER").alias("EBSWarningRedCount"), \
                                                        max("FIRST_POWER_CHARGE_LEVEL").alias("PowerChargeLevelFirst"), \
                                                        max("LAST_POWER_CHARGE_LEVEL").alias("PowerChargeLevelLast"), \
                                                        when(max("FIRST_POWER_SOURCE") == 1, 'MAIN_SOURCE').otherwise('BATTERY_SOURCE').alias("PowerSource"), \
                                                        sum("POSITIVE_GRADIENT").alias("PositCumulGradient"), \
                                                        sum("NEGATIVE_GRADIENT").alias("NegatCumulGradient"))
    
    tripEbsDiagConsolidated = tripEbsDiagTemp.select("vehicle_ebs_diag", "dt_begin_ebs_diag", \
                                col("DIAG_POWER_STATE").cast("integer").alias("DIAG_POWER_STATE"), \
                                col("EBS11ebsVdcActiveTowedCounter").cast("integer").alias("EBS11ebsVdcActiveTowedCounter"), \
                                col("EBS21ebsVdcActiveTowedCounter").cast("integer").alias("EBS21ebsVdcActiveTowedCounter"), \
                                col("EBS12ropSystemCounter").cast("integer").alias("EBS12ropSystemCounter"), \
                                col("EBS12ycSystemCounter").cast("integer").alias("EBS12ycSystemCounter"), \
                                col("EBSABSCount").cast("integer").alias("EBSABSCount"), \
                                col("EBSLoadAverage").cast("integer").alias("EBSLoadAverage"), \
                                col("SERVICE_BRAKE_COUNTER").cast("integer").alias("SERVICE_BRAKE_COUNTER"), \
                                col("PowerBatteryStartVoltage").cast("integer").alias("PowerBatteryStartVoltage"), \
                                col("PowerBatteryStopVoltage").cast("integer").alias("PowerBatteryStopVoltage"), \
                                "EBSCanConnectionState", \
                                col("EBSWarningYellowCount").cast("integer").alias("EBSWarningYellowCount"),\
                                col("EBSWarningRedCount").cast("integer").alias("EBSWarningRedCount"), \
                                col("PowerChargeLevelFirst").cast("integer").alias("PowerChargeLevelFirst"),\
                                col("PowerChargeLevelLast").cast("integer").alias("PowerChargeLevelLast"), \
                                col("PowerSource").cast("string").alias("PowerSource"), \
                                col("PositCumulGradient").cast("integer").alias("PositCumulGradient"), \
                                col("NegatCumulGradient").cast("integer").alias("NegatCumulGradient"))
    
    # Get TPMS DATA
    #tpmsDataFrame = tripsToProcess.filter("DEVICE_SERIAL IS NOT NULL")\
    tpmsDataFrame = tripsToProcess.filter("DEVICE_SERIAL IS NOT NULL AND SENSOR_COM = 2 AND SENSOR_IDENTIFIER IS NOT NULL AND SENSOR_IDENTIFIER > 0 AND SENSOR_IDENTIFIER < 4294967293 AND SENSOR_STATUS = 'activated'")\
                                  .select(col("vehicle").alias("vehicle_tpms"), \
                                col("dt_begin_trip").alias("dt_begin_tpms"), "DATE_POSITION_DETAILED", \
                                col("DEVICE_SERIAL").cast("long").alias("DEVICE_SERIAL"),\
                                col("DEVICE_POSITION").cast("integer").alias("DEVICE_POSITION"), \
                                col("HIGH_TEMPERATURE_THR").cast("integer").alias("HIGH_TEMPERATURE_THR"), \
                                col("LOW_PRESSURE_ALERT_THR").cast("integer").alias("LOW_PRESSURE_ALERT_THR"), \
                                col("LOW_PRESSURE_WARNING_THR").cast("integer").alias("LOW_PRESSURE_WARNING_THR"), \
                                col("NOMINAL_PRESSURE").cast("integer").alias("NOMINAL_PRESSURE"), \
                                col("SENSOR_IDENTIFIER").cast("long").alias("SENSOR_IDENTIFIER"), \
                                col("SENSOR_INDEX").cast("integer").alias("SENSOR_INDEX"), \
                                col("SENSOR_PRESSURE").cast("long").alias("SENSOR_PRESSURE"), \
                                col("SENSOR_TEMPERATURE").cast("float").alias("SENSOR_TEMPERATURE"), \
                                col("SENSOR_ALERT_BATTERY").cast("integer").alias("SENSOR_ALERT_BATTERY"), \
                                col("SENSOR_COM").cast("integer").alias("SENSOR_COM"), \
                                col("SENSOR_CONF").cast("integer").alias("SENSOR_CONF"))
    
    tpmWindow = Window.partitionBy(tpmsDataFrame["vehicle_tpms"],\
                                   tpmsDataFrame["dt_begin_tpms"],\
                                   tpmsDataFrame["DEVICE_SERIAL"], 
                                   tpmsDataFrame["DEVICE_POSITION"],\
                                   tpmsDataFrame["SENSOR_IDENTIFIER"],\
                                   tpmsDataFrame["SENSOR_INDEX"])\
                      .orderBy(tpmsDataFrame["DATE_POSITION_DETAILED"].asc())\
                      .rowsBetween(-sys.maxsize, sys.maxsize)
    
    highTemperatureThrColumn = last(tpmsDataFrame['HIGH_TEMPERATURE_THR'], ignorenulls=True).over(tpmWindow)
    lowPressureThrColumn = last(tpmsDataFrame['LOW_PRESSURE_ALERT_THR'], ignorenulls=True).over(tpmWindow)
    lowPressureWarningColumn = last(tpmsDataFrame['LOW_PRESSURE_WARNING_THR'], ignorenulls=True).over(tpmWindow)
    nominalPressureColumn = last(tpmsDataFrame['NOMINAL_PRESSURE'], ignorenulls=True).over(tpmWindow)
    firstSensorPressureColumn = first(tpmsDataFrame['SENSOR_PRESSURE'], ignorenulls=True).over(tpmWindow)
    lastSensorPressureColumn = last(tpmsDataFrame['SENSOR_PRESSURE'], ignorenulls=True).over(tpmWindow)
    firstSensorTemperatureColumn = first(tpmsDataFrame['SENSOR_TEMPERATURE'], ignorenulls=True).over(tpmWindow)
    lastSensorTemperatureColumn = last(tpmsDataFrame['SENSOR_TEMPERATURE'], ignorenulls=True).over(tpmWindow)
    
    lastSensorAlertBatteryColumn = last(tpmsDataFrame['SENSOR_ALERT_BATTERY'], ignorenulls=True).over(tpmWindow)
    lastSensorComColumn = last(tpmsDataFrame['SENSOR_COM'], ignorenulls=True).over(tpmWindow)
    lastSensorConfColumn = last(tpmsDataFrame['SENSOR_CONF'], ignorenulls=True).over(tpmWindow)
    
    tpmsTransformed = tpmsDataFrame.withColumn('HIGH_TEMPERATURE_THR_LAST', highTemperatureThrColumn)\
                                   .withColumn('LOW_PRESSURE_ALERT_THR_LAST', lowPressureThrColumn)\
                                   .withColumn('LOW_PRESSURE_WARNING_THR_LAST', lowPressureWarningColumn)\
                                   .withColumn('TIRE_NOMINAL_PRESSURE_LAST', nominalPressureColumn)\
                                   .withColumn('FIRST_TIRE_PRESSURE', firstSensorPressureColumn)\
                                   .withColumn('LAST_TIRE_PRESSURE', lastSensorPressureColumn)\
                                   .withColumn('FIRST_TIRE_TEMPERATURE', firstSensorTemperatureColumn)\
                                   .withColumn('LAST_TIRE_TEMPERATURE', lastSensorTemperatureColumn)\
                                   .withColumn('WUS_LOW_BATTERY', lastSensorAlertBatteryColumn)\
                                   .withColumn('WUS_COM_STATE', lastSensorComColumn)\
                                   .withColumn('SENSOR_CONF', lastSensorConfColumn)
                                   
    
    tpmsConsolidated = tpmsTransformed.groupBy("vehicle_tpms",\
                                             "dt_begin_tpms",\
                                             "DEVICE_SERIAL", 
                                             "DEVICE_POSITION",\
                                             "SENSOR_IDENTIFIER",\
                                             "SENSOR_INDEX")\
                                    .agg(max("HIGH_TEMPERATURE_THR_LAST").alias("HIGH_TEMPERATURE_THR"), \
                                         max("LOW_PRESSURE_ALERT_THR_LAST").alias("LOW_PRESSURE_ALERT_THR"), \
                                         max("LOW_PRESSURE_WARNING_THR_LAST").alias("LOW_PRESSURE_WARNING_THR"), \
                                         max("TIRE_NOMINAL_PRESSURE_LAST").alias("TIRE_NOMINAL_PRESSURE"), \
                                         max("FIRST_TIRE_PRESSURE").alias("FIRST_TIRE_PRESSURE"), \
                                         max("LAST_TIRE_PRESSURE").alias("LAST_TIRE_PRESSURE"), \
                                         max("SENSOR_PRESSURE").alias("MAX_TIRE_PRESSURE"), \
                                         min("SENSOR_PRESSURE").alias("MIN_TIRE_PRESSURE"), \
                                         max("FIRST_TIRE_TEMPERATURE").alias("FIRST_TIRE_TEMPERATURE"), \
                                         max("LAST_TIRE_TEMPERATURE").alias("LAST_TIRE_TEMPERATURE"), \
                                         max("SENSOR_TEMPERATURE").alias("MAX_TIRE_TEMPERATURE"), \
                                         min("SENSOR_TEMPERATURE").alias("MIN_TIRE_TEMPERATURE"), \
                                         max("WUS_LOW_BATTERY").alias("WUS_LOW_BATTERY"), \
                                         max("WUS_COM_STATE").alias("WUS_COM_STATE"), \
                                         max("SENSOR_CONF").alias("SENSOR_CONF"))
    
    # Get WSN (TES) DATA
    wsnTesDataFrame = tripsToProcess.filter("WSN_TEMPERATURE IS NOT NULL AND WSN_TEMPERATURE > 0")\
                                    .select(col("vehicle").alias("vehicle_wsn_tes"), \
                                            col("dt_begin_trip").alias("dt_begin_wsn_tes"), \
                                            "DATE_POSITION_DETAILED", "WSN_TEMPERATURE", \
                                            "WSN_INDEX", "WSN_TES_BATTERY_STATUS")
    
    wsnTesWindow = Window.partitionBy(wsnTesDataFrame["vehicle_wsn_tes"],\
                                      wsnTesDataFrame["dt_begin_wsn_tes"],\
                                      wsnTesDataFrame["WSN_INDEX"])\
                   .orderBy(wsnTesDataFrame["DATE_POSITION_DETAILED"].asc())\
                   .rowsBetween(-sys.maxsize, sys.maxsize)
    
    
    firstWsnTesTemperatureColumn = first(wsnTesDataFrame['WSN_TEMPERATURE'], ignorenulls=True).over(wsnTesWindow)
    
    wnsTesTransformed = wsnTesDataFrame.withColumn('WSN_TEMPERATURE_FIRST', firstWsnTesTemperatureColumn)
    
    wsnTesConsolidated = wnsTesTransformed.groupBy("vehicle_wsn_tes", \
                                                 "dt_begin_wsn_tes",\
                                                 "WSN_INDEX")\
                                        .agg(min("WSN_TEMPERATURE_FIRST").alias("FIRST_WSN_TEMPERATURE"), \
                                             max("WSN_TEMPERATURE").alias("MAX_WSN_TEMPERATURE"), \
                                             min("WSN_TEMPERATURE").alias("MIN_WSN_TEMPERATURE"))
    
    # Get WSN (VID) DATA
    wsnVidDataFrame = tripsToProcess.filter("WSNVIDID IS NOT NULL AND WSNVIDID > 0")\
                                    .select(col("vehicle").alias("vehicle_wsn_vid"), \
                                            col("dt_begin_trip").alias("dt_begin_wsn_vid"), \
                                            "DATE_POSITION_DETAILED", "WSNVIDPOSITION", "WSNVIDID", \
                                            "WSNVIDTAG", "WSN_VID_BATTERY_STATUS")
    
    wsnVidWindow = Window.partitionBy(wsnVidDataFrame["vehicle_wsn_vid"],\
                                      wsnVidDataFrame["dt_begin_wsn_vid"],\
                                      wsnVidDataFrame["WSNVIDPOSITION"],\
                                      wsnVidDataFrame["WSNVIDID"])\
                         .orderBy(wsnTesDataFrame["DATE_POSITION_DETAILED"].asc())\
                         .rowsBetween(-sys.maxsize, sys.maxsize)
    
    lastWsnVidTagColumn = last(wsnVidDataFrame['WSNVIDTAG'], ignorenulls=True).over(wsnVidWindow)
    lastWsnVidBatteryStatusColumn = last(wsnVidDataFrame['WSN_VID_BATTERY_STATUS'], ignorenulls=True).over(wsnVidWindow)
    
    wnsVidTransformed = wsnVidDataFrame.withColumn('WSNVIDTAG_LAST', lastWsnVidTagColumn)\
                                       .withColumn('WSN_VID_BATTERY_STATUS_LAST', lastWsnVidBatteryStatusColumn)
    
    wsnVidConsolidated = wnsVidTransformed.groupBy("vehicle_wsn_vid", \
                                                 "dt_begin_wsn_vid", \
                                                 "WSNVIDPOSITION", \
                                                 "WSNVIDID")\
                                        .agg(max("WSNVIDTAG_LAST").alias("WSNVIDTag"), \
                                             max("WSN_VID_BATTERY_STATUS_LAST").alias("WSNVIDLastBatteryStatus"))
    
    ## Put all results together
    # Create a temporary table of the dataframe
    dfJoinEbsDiag = finalVehicleDataFrame.join(tripEbsDiagConsolidated, (finalVehicleDataFrame.vehicle == tripEbsDiagConsolidated.vehicle_ebs_diag) & (finalVehicleDataFrame.dt_begin_trip == tripEbsDiagConsolidated.dt_begin_ebs_diag), "left_outer")
    
    dfJoinTpms = dfJoinEbsDiag.join(tpmsConsolidated, (dfJoinEbsDiag.vehicle == tpmsConsolidated.vehicle_tpms) & (dfJoinEbsDiag.dt_begin_trip == tpmsConsolidated.dt_begin_tpms), "left_outer")
    
    dfJoinWsnTes = dfJoinTpms.join(wsnTesConsolidated, (dfJoinTpms.vehicle == wsnTesConsolidated.vehicle_wsn_tes) & (dfJoinTpms.dt_begin_trip == wsnTesConsolidated.dt_begin_wsn_tes), "left_outer")
    
    dfJoinWsnVid = dfJoinWsnTes.join(wsnVidConsolidated, (dfJoinWsnTes.vehicle == wsnVidConsolidated.vehicle_wsn_vid) & (dfJoinWsnTes.dt_begin_trip == wsnVidConsolidated.dt_begin_wsn_vid), "left_outer")
    
    tripReportPartitioned = dfJoinWsnVid.repartition(5, "partition_key")
    
    tripReportRdd = tripReportPartitioned.rdd
    
    tripReportRddMapped = tripReportRdd.map(lambda x: (x['partition_key'], (x['roname'], x['tyrecheckid'], x['vid'], \
                                                       x['evtdatetime'], x['eventdatetimefirstindex'], x['gpsstartlatitude'], \
                                                       x['gpsendlatitude'], x['gpsstartlongitude'], x['gpsendlongitude'], x['gpsstartdatetime'], \
                                                       x['gpsenddatetime'], x['gpsvehdist'], x['gpsstartvehdist'], x['gpsstopvehdist'], \
                                                       x['DIAG_POWER_STATE'], x['EBS11ebsVdcActiveTowedCounter'], x['EBS12ropSystemCounter'], \
                                                       x['EBS12ycSystemCounter'], x['EBS21ebsVdcActiveTowedCounter'], x['EBSABSCount'], \
                                                       x['EBSLoadAverage'], x['SERVICE_BRAKE_COUNTER'], x['PowerBatteryStartVoltage'], \
                                                       x['PowerBatteryStopVoltage'], x['EBSCanConnectionState'], x['EBSWarningYellowCount'], \
                                                       x['EBSWarningRedCount'], x['DEVICE_SERIAL'], x['DEVICE_POSITION'], \
                                                       x['SENSOR_IDENTIFIER'], x['SENSOR_INDEX'], x['HIGH_TEMPERATURE_THR'], x['LOW_PRESSURE_ALERT_THR'], \
                                                       x['LOW_PRESSURE_WARNING_THR'], x['TIRE_NOMINAL_PRESSURE'], x['FIRST_TIRE_PRESSURE'], \
                                                       x['LAST_TIRE_PRESSURE'], x['MAX_TIRE_PRESSURE'], x['MIN_TIRE_PRESSURE'], \
                                                       x['FIRST_TIRE_TEMPERATURE'], x['LAST_TIRE_TEMPERATURE'], x['MAX_TIRE_TEMPERATURE'], \
                                                       x['MIN_TIRE_TEMPERATURE'], x['WUS_LOW_BATTERY'], x['WUS_COM_STATE'], \
                                                       x['SENSOR_CONF'], x['WSN_INDEX'], x['FIRST_WSN_TEMPERATURE'], \
                                                       x['MAX_WSN_TEMPERATURE'], x['MIN_WSN_TEMPERATURE'], x['WSNVIDPOSITION'], \
                                                       x['WSNVIDID'], x['WSNVIDTag'], x['WSNVIDLastBatteryStatus'], \
                                                       x['PowerChargeLevelFirst'], x['PowerChargeLevelLast'], x['PowerSource'], \
                                                       x['PositCumulGradient'], x['NegatCumulGradient'],x['RORelease'])))
    
    vehiclesRddGrouped = tripReportRddMapped.groupByKey().mapValues(lambda vs: sorted(vs, key=lambda x: x[0:1]))
    
    processedRdd = vehiclesRddGrouped.flatMap(generateStatusReport)
    
    ### CREATE FILE NAME ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    fileName = 'STATUS_'+str(correlationId)+"_"+timeStr
    bucketUrl = awsconf.AWS_S3_BUCKET_MSOL
    hdfsPath  = "%s/%s" % ( genconf.PATH_ELECTRUM_HDFS, correlationId )
    publicUrlS3 = 'https://s3-eu-west-1.amazonaws.com/'
    
    print(" Salvando os dados em %s - com nome %s" % ( hdfsPath, fileName ) )
    
    #if(processedRdd.isEmpty() == False):
    '''
    dfRdd = sc.parallelize(lista)
    newDf = sqlContext.createDataFrame(dfRdd, documentBody)
    '''
    
    statusReporDataFrame = processedRdd.toDF(schema=documentBody)
    
    ### save xml ###
    statusReporDataFrame.select("ROName", "EvtDateTime", "EventDateTimeFirstIndex", "VID", "TyreCheckID", "RORelease", "VEvtID", "GPSStartLatitude", "GPSEndLatitude",
                                "GPSStartLongitude", "GPSEndLongitude", "GPSStartDateTime", "GPSEndDateTime", "GPSVehDist", "GPSStartVehDist", "GPSStopVehDist",
                                "TPMSStatusList", "TPMSRcuList", "WSNVIDStatusList", "NumberOfPowerOn", "EBSLoadAverage", "EBSLoadedState", "VEmptyLoad", "EBSBrakeCount", 
                                "EBSCanConnectionState", "EBSWarningYellowCount", "EBSWarningRedCount", "EBSABSCount", "PowerBatteryStartVoltage", "PowerBatteryStopVoltage", 
                                "EBS11ebsVdcActiveTowedCounter", "EBS12ropSystemCounter","EBS12ycSystemCounter", "EBS21ebsVdcActiveTowedCounter",
                                "PowerSource", "PowerChargeLevelFirst", "PowerChargeLevelLast", "PositCumulGradient", "NegatCumulGradient") \
        .coalesce( 10 ) \
        .write \
        .mode("overwrite") \
        .format('com.databricks.spark.xml').options(rowTag='trailerStatus') \
        .save(hdfsPath) # +fileName)
   
    ### config python to update report status ###
    conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_HOST, dbconf.OP_PASSWORD))
    cur = conn.cursor()
    
    sql = "update report.report_partner_solicitation \
           set rpsreportfailuremessage = '{0}', \
               rpsreportfullpath = '{1}', \
               rpsfilename = '{2}', \
               rpslastinfotimestamp = '{3}' \
         where rpsoid = {4}".format(errorMsg, publicUrlS3+bucketUrl[5:], fileName, lastObjectDate, correlationId)
   
    cur.execute(sql)
    conn.commit()
    
    cur.close()
    conn.close()
    
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()
