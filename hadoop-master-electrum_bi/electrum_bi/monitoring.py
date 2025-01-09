# -*- coding: utf-8 -*-

### Programa para gerar o relatório Monitoring
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, lit, col, max, when
from pyspark.sql import Row
from pyspark.sql.types import *
import psycopg2
import ast
import json
import sys
import time
#import os
from datetime import datetime, timedelta
### O import abaixo não funciona qunado executado diretamente no spark   ###
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf
import config_aws_masternaut as awsconf_masternaut

# Flag apenas para controlar se deve gravar o arquivo final no S3 da Masternaut
integration_masternaut = True

conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_HOST, dbconf.OP_PASSWORD))
REPORT = 'MONITORING'

### Definindo a estrutura "document"
documentBody = StructType([StructField("ROTemperature", StringType()),
                           StructField("VBrakeEnergy", StringType()),
                           StructField("VMotion", StringType()),
                           StructField("EBSCanConnection", StringType()),
                           StructField("ThresholdMotionStart", StringType()),
                           StructField("ThresholdMotionEnd", StringType()),
                           StructField("ThresholdHdop", StringType()),
                           StructField("ScheduledPeriod", StringType()),
                           StructField("CanLoadTermination", StringType()),
                           StructField("EBSDist", StringType()),
                           StructField("EBSDistMsgCounter", StringType()),
                           StructField("VROPSystemState", StringType()),
                           StructField("VTransportMode", StringType()),
                           StructField("VYCSystemState", StringType()),
                           StructField("TPMSPairing", StringType()),
                           StructField("TPMSNominalPressure", StringType()),
                           StructField("TPMSLowPressureWarningThreshold", StringType()),
                           StructField("TPMSLowPressureAlertThreshold", StringType()),
                           StructField("TPMSHighTemperatureThreshold", StringType()),
                           StructField("VRoadTrainList", StringType()), ## Because it is not part of MVP its a empty String After that we will change to a structure 
                           StructField("ReservedLDL", StringType()), ## Because it is not part of MVP its a empty String After that we will change to a structure 
                           StructField("CANDataMonitoringCounter", StringType()), ## Because it is not part of MVP its a empty String After that we will change to a structure  
                           StructField("CANFrameMonitoringList", StringType()), ## Because it is not part of MVP its a empty String After that we will change to a structure  
                           StructField("EBS11MsgCounter", StringType()),
                           StructField("EBSAbsActiveTowingCounter", StringType()),
                           StructField("EBSBrakeLightCounter", StringType()),
                           StructField("EBSServiceBrakeDemandMin", StringType()),
                           StructField("EBSServiceBrakeDemandMax", StringType()),
                           StructField("EBSServiceBrakeDemandMean", StringType()),
                           StructField("EBSVdcActiveTowingCounter", StringType()),
                           StructField("EBS12MsgCounter", StringType()),
                           StructField("EBSRopSystemCounter", StringType()),
                           StructField("EBSYcSystemCounter", StringType()),
                           StructField("EBSAbsOffRoadCounter", StringType()),
                           StructField("EBSWheelBasedSpeedTowingMin", StringType()),
                           StructField("EBSWheelBasedSpeedTowingMax", StringType()),
                           StructField("EBSWheelBasedSpeedTowingMean", StringType()),
                           StructField("EBS21MsgCounter", StringType()),
                           StructField("EBSAbsActiveTowedCounter", StringType()),
                           StructField("EBSServiceBrakeCounter", StringType()),
                           StructField("EBSAutoTowedVehBrakeCounter", StringType()),
                           StructField("EBSVdcActiveTowedCounter", StringType()),
                           StructField("EBSLateralAccMin", StringType()),
                           StructField("EBSLateralAccMax", StringType()),
                           StructField("EBSLateralAccMean", StringType()),
                           StructField("EBSWheelBasedSpeedTowedMin", StringType()),
                           StructField("EBSWheelBasedSpeedTowedMax", StringType()),
                           StructField("EBSWheelBasedSpeedTowedMean", StringType()),
                           StructField("EBS22MsgCounter", StringType()),
                           StructField("EBSAxleLoadSumMin", StringType()),
                           StructField("EBSAxleLoadSumMax", StringType()),
                           StructField("EBSAxleLoadSumMean", StringType()),
                           StructField("EBSVehElectricalSupplyCounter", StringType()),
                           StructField("EBSRedWarningSignalCounter", StringType()),
                           StructField("EBSAmberWarningSignalCounter", StringType()),
                           StructField("EBSLoadingRampCounter", StringType()),
                           StructField("EBSSupplyLineBrakingCounter", StringType()),
                           StructField("EBS23MsgCounter", StringType()),
                           StructField("EBSBrakeLiningCounter", StringType()),
                           StructField("EBSBrakeTemperatureStatusCounter", StringType()),
                           StructField("EBSVehPneumaticSupplyCounter", StringType()),
                           StructField("EBSBrakeLiningMin", StringType()),
                           StructField("EBSBrakeLiningMax", StringType()),
                           StructField("EBSBrakeLiningMean", StringType()),
                           StructField("EBSBrakeLiningPosition", StringType()),
                           StructField("EBSBrakeTemperatureMin", StringType()),
                           StructField("EBSBrakeTemperatureMax", StringType()),
                           StructField("EBSBrakeTemperatureMean", StringType()),
                           StructField("EBSBrakeTemperaturePosition", StringType()),
                           StructField("EBSBrakeLightState", StringType()),
                           StructField("EBSAbsActiveTowingState", StringType()),
                           StructField("EBSVdcActiveTowingState", StringType()),
                           StructField("EBSRopSystemState", StringType()),
                           StructField("EBSYcSystemState", StringType()),
                           StructField("EBSAbsOffRoadState", StringType()),
                           StructField("EBSAbsActiveTowedState", StringType()),
                           StructField("EBSServiceBrakeState", StringType()),
                           StructField("EBSAutoTowedVehBrakeState", StringType()),
                           StructField("EBSVdcActiveTowedState", StringType()),
                           StructField("EBSVehElectricalSupplyState", StringType()),
                           StructField("EBSAmberWarningSignalState", StringType()),
                           StructField("EBSRedWarningSignalState", StringType()),
                           StructField("EBSLoadingRampState", StringType()),
                           StructField("EBSSupplyLineBrakingState", StringType()),
                           StructField("EBSBrakeLiningState", StringType()),
                           StructField("EBSBrakeTemperatureStatusState", StringType()),
                           StructField("EBSVehPneumaticSupplyState", StringType()),
                           StructField("EBSSegPneumaticSupplyPressureMin", StringType()),
                           StructField("EBSSegPneumaticSupplyPressureMax", StringType()),
                           StructField("RGE21MsgCounter", StringType()),
                           StructField("LiftAxle1Position", StringType()),
                           StructField("LiftAxle2Position", StringType()),
                           StructField("ReeferBrandModel", StringType()),
                           StructField("ReeferUnitRunMode", StringType()),
                           StructField("ReeferUnitPowerType", StringType()),
                           StructField("ReeferUnitCityMode", StringType()),
                           StructField("ReeferFuelGaugeState", StringType()),
                           StructField("ReeferFuelLevel", StringType()),
                           StructField("ReeferEngineUpTime", StringType()),
                           StructField("ReeferStandbyTime", StringType()),
                           StructField("ReeferElectricUpTime", StringType()),
                           StructField("ReeferGroupUpTime", StringType()),
                           StructField("ReeferBatteryState", StringType()),
                           StructField("ReeferBatteryVoltage", StringType()),
                           StructField("ReeferAlarmCode", StringType()),
                           StructField("VEmptyLoad", StringType()),
                           StructField("VLoadedState", StringType())
                        ])


### define lambda function to convert string to hex
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


#### GLOBAL VARIABLES ####
errorMsg = ''


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


## pegando o state value do atributo do ebs
def getEBSState(param):
    try:
        ebsTransitionValue = int(param) % 2
    except Exception as e:
        ebsTransitionValue =  None
    return str(ebsTransitionValue)

## pegando o counter value do atributo do ebs
def getEBSCounter(param):
    try:
        ebsCounterValue = int(int(param)/2)
    except Exception as e:
        ebsCounterValue = None
    return str(ebsCounterValue)

def createDocument(diag, ebs, tpms, reefer):
    element = {}
    # Get DIAG payload fields
    if diag is not None:
        element["EBSCanConnection"] = '1' if diag["content"]["can_activity"] == 2 else '0'
        element["ROTemperature"] = diag["content"]["device_temperature"]
        element["VMotion"] =  '0' if diag["content"]["vehicle_motion"] == 1 else '1'
        element["ThresholdMotionStart"] = diag["content"]["threshold_motion"]
        element["ThresholdMotionEnd"] = diag["content"]["threshold_unmotion"]
        element["ThresholdHdop"] = int(diag["content"]["threshold_hdop"])
        element["ScheduledPeriod"] = diag["content"]["scheduled_period"]
        element["CanLoadTermination"] = diag["content"]["can_load"]
        element["VTransportMode"] = str(diag["content"]["vehicle_motion"])
    else:
        element["EBSCanConnection"] = None
        element["ROTemperature"] = None
        element["VMotion"] =  None
        element["ThresholdMotionStart"] = None
        element["ThresholdMotionEnd"] = None
        element["ThresholdHdop"] = None
        element["ScheduledPeriod"] = None
        element["CanLoadTermination"] = None
        element["VTransportMode"] = None
    element["VAxleList"] = None
    element["VBrakeEnergy"] = None
    element["EBSDist"] = None ## Not part of MVP1
    element["EBSDistMsgCounter"] = None ## VDHR_MSG_COUNTER doesn't exists in payload ebs
    element["VROPSystemState"] = None ## Not part of MVP1
    element["VYCSystemState"] = None ## Not part of MVP1
    
    # Get TPMS payload fields
    if tpms is not None:
        element["TPMSPairing"] = tpms[0]["content"]["system_pairing"] 
        element["TPMSNominalPressure"] = tpms[0]["content"]["nominal_pressure"] 
        element["TPMSLowPressureWarningThreshold"] = tpms[0]["content"]["low_pressure_warning_thr"] 
        element["TPMSLowPressureAlertThreshold"] = tpms[0]["content"]["low_pressure_alert_thr"]
        element["TPMSHighTemperatureThreshold"] = tpms[0]["content"]["high_temperature_thr"]
    element["ReservedLDL"] = None
    element["CANDataMonitoringCounter"] = None
    element["CANFrameMonitoringList"] = None
    
    # Get EBS payload fields
    if ebs is not None:
        element["EBS11MsgCounter"] = str(ebs[0]["content"]["11_msg_counter"])
        element["EBSServiceBrakeDemandMin"] = str(ebs[0]["content"]["service_brake_demand_min"])
        element["EBSServiceBrakeDemandMax"] = str(ebs[0]["content"]["service_brake_demand_max"])
        element["EBSServiceBrakeDemandMean"] = str(ebs[0]["content"]["service_brake_demand_mean"])
        element["EBS12MsgCounter"] = str(ebs[0]["content"]["12_msg_counter"])
        element["EBSWheelBasedSpeedTowingMin"] = str(ebs[0]["content"]["wheel_based_speed_towing_min"])
        element["EBSWheelBasedSpeedTowingMax"] = str(ebs[0]["content"]["wheel_based_speed_towing_max"])
        element["EBSWheelBasedSpeedTowingMean"] = str(ebs[0]["content"]["wheel_based_speed_towing_mean"])
        element["EBS21MsgCounter"] = str(ebs[0]["content"]["21_msg_counter"])
        element["EBSLateralAccMin"] = str(ebs[0]["content"]["lateral_acc_min"])
        element["EBSLateralAccMax"] = str(ebs[0]["content"]["lateral_acc_max"])
        element["EBSLateralAccMean"] = str(ebs[0]["content"]["lateral_acc_mean"])
        element["EBSWheelBasedSpeedTowedMin"] = str(ebs[0]["content"]["wheel_based_speed_towed_min"])
        element["EBSWheelBasedSpeedTowedMax"] = str(ebs[0]["content"]["wheel_based_speed_towed_max"])
        element["EBSWheelBasedSpeedTowedMean"] = str(ebs[0]["content"]["wheel_based_speed_towed_mean"])
        element["EBS22MsgCounter"] = str(ebs[0]["content"]["22_msg_counter"])
        element["EBSAxleLoadSumMin"] = str(ebs[0]["content"]["axle_load_sum_min"])
        element["EBSAxleLoadSumMax"] = str(ebs[0]["content"]["axle_load_sum_max"])
        element["EBSAxleLoadSumMean"] = str(ebs[0]["content"]["axle_load_sum_mean"])
        element["EBSBrakeLiningMin"] = str(ebs[0]["content"]["brake_lining_min"])
        element["EBSBrakeLiningMax"] = str(ebs[0]["content"]["brake_lining_max"])
        element["EBSBrakeLiningMean"] = str(ebs[0]["content"]["brake_lining_mean"])
        element["EBS23MsgCounter"] = str(ebs[0]["content"]["23_msg_counter"])
        element["EBSBrakeLiningPosition"] = None ## Not part of MVP1 
        element["EBSBrakeTemperatureMin"] = str(ebs[0]["content"]["brake_temperature_min"])
        element["EBSBrakeTemperatureMax"] = str(ebs[0]["content"]["brake_temperature_max"])
        element["EBSBrakeTemperatureMean"] = str(ebs[0]["content"]["brake_temperature_mean"])
        element["EBSBrakeTemperaturePosition"] = None ## Talk to Bruno to understand this field
        element["EBSSegPneumaticSupplyPressureMin"] = None ## Talk to Bruno to understand this field
        element["EBSSegPneumaticSupplyPressureMax"] = None ## Talk to Bruno to understand this field
        element["RGE21MsgCounter"] = None ## Talk to Bruno to understand this field
        element["LiftAxle1Position"] = None ## Talk to Bruno to understand this field
        element["LiftAxle2Position"] = None ## Talk to Bruno to understand this field
        ## Counter fields
        element["EBSAbsActiveTowingCounter"] = getEBSCounter(ebs[0]["content"]["abs_active_towing_counter"]) # EBS_EBS11_ABS_ACTIVE_TOWING_TRANSITIONS
        element["EBSBrakeLightCounter"] = getEBSCounter(ebs[0]["content"]["brake_light_counter"]) # EBS_EBS11_BRAKE_LIGHT_TRANSITIONS
        element["EBSVdcActiveTowingCounter"] = getEBSCounter(ebs[0]["content"]["vdc_active_towing_counter"]) # EBS_EBS11_VDC_ACTIVE_TOWING_TRANSITIONS
        element["EBSRopSystemCounter"] = getEBSCounter(ebs[0]["content"]["rop_system_counter"]) # EBS_EBS12_ROP_SYSTEM_TRANSITIONS
        element["EBSYcSystemCounter"] = getEBSCounter(ebs[0]["content"]["yc_system_counter"]) # EBS_EBS12_YC_SYSTEM_TRANSITIONS
        element["EBSAbsOffRoadCounter"] = getEBSCounter(ebs[0]["content"]["abs_off_road_counter"]) # EBS_EBS12_ABS_OFF_ROAD_TRANSITIONS
        element["EBSAbsActiveTowedCounter"] = getEBSCounter(ebs[0]["content"]["abs_active_towed_counter"]) # EBS_EBS21_ABS_ACTIVE_TOWED_TRANSITIONS
        element["EBSServiceBrakeCounter"] = getEBSCounter(ebs[0]["content"]["service_brake_counter"]) # EBS_EBS21_SERVICE_BRAKE_TRANSITIONS
        element["EBSAutoTowedVehBrakeCounter"] = getEBSCounter(ebs[0]["content"]["auto_towed_veh_brake_counter"]) # EBS_EBS21_AUTO_TOWED_VEH_BRAKE_TRANSITIONS
        element["EBSVdcActiveTowedCounter"] = getEBSCounter(ebs[0]["content"]["vdc_active_towed_counter"]) # EBS_EBS21_VDC_ACTIVE_TOWED_TRANSITIONS
        element["EBSVehElectricalSupplyCounter"] = getEBSCounter(ebs[0]["content"]["veh_electrical_supply_counter"]) # EBS_EBS22_VEH_ELECTRICAL_SUPPLY_TRANSITIONS
        element["EBSRedWarningSignalCounter"] = getEBSCounter(ebs[0]["content"]["red_warning_signal_counter"]) # EBS_EBS22_RED_WARNING_SIGNAL_TRANSITIONS
        element["EBSAmberWarningSignalCounter"] = getEBSCounter(ebs[0]["content"]["amber_warning_signal_counter"]) # EBS_EBS22_AMBER_WARNING_SIGNAL_TRANSITIONS
        element["EBSLoadingRampCounter"] = getEBSCounter(ebs[0]["content"]["loading_ramp_counter"]) # EBS_EBS22_LOADING_RAMP_TRANSITIONS
        element["EBSSupplyLineBrakingCounter"] = getEBSCounter(ebs[0]["content"]["supply_line_braking_counter"]) # EBS_EBS22_SUPPLY_LINE_BRAKING_TRANSITIONS
        element["EBSBrakeLiningCounter"] = getEBSCounter(ebs[0]["content"]["brake_lining_counter"]) # EBS_EBS23_BRAKE_LINING_TRANSITIONS
        element["EBSBrakeTemperatureStatusCounter"] = getEBSCounter(ebs[0]["content"]["brake_temperature_status_counter"]) # EBS_EBS23_BRAKE_TEMPERATURE_STATUS_TRANSITIONS
        element["EBSVehPneumaticSupplyCounter"] = getEBSCounter(ebs[0]["content"]["veh_pneumatic_supply_counter"]) # EBS_EBS23_VEH_PNEUMATIC_SUPPLY_TRANSITIONS
        ## State fields
        element["EBSAbsActiveTowingState"] = getEBSState(ebs[0]["content"]["abs_active_towing_counter"]) # EBS_EBS11_ABS_ACTIVE_TOWING_TRANSITIONS
        element["EBSBrakeLightState"] = getEBSState(ebs[0]["content"]["brake_light_counter"]) # EBS_EBS11_BRAKE_LIGHT_TRANSITIONS
        element["EBSVdcActiveTowingState"] = getEBSState(ebs[0]["content"]["vdc_active_towing_counter"]) # EBS_EBS11_VDC_ACTIVE_TOWING_TRANSITIONS
        element["EBSRopSystemState"] = getEBSState(ebs[0]["content"]["rop_system_counter"]) # EBS_EBS12_ROP_SYSTEM_TRANSITIONS
        element["EBSYcSystemState"] = getEBSState(ebs[0]["content"]["yc_system_counter"]) # EBS_EBS12_YC_SYSTEM_TRANSITIONS
        element["EBSAbsOffRoadState"] = getEBSState(ebs[0]["content"]["abs_off_road_counter"]) # EBS_EBS12_ABS_OFF_ROAD_TRANSITIONS
        element["EBSAbsActiveTowedState"] = getEBSState(ebs[0]["content"]["abs_active_towed_counter"]) # EBS_EBS21_ABS_ACTIVE_TOWED_TRANSITIONS
        element["EBSServiceBrakeState"] = getEBSState(ebs[0]["content"]["service_brake_counter"]) # EBS_EBS21_SERVICE_BRAKE_TRANSITIONS
        element["EBSAutoTowedVehBrakeState"] = getEBSState(ebs[0]["content"]["auto_towed_veh_brake_counter"]) # EBS_EBS21_AUTO_TOWED_VEH_BRAKE_TRANSITIONS
        element["EBSVdcActiveTowedState"] = getEBSState(ebs[0]["content"]["vdc_active_towed_counter"]) # EBS_EBS21_VDC_ACTIVE_TOWED_TRANSITIONS
        element["EBSVehElectricalSupplyState"] = getEBSState(ebs[0]["content"]["veh_electrical_supply_counter"]) # EBS_EBS22_VEH_ELECTRICAL_SUPPLY_TRANSITIONS
        element["EBSRedWarningSignalState"] = getEBSState(ebs[0]["content"]["red_warning_signal_counter"]) # EBS_EBS22_RED_WARNING_SIGNAL_TRANSITIONS
        element["EBSAmberWarningSignalState"] = getEBSState(ebs[0]["content"]["amber_warning_signal_counter"]) # EBS_EBS22_AMBER_WARNING_SIGNAL_TRANSITIONS
        element["EBSLoadingRampState"] = getEBSState(ebs[0]["content"]["loading_ramp_counter"]) # EBS_EBS22_LOADING_RAMP_TRANSITIONS
        element["EBSSupplyLineBrakingState"] = getEBSState(ebs[0]["content"]["supply_line_braking_counter"]) # EBS_EBS22_SUPPLY_LINE_BRAKING_TRANSITIONS
        element["EBSBrakeLiningState"] = getEBSState(ebs[0]["content"]["brake_lining_counter"]) # EBS_EBS23_BRAKE_LINING_TRANSITIONS
        element["EBSBrakeTemperatureStatusState"] = getEBSState(ebs[0]["content"]["brake_temperature_status_counter"]) # EBS_EBS23_BRAKE_TEMPERATURE_STATUS_TRANSITIONS
        element["EBSVehPneumaticSupplyState"] = getEBSState(ebs[0]["content"]["veh_pneumatic_supply_counter"]) # EBS_EBS23_VEH_PNEUMATIC_SUPPLY_TRANSITIONS
        
        
    # Get REEFER payload fields
    if reefer is not None:
        element["ReeferBrandModel"] = str(reefer[0]["content"]["reefer_type"])
        element["ReeferUnitRunMode"] = str(reefer[0]["content"]["unit_run_mode"])
        element["ReeferUnitPowerType"] = str(reefer[0]["content"]["unit_power_mode"])
        element["ReeferUnitCityMode"] = str(reefer[0]["content"]["unit_city_mode"])  
        element["ReeferFuelGaugeState"] = str(reefer[0]["content"]["fuel_state"]) 
        element["ReeferFuelLevel"] = str(reefer[0]["content"]["fuel_level"]) 
        element["ReeferEngineUpTime"] = str(reefer[0]["content"]["time_engine"]) 
        element["ReeferStandbyTime"] = str(reefer[0]["content"]["time_standby"]) 
        element["ReeferElectricUpTime"] = str(reefer[0]["content"]["time_electric"]) 
        element["ReeferGroupUpTime"] = str(reefer[0]["content"]["time_switch_on"])        
        element["ReeferBatteryState"] = str(reefer[0]["content"]["reefer_battery_state"])
        element["ReeferBatteryVoltage"] = str(reefer[0]["content"]["reefer_battery_voltage"])
        element["ReeferAlarmCode"] = str(reefer[0]["content"]["reefer_alarm_number"])
    element["VEmptyLoad"] = None ## Not Part of MVP
    element["VLoadedState"] = None ## Not Part of MVP
    
    return documentBody.toInternal(element)

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

def executeMonitoring():
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    # new configuration to masternaut integration
    hadoopConf = sc._jsc.hadoopConfiguration()

    hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("spark.hadoop.fs.s3a.path.style.access", "true")

    hadoopConf.set("fs.s3a.endpoint", awsconf_masternaut.AWS_HOST)
    hadoopConf.set("fs.s3a.access.key", awsconf_masternaut.AWS_ACCESS_KEY_ID)
    hadoopConf.set("fs.s3a.secret.key", awsconf_masternaut.AWS_SECRET_ACCESS_KEY)

    # string de conexão
    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    
    ### NSF - Como é a chamada do programa, i.é, quais os parâmetros de chada que estão sendo passados? ###
    ### get args ####
    #correlationId = 999999
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure UDF Function ###
    getROReleaseUDF = udf(getRORelease, StringType())
    getTyrePositionUDF = udf(getTyrePosition, StringType())
    createDocumentUDF = udf(createDocument, documentBody)
    ### get args ####
    #cusoid = sys.argv[1]
    #dateFrom = sys.argv[2]
    #dateTo = sys.argv[3]
    
    ### Query get dateFrom and DateTo for Monitoring Report
    queryGetDate = """(
        SELECT 
            e.ebistartdate as start_date, 
            e.ebienddate as end_date 
        FROM public.electrum_bi e 
        WHERE e.ebitypereport = '{0}'
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

    # ROName antigo: 'LDL_CRCU1_'||m.matserialoid as ROName \
    # sem inner join com material.material_model
    queryGetIdVehicles = """(WITH electrum_vehicles AS (
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
        JOIN customer.customer_vehicle cv on CVVEIOID = vts.VTSINFOVEIOID
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
    hiveSql = """
        select 
            from_unixtime(unix_timestamp(data_posicao_gmt0 , 'yyyy-MM-dd hh:mm:ss.SSS'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as EvtDateTime, 
            data_posicao_gmt0, 
            veiculo, 
            software_version as sv, 
            payload_diag.header.payload_cause VEvtID, 
            latitude as GPSLatitude, 
            longitude as GPSLongitude, 
            payload_diag.content.gnss_altitude GPSAltitude, 
            '' as GPSFixQuality, 
            from_unixtime(unix_timestamp(data_posicao_gmt0 , 'yyyy-MM-dd hh:mm:ss.SSS'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as GPSDateTime, 
            payload_diag.content.gnss_heading GPSHeading, 
            odometro as GPSDist, 
            payload_diag.content.gnss_hdop GPSHdop, 
            payload_diag.content.gnss_vdop GPSVdop, 
            payload_diag.content.gnss_speed GPSSpeed, 
            CASE WHEN payload_diag.content.power_source = False THEN 0 ELSE 1 END AS PowerSource,
            CASE WHEN payload_diag.content.power_source = False THEN 1 ELSE 0 END AS PowerState,
            '' AS PowerStateChange,
            CASE WHEN payload_diag.content.power_source = FALSE THEN payload_diag.content.battery_voltage ELSE payload_diag.content.vehicle_voltage END AS PowerVoltage,
            CASE WHEN payload_diag.content.power_source = FALSE THEN payload_diag.content.battery_charge_level*20 ELSE '' END AS PowerChargeLevel,
            CASE WHEN payload_diag.content.power_source = FALSE THEN PAYLOAD_DIAG.content.battery_charge_state ELSE '' END AS PowerChargeState,
            '' AS PowerChargerFault,
            CASE WHEN payload_diag.content.power_source = FALSE THEN PAYLOAD_DIAG.CONTENT.BATTERY_TEMPERATURE ELSE '' END AS PowerBatteryTemp,
            payload_diag, 
            payload_wsn, 
            payload_tpm, 
            payload_ebs, 
            payload_reefer 
        from 
            posicoes_crcu
        where 
            data_posicao_short >= '{0}'
            and  data_posicao_short <= '{1}'
            and  data_posicao_gmt0 >= '{2}'
            and  data_posicao_gmt0 <= '{3}'
            and cliente in ({4})
                    """.format(dateFromShort, dateToShort, dateFrom, dateTo, customerListString)
    
    ### Define Hive Query (WSN)
    hiveSqlWSN = """
    SELECT 
        veiculo                 AS VEICULO,
        DATA_POSICAO_GMT0       AS DATA_POSICAO_GMT0,
        vid.INDEX               AS WSNVIDPosition,
        vid.id                  AS WSNVIDId,
        vid.tag                 AS WSNVIDTag,
        vid.battery_status      AS WSNVIDBatteryStatus,
        tes.index               AS WSNTesId,
        tes.index               AS WSNTesRank,
        tes.temperature         AS WSNTesTemperature,
        tes.battery_status      AS WSNTesBatteryStatus,
        dos.index               AS WSNDosId,
        dos.index               AS WSNDosRank,
        dos.status              AS WSNDosStatus,
        dos.counter             AS WSNDosCounter,
        dos.battery_status      AS WSNDosBatteryStatus
    FROM 
        posicoes_crcu
        LATERAL VIEW OUTER explode(payload_wsn) a AS wsn
        LATERAL VIEW OUTER explode(a.wsn.content.vid) b AS vid
        LATERAL VIEW OUTER explode(a.wsn.content.tes) c AS tes
        LATERAL VIEW OUTER explode(a.wsn.content.dos) d AS dos
    WHERE data_posicao_short >= '{0}'
                    and  data_posicao_short <= '{1}'
                    and  data_posicao_gmt0 >= '{2}'
                    and  data_posicao_gmt0 <= '{3}'
                    and cliente in ({4})
                    --and vid.id is not null and vid.id > 0 --Precisa adicionar este filtro ao visualizar estes dados
                    """.format(dateFromShort, dateToShort, dateFrom, dateTo, customerListString)
    
    ### Define Hive Query (TPM)
    hiveSqlTPM = """
    SELECT 
        veiculo                                                                   AS VEICULO,
        DATA_POSICAO_GMT0                                                         AS DATA_POSICAO_GMT0,
        tpm.content.device_position                                               AS DEVICE_POSITION, --
        tpm.content.device_serial                                                 AS RcuId, --
        sensor.INDEX                                                              AS INDEX, --
        sensor.identifier                                                         AS WusId, --
        sensor.pressure                                                           AS TirePressure,
        sensor.temperature                                                        AS TireTemperature,
        CASE WHEN sensor.alert_battery == TRUE THEN 1 ELSE 0 END                  AS WusLowBattery,
        CASE WHEN sensor.alert_pressure == TRUE THEN 1 ELSE 0 END                 AS TireAlertPressure, 
        CASE WHEN sensor.alert_temp == TRUE THEN 1 ELSE 0 END                     AS TireAlertTemperature,
        sensor.com                                                                AS WusComState,
        CASE WHEN sensor.`conf` == TRUE THEN 1 ELSE 0 END                         AS WusConfigError,
        ''                                                                        AS WusLocation,
        ''                                                                        AS WusRotation,
        ''                                                                        AS WusRotationState,
        sensor.mode                                                               AS WusMode
    from 
        posicoes_crcu
        LATERAL VIEW OUTER explode(payload_tpm) a AS tpm
        LATERAL VIEW OUTER explode(a.tpm.content.sensors) b AS sensor
    WHERE data_posicao_short >= '{0}'
                    and  data_posicao_short <= '{1}'
                    and  data_posicao_gmt0 >= '{2}'
                    and  data_posicao_gmt0 <= '{3}'
        AND sensor.status = 'activated'
        AND sensor.com = 2
        AND (sensor.identifier > 0 AND sensor.identifier < 4294967293)
        and cliente in ({4})""".format(dateFromShort, dateToShort, dateFrom, dateTo, customerListString)
    
    ### Define Hive Query (payload_reefer)
    hiveSqlReefer = """
    SELECT 
        veiculo,
        DATA_POSICAO_GMT0,
        COMPARTMENT.index       AS ReeferCompartmentElement_Rank,
        COMPARTMENT.state       AS CurState,
        COMPARTMENT.mode        AS SetMode,
        COMPARTMENT.setpoint    AS SetPoint,
        payload_reefer[0].content.temperature[CAST(COMPARTMENT.INDEX AS integer)].state AS TempProbeState,
        payload_reefer[0].content.temperature[CAST(COMPARTMENT.INDEX AS integer)].value AS Temperature,
        COMPARTMENT.supply_air_state AS SupplyAirState,
        COMPARTMENT.supply_air_value AS SupplyAirTemperature,
        COMPARTMENT.return_air_state AS ReturnAirState,
        COMPARTMENT.return_air_value AS ReturnAirTemperature,
        digital_input.INDEX AS ReeferDigitalInputsElement_Rank,
        digital_input.state AS ReeferDigitalInputsElement_Value,
        maintenance.INDEX AS ReeferToNextMaintenanceElement_Rank,
        MAINTENANCE.time AS ReeferToNextMaintenanceElement_Time
    FROM 
        posicoes_crcu
        LATERAL VIEW OUTER EXPLODE(payload_reefer) a AS reefer
        LATERAL VIEW OUTER EXPLODE(a.reefer.content.compartment) b AS compartment
        LATERAL VIEW OUTER EXPLODE(a.reefer.content.digital_input) c AS digital_input
        LATERAL VIEW OUTER EXPLODE(a.reefer.content.maintenance) d AS maintenance
    WHERE 
        data_posicao_short >= '{0}'
        and  data_posicao_short <= '{1}'
        and  data_posicao_gmt0 >= '{2}'
        and  data_posicao_gmt0 <= '{3}'
        and cliente in ({4})""".format(dateFromShort, dateToShort, dateFrom, dateTo, customerListString)
    
    ## GERANDO OS DFs
    df = sqlContext.sql(hiveSql)
    dfWSNPre = sqlContext.sql(hiveSqlWSN)
    dfTPMPre = sqlContext.sql(hiveSqlTPM)
    dfReeferPre = sqlContext.sql(hiveSqlReefer)
    
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
    dfPar = df.alias('a').join(tableGetIdVehicles.alias('b'),col('a.veiculo') == col('b.vtsinfoveioid')).select("*")
    
    ### join with wdlData and DataFrame  ####
    dfVeiAux = dfPar.join(lastDateVehicles, (lastDateVehicles.roname_last_position == dfPar.roname) & (dfPar.data_posicao_gmt0 <= lastDateVehicles.date_position ), 'left_outer').select("*")
    
    ### filter WDL data  ####
    dfVei = dfVeiAux.filter('date_position is null')
    
    ### Get the last position date ###
    lastObjectDate = dfVei.select("data_posicao_gmt0").agg(max("data_posicao_gmt0").alias("data_posicao_gmt0")).collect()[0]['data_posicao_gmt0']
    
    if lastObjectDate == None:
        lastObjectDate = dateFrom
    
    ###########################################################################    
    ## SOLUÇÃO PALIATIVA PARA PEGAR O SOFTWARE VERSION CORRETO DO VEÍCULO
    dfVeiSV = dfVei.select(col("veiculo").alias("veiculo_with_sv"), col("sv").alias('software_version')).filter("sv != '0'").distinct()
    # incluíndo o software version no df
    dfVeiOk = dfVei.alias('a').join(dfVeiSV.alias('b'),col('a.veiculo') == col('b.veiculo_with_sv'), 'left_outer').select("*")
    ###########################################################################
    
    # ENRIQUECENDO OS DFS
    dfReport = dfVeiOk.withColumn('RORelease', getROReleaseUDF('software_version'))
    dfReportFinal = dfReport.withColumn('Document', createDocumentUDF('payload_diag', 'payload_ebs', 'payload_tpm', 'payload_reefer'))
    dfTPM = dfTPMPre.withColumn('TireLocation', getTyrePositionUDF('device_position', 'index'))
    
    # eliminando possíveis duplicidades
    dfReportFinal = dfReportFinal.distinct()
    dfWSNFinal = dfWSNPre.distinct()
    dfTPMFinal = dfTPM.distinct()
    dfReeferFinal = dfReeferPre.distinct()
    
    ### CREATE FILE NAME ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    fileName = 'MONITORING_'+timeStr
    
    #print(" Salvando os dados em %s - com nome %s" % ( bucketUrl, fileName ) )
    
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    
    ### Make a connection to Redshift and write the data into the table 'stage.MONITORING_REEFER' ###
    dfReeferFinal.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "stage.MONITORING_REEFER") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()
    
    ### Make a connection to Redshift and write the data into the table 'stage.MONITORING_WSN' ###
    dfWSNFinal.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "stage.MONITORING_WSN") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()

    ### Make a connection to Redshift and write the data into the table 'stage.MONITORING_TPMS' ###
    dfTPMFinal.select(
        "VEICULO",
        "DATA_POSICAO_GMT0",
        "TireLocation",
        "WusId",
        "TirePressure",
        "WusLowBattery",
        "TireAlertPressure",
        "TireAlertTemperature",
        "WusComState",
        "WusConfigError",
        when(col("WusLocation") != '', col("WusLocation")).otherwise(None).alias("WusLocation"),
        when(col("WusRotation") != '', col("WusRotation")).otherwise(None).alias("WusRotation"),
        when(col("WusRotationState") != '', col("WusRotationState")).otherwise(None).alias("WusRotationState"),
        "WusMode"
        ).write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "stage.MONITORING_TPMS") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()

    
    ### Make a connection to Redshift and write the data into the table 'stage.MONITORING' ###
    dfReportFinal.select(
        "VEICULO",
        "DATA_POSICAO_GMT0",
        "ROName", 
        "EvtDateTime", 
        "VID", 
        "RORelease", 
        "VEvtID", 
        "TyreCheckID", 
        col("GPSLatitude").cast('float').alias('GPSLatitude'), 
        col("GPSLongitude").cast('float').alias('GPSLongitude'), 
        "GPSAltitude", 
        when(col("GPSFixQuality") != '', col("GPSFixQuality")).otherwise(None).alias('GPSFixQuality'), 
        "GPSDateTime", 
        "GPSHeading",
        "GPSDist",  
        "GPSHdop", 
        "GPSVdop", 
        "GPSSpeed", 
        col("Document.*")
    ).write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "stage.MONITORING") \
        .option("tempdir", s3TempDir) \
        .mode("append") \
        .save()
    
    # Gravando uma copia dos dados no S3 da Masternaut
    if integration_masternaut:
        
        mBucket = awsconf_masternaut.AWS_S3_BUCKET_NAME
        url = "s3a://" + mBucket + "/" + fileName

        dfReportFinal.repartition( 10 ) \
            .write \
            .format('json') \
            .options(charset='UTF-8', header='true') \
            .mode("overwrite") \
            .save(url)


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
    
    RunSQL(conn, query_update_report)
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()

if __name__ == "__main__":
    # Executa o report
    try:
        executeMonitoring()
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
