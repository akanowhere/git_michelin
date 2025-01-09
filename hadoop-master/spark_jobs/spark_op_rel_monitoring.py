# -*- coding: utf-8 -*-
"""
Updated on 2019-12-18 16:25
@author: thomas.lima.ext
@description: Alteração da query queryGetIdVehicles para buscar das novas tabelas 
              vehicle.vehicle_technical_solution e vehicle.vehicle_product_list
"""
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
from datetime import datetime, timedelta
### O import abaixo não funciona qunado executado diretamente no spark   ###
import config_database as dbconf
import config_aws as awsconf
import general_config as genconf

### GLOBAL TYPES DEFINITION ####

### payload_diag
### PowerSourceList
PowerSourceElement = StructType([
		StructField("PowerSourceElement", ArrayType(
                StructType([
						StructField("PowerSource", StringType()),
						StructField("PowerState", StringType()),
						StructField("PowerStateChange", StringType()),
						StructField("PowerVoltage", StringType()),
						StructField("PowerChargeLevel", StringType()),
						StructField("PowerChargeState", StringType()),
						StructField("PowerChargerFault", StringType()),
						StructField("PowerBatteryTemp", StringType())
				       ])
				))
       ])

### Não existem.  Vamos retornar nulo.
### VAxleList
VAxleElement = StructType([
		StructField("VAxleElement", ArrayType(
			StructType([
				StructField("VAxlePosition", StringType()),
				StructField("VAxleLiftPosition", StringType()),
				StructField("VAxleLoad", StringType()),
				StructField("VWheelBasedSpeedDiff", StringType())
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
				StructField("WSNVIDBatteryStatus", StringType())
			])
		))
	])

### WSNTesList
WSNTesElement = StructType([
		StructField("WSNTesElement", ArrayType(
			StructType([
				StructField("WSNTesId", StringType()),
				StructField("WSNTesRank", StringType()),
				StructField("WSNTesTemperature", StringType()),
				StructField("WSNTesBatteryStatus", StringType())
			])
		))
	])

### WSNTesList
WSNDosElement = StructType([
		StructField("WSNDosElement", ArrayType(
			StructType([
				StructField("WSNDosId", StringType()),
				StructField("WSNDosRank", StringType()),
				StructField("WSNDosStatus", StringType()),
				StructField("WSNDosCounter", StringType()),
              StructField("WSNDosBatteryStatus", StringType()) 
			])
		))
	])


TPMSElement = StructType([
		StructField("TPMSElement", ArrayType(
			StructType([
				StructField("TireLocation", StringType()),
				StructField("WusId", StringType()),
				StructField("TirePressure", StringType()),
				StructField("TireTemperature", StringType()),
              StructField("WusComState", StringType()),
              StructField("WusConfigError", StringType()),
              StructField("WusLowBattery", StringType()),
              StructField("TireAlertPressure", StringType()),
              StructField("TireAlertTemperature", StringType()),
              StructField("WusLocation", StringType()),
              StructField("WusRotation", StringType()),
              StructField("WusRotationState", StringType()),
              StructField("WusMode", StringType())
			])
		))
	])

TPMSRCUElement = StructType([
		StructField("TPMSRCUElement", ArrayType(
			StructType([
				StructField("RcuId", StringType())
			])
		))
	])


### Definindo a estrutura "ReeferCompartmentElement"
ReeferCompartmentElement = StructType([
		StructField("ReeferCompartmentElement", ArrayType(
			StructType([
              StructField("Rank", StringType()),
              StructField("CurState", StringType()),
              StructField("SetMode", StringType()),
              StructField("Setpoint", StringType()),
              StructField("TempProbeState", StringType()),
              StructField("Temperature", StringType()),
              StructField("SupplyAirState", StringType()),
              StructField("SupplyAirTemperature", StringType()),
              StructField("ReturnAirState", StringType()),
              StructField("ReturnAirTemperature", StringType())
			])
		))
	])


### Definindo a estrutura "ReeferDigitalInputsElement"
ReeferDigitalInputsElement = StructType([
		StructField("ReeferDigitalInputsElement", ArrayType(
			StructType([
              StructField("Rank", StringType()),
              StructField("Value", StringType())
			])
		))
	])

### Definindo a estrutura "ReeferDigitalInputsElement"
ReeferToNextMaintenanceElement = StructType([
		StructField("ReeferToNextMaintenanceElement", ArrayType(
			StructType([
              StructField("Rank", StringType()),
              StructField("Time", StringType())
			])
		))
	])

### Definindo a estrutura "document"
documentBody = StructType([StructField("PowerSourceList", PowerSourceElement),
                           StructField("ROTemperature", StringType()),
                           StructField("VAxleList", VAxleElement),
                           StructField("VBrakeEnergy", StringType()),
                           StructField("VMotion", StringType()),
                           StructField("EBSCanConnection", StringType()),
                           StructField("ThresholdMotionStart", StringType()),
                           StructField("ThresholdMotionEnd", StringType()),
                           StructField("ThresholdHdop", StringType()),
                           StructField("ScheduledPeriod", StringType()),
                           StructField("CanLoadTermination", StringType()),
                           StructField("TPMSList", TPMSElement),
                           StructField("TPMSRcuList", TPMSRCUElement),
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
                           StructField("WSNVIDList", WSNVIDElement),
                           StructField("WSNTesList", WSNTesElement),
                           StructField("WSNDosList", WSNDosElement),
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
                           StructField("ReeferCompartmentList", ReeferCompartmentElement),
                           StructField("ReeferDigitalInputsList", ReeferDigitalInputsElement),
                           StructField("ReeferFuelGaugeState", StringType()),
                           StructField("ReeferFuelLevel", StringType()),
                           StructField("ReeferEngineUpTime", StringType()),
                           StructField("ReeferStandbyTime", StringType()),
                           StructField("ReeferElectricUpTime", StringType()),
                           StructField("ReeferGroupUpTime", StringType()),
                           StructField("ReeferToNextMaintenanceList", ReeferToNextMaintenanceElement),
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


## Function to create TPMSRcuList
def getTPMSRcuList(payload_tpm):
    element = {}
    array = []
    obj = {}
    try:
        obj = {}
        obj["RcuId"] =  intToHex(payload_tpm[0]["content"]["device_serial"])
        array.append(obj)
    except Exception:
        array = None
    element["TPMSRCUElement"] = array
    return TPMSRCUElement.toInternal(element)

#### GLOBAL VARIABLES ####
errorMsg = ''

### Generate row with array of payload_diag.content (PowerSourceElement) ###
def getPSEData(payloadDiagC):
    element = {}
    array = []
    obj = {}
    try:
        # Vehicle power source information
        obj = {}
        obj["PowerSource"] =  1 # Main power source
        obj["PowerState"] = 1 if payloadDiagC["content"]["power_source"] == True else 0
        obj["PowerStateChange"] = None # str( '1' if payloadDiagC["content"]["vehicle_voltage"] != 0 else '0'  ) - Validate this field correctly
        obj["PowerVoltage"] = str(payloadDiagC["content"]["vehicle_voltage"])
        obj["PowerChargeLevel"] =  None # str(payloadDiagC["content"]["battery_charge_level"]) - Vehicle does not have charge level
        obj["PowerChargeState"] = None # str(payloadDiagC["content"]["battery_charge_state"]) - Vehicle does not have charge state
        obj["PowerChargerFault"] = None
        obj["PowerBatteryTemp"] = None # str(payloadDiagC["content"]["battery_temperature"]) - Vehicle does not have temperature
        
        array.append(obj)
        
        # Battery power source information
        obj = {}
        obj["PowerSource"] =  0 # Battery power source
        obj["PowerState"] = 1 if payloadDiagC["content"]["power_source"] == False else 0
        obj["PowerStateChange"] = None # str( '1' if payloadDiagC["content"]["vehicle_voltage"] != 0 else '0'  ) - Validate this field correctly
        obj["PowerVoltage"] = payloadDiagC["content"]["battery_voltage"]
        obj["PowerChargeLevel"] = payloadDiagC["content"]["battery_charge_level"] * 20
        obj["PowerChargeState"] = payloadDiagC["content"]["battery_charge_state"]
        obj["PowerChargerFault"] = None
        obj["PowerBatteryTemp"] = str(payloadDiagC["content"]["battery_temperature"]) if payloadDiagC["content"]["battery_temperature"] > -50 else None
        
        array.append(obj)
        
    except Exception:
        obj = None
    element["PowerSourceElement"] = array
    return PowerSourceElement.toInternal(element)

### Generate empty row.  Only structure wil be generated.  (VAxleElement) ###
##def getVAEData(payloadVAE):
def getVAEData():
    element = {}
    element["VAxleElement"] = None
    return VAxleElement.toInternal(element)

### Generate row with array of WSNVIDElement ###
def getWSNVidData(payloadWSN):
	element = {}
	array = []
	obj = {}
	try:
		for line in payloadWSN[0]["content"]["vid"]:
			if line["id"] != None and line["id"] > 0:
				obj = {}
				obj["WSNVIDPosition"] =  str(line["index"])
				obj["WSNVIDId"] = str(line["id"])
				obj["WSNVIDTag"] = str(line["tag"])
				obj["WSNVIDBatteryStatus"] = str(line["battery_status"])
				array.append(obj)
	except Exception:
		array = None
	element["WSNVIDElement"] = array
	return WSNVIDElement.toInternal(element)


### Generate row with array of WSNTesData ###
### Index must to begin in 1
def getWSNTesData(payloadWSNT):
    element = {}
    array = []
    obj = {}
    try:
        for line in payloadWSNT[0]["content"]["tes"]:
            obj = {}
            obj["WSNTesId"] = str(int(line["index"])+1)
            obj["WSNTesRank"] = str(int(line["index"])+1)
            obj["WSNTesTemperature"] = str(int(line["temperature"]))
            obj["WSNTesBatteryStatus"] = str(line["battery_status"])
            array.append(obj)
    except Exception:
        array = None
    element["WSNTesElement"] = array
    return WSNTesElement.toInternal(element)


### Generate row with array of WSNVIDElement ###
def getWSNDosData(payloadWSN):
    element = {}
    array = []
    obj = {}
    try:
        for line in payloadWSN[0]["content"]["dos"]:
            obj = {}
            obj["WSNDosId"] =  str(line["index"])
            obj["WSNDosRank"] = str(line["index"])
            obj["WSNDosStatus"] = str(line["status"])
            obj["WSNDosCounter"] = str(line["counter"])
            obj["WSNDosBatteryStatus"] = str(line["battery_status"])
            array.append(obj)
    except Exception:
        array = None
    element["WSNDosElement"] = array
    return WSNDosElement.toInternal(element)



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


### Generate row with array of TPMSData ###
def getTPMSData(payload_tpm):
    element = {}
    array = []
    obj = {}
    seq = 0
    
    try:
        for line in payload_tpm[0]["content"]["sensors"]:
            
            if line['status'] == 'activated' and line['com'] == 2 and (line['identifier'] > 0 and line['identifier'] < 4294967293):
                
                obj = {}
                obj["TireLocation"] =  getTyrePosition(payload_tpm[0]["content"]["device_position"], seq)
                seq = seq + 1
                
                obj["WusId"] = intToHex(line["identifier"])
                obj["TirePressure"] = str(line["pressure"])
                obj["TireTemperature"] = int(line["temperature"])
                obj["WusLowBattery"] = 0 if line["alert_battery"] == False else 1
                obj["TireAlertPressure"] = 0 if line["alert_pressure"] == 0 else 1
                obj["TireAlertTemperature"] = 0 if line["alert_temp"] == False else 1
                obj["WusComState"] = str(line["com"])
                obj["WusConfigError"] = 0 if line["conf"] == 1 else 1
                obj["WusLocation"] = None
                obj["WusRotation"] = None
                obj["WusRotationState"] = None
                obj["WusMode"] = str(line["mode"]) 
                array.append(obj)
    except Exception:
        array = None
    element["TPMSElement"] = array
    return TPMSElement.toInternal(element)

def getReeferCompartmentData(payload_reefer):
    element = {}
    array = []
    obj = {}
    seq = 0
    try:
        for line in payload_reefer[0]["content"]["compartment"]:
            obj = {}
            obj["Rank"] =   str(seq)
            obj["CurState"] = str(line["state"])
            obj["SetMode"] = str(line["mode"])
            obj["Setpoint"] = str(line["setpoint"])
            obj["TempProbeState"] = str(payload_reefer[0]["content"]["temperature"][seq]["state"])
            obj["Temperature"] = str(payload_reefer[0]["content"]["temperature"][seq]["value"])
            obj["SupplyAirState"] = str(line["supply_air_state"])
            obj["SupplyAirTemperature"] = str(line["supply_air_value"])
            obj["ReturnAirState"] = str(line["return_air_state"])
            obj["ReturnAirTemperature"] = str(line["return_air_value"])
            seq = seq + 1
            array.append(obj)
    except Exception:
        array = None
    element["ReeferCompartmentElement"] = array
    return ReeferCompartmentElement.toInternal(element)



def getReeferDigitalInputsElementData(payload_reefer):
    element = {}
    array = []
    obj = {}
    seq = 0
    try:
        for line in payload_reefer[0]["content"]["digital_input"]:
            obj = {}
            obj["Rank"] =   str(seq)
            obj["Value"] = '' if line["state"] is None else str(line["state"])
            seq = seq + 1
            array.append(obj)
    except Exception:
        array = None
    element["ReeferDigitalInputsElement"] = array
    return ReeferDigitalInputsElement.toInternal(element)

def getReeferToNextMaintenanceElementData(payload_reefer):
    element = {}
    array = []
    obj = {}
    seq = 0
    try:
        for line in payload_reefer[0]["content"]["maintenance"]:
            obj = {}
            obj["Rank"] =   str(seq)
            obj["Time"] =   str(line["time"])
            seq = seq + 1
            array.append(obj)
    except Exception:
        array = None
    element["ReeferToNextMaintenanceElement"] = array
    return ReeferToNextMaintenanceElement.toInternal(element)

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

def createDocument(PowerSourceElement, diag, TPMSList, TPMSRcuList, ebs, tpms, WSNVIDList, WSNTesList, WSNDosList, reefer, ReeferCompartmentList, ReeferDigitalInputsList, ReeferToNextMaintenanceList ):
    element = {}
    element["PowerSourceList"] = PowerSourceElement
    
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
    element["TPMSList"] = TPMSList #if len(TPMSList) > 0 else None
    element["TPMSRcuList"] = TPMSRcuList #if len(TPMSRcuList) > 0 else None
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
        
    element["WSNVIDList"] = WSNVIDList
    element["WSNTesList"] = WSNTesList
    element["WSNDosList"] = WSNDosList
    element["VRoadTrainList"] = None
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
    element["ReeferCompartmentList"] = ReeferCompartmentList
    element["ReeferDigitalInputsList"] = ReeferDigitalInputsList
    element["ReeferToNextMaintenanceList"] = ReeferToNextMaintenanceList
    element["VEmptyLoad"] = None ## Not Part of MVP
    element["VLoadedState"] = None ## Not Part of MVP
    
    return documentBody.toInternal(element)

### FUNCTION TO CREATE THE FILTER LINES AND RETURN AN ARRAY WITH ALL FILTERS  ###
### NSF - entender como é  ###
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
        
    ### NSF - Como é a chamada do programa, i.é, quais os parâmetros de chada que estão sendo passados? ###
    ### get args ####
    correlationId = sys.argv[3]
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure UDF Function ###
    getPSEDataUDF = udf(getPSEData, PowerSourceElement)
    getVAEDataUDF = udf(getVAEData, VAxleElement)
    getWSNVidDataUDF = udf(getWSNVidData, WSNVIDElement)
    getWSNTesDataUDF = udf(getWSNTesData, WSNTesElement)
    getWSNDosDataUDF = udf(getWSNDosData, WSNDosElement)
    getTPMSDataUDF = udf(getTPMSData, TPMSElement)
    getTPMSRcuListUDF = udf(getTPMSRcuList, TPMSRCUElement)
    getReeferCompartmentDataUDF = udf(getReeferCompartmentData, ReeferCompartmentElement)
    getReeferDigitalInputsElementDataUDF = udf(getReeferDigitalInputsElementData, ReeferDigitalInputsElement)
    getReeferToNextMaintenanceElementDataUDF = udf(getReeferToNextMaintenanceElementData, ReeferToNextMaintenanceElement)
    getROReleaseUDF = udf(getRORelease, StringType())
    
    
    createDocumentUDF = udf(createDocument, documentBody)
    ### SQL no Postgresql  ###
    sql = "(select * from report.report_partner_solicitation where rpsoid = {0}) as reportSolicitation".format(correlationId)
    
    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    ### Execute SQL  ###
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

    # ROName antigo: 'LDL_CRCU1_'||m.matserialoid as ROName \
    # sem inner join com material.material_model
    queryGetIdVehicles = """(select 
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
    
    dateFromShort = datetime.strptime(dateFrom, "%Y-%m-%d %H:%M:%S") - timedelta(days = 1)
    dateToShort = datetime.strptime(dateTo, "%Y-%m-%d %H:%M:%S") + timedelta(days = 1)
    
    dateFromShort = dateFromShort.strftime("%Y-%m-%d")
    dateToShort = dateToShort.strftime("%Y-%m-%d")
    
    ### define hive query ### 
    hiveSql = """select \
				       from_unixtime(unix_timestamp(data_posicao_gmt0 , 'yyyy-MM-dd hh:mm:ss.SSS'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as EvtDateTime, \
                     data_posicao_gmt0, \
                     veiculo, \
                     software_version as sv, \
				       payload_diag.header.payload_cause VEvtID, \
				       latitude as GPSLatitude, \
				       longitude as GPSLongitude, \
				       payload_diag.content.gnss_altitude GPSAltitude, \
				       '' as GPSFixQuality, \
				       from_unixtime(unix_timestamp(data_posicao_gmt0 , 'yyyy-MM-dd hh:mm:ss.SSS'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as GPSDateTime, \
				       payload_diag.content.gnss_heading GPSHeading, \
				       odometro as GPSDist, \
				       payload_diag.content.gnss_hdop GPSHdop, \
				       payload_diag.content.gnss_vdop GPSVdop, \
				       payload_diag.content.gnss_speed GPSSpeed, \
				       payload_diag, \
                     payload_wsn, \
                     payload_tpm, \
                     payload_ebs, \
                     payload_reefer  \
                 from posicoes_crcu
                 where data_posicao_short >= '{0}'
                  and  data_posicao_short <= '{1}'
                  and  data_posicao_gmt0 > '{2}'
                  and  data_posicao_gmt0 <= '{3}'""".format(dateFromShort, dateToShort, dateFrom, dateTo)

    ### EXECUTE REPORT QUERY ###
    df = sqlContext.sql(hiveSql)

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
    
    dfReport = dfVeiOk.withColumn('PowerSourceList', getPSEDataUDF('payload_diag')) \
    	.withColumn('VAxleList', getVAEDataUDF()) \
    	.withColumn('WSNVIDList', getWSNVidDataUDF('payload_wsn')) \
    	.withColumn('WSNTesList', getWSNTesDataUDF('payload_wsn')) \
       .withColumn('WSNDosList', getWSNDosDataUDF('payload_wsn')) \
       .withColumn('TPMSList', getTPMSDataUDF('payload_tpm')) \
       .withColumn('TPMSRcuList', getTPMSRcuListUDF('payload_tpm')) \
       .withColumn('ReeferCompartmentList', getReeferCompartmentDataUDF('payload_reefer')) \
       .withColumn('ReeferDigitalInputsList', getReeferDigitalInputsElementDataUDF('payload_reefer')) \
       .withColumn('ReeferToNextMaintenanceList', getReeferToNextMaintenanceElementDataUDF('payload_reefer')) \
       .withColumn('RORelease', getROReleaseUDF('software_version'))
        
    dfReportFinal = dfReport.withColumn('Document', createDocumentUDF('PowerSourceList', 'payload_diag', 'TPMSList', 'TPMSRcuList', 'payload_ebs', 'payload_tpm', 'WSNVIDList', 'WSNTesList', 'WSNDosList', 'payload_reefer', 'ReeferCompartmentList', 'ReeferDigitalInputsList', 'ReeferToNextMaintenanceList'))
    
    # eliminando possíveis duplicidades
    dfReportFinal = dfReportFinal.distinct()
    
    ### CREATE FILE NAME ###
    timeStr = str(datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
    #fileName = 'MONITORING_'+str(correlationId)+"_"+timeStr+".xml"
    fileName = 'MONITORING_'+str(correlationId)+"_"+timeStr
    bucketUrl = awsconf.AWS_S3_BUCKET_MSOL
    hdfsPath  = "%s/%s" % ( genconf.PATH_ELECTRUM_HDFS, correlationId )
    publicUrlS3 = 'https://s3-eu-west-1.amazonaws.com/'

    print(" Salvando os dados em %s - com nome %s" % ( hdfsPath, fileName ) )
    
    ## generate xml file in hdfs
    dfReportFinal.select("ROName", "EvtDateTime", "VID", "RORelease", 
    	 			"VEvtID", "TyreCheckID", col("GPSLatitude").cast('float').alias('GPSLatitude'), col("GPSLongitude").cast('float').alias('GPSLongitude'), 
    	 			"GPSAltitude", when(col("GPSFixQuality") != '', col("GPSFixQuality")).otherwise(None), "GPSDateTime", "GPSHeading",
    	 			"GPSDist",  "GPSHdop", "GPSVdop", "GPSSpeed", "Document") \
        .coalesce( 5 ) \
        .write \
        .format('com.databricks.spark.xml') \
        .options(charset='UTF-8', rowTag='trailerMonitoring') \
        .mode("overwrite") \
        .save(hdfsPath) # +fileName)
    
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
