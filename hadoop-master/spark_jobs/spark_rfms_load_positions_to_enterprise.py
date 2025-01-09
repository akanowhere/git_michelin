# -*- coding: utf-8 -*-
"""
Created on 2019-04-29 09:40:00 +3

@author: thomas.lima.ext
@description: Processo de carga de posições do HIVE para o ENTERPRISE já processando e formatando os dados
no pradrão esperado pelo serviço de status extendido para as histórias do RFMS
"""

### Import libraries 
from __future__ import print_function
from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import udf, lit, col, max, when
from pyspark.sql.types import *
import psycopg2
import sys
import logging
import json
import config_database as dbconf
import config_aws as awsconf


#############################################
################ Functions ##################
#############################################

### define lambda function to convert string to hex
intToHex = lambda x: hex(x)[2:].zfill(2)

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



# Get Trigger type by trigger code
def getTriggerType(trigger_code):
    trigger_type = {
        1:"TIMER",
        2:"IGNITION_ON",
        3:"IGNITION_OFF",
        4:"VEHICLE_MOTION_START",
        5:"VEHICLE_MOTION_END",
        6:"GPS_FIX",
        9:"TELL_TALE",
        10:"TIMER",
        11:"TELL_TALE",
    }
    try:
        return str(trigger_type[trigger_code])
    except Exception as error:
        return None

# Get Transport mode
def getTransportMode(vehicle_motion):
    transport_mode = {
        0:"OMITTED",
        1:"OMITTED",
        2:"STANDARD",
        3:"STANDARD",
        4:"COMBINED",
        5:"OMITTED",
        6:"OMITTED"
    }
    try:
        return str(transport_mode[vehicle_motion])
    except Exception as error:
        return None

### Get VID data from payload_wsn ###
def getWSNVidData(payloadWSN):
    vid = []
    try:
        for sensor in payloadWSN[0]["content"]["vid"]:
            if sensor["status"] == "active": 
                vidSensor = {
                    "Rank": sensor['index'],
                    "Identifier": str(sensor["id"]),
                    "Label": str(sensor["tag"])
                }
                vid.append(vidSensor)
    except Exception as error:
        pass
    return json.dumps(vid) if len(vid) > 0 else None


### Get TES data from payload_wsn ###
def getWSNTesData(payloadWSNT):
    tes = []
    try:
        for sensor in payloadWSNT[0]["content"]["tes"]:
            if sensor["temperature"] > -50:
                tesSensor = {
                    "Rank": sensor['index'],
                    "Temperature": sensor['temperature']
                }
                tes.append(tesSensor)
    except Exception as error:
        pass
    return json.dumps(tes) if len(tes) > 0 else None


### Get DOS data from payload_wsn ###
def getWSNDosData(payloadWSN):
    dos = []
    try: 
        for sensor in payloadWSN[0]["content"]["dos"]: 
            dosSensor = {
                "Rank": sensor['index'],
                "State": "OPEN" if sensor['status'] == 1 else "CLOSED"
            }
            dos.append(dosSensor)
    except Exception as error:
        pass
    return json.dumps(dos) if len(dos) > 0 else None


## Get TPM Data from payload_tpm ##
def getTPMSData(payload_tpm):
    tire = []
    try:
        for sensor in payload_tpm[0]["content"]["sensors"]:
            # check if the status is activated and the communication is ok
            if sensor['status'] == 'activated' and sensor['com'] == 2 and (sensor['identifier'] > 0 and sensor['identifier'] < 4294967293):
                tireSensor = {}
                # Location of the sensor associated to the tire
                tireSensor["Location"] = str(getTyrePosition(payload_tpm[0]["content"]["device_position"], sensor["index"]))
                # Pressure of the tire in Bar. Decimal precision: 2 digits
                tireSensor["Pressure"] = float(round(sensor["pressure"]/100000.0, 2))
                # Temperature of the tire in Degree Celsius. Decimal precision: 1 digit
                tireSensor["Temperature"] = float(round(sensor["temperature"], 1))
                # append in array
                tire.append(tireSensor)
    except Exception as error:
        pass
    return json.dumps(tire) if len(tire) > 0 else None



## Define UDF functions ##
getWSNDosDataUDF = udf(getWSNDosData,StringType())
getWSNTesDataUDF = udf(getWSNTesData,StringType())
getWSNVidDataUDF = udf(getWSNVidData,StringType())
getTPMSDataUDF = udf(getTPMSData,StringType())
getTriggerTypeUDF = udf(getTriggerType,StringType())
getTransportModeUDF = udf(getTransportMode,StringType())



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
    
    ### Select all positions from hive ###
    ### all CRCU positions have 0 to satelital ###

    sqlHive = """
        SELECT 
            veiculo as vehicle_id,
            data_posicao_gmt0 AS created_date_time,
            data_posicao_gmt0 AS position_date_time,
            cast(payload_diag.header.message_arrival_date.date_time AS timestamp) AS received_date_time,
            odometro AS hr_total_vehicle_distance,
            latitude, 
            longitude,
            PAYLOAD_DIAG.CONTENT.GNSS_HEADING AS heading,
            PAYLOAD_DIAG.CONTENT.GNSS_ALTITUDE AS altitude,
            velocidade AS speed,
            CASE WHEN bateriaext = 0 THEN 'BATTERY' ELSE 'MAIN' END AS power_source,
            PAYLOAD_EBS.CONTENT.AXLE_LOAD_SUM_MEAN[0] AS axle_load,
            PAYLOAD_DIAG.HEADER.PAYLOAD_CAUSE AS trigger_code,
            'rFMS' AS trigger_context,
            NULL AS tacho_driver_identification,
            NULL AS oem_driver_identification_id_type,
            'NODRIVER' AS oem_driver_identification_driver_identification,
            0 AS engine_total_fuel_used,
            NULL AS gross_combination_vehicle_weight,
            NULL AS ambient_air_temperature,
            NULL AS service_distance,
            PAYLOAD_DIAG.CONTENT.vehicle_motion AS vehicle_motion,
            payload_wsn,
            payload_tpm
        FROM  
            posicoes_crcu 
        WHERE 
            data_posicao_short >= '{0}' 
            AND  data_posicao_short <= '{1}' 
            AND  data_insert > '{2}' 
            AND  data_insert <= '{3}'
        """.format(dateProcessShortBegin, dateProcessShortEnd, dateProcessBegin, dateProcessEnd)
    
    dfHive = sqlContext.sql(sqlHive)
    
    # adding door sensor data, temperature data, asset data, tire sensor data, trigger type and transport mode
    newDf = dfHive.withColumn('door_sensor', getWSNDosDataUDF('payload_wsn')) \
            .withColumn('temperature_sensor', getWSNTesDataUDF('payload_wsn')) \
            .withColumn('asset', getWSNVidDataUDF('payload_wsn')) \
            .withColumn('tire_sensor', getTPMSDataUDF('payload_tpm')) \
            .withColumn('trigger_type', getTriggerTypeUDF('trigger_code')) \
            .withColumn('transport_mode', getTransportModeUDF('vehicle_motion')) \
            .withColumn('gross_combination_vehicle_weight', lit(None).cast('integer')) \
            .withColumn('ambient_air_temperature', lit(None).cast('float')) \
            .withColumn('service_distance', lit(None).cast('integer')) \
            .withColumn('tacho_driver_identification', lit(None).cast('string')) \
            .withColumn('oem_driver_identification_id_type', lit(None).cast('string'))
    
    # remove desnecessary columns
    dfFinal = newDf.select('vehicle_id', 'created_date_time', 'position_date_time', 'received_date_time', \
        'hr_total_vehicle_distance', 'latitude', 'longitude', 'heading', 'altitude', 'speed', 'power_source', \
        'axle_load', 'trigger_code', 'trigger_type', 'trigger_context', 'tacho_driver_identification', 'oem_driver_identification_id_type', \
        'oem_driver_identification_driver_identification', 'engine_total_fuel_used', 'gross_combination_vehicle_weight', \
        'ambient_air_temperature', 'service_distance', 'transport_mode', 'asset', 'temperature_sensor', 'door_sensor', 'tire_sensor')
    
    # Write dataframe on Enterprise DB
    writer = DataFrameWriter(dfFinal)

    mode = "append"
    table = "report.rfms_position"
    jdbc_url = "jdbc:postgresql://{0}:5432/{1}".format(dbconf.OP_HOST, dbconf.OP_DATABASE)
    user = dbconf.OP_USERNAME
    password = dbconf.OP_PASSWORD
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    writer.jdbc(url=jdbc_url, table=table, mode=mode, properties=properties)

    ## delete rows with received date time is older than now - 20 days
    dateNow = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=20)
    dateLimitToStorage = dateNow.strftime('%Y-%m-%d %H:%M:%S')
    ## Execute delete on Enterprise DB
    conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_HOST, dbconf.OP_PASSWORD))
    cur = conn.cursor()
    sql = "DELETE FROM report.rfms_position rp WHERE rp.received_date_time < '{0}'".format(dateLimitToStorage)
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()