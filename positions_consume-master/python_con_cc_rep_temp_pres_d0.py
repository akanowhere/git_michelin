#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
Created on Wed May 22 09:05:18 2019
@author: carlos.santanna.ext
Updated on 10-01-2020 - @author: igor.seibel.ext (removendo Hazelcast)
Updated on 13-01-2020 - @author: igor.seibel.ext (Tratamento de exceções)
Updated on 14-01-2020 - @author: igor.seibel.ext (Dockerizando script)
Updated on 15-01-2020 - @author: igor.seibel.ext (Adding more parameters to the script)
Updated on 29-01-2020 - @author: igor.seibel.ext (Refactored for Unit Tests purpose)
Updated on 12/01/2021 - @author: ricardo.costardi.ext (Add new columns)
"""

from itertools import count
import os
import json
from datetime import datetime, timezone, timedelta
import pytz
import sys, errno
import cx_Oracle
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from sql_codes import merge_sensor, update_position_sql
from python_con_cc_posicao_crcu import PosicaoCRCUProcess
from python_con_cc_posicao_ttu import PosicaoTTUProcess
import logging

KAFKA_AUTO_OFFSET_RESET = "latest"
KAFKA_ENABLE_AUTO_COMMIT = False
KAFKA_CONSUMER_TIMEOUT_MS = 150000
KAFKA_MAX_POLL_INTERVAL_MS = 100000
KAFKA_SESSION_TIMEOUT_MS = 300000
KAFKA_USE_RD = False

class TpmsD0Process():

    def __init__(self, dbclass=cx_Oracle, kafka_client_class=KafkaConsumer, kafka_producer_class=KafkaProducer):
        self.sql=None
        self.kafka_client_class = kafka_client_class
        self.kafka_producer_class = kafka_producer_class
        self.dbclass = dbclass
        self.message_count = 0
        
    @staticmethod
    def get_tyre_position(device_position, tire):
        device_position1 = {
                    1: '0x00', 2: '0x01', 3: '0x02', 4: '0x03',  5: '0x10', 6: '0x11',
                    7: '0x12', 8: '0x13', 9: '0x20', 10: '0x21', 11: '0x22', 12: '0x23'
                }
        device_position2 = {
                    1: '0x40', 2: '0x41', 3: '0x42', 4: '0x43',  5: '0x50',  6: '0x51',
                    7: '0x52', 8: '0x53', 9: '0x60', 10: '0x61', 11: '0x62', 12: '0x63'
                }
        device_position3 = {
                    1: '0x80', 2: '0x81', 3: '0x82', 4: '0x83',  5: '0x90',  6: '0x91',
                    7: '0x92', 8: '0x93', 9: '0xA0', 10: '0xA1', 11: '0xA2', 12: '0xA3'
                }
        if device_position == 1:
            return device_position1.get(tire+1, None)
        if device_position == 2:
            return device_position2.get(tire+1, None)
        if device_position == 3:
            return device_position3.get(tire+1, None)
        return None

    def set_database_connection(self, login, password, host, database):
        self.con = None
        self.con = self.dbclass.connect(
        "{0}/{1}@{2}/{3}".format(login,password,host,database)
        )
        self.cur = self.con.cursor()        
        
    def set_kafka_consumer(self, kafka_hosts, kafka_zookeeper, kafka_topic, kafka_consumer_group_id):        
        self.tpms_consumer = self.kafka_client_class(kafka_topic,
                                                     bootstrap_servers=kafka_hosts,
                                                     auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
                                                     enable_auto_commit=KAFKA_ENABLE_AUTO_COMMIT,
                                                     group_id=kafka_consumer_group_id,
                                                     session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS)  

    def set_kafka_producer(self, bootstrap_servers, kafka_topic):
        self.tpms_producer = self.kafka_producer_class(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))
        self.kafka_topic_producer = kafka_topic

    def produce_kafka_topic(self, payload):
        future = self.tpms_producer.send(self.kafka_topic_producer, value=payload)
        future.add_callback(lambda x: logging.info("Topic {topic} populada. {x}."))
        future.add_errback(lambda e: logging.error("Erro populando topic {topic}. {e}."))
        
    def execute_merge(self, sql_params):
        try:
            ### Execute sql instruction
            logging.debug(sql_params)
            self.cur.execute(sql_params)
            logging.warning("Merge executado!")
            ### Commit transaction
            self.con.commit()
            return True
        except cx_Oracle.DatabaseError as e:
            logging.error(str(e))
            if self.dbclass==cx_Oracle:
                logging.error("forcing exit!")
                sys.exit(errno.EINTR)
            return False

    def prepare_sql(self):
        self.sql = '''
              BEGIN
                --sensors data
                BEGIN
                  {7}
                EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
                  NULL;
                END;
                --sensors healthcheck
                {9}

                BEGIN
                    UPDATE FT_CC_SENSOR_PNEU_AGREGADO sa
                       SET sa.DAT_ULT_POSICAO_VEICULO = CASE
  		                                                WHEN TO_DATE('{8}', 'YYYY-MM-DD HH24:MI:SS') > sa.DAT_ULT_POSICAO_VEICULO THEN 
  		                                                  TO_DATE('{8}', 'YYYY-MM-DD HH24:MI:SS')
  		                                                ELSE 
  		                                                  sa.DAT_ULT_POSICAO_VEICULO
                                                        END
                       WHERE ID_CLIENTE = {2}
                         AND ID_VEICULO = {1};
                EXCEPTION WHEN OTHERS THEN
                  NULL;
                END;
                
              END;     
        '''
    
        # Prepare SQL statement
        self.cur.prepare(self.sql)
    
        return(self.sql)

    def extract_payload_tpm(self, message):

        try:            
            position = json.loads(message)
        except:
            return (None, None, None, None, None)            

        ### Get main data from json
        json_data = position.get("summaryDevice").get("jsonData")
        json_ebs = json_data.get("payload_ebs")
        json_wsn = json_data.get("payload_wsn")
        ### Get the TPMS payload
        return (json_data, json_data.get("payload_tpm"), json_data.get("pyaload_diag"), json_ebs, json_wsn)

    @staticmethod
    def sensor_is_valid(sensor_status, sensor_com, sensor_identifier):
        ### Check if it is a valid sensor according to the following rules: 
        ### 1) The 'status' should be 'activated'; 
        #   2) The 'com' should be 2; 
        #   3) The 'identifier' must be higher than 0 and smaller than 4294967293. 
        # All rules are according to the CRCU documentation and the instructions provided by Michelin Engeneering (Bruno Arluison/Renaud Lozier)
        if (sensor_status == 'activated' and sensor_com == 2 and sensor_identifier > 0 and sensor_identifier < 4294967293 ):
            return True
        else:
            return False                

        
    def process_sensor(self, sensors, max_temperature, min_pressure, device_position):
        sensor_detail = []
        ### Check all the sensors in the current TPMS payload. The exactly number of sensors in a TPMS payload is 12

        valid_sensors = list(filter(lambda s: self.is_valid(s), sensors))

        if valid_sensors.count == 0:
            return (None, None, [], [])

        for sensor in valid_sensors:
            sensor_temperature = int(sensor["temperature"])
            sensor_pressure = int(sensor["pressure"])
            sensor_detail.append({"id_pneu": sensor["identifier"], 
                                    "pressure": sensor_pressure,
                                    "temperature": sensor_temperature,
                                    "tire_position": TpmsD0Process.get_tyre_position(device_position=device_position, tire=sensor["index"]),
                                    "alert_battery": sensor["alert_battery"],
                                    "sensor_index": sensor["index"],
                                    "sensor_conf": sensor["conf"],
                                    "sensor_com": sensor["com"],
                                    "sensor_status": sensor["status"]  })
            
            if (sensor_pressure < min_pressure):
                min_pressure = sensor_pressure

        sensors = list(sensor_detail)

        if len(sensors) > 0:
            sensors.sort(reverse=True, key=lambda x: x["temperature"])
            max_temperature = sensors[0]['temperature']
        else:
            max_temperature = None

        return (max_temperature, min_pressure, sensor_detail, sensors)

    def is_valid(self, sensor):
        sensor_status = sensor["status"]
        sensor_com = sensor["com"]
        sensor_identifier = sensor["identifier"]
        return TpmsD0Process.sensor_is_valid(sensor_status=sensor_status, sensor_com=sensor_com, sensor_identifier=sensor_identifier)

    @staticmethod
    def build_insert_detail(id_cliente, id_veiculo, dat_posicao, dat_referencia, sensor_detail):
        wsql_detail = 'INSERT INTO FT_CC_TEMP_PRES_PNEU_DETALHADO(ID_CLIENTE, ID_VEICULO, ID_PNEU, DAT_POSICAO, PRESSAO_PNEU, TEMPERATURA_PNEU, POSICAO_PNEU, DAT_REFERENCIA, SENSOR_INDEX, SENSOR_ALERT_BATTERY, SENSOR_COM, SENSOR_CONF, SENSOR_STATUS)'
        wsensor_count = 0
        for sensor in sensor_detail:
            if wsensor_count == 0:
                wsql_detail = wsql_detail + " SELECT "+str(id_cliente)+', '+str(id_veiculo)+', '+str(sensor["id_pneu"])+", to_date('"+str(dat_posicao)+"','yyyy-mm-dd hh24:mi:ss'), "+str(sensor["pressure"])+", "+str(sensor["temperature"])+", '"+str(sensor["tire_position"])+"', to_date('"+str(dat_referencia)+"','yyyy-mm-dd hh24:mi:ss'), "+str(sensor["sensor_index"])+", '"+str(sensor["alert_battery"])+"', "+str(sensor["sensor_com"])+", "+str(sensor["sensor_conf"])+", '"+str(sensor["sensor_status"])+"' FROM DUAL"
            else:
                wsql_detail = wsql_detail + " UNION SELECT "+str(id_cliente)+", "+str(id_veiculo)+", "+str(sensor["id_pneu"])+", to_date('"+str(dat_posicao)+"','yyyy-mm-dd hh24:mi:ss'), "+str(sensor["pressure"])+", "+str(sensor["temperature"])+", '"+str(sensor["tire_position"])+"', to_date('"+str(dat_referencia)+"','yyyy-mm-dd hh24:mi:ss'), "+str(sensor["sensor_index"])+", '"+str(sensor["alert_battery"])+"', "+str(sensor["sensor_com"])+", "+str(sensor["sensor_conf"])+", '"+str(sensor["sensor_status"])+"' FROM DUAL"
            wsensor_count += 1
        if wsensor_count > 0:
            wsql_detail = wsql_detail +';'
            return wsql_detail
        else:
            return "DBMS_OUTPUT.PUT_LINE('SEM DETALHADO');"

    @staticmethod
    def build_sensors_merge(id_cliente, id_veiculo, dat_posicao, sensor_detail):
        wfull_merge = ''
        wsensor_count = 0
        for sensor in sensor_detail:
            if sensor["alert_battery"] == True:
                dat_battery_alert = "to_date('"+str(dat_posicao)+"','yyyy-mm-dd hh24:mi:ss')"
            else:
                dat_battery_alert = 'NULL'
            wfull_merge = wfull_merge +'\n'+ merge_sensor.format(id_cliente,
                                                            id_veiculo,
                                                            sensor["id_pneu"],
                                                            sensor["tire_position"],
                                                            str(dat_posicao),
                                                            dat_battery_alert
                                                            )
            wsensor_count += 1
        if wsensor_count > 0:
            return wfull_merge
        else:
            return "DBMS_OUTPUT.PUT_LINE('NO SENSORS');"

    def process_tpm(self, payload_tpm, param_dict):        
        if not payload_tpm:
            return False
        result = False
        ### Check all the TPMS payloads in the position. The max number of TPMS payloads in a position is 3
        for payload in payload_tpm:
            
            ### Get device serial
            device_serial = payload["content"].get("device_serial")
            ### Validate the device serial according to the following rule: 1) Device serial can not be null and should be higher than 0
            if (device_serial > 0):
                ### Get the sensors (tires) of the current payload of TPMS
                sensors = payload["content"].get("sensors")
                ### Check if there is a sensors information in the current payload of TPMS
                if (sensors):
                    max_temperature, min_pressure, sensor_detail, all_sensors = self.process_sensor(sensors=sensors,
                                                                                       max_temperature=param_dict["max_temperature"],
                                                                                       min_pressure=param_dict["min_pressure"],
                                                                                       device_position=payload["content"].get("device_position")
                                                                                      )
                    payloadTemperature = {
                        "customer": param_dict["customer"],
                        "vehicle": param_dict["vehicle"],
                        "dat_posicao": param_dict["position_reference_full"],
                        "dat_referencia": param_dict["todaywithtime"],
                        "latitude": param_dict["latitude"],
                        "longitude": param_dict["longitude"],
                        "all_sensors": all_sensors
                    }

                    self.produce_kafka_topic(payloadTemperature)
                            
                    wsql_detail = TpmsD0Process.build_insert_detail(id_cliente=param_dict["customer"],
                                                                    id_veiculo=param_dict["vehicle"],
                                                                    dat_posicao=param_dict["position_reference_full"],
                                                                    dat_referencia=param_dict["todaywithtime"],
                                                                    sensor_detail=sensor_detail)
                    wsql_health = TpmsD0Process.build_sensors_merge(id_cliente=param_dict["customer"],
                                                                    id_veiculo=param_dict["vehicle"],
                                                                    dat_posicao=param_dict["position_reference_full"],
                                                                    sensor_detail=sensor_detail)
                    ### Prepare the SQL estatment with the correct params
                    sql_params = self.sql.format(
                            param_dict["position_reference"],
                            param_dict["vehicle"],
                            param_dict["customer"],
                            min_pressure,
                            max_temperature,
                            param_dict["distance"],
                            param_dict["todaywithtime"],
                            wsql_detail,
                            param_dict["position_reference_full"],
                            wsql_health
                    )
                    if self.execute_merge(sql_params=sql_params):
                        result = True
                    else:
                        result = False
        return result

    def update_position_date(self, customer, vehicle, position_date, found_tpms):
        if self.is_crcu and not found_tpms:
            sql_params = update_position_sql.format(
                    position_date,
                    vehicle,
                    customer)
            logging.debug(sql_params)
            if self.execute_merge(sql_params=sql_params):
                return True
            else:
                return False
        else:
            return False
        
    
    def process_message(self, message):
        merge_count = 0
        if message is not None:
            self.is_crcu = False
            self.is_ttu = False

            json_data, payload_tpm, payload_diag, payload_ebs, payload_wsn = self.extract_payload_tpm(message=message.value)

            try:
                protocolo = json_data['protocolo']
            except:
                return (False, None)
            
            if protocolo in (81, 82):
                self.message_count += 1
                self.is_ttu = True
                processa_posicao = PosicaoTTUProcess(con=self.con,cur=self.cur, dbclass=self.dbclass)
                processa_posicao.process_message(json_data=json_data)
                return(True, self.sql)                        
            
            ### Set base parameters
            # initialize max_temperature variable with minimal possible value for a TPMS sensor (according to the CRCU documentation).
            max_temperature = -50
    
            # initialize min_pressure variable with maximum possible value for a TPMS sensor (according to the CRCU documentation).
            min_pressure = 1399950
                    
            ### Get position date in UTC time
            position_date = datetime.utcfromtimestamp(json_data["data_posicao"]).replace(tzinfo=timezone.utc)
            ### Get today date in UTC
            today = datetime.utcnow().replace(tzinfo=timezone.utc)
            ### Get necessary fields
            customer = int(json_data["cliente"])
            vehicle = int(json_data["veiculo"])

            # ### Extract the date reference in the format 'YYYY-MM-DD'
            todaywithtime  = today.strftime("%Y-%m-%d %H:%M:%S")  
            today += timedelta(minutes=5)
            position_reference = position_date.strftime("%Y-%m-%d")
            position_reference_full = position_date.strftime("%Y-%m-%d %H:%M:%S")
            payload_tpms_found = False                
            if (protocolo in (83, 131)) and (position_date <= today and payload_diag):
                self.is_crcu = True
                self.processa_posicao_crcu = PosicaoCRCUProcess(con=self.con,cur=self.cur, dbclass=self.dbclass) 
                self.processa_posicao_crcu.process_message(json_data=json_data,
                                                      payload_diag=payload_diag,
                                                      payload_ebs=payload_ebs,
                                                      payload_wsn=payload_wsn,
                                                      payload_tpm=payload_tpm)
            
                ### Distance (not calculated at this time)
                distance = 0
                param_dict = {"vehicle": vehicle,
                              "customer": customer,
                              "max_temperature": max_temperature,
                              "min_pressure": min_pressure,
                              "distance": distance,
                              "position_reference": position_reference,
                              "todaywithtime": todaywithtime,
                              "position_reference_full": position_reference_full,
                              "latitude": json_data["latitude"],
                              "longitude": json_data["longitude"]}
                processed = self.process_tpm(payload_tpm=payload_tpm, 
                                             param_dict=param_dict)
                if processed:
                    merge_count += 1
                else:
                    return(False, self.sql)

                self.update_position_date(customer=customer, vehicle=vehicle, position_date=position_reference_full, found_tpms=payload_tpms_found)

        if merge_count > 0:
            return(True, self.sql)
        else:
            return (False, None)

    def main(self, kafka_error_retries=0):
        self.prepare_sql()
        try:
            for message in self.tpms_consumer:
                try:
                    logging.info('leu mensagem!')
                    is_ok, w_sql = self.process_message(message=message)
                    if is_ok:
                        self.tpms_consumer.commit()
                except Exception as e:
                    logging.error("----------- Error to process vehicle -----------")
                    logging.error(e)
                    logging.error("Offset: "+str(message.offset))
                    sys.exit(errno.EINTR)
            return True
        except Exception as e:            
            logging.error(e)
            sys.exit(errno.EINTR)
            