#!/usr/bin/env python

# -*- coding: utf-8 -*-
import json
from datetime import datetime, timezone
import pytz
import sys
import cx_Oracle
from pykafka import KafkaClient
from pykafka.common import OffsetType
import json
from pykafka.exceptions import ConsumerStoppedException, KafkaException, NoBrokersAvailableError
import sys, errno
import logging

VEHICLE_CONTROL = 'VEI_GMT_PROCESSAMENTO'
POSITIONS_TABLE = 'FT_CC_POSICAO'

class PosicaoTTUProcess():

    def __init__(self, con, cur, dbclass):
        self.sql=None
        self.con = con
        self.cur = cur
        self.msg_count = 0
        self.dbclass = dbclass
        
    def execute_sql(self, sql_params):
        try:
            # Execute sql instruction
            self.cur.execute(sql_params)
            logging.debug("insert executado!")
            # Commit transaction
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
                 BEGIN
                   INSERT INTO VEI_GMT_PROCESSAMENTO (ID_VEICULO, DAT_ULT_POS_CHEGA_PROC, STATUS, TIPO_EQUIP)
                   VALUES({2}, to_date('1970-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'), 'PENDING', 'TTU');
                 EXCEPTION 
                 WHEN DUP_VAL_ON_INDEX THEN
                   NULL;
                 END;

                 BEGIN
                   INSERT INTO {0}(ID_CLIENTE, --1
                                   ID_VEICULO, --2
                                   DAT_POSICAO, --3
                                   DAT_RECEBIDO, --4
                                   ODOMETRO, --5
                                   LATITUDE, --6
                                   LONGITUDE, --7
                                   VELOCIDADE, --8
                                   POS_MEMORIA, --9
                                   HORIMETRO, --10
                                   BLOQUEIO, --11
                                   SATELITAL, --12
                                   GPS_VALIDO, --13
                                   ESTADO_BATERIA_INT, --14
                                   IGNICAO, --15
                                   ESTADO_BATERIA_EXT, --16
                                   POWER_SOURCE, --17
                                   THRESHOLD_MOTION, --18
                                   THRESHOLD_UNMOTION) 
                    VALUES({1}, {2}, to_date('{3}','yyyy-mm-dd hh24:mi:ss'), to_date('{4}','yyyy-mm-dd hh24:mi:ss'), 
                          '{5}', {6}, {7}, {8}, {9}, '{10}', {11}, {12}, {13}, {14}, '{15}', {16}, '{17}', {18}, {19});
                 EXCEPTION
                 WHEN DUP_VAL_ON_INDEX THEN
                   NULL;
                END;   
              END;     
        '''

        # Prepare SQL statement

        self.cur.prepare(self.sql)
    
        return(self.sql)

    def get_baterry_pecent(self, battery_charge_level):
        if battery_charge_level == 0:
            return -1
        elif battery_charge_level > 5:
            return None
        else:
            return (battery_charge_level -1)*25

    def process_payload(self, param_dict):
        # TTU threshold parameters:
        threshold_motion = 8
        threshold_unmotion = 0
        try:
            logging.debug(param_dict)
            sql_params = self.sql.format(
                                POSITIONS_TABLE,
                                param_dict["customer"],
                                param_dict["vehicle"], 
                                param_dict["position_reference_full"],
                                param_dict["todaywithtime"],
                                param_dict["odometro"],
                                param_dict["latitude"],
                                param_dict["longitude"],
                                param_dict["velocidade"],
                                param_dict["pos_memoria"],
                                param_dict["horimetro"],
                                param_dict["bloqueio"],
                                param_dict["satelital"],
                                param_dict["gps_valido"],
                                param_dict["bateria_int"],
                                param_dict["ignicao"],
                                param_dict["bateria_ext"],
                                param_dict["power_source"],
                                threshold_motion,
                                threshold_unmotion
                    )
            logging.debug(sql_params)
        except:
            raise
        if self.execute_sql(sql_params=sql_params.encode('UTF-8')):
            return True    

    def process_message(self, json_data):
        self.prepare_sql()
        customer = int(json_data["cliente"])
        vehicle = int(json_data["veiculo"])
        odometro = int(json_data["odometro"])
        latitude = float(json_data["latitude"])
        longitude = float(json_data["longitude"])
        velocidade_ttu = int(json_data["velocidade"])
        pos_memoria_ttu = json_data["pos_memoria"]
        horimetro_ttu = int(json_data['horimetro'])
        bloqueio_ttu = json_data['blockVehicle']
        gps_valido_ttu = json_data['gps_valido']
        bateria_ext_ttu = json_data['bateriaExt']
        bateria_int_ttu = int(json_data['bateriaInt'])
        ignicao_ttu = json_data['ignicao']

        ### Get position date in UTC time
        position_date = datetime.utcfromtimestamp(json_data["data_posicao"]).replace(tzinfo=timezone.utc)
                    
        ### Get today date in UTC
        today = datetime.utcnow().replace(tzinfo=timezone.utc)
            
        todaywithtime  = today.strftime("%Y-%m-%d %H:%M:%S")
        position_reference_ttu = position_date.strftime("%Y-%m-%d")
        position_reference_full_ttu = position_date.strftime("%Y-%m-%d %H:%M:%S")

        if bateria_ext_ttu == 1:
            power_source = '1-BATERIA EXTERNA'
        else:
            power_source = '0-ALIMENTACAO INTERNA'

        param_dict_ttu = {"vehicle": vehicle,
                        "customer": customer,
                        "position_reference": position_reference_ttu,
                        "todaywithtime": todaywithtime,
                        "position_reference_full": position_reference_full_ttu,
                        "velocidade": velocidade_ttu,
                        "latitude": latitude,
                        "longitude": longitude,
                        "odometro": odometro,
                        "pos_memoria": pos_memoria_ttu,
                        "horimetro": horimetro_ttu,
                        "bloqueio": bloqueio_ttu,
                        "gps_valido": gps_valido_ttu, 
                        "bateria_ext": bateria_ext_ttu,
                        "bateria_int": bateria_int_ttu,
                        "ignicao": ignicao_ttu,
                        "power_source": power_source,
                        "satelital": 0}

        merge_count = 0
        processed = self.process_payload(param_dict=param_dict_ttu)
        if processed:
            merge_count += 1
        
        if merge_count > 0:
            return(True, self.sql)
        else:
            return (False, None)