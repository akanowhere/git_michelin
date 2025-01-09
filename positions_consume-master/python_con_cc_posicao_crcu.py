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
from util import treat_db_null

VEHICLE_CONTROL = 'VEI_GMT_PROCESSAMENTO'
POSITIONS_TABLE = 'FT_CC_POSICAO'

class PosicaoCRCUProcess():

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
                logging.error(sql_params)
                logging.error("forcing exit!")
                sys.exit(errno.EINTR)
            return False            


    def prepare_sql(self):
        self.sql = '''
              BEGIN
                 BEGIN
                   INSERT INTO VEI_GMT_PROCESSAMENTO (ID_VEICULO, DAT_ULT_POS_CHEGA_PROC, STATUS, TIPO_EQUIP)
                   VALUES({2}, to_date('1970-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'), 'PENDING', 'CRCU');
                 EXCEPTION 
                 WHEN DUP_VAL_ON_INDEX THEN
                   NULL;
                 END;
                 
                 BEGIN
                   INSERT INTO VEI_INFRACTIONS_PROCESSAMENTO (ID_VEICULO, DAT_ULT_POS_CHEGA_PROC, STATUS)
                   VALUES({2}, to_date('{3}','yyyy-mm-dd hh24:mi:ss'), 'PENDING');
                 EXCEPTION 
                 WHEN DUP_VAL_ON_INDEX THEN
                   NULL;
                 END;

                 BEGIN
                   INSERT INTO {0}(ID_CLIENTE, --1
                                   ID_VEICULO, --2
                                   DAT_POSICAO, --3
                                   DAT_RECEBIDO, --4
                                   VEHICLE_MOTION, --5
                                   PAYLOAD_CAUSE, --6
                                   THRESHOLD_MOTION, --7
                                   THRESHOLD_UNMOTION, --8
                                   THRESHOLD_HDOP, --9
                                   POWER_SOURCE, --10
                                   ODOMETRO, --11
                                   LATITUDE, --12
                                   LONGITUDE, --13
                                   VELOCIDADE, --14
                                   POS_MEMORIA, --15
                                   HORIMETRO, --16
                                   BLOQUEIO, --17
                                   SATELITAL, --18
                                   GPS_VALIDO, --19
                                   VCC_ALIM, --20
                                   ESTADO_BATERIA_INT, --21
                                   PERC_BAT_CALC, --22
                                   PESO, --23
                                   DIRECAO_VEICULO, --24
                                   ALTITUDE, --25
                                   IGNICAO, --26
                                   ESTADO_BATERIA_EXT, --27
                                   CAN_ACTIVITY, --28
                                   SESSION_NUMBER, --29
	                               OWNER_ID, --30
	                               ABS_ACTIVE_TOWING_COUNTER, --31
	                               ABS_ACTIVE_TOWED_COUNTER, --32
                                   VHDR_VEHICLE_DISTANCE, --33
                                   TRUCK_ID_0, --34
                                   TRUCK_ID_1, --35
                                   TRUCK_ID_2, --36
                                   WSN_TEMPERATURE_IDX_0, --37
                                   WSN_BATTERY_STATUS_IDX_0, --38
                                   WSN_TEMPERATURE_IDX_1, --39
                                   WSN_BATTERY_STATUS_IDX_1, --40
                                   WSN_TEMPERATURE_IDX_2, --41
                                   WSN_BATTERY_STATUS_IDX_2, --42
                                   WSN_TEMPERATURE_IDX_3, --43
                                   WSN_BATTERY_STATUS_IDX_3, --44
                                   YC_SYSTEM_COUNTER, --45
                                   ROP_SYSTEM_COUNTER, --46
                                   VDC_ACTIVE_TOWING_COUNTER, --47
                                   SERVICE_BRAKE_COUNTER, --48
                                   AMBER_WARNING_SIGNAL_COUNTER, --49
                                   RED_WARNING_SIGNAL_COUNTER, --50
                                   SOFTWARE_VERSION, --51
                                   HIGH_TEMPERATURE_THR, --52
                                   LOW_PRESSURE_ALERT_THR, --53
                                   NOMINAL_PRESSURE, --54
                                   DEVICE_SERIAL, --55
                                   LOW_PRESSURE_WARNING_THR, --56
                                   DEVICE_POSITION, --57
                                   AXLE_LOAD_SUM_MIN, --58
                                   AXLE_LOAD_SUM_MAX, --59
                                   TRUCK_ID_0_TAG, --60
                                   TRUCK_ID_0_BATTERY_STATUS, --61
                                   TRUCK_ID_1_TAG, --62
                                   TRUCK_ID_1_BATTERY_STATUS, --63
                                   TRUCK_ID_2_TAG, --64
                                   TRUCK_ID_2_BATTERY_STATUS, --65    
                                   LATERAL_ACC_MAX, --66
                                   LATERAL_ACC_MIN, --67
                                   FLAG_ACC_LATERAL, --68
                                   FLAG_FRENAGEM_BRUSCA, --69
                                   wheel_based_speed_towing_mean, --70
                                   wheel_based_speed_towed_mean,  --71
                                   diff_towing_towed_pct, --72  
                                   VDC_ACTIVE_TOWED_COUNTER, --73    
                                   veh_pneumatic_supply_counter, --74
                                   veh_electrical_supply_counter --75
                                   ) 
                    VALUES({1}, {2}, to_date('{3}','yyyy-mm-dd hh24:mi:ss'), to_date('{4}','yyyy-mm-dd hh24:mi:ss'), 
                          '{5}', '{6}', {7}, {8}, {9}, '{10}', {11}, {12}, {13}, {14}, '{15}', {16}, '{17}','{18}',
                          '{19}', {20}, '{21}', {22}, {23}, {24}, {25}, '{26}', '{27}', '{28}', {29}, {30}, {31}, {32}, {33}, 
                          {34}, {35}, {36}, {37}, '{38}', {39}, '{40}', {41}, '{42}', {43}, '{44}', {45}, {46}, {47}, {48}, {49},
                          {50}, {51}, {52}, {53}, {54}, {55}, {56}, {57}, {58}, {59}, '{60}', '{61}', '{62}', '{63}',
                          '{64}', '{65}', {66}, {67}, {68}, {69}, {70}, {71}, {72}, {73}, {74}, {75});
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
            return 100
        else:
            return (battery_charge_level -1)*25

    def get_vehicle_motion(self, vm):
        vehicle_motion = 'INDEFINIDO'
        if vm == 1:
            vehicle_motion = '1-STATIC'
        if vm == 2:
            vehicle_motion = '2-TRIP'
        if vm == 3: 
            vehicle_motion = '3-UNPOWERED'
        if vm == 4: 
            vehicle_motion = '4-MODAL-EM MOV COM BATERIA'
        if vm == 5: 
            vehicle_motion = '5-ANORMAL'
        if vm == 6:
            vehicle_motion = '6-N/A'

        return vehicle_motion

    def get_payload_cause(self, pc):
        payload_cause = 'INDEFINIDO'
        if pc == 0:
            payload_cause = '0-NOT DEFINED'
        if pc == 1:
            payload_cause = '1-SCHEDULED'
        if pc == 2:
            payload_cause = '2-POWER_ON'
        if pc == 3:
            payload_cause = '3-POWER_OFF'
        if pc == 4:
            payload_cause = '4-MOTION'
        if pc == 5:
            payload_cause = '5-UNMOTION'
        if pc == 6:
            payload_cause = '6-GNSS_FIX'
        if pc == 7:
            payload_cause = '7-REQUEST'
        if pc == 8:
            payload_cause = '8-TPM_EVT'
        if pc == 9:
            payload_cause = '9-DOOR_EVT'
        if pc == 10:
            payload_cause = '10-MOVING'
        if pc == 11:
            payload_cause = '11-SAFETY'
        if pc == 12:
            payload_cause = '12-N/A'
        if pc == 13:
            payload_cause = '13-N/A'
        
        return payload_cause

    def process_diag(self, payload_diag, param_dict):
        ######
        vm = payload_diag["content"].get("vehicle_motion")
        vehicle_motion = self.get_vehicle_motion(vm=vm)
        ######
        pc = payload_diag["header"].get("payload_cause")
        payload_cause = self.get_payload_cause(pc=pc)

        ps = payload_diag["content"].get("power_source")
        if ps:
            power_source = '1-BATERIA EXTERNA'
            vcc_alim = payload_diag["content"].get("vehicle_voltage")
        else:
            power_source = '0-ALIMENTACAO INTERNA'
            vcc_alim = payload_diag["content"].get("battery_voltage")

        gnss_heading = payload_diag["content"].get("gnss_heading")
        gnss_altitude = payload_diag["content"].get("gnss_altitude")
        can_activity =  payload_diag["content"].get("can_activity")

        battery_charge_level = payload_diag["content"].get("battery_charge_level")
        perc_bat_calc = self.get_baterry_pecent(battery_charge_level=battery_charge_level)

        session_number = payload_diag["header"].get("session_number")
        owner_id = payload_diag["header"].get("owner_id")


        try:
            logging.debug(param_dict)
            sql_params = self.sql.format(
                                POSITIONS_TABLE,
                                param_dict["customer"],
                                param_dict["vehicle"], 
                                param_dict["position_reference_full"],
                                param_dict["todaywithtime"],
                                vehicle_motion,
                                payload_cause,
                                payload_diag["content"].get("threshold_motion"),
                                payload_diag["content"].get("threshold_unmotion"),
                                payload_diag["content"].get("threshold_hdop"),
                                power_source,
                                param_dict["odometro"],
                                param_dict["latitude"],
                                param_dict["longitude"],
                                param_dict["velocidade"],
                                param_dict["pos_memoria"],
                                param_dict["horimetro"],
                                param_dict["bloqueio"],
                                param_dict["satelital"],
                                param_dict["gps_valido"],
                                vcc_alim,
                                param_dict["bateria_int"],
                                perc_bat_calc,
                                param_dict["peso"],
                                gnss_heading,
                                gnss_altitude,
                                param_dict["ignicao"],
                                param_dict["bateria_ext"],
                                can_activity,
                                session_number,
                                owner_id,
                                param_dict["abs_active_towing_counter"],
                                param_dict["abs_active_towed_counter"],
                                param_dict["vhdr_vehicle_distance"],
                                treat_db_null(param_dict['truck_id_0']),
                                treat_db_null(param_dict['truck_id_1']),
                                treat_db_null(param_dict['truck_id_2']),
                                treat_db_null(param_dict['wsn_temperature_idx_0']),
                                treat_db_null(param_dict['wsn_battery_status_idx_0'], True),
                                treat_db_null(param_dict['wsn_temperature_idx_1']),
                                treat_db_null(param_dict['wsn_battery_status_idx_1'], True),
                                treat_db_null(param_dict['wsn_temperature_idx_2']),
                                treat_db_null(param_dict['wsn_battery_status_idx_2'], True),
                                treat_db_null(param_dict['wsn_temperature_idx_3']),
                                treat_db_null(param_dict['wsn_battery_status_idx_3'], True),
                                param_dict['yc_system_counter'],
                                param_dict['rop_system_counter'],
                                param_dict['vdc_active_towing_counter'],
                                param_dict['service_brake_counter'],
                                param_dict['amber_warning_signal_counter'],
                                param_dict['red_warning_signal_counter'],
                                param_dict['software_version'],
                                treat_db_null(param_dict['high_temperature_thr']),
                                treat_db_null(param_dict['low_pressure_alert_thr']),
                                treat_db_null(param_dict['nominal_pressure']),
                                treat_db_null(param_dict['device_serial']),
                                treat_db_null(param_dict['low_pressure_warning_thr']),
                                param_dict['device_position'],
                                param_dict["axle_load_sum_min"],
                                param_dict["axle_load_sum_max"],
                                treat_db_null(param_dict['truck_id_0_tag'], True),
                                treat_db_null(param_dict['truck_id_0_battery_status'], True),
                                treat_db_null(param_dict['truck_id_1_tag'], True),
                                treat_db_null(param_dict['truck_id_1_battery_status'], True),
                                treat_db_null(param_dict['truck_id_2_tag'], True),
                                treat_db_null(param_dict['truck_id_2_battery_status'], True),
                                param_dict['lateral_acc_max'],
                                param_dict['lateral_acc_min'],
                                param_dict['aceleracao_lateral'],
                                param_dict['frenagem_brusca'], 
                                param_dict['wheel_based_speed_towing_mean'],
                                param_dict['wheel_based_speed_towed_mean'],
                                param_dict['diff_towing_towed_pct'],
                                param_dict['vdc_active_towed_counter'],
                                param_dict['veh_pneumatic_supply_counter'],
                                param_dict['veh_electrical_supply_counter']

                )
            logging.debug(sql_params)


        except:
            raise

        if self.execute_sql(sql_params=sql_params.encode('UTF-8')):
            return True

    @staticmethod
    def calc_traillers_infractions(vdc_active_towed_counter, abs_active_towed_counter, service_brake_counter):

        aceleracao_lateral = 0
        frenagem_brusca = 0

        if vdc_active_towed_counter  > 0 : #mudança kss-1977
            aceleracao_lateral = 1

        if abs_active_towed_counter >= 2:
            frenagem_brusca = 1



        return aceleracao_lateral, frenagem_brusca

    @staticmethod
    def calc_diff_towing_towed_pct(towing_speed, towed_speed):
        if towing_speed == 0:
            result = 0
        else:
            try:
                result = ((towing_speed/towed_speed)-1)*100
            except:
                result = 0

        return result

    def extract_payload_diag(self, position):
        ### Get main data from json
        json_data = position.get("summaryDevice").get("jsonData")
        json_ebs = json_data.get("payload_ebs")
        ### Get the TPMS payload
        return (json_data, json_data.get("pyaload_diag"), json_ebs)        

    def process_message(self, json_data, payload_diag, payload_ebs, payload_wsn, payload_tpm):
                  
        software_version = json_data.get("software_version")
        #ebs payload treatment

        peso = 0
        abs_active_towing_counter = 0
        abs_active_towed_counter = 0
        vhdr_vehicle_distance = 0
        yc_system_counter = 0
        rop_system_counter = 0
        vdc_active_towing_counter = 0
        axle_load_sum_min = 0
        axle_load_sum_max = 0
        service_brake_counter = 0
        amber_warning_signal_counter = 0
        red_warning_signal_counter = 0
        vdc_active_towed_counter = 0
        lateral_acc_max = 0.0
        lateral_acc_min = 0.0
        aceleracao_lateral = 0
        frenagem_brusca = 0
        wheel_based_speed_towing_mean = 0.0
        wheel_based_speed_towed_mean = 0.0
        diff_towing_towed_pct = 0.0
        veh_pneumatic_supply_counter = 0
        veh_electrical_supply_counter = 0

        if payload_ebs:
            logging.debug(payload_ebs)
            try:
                peso = int(payload_ebs[0]['content'].get('axle_load_sum_mean'))
                abs_active_towing_counter = int(payload_ebs[0]['content'].get('abs_active_towing_counter'))
                abs_active_towed_counter = int(payload_ebs[0]['content'].get('abs_active_towed_counter'))
                yc_system_counter = int(payload_ebs[0]['content'].get('yc_system_counter'))
                rop_system_counter = int(payload_ebs[0]['content'].get('rop_system_counter'))
                vdc_active_towing_counter = int(payload_ebs[0]['content'].get('vdc_active_towing_counter'))
                service_brake_counter = int(payload_ebs[0]['content'].get('service_brake_counter'))
                amber_warning_signal_counter = int(payload_ebs[0]['content'].get('amber_warning_signal_counter'))
                red_warning_signal_counter = int(payload_ebs[0]['content'].get('red_warning_signal_counter'))
                axle_load_sum_min = int(payload_ebs[0]['content'].get('axle_load_sum_min'))
                axle_load_sum_max = int(payload_ebs[0]['content'].get('axle_load_sum_max'))
                vdc_active_towed_counter = int(payload_ebs[0]['content'].get('vdc_active_towed_counter'))
                lateral_acc_max = float(payload_ebs[0]['content'].get('lateral_acc_max'))
                lateral_acc_min = float(payload_ebs[0]['content'].get('lateral_acc_min'))
                veh_pneumatic_supply_counter = int(payload_ebs[0]['content'].get('veh_pneumatic_supply_counter'))
                veh_electrical_supply_counter = int(payload_ebs[0]['content'].get('veh_electrical_supply_counter'))
                aceleracao_lateral, frenagem_brusca = PosicaoCRCUProcess.calc_traillers_infractions(vdc_active_towed_counter,
                                                                                 abs_active_towed_counter,
                                                                                 service_brake_counter)
                wheel_based_speed_towing_mean = float(payload_ebs[0]['content'].get('wheel_based_speed_towing_mean'))
                wheel_based_speed_towed_mean  = float(payload_ebs[0]['content'].get('wheel_based_speed_towed_mean'))
                diff_towing_towed_pct = PosicaoCRCUProcess.calc_diff_towing_towed_pct(wheel_based_speed_towing_mean,
                                                                                      wheel_based_speed_towed_mean)
            except:
                peso = 0
                abs_active_towing_counter = 0
                abs_active_towed_counter = 0
                yc_system_counter = 0
                rop_system_counter = 0
                vdc_active_towing_counter = 0
                service_brake_counter = 0
                amber_warning_signal_counter = 0
                red_warning_signal_counter = 0
                axle_load_sum_min = 0
                axle_load_sum_max = 0
                wheel_based_speed_towing_mean = 0
                wheel_based_speed_towed_mean = 0
                veh_pneumatic_supply_counter = 0
                veh_electrical_supply_counter = 0

            try:
                vhdr_vehicle_distance = int(payload_ebs[0]['content'].get('vhdr_vehicle_distance'))
            except:
                vhdr_vehicle_distance = 0

        wsn_temperature_idx_0    = None
        wsn_battery_status_idx_0 = None
        wsn_temperature_idx_1    = None
        wsn_battery_status_idx_1 = None
        wsn_temperature_idx_2    = None
        wsn_battery_status_idx_2 = None
        wsn_temperature_idx_3    = None
        wsn_battery_status_idx_3 = None
        truck_id_0 = None
        truck_id_0_tag = None
        truck_id_0_battery_status = None
        truck_id_1 = None
        truck_id_1_tag = None
        truck_id_1_battery_status = None
        truck_id_2 = None
        truck_id_2_tag = None
        truck_id_2_battery_status = None


        if payload_wsn:

            wsn_temperature_idx_0    = payload_wsn[0]['content']['tes'][0]['temperature']
            wsn_battery_status_idx_0 = payload_wsn[0]['content']['tes'][0]['battery_status']
            wsn_temperature_idx_1    = payload_wsn[0]['content']['tes'][1]['temperature']
            wsn_battery_status_idx_1 = payload_wsn[0]['content']['tes'][1]['battery_status']
            wsn_temperature_idx_2    = payload_wsn[0]['content']['tes'][2]['temperature']
            wsn_battery_status_idx_2 = payload_wsn[0]['content']['tes'][2]['battery_status']
            wsn_temperature_idx_3    = payload_wsn[0]['content']['tes'][3]['temperature']
            wsn_battery_status_idx_3 = payload_wsn[0]['content']['tes'][3]['battery_status']
            truck_id_0 = payload_wsn[0]['content']['vid'][0]['id']
            truck_id_0_tag = payload_wsn[0]['content']['vid'][0]['tag']
            truck_id_0_battery_status = payload_wsn[0]['content']['vid'][0]['battery_status']
            truck_id_1 = payload_wsn[0]['content']['vid'][1]['id']
            truck_id_1_tag = payload_wsn[0]['content']['vid'][1]['tag']
            truck_id_1_battery_status = payload_wsn[0]['content']['vid'][1]['battery_status']
            truck_id_2 = payload_wsn[0]['content']['vid'][2]['id']
            truck_id_2_tag = payload_wsn[0]['content']['vid'][2]['tag']
            truck_id_2_battery_status = payload_wsn[0]['content']['vid'][2]['battery_status']

        high_temperature_thr = None
        low_pressure_alert_thr = None
        nominal_pressure = None
        device_serial = None
        low_pressure_warning_thr = None
        device_position = 0

        if payload_tpm:

            high_temperature_thr = payload_tpm[0]['content']['high_temperature_thr']
            low_pressure_alert_thr = payload_tpm[0]['content']['low_pressure_alert_thr']
            nominal_pressure = payload_tpm[0]['content']['nominal_pressure']
            device_serial = payload_tpm[0]['content']['device_serial']
            low_pressure_warning_thr = payload_tpm[0]['content']['low_pressure_warning_thr']
            device_position = payload_tpm[0]['content']['device_position']

                
        merge_count = 0
        if (payload_diag):
            logging.warning("Payload Diag Encontrado")
            self.prepare_sql()
            ### Get necessary fields
            customer = int(json_data["cliente"])
            vehicle = int(json_data["veiculo"])
            odometro = int(json_data["odometro"])
            latitude = float(json_data["latitude"])
            longitude = float(json_data["longitude"])
            velocidade = int(json_data["velocidade"])
            pos_memoria = json_data["pos_memoria"]
            horimetro = int(json_data['horimetro'])
            bloqueio = json_data['blockVehicle']
            gps_valido = json_data['gps_valido']
            bateria_ext = json_data['bateriaExt']
            bateria_int = int(json_data['bateriaInt'])
            ignicao = json_data['ignicao']

            ### Get position date in UTC time
            position_date = datetime.utcfromtimestamp(json_data["data_posicao"]).replace(tzinfo=timezone.utc)
                        
            ### Get today date in UTC
            today = datetime.utcnow().replace(tzinfo=timezone.utc)
                
            todaywithtime  = today.strftime("%Y-%m-%d %H:%M:%S")
            position_reference = position_date.strftime("%Y-%m-%d")
            position_reference_full = position_date.strftime("%Y-%m-%d %H:%M:%S")
    
            ### Distance (not calculated at this time)
            #distance = 0
            param_dict = {"vehicle": vehicle,
                          "customer": customer,
                          "position_reference": position_reference,
                          "todaywithtime": todaywithtime,
                          "position_reference_full": position_reference_full,
                          "velocidade": velocidade,
                          "latitude": latitude,
                          "longitude": longitude,
                          "odometro": odometro,
                          "pos_memoria": pos_memoria,
                          "horimetro": horimetro,
                          "bloqueio": bloqueio,
                          "gps_valido": gps_valido, 
                          "bateria_ext": bateria_ext,
                          "bateria_int": bateria_int,
                          "peso": peso,
                          "abs_active_towing_counter": abs_active_towing_counter,
                          "abs_active_towed_counter": abs_active_towed_counter,
                          "vhdr_vehicle_distance": vhdr_vehicle_distance,
                          "ignicao": ignicao,
                          "satelital": 0,
                          "wsn_temperature_idx_0": wsn_temperature_idx_0,
                          "wsn_battery_status_idx_0": wsn_battery_status_idx_0,
                          "wsn_temperature_idx_1": wsn_temperature_idx_1,
                          "wsn_battery_status_idx_1": wsn_battery_status_idx_1,
                          "wsn_temperature_idx_2": wsn_temperature_idx_2,
                          "wsn_battery_status_idx_2": wsn_battery_status_idx_2,
                          "wsn_temperature_idx_3": wsn_temperature_idx_3,
                          "wsn_battery_status_idx_3": wsn_battery_status_idx_3,
                          "truck_id_0": truck_id_0,
                          "truck_id_1": truck_id_1,
                          "truck_id_2": truck_id_2,
                          "yc_system_counter": yc_system_counter,
                          "rop_system_counter": rop_system_counter,
                          "vdc_active_towing_counter": vdc_active_towing_counter,
                          "service_brake_counter": service_brake_counter,
                          "amber_warning_signal_counter": amber_warning_signal_counter,
                          "red_warning_signal_counter": red_warning_signal_counter,
                          "software_version": software_version,
                          "high_temperature_thr": high_temperature_thr,
                          "low_pressure_alert_thr": low_pressure_alert_thr,
                          "nominal_pressure": nominal_pressure,
                          "device_serial": device_serial,
                          "low_pressure_warning_thr": low_pressure_warning_thr,
                          "device_position": device_position,
                          "axle_load_sum_min": axle_load_sum_min,
                          "axle_load_sum_max": axle_load_sum_max,
                          "truck_id_0_tag": truck_id_0_tag,
                          "truck_id_0_battery_status": truck_id_0_battery_status,
                          "truck_id_1_tag": truck_id_1_tag,
                          "truck_id_1_battery_status": truck_id_1_battery_status,
                          "truck_id_2_tag": truck_id_2_tag,
                          "truck_id_2_battery_status": truck_id_2_battery_status,
                          "lateral_acc_max": lateral_acc_max,
                          "lateral_acc_min": lateral_acc_min ,
                          "aceleracao_lateral": aceleracao_lateral,
                          "frenagem_brusca": frenagem_brusca,
                          "wheel_based_speed_towing_mean": wheel_based_speed_towing_mean,
                          "wheel_based_speed_towed_mean": wheel_based_speed_towed_mean,
                          "diff_towing_towed_pct": diff_towing_towed_pct,
                          "vdc_active_towed_counter": vdc_active_towed_counter,
                          "veh_pneumatic_supply_counter": veh_pneumatic_supply_counter,
                          "veh_electrical_supply_counter": veh_electrical_supply_counter
            }

            processed = self.process_diag(payload_diag=payload_diag, param_dict=param_dict)
            if processed:
                merge_count += 1
        if merge_count > 0:
            return(True, self.sql)
        else:
            return (False, None)

