import unittest
from datetime import datetime, timezone
from python_con_cc_posicao_crcu import PosicaoCRCUProcess

from python_con_cc_rep_temp_pres_d0 import TpmsD0Process
from pykafka.protocol.message import Message
import cx_Oracle
from pykafka.exceptions import ConsumerStoppedException

from unittest.mock import Mock

class MyOracleClass():

    def connect(self, *args):
        return MyConnection()

class MyConnection():

    def cursor(self):
        return MyCursor()

    def commit(self):
        return True

class MyCursor():

    def prepare(self, sql):
        return True

    def execute(self, sql):
        return True

class MyOracleClassFail():

    def connect(self, *args):
        return MyConnectionFail()

class MyConnectionFail():

    def cursor(self):
        return MyCursorFail()

class MyConnectionFailKafka():

    def cursor(self):
        return MyCursorFailKafka()

class MyCursorFail():

    def prepare(self, sql):
        return True

    def execute(self, sql):
        raise cx_Oracle.DatabaseError("ORA-12541: TNS:no listener")

class MyOracleClassFailKafka():

    def connect(self, *args):
        return MyConnectionFailKafka()

class MyCursorFailKafka():

    def prepare(self, sql):
        return True

    def execute(self, sql):
        raise ConsumerStoppedException("Consumer stoped error")
     

class MyKafkaProducer(object):

    def __init__(self, **configs):
        pass

    def send(self, topic, value):
        return Mock()


class MyKafkaConsumer():

    def __init__(self, topics, **configs):
        self.set_topic_content()

    def set_topic_content(self):

        today = datetime.utcnow().replace(tzinfo=timezone.utc)

        self.msg_valid_sem_tpm = Message(('''
  {
"logDateTimeTopic":[
{
"dateTime":"21/06/2021 14:09:06",
"topicName":"positions_central",
"applicationName":"positions-protobuf-to-json"
}],
 "summaryDevice":{
  "jsonData":{
  "bateriaExt":1,
  "payload_tpm":[
  ],
  "payload_reefer":[
  ],
  "velocidade":17,
  "payload_ebs":[
  ],
  "id_motorista":"0",
  "latitude":44.31312,
  "direcao":22,
  "horimetro":0,
  "temperatura_info":{
  "temperatura3":0,
  "temperatura2":0,
  "temperatura1":0,
  "temperatura4":0
  },
  "ibuttonHex":"0",
  "protocolo":83,
  "gps_valido":"1",
  "bateriaInt":4,
  "entradas":[
  {
  "id":0,
  "ativa":false,
  "porta":0
  },
  {
  "id":0,
  "ativa":false,
  "porta":1
  },
  {
  "id":0,
  "ativa":false,
  "porta":2
  },
  {
  "id":0,
  "ativa":false,
  "porta":3
  },
  {
  "id":0,
  "ativa":false,
  "porta":4
  },
  {
  "id":0,
  "ativa":false,
  "porta":5
  },
  {
  "id":0,
  "ativa":false,
  "porta":6
  },
  {
  "id":0,
  "ativa":false,
  "porta":7
  }
  ],
  "blockVehicle":0,
  "software_version":"1096824868",
  "pyaload_diag":{
  "header":{
  "session_number":1624284544,
  "payload_type":1,
  "message_date_time":{
  "date_time":"2021-06-21 14:08:08",
  "epoch":1624284488
  },
  "payload_version":1,
  "message_arrival_date":{
  "date_time":"2021-06-21 14:09:06",
  "epoch":1624284546
  },
  "owner_id":2514084271,
  "payload_cause":4,
  "generation_number":107,
  "sender_id":2514084271,
  "payload_number":1
  },
  "content":{
  "device_temperature":37,
  "gnss_speed":17,
  "gnss_vdop":1.2000000000000002,
  "battery_voltage":4.05,
  "vehicle_voltage":28.2,
  "can_activity":2,
  "can_load":1,
  "scheduled_period":120,
  "gnss_hdop":0.8,
  "battery_charge_level":5,
  "vehicle_motion":2,
  "battery_temperature":-50,
  "gnss_distance":15003,
  "battery_charge_state":3,
  "gnss_latitude":44.31312,
  "gnss_longitude":8.468335999999994,
  "gnss_altitude":95,
  "gnss_date_time":{
  "date_time":"2021-06-21 14:08:07",
  "epoch":1624284487
  },
  "gnss_heading":22,
  "threshold_motion":15,
  "threshold_unmotion":10,
  "power_source":true,
  "threshold_hdop":10
  }
  },
  "posicao_satelital":0,
  "saidas":[
  {
  "id":0,
  "ativa":false,
  "porta":0
  },
  {
  "id":0,
  "ativa":false,
  "porta":1
  },
  {
  "id":0,
  "ativa":false,
  "porta":2
  },
  {
  "id":0,
  "ativa":false,
  "porta":3
  },
  {
  "id":0,
  "ativa":false,
  "porta":4
  },
  {
  "id":0,
  "ativa":false,
  "porta":5
  },
  {
  "id":0,
  "ativa":false,
  "porta":6
  },
  {
  "id":0,
  "ativa":false,
  "porta":7
  }
  ],
  "longitude":8.468335999999994,
  "veiculo":1434897,
  "payload_wsn":[
  ],
  "ibuttonPart1":"0",
  "ibuttonPart2":"0",
  "ignicao":"0",
  "pos_memoria":"0",
  "cliente":413662,
  "protocol_version":"1",
  "odometro":150,
  "telemetria":{
  "embreagem_acionada":false,
  "motor_acionado":false,
  "freio_acionado":false,
  "limpador_acionado":false
  },
  "panico_acionado":false,
  "classeEq":181,
      "data_posicao": %s
      }
    }
  }''' % today.timestamp()) )

        self.msg_valid = Message(('''{
  "summaryDevice": {
    "jsonData": {
      "bateriaExt": 1,
      "payload_tpm": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 2,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "payload_cause": 1,
            "device_type": "CRCU",
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "nominal_pressure": 900360,
            "low_pressure_alert_thr": 60,
            "device_serial": 333078537,
            "sensors": [
              {
                "com": 2,
                "  alert_pressure": 0,
                "mode": 1,
                "identifier": 481839967,
                "alert_battery": true,
                "alert_temp": false,
                "temperature": 29,
                "index": 0,
                "conf": 3,
                "pressure": 955260,
                "status": "activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 1,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 2,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840354,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": 27,
                "index": 3,
                "conf": 3,
                "pressure": 900360,
                "status": "activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840226,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": 28,
                "index": 4,
                "conf": 3,
                "pressure": 878400,
                "status": "activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 5,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 6,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840218,
                "alert_battery": true,
                "alert_temp": false,
                "temperature": 29,
                "index": 7,
                "conf": 3,
                "pressure": 938790,
                "status": "activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840299,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": 30,
                "index": 8,
                "conf": 3,
                "pressure": 993690,
                "status": "activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 9,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 10,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840223,
                "alert_battery": true,
                "alert_temp": false,
                "temperature": 26,
                "index": 11,
                "conf": 3,
                "pressure": 905850,
                "status": "activated"
              }
            ],
            "low_pressure_warning_thr": 80,
            "device_position": 2,
            "high_temperature_thr": 100,
            "system_pairing": 1
          }
        }
      ],
      "payload_reefer": [],
      "velocidade": 87,
      "protocolo": 83,
      "payload_ebs": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 3,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "payload_cause": 1,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "brake_lining_counter": 1,
            "brake_lining_min": 102,
            "wheel_based_speed_towing_mean": 85.9921875,
            "axle_load_sum_mean": 19834,
            "rop_system_counter": 1,
            "supply_line_braking_counter": 0,
            "auto_towed_VEH_brake_counter": 0,
            "service_brake_demand_min": 0,
            "brake_temperature_mean": 0,
            "brake_temperature_status_counter": 1,
            "brake_lining_mean": 0,
            "abs_off_road_counter": 0,
            "abs_active_towing_counter": 0,
            "amber_warning_signal_counter": 0,
            "brake_temperature_max": 0,
            "axle_load_sum_min": 19574,
            "22_msg_counter": 1199,
            "11_msg_counter": 11994,
            "yc_system_counter": 1,
            "21_msg_counter": 11991,
            "service_brake_demand_max": 0,
            "wheel_based_speed_towing_min": 76.79296875,
            "vdc_active_towing_counter": 0,
            "12_msg_counter": 1199,
            "23_msg_counter": 1199,
            "wheel_based_speed_towed_min": 74.96875,
            "veh_pneumatic_supply_counter": 1,
            "lateral_acc_max": 0.9000000000000004,
            "brake_temperature_min": 2550,
            "loading_ramp_counter": 0,
            "axle_load_sum_max": 20250,
            "red_warning_signal_counter": 0,
            "wheel_based_speed_towing_max": 90.99609375,
            "lateral_acc_mean": 0.20000000000000107,
            "brake_lining_max": 0,
            "vdc_active_towed_counter": 0,
            "brake_light_counter": 0,
            "service_brake_demand_mean": 0,
            "wheel_based_speed_towed_max": 89.0859375,
            "service_brake_counter": 0,
            "lateral_acc_min": -0.09999999999999964,
            "veh_electrical_supply_counter": 1,
            "abs_active_towed_counter": 0,
            "wheel_based_speed_towed_mean": 84.29296875
          }
        }
      ],
      "id_motorista": "0",
      "latitude": 46.179776000000004,
      "horimetro": 0,
      "temperatura_info": {
        "temperatura3": 0,
        "temperatura2": 0,
        "temperatura1": 0,
        "temperatura4": 0
      },
      "ibuttonHex": "0",
      "gps_valido": "1",
      "bateriaInt": -1,
      "entradas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "blockVehicle": 0,
      "software_version": "1093747492",
      "pyaload_diag": {
        "header": {
          "session_number": 1579885223,
          "payload_type": 1,
          "message_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "payload_version": 1,
          "message_arrival_date": {
            "date_time": "2020-01-24 17:00:27",
            "epoch": 1579885227
          },
          "owner_id": 333078537,
          "payload_cause": 1,
          "generation_number": 56,
          "sender_id": 333078537,
          "payload_number": 4
        },
        "content": {
          "device_temperature": 19,
          "gnss_speed": 87,
          "gnss_vdop": 1.1,
          "battery_voltage": 0,
          "vehicle_voltage": 28.349999999999998,
          "can_activity": 2,
          "can_load": 1,
          "scheduled_period": 120,
          "gnss_hdop": 0.7000000000000001,
          "battery_charge_level": 0,
          "vehicle_motion": 2,
          "battery_temperature": -50,
          "gnss_distance": 14088,
          "battery_charge_state": 3,
          "gnss_latitude": 46.179776000000004,
          "gnss_longitude": 3.251903999999996,
          "gnss_altitude": 372,
          "gnss_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "gnss_heading": 214,
          "threshold_motion": 15,
          "threshold_unmotion": 10,
          "power_source": true,
          "threshold_hdop": 10
        }
      },
      "posicao_satelital": 0,
      "saidas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "longitude": 3.251903999999996,
      "veiculo": 1212769,
      "payload_wsn": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 4,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "vid": [
              {
                "index": 0,
                "id": 2167101684,
                "tag": "ED840RN",
                "status": "active",
                "battery_status": 1
              },
              {
                "index": 1,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              },
              {
                "index": 2,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              }
            ],
            "tes": [
              {
                "temperature": -50,
                "index": 0,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 1,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 2,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 3,
                "battery_status": 0
              }
            ],
            "dos": [
              {
                "index": 0,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              },
              {
                "index": 1,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              }
            ]
          }
        }
      ],
      "ibuttonPart1": "0",
      "ibuttonPart2": "0",
      "ignicao": "0",
      "pos_memoria": "0",
      "cliente": 394342,
      "protocol_version": "1",
      "odometro": 140,
      "telemetria": {
        "embreagem_acionada": false,
        "motor_acionado": false,
        "freio_acionado": false,
        "limpador_acionado": false
      },
      "panico_acionado": false,
      "classeEq": 0,
      "data_posicao": %s
    }
  }
}''' % today.timestamp()) )

        self.msg_invalid = Message('''
{
  "summaryDevice": {
    "jsonData": {
      "bateriaExt": 1,
      "payload_reefer": [],
      "velocidade": 87,
      "payload_ebs": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 3,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "payload_cause": 1,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "brake_lining_counter": 1,
            "brake_lining_min": 102,
            "wheel_based_speed_towing_mean": 85.9921875,
            "axle_load_sum_mean": 19834,
            "rop_system_counter": 1,
            "supply_line_braking_counter": 0,
            "auto_towed_VEH_brake_counter": 0,
            "service_brake_demand_min": 0,
            "brake_temperature_mean": 0,
            "brake_temperature_status_counter": 1,
            "brake_lining_mean": 0,
            "abs_off_road_counter": 0,
            "abs_active_towing_counter": 0,
            "amber_warning_signal_counter": 0,
            "brake_temperature_max": 0,
            "axle_load_sum_min": 19574,
            "22_msg_counter": 1199,
            "11_msg_counter": 11994,
            "yc_system_counter": 1,
            "21_msg_counter": 11991,
            "service_brake_demand_max": 0,
            "wheel_based_speed_towing_min": 76.79296875,
            "vdc_active_towing_counter": 0,
            "12_msg_counter": 1199,
            "23_msg_counter": 1199,
            "wheel_based_speed_towed_min": 74.96875,
            "veh_pneumatic_supply_counter": 1,
            "lateral_acc_max": 0.9000000000000004,
            "brake_temperature_min": 2550,
            "loading_ramp_counter": 0,
            "axle_load_sum_max": 20250,
            "red_warning_signal_counter": 0,
            "wheel_based_speed_towing_max": 90.99609375,
            "lateral_acc_mean": 0.20000000000000107,
            "brake_lining_max": 0,
            "vdc_active_towed_counter": 0,
            "brake_light_counter": 0,
            "service_brake_demand_mean": 0,
            "wheel_based_speed_towed_max": 89.0859375,
            "service_brake_counter": 0,
            "lateral_acc_min": -0.09999999999999964,
            "veh_electrical_supply_counter": 1,
            "abs_active_towed_counter": 0,
            "wheel_based_speed_towed_mean": 84.29296875
          }
        }
      ],
      "id_motorista": "0",
      "latitude": 46.179776000000004,
      "horimetro": 0,
      "temperatura_info": {
        "temperatura3": 0,
        "temperatura2": 0,
        "temperatura1": 0,
        "temperatura4": 0
      },
      "ibuttonHex": "0",
      "gps_valido": "1",
      "bateriaInt": -1,
      "entradas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "blockVehicle": 0,
      "software_version": "1093747492",
      "pyaload_diag": {
        "header": {
          "session_number": 1579885223,
          "payload_type": 1,
          "message_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "payload_version": 1,
          "message_arrival_date": {
            "date_time": "2020-01-24 17:00:27",
            "epoch": 1579885227
          },
          "owner_id": 333078537,
          "payload_cause": 1,
          "generation_number": 56,
          "sender_id": 333078537,
          "payload_number": 4
        },
        "content": {
          "device_temperature": 19,
          "gnss_speed": 87,
          "gnss_vdop": 1.1,
          "battery_voltage": 0,
          "vehicle_voltage": 28.349999999999998,
          "can_activity": 2,
          "can_load": 1,
          "scheduled_period": 120,
          "gnss_hdop": 0.7000000000000001,
          "battery_charge_level": 0,
          "vehicle_motion": 2,
          "battery_temperature": -50,
          "gnss_distance": 14088,
          "battery_charge_state": 3,
          "gnss_latitude": 46.179776000000004,
          "gnss_longitude": 3.251903999999996,
          "gnss_altitude": 372,
          "gnss_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "gnss_heading": 214,
          "threshold_motion": 15,
          "threshold_unmotion": 10,
          "power_source": true,
          "threshold_hdop": 10
        }
      },
      "posicao_satelital": 0,
      "saidas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "longitude": 3.251903999999996,
      "veiculo": 1212769,
      "protocolo": 83,
      "payload_wsn": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 4,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "vid": [
              {
                "index": 0,
                "id": 2167101684,
                "tag": "ED840RN",
                "status": "active",
                "battery_status": 1
              },
              {
                "index": 1,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              },
              {
                "index": 2,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              }
            ],
            "tes": [
              {
                "temperature": -50,
                "index": 0,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 1,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 2,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 3,
                "battery_status": 0
              }
            ],
            "dos": [
              {
                "index": 0,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              },
              {
                "index": 1,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              }
            ]
          }
        }
      ],
      "ibuttonPart1": "0",
      "ibuttonPart2": "0",
      "ignicao": "0",
      "pos_memoria": "0",
      "cliente": 394342,
      "protocol_version": "1",
      "odometro": 140,
      "telemetria": {
        "embreagem_acionada": false,
        "motor_acionado": false,
        "freio_acionado": false,
        "limpador_acionado": false
      },
      "panico_acionado": false,
      "classeEq": 0,
      "data_posicao": 1579885217
    }
  }
}            
                                   ''')

        list_valid = MyList([self.msg_valid])
        list_invalid = MyList([self.msg_invalid])
        list_valid_sem_tpm = MyList([self.msg_valid_sem_tpm])

        self.topics = {"Valid": MyTopic(list_valid), 
                       "Invalid": MyTopic(list_invalid),
                       "ValidSemTpm": MyTopic(list_valid_sem_tpm)}
        
#        self.topics = {"Valid": MyTopic([self.msg_valid]), 
#                       "Invalid": MyTopic([self.msg_invalid])}

class MyList(list):

    def __init__(self, liste):
        list.__init__(self,liste)  

    def commit(self):
        return True        

    def commit(self):
        return True

class MyKafkaConsumerNone():

    def __init__(self, topics, **configs):
        self.set_topic_content()

    def set_topic_content(self):
        today = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.msg_valid_sem_tpm = Message(('''
  "summaryDevice":{
  "jsonData":{
  "bateriaExt":1,
  "payload_tpm":[
  ],
  "payload_reefer":[
  ],
  "velocidade":17,
  "payload_ebs":[
  ],
  "id_motorista":"0",
  "latitude":44.31312,
  "direcao":22,
  "horimetro":0,
  "temperatura_info":{
  "temperatura3":0,
  "temperatura2":0,
  "temperatura1":0,
  "temperatura4":0
  },
  "ibuttonHex":"0",
  "protocolo":83,
  "gps_valido":"1",
  "bateriaInt":4,
  "entradas":[
  {
  "id":0,
  "ativa":false,
  "porta":0
  },
  {
  "id":0,
  "ativa":false,
  "porta":1
  },
  {
  "id":0,
  "ativa":false,
  "porta":2
  },
  {
  "id":0,
  "ativa":false,
  "porta":3
  },
  {
  "id":0,
  "ativa":false,
  "porta":4
  },
  {
  "id":0,
  "ativa":false,
  "porta":5
  },
  {
  "id":0,
  "ativa":false,
  "porta":6
  },
  {
  "id":0,
  "ativa":false,
  "porta":7
  }
  ],
  "blockVehicle":0,
  "software_version":"1096824868",
  "pyaload_diag":{
  "header":{
  "session_number":1624284544,
  "payload_type":1,
  "message_date_time":{
  "date_time":"2021-06-21 14:08:08",
  "epoch":1624284488
  },
  "payload_version":1,
  "message_arrival_date":{
  "date_time":"2021-06-21 14:09:06",
  "epoch":1624284546
  },
  "owner_id":2514084271,
  "payload_cause":4,
  "generation_number":107,
  "sender_id":2514084271,
  "payload_number":1
  },
  "content":{
  "device_temperature":37,
  "gnss_speed":17,
  "gnss_vdop":1.2000000000000002,
  "battery_voltage":4.05,
  "vehicle_voltage":28.2,
  "can_activity":2,
  "can_load":1,
  "scheduled_period":120,
  "gnss_hdop":0.8,
  "battery_charge_level":5,
  "vehicle_motion":2,
  "battery_temperature":-50,
  "gnss_distance":15003,
  "battery_charge_state":3,
  "gnss_latitude":44.31312,
  "gnss_longitude":8.468335999999994,
  "gnss_altitude":95,
  "gnss_date_time":{
  "date_time":"2021-06-21 14:08:07",
  "epoch":1624284487
  },
  "gnss_heading":22,
  "threshold_motion":15,
  "threshold_unmotion":10,
  "power_source":true,
  "threshold_hdop":10
  }
  },
  "posicao_satelital":0,
  "saidas":[
  {
  "id":0,
  "ativa":false,
  "porta":0
  },
  {
  "id":0,
  "ativa":false,
  "porta":1
  },
  {
  "id":0,
  "ativa":false,
  "porta":2
  },
  {
  "id":0,
  "ativa":false,
  "porta":3
  },
  {
  "id":0,
  "ativa":false,
  "porta":4
  },
  {
  "id":0,
  "ativa":false,
  "porta":5
  },
  {
  "id":0,
  "ativa":false,
  "porta":6
  },
  {
  "id":0,
  "ativa":false,
  "porta":7
  }
  ],
  "longitude":8.468335999999994,
  "veiculo":1434897,
  "payload_wsn":[
  ],
  "ibuttonPart1":"0",
  "ibuttonPart2":"0",
  "ignicao":"0",
  "pos_memoria":"0",
  "cliente":413662,
  "protocol_version":"1",
  "odometro":150,
  "telemetria":{
  "embreagem_acionada":false,
  "motor_acionado":false,
  "freio_acionado":false,
  "limpador_acionado":false
  },
  "panico_acionado":false,
  "classeEq":181,
      "data_posicao": %s
      }
    }
  }''' % today.timestamp()) )

        self.msg_valid = Message(('''{
  "summaryDevice": {
    "jsonData": {
      "bateriaExt": 1,
      "payload_tpm": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 2,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "payload_cause": 1,
            "device_type": "CRCU",
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "nominal_pressure": 900360,
            "low_pressure_alert_thr": 60,
            "device_serial": 333078537,
            "sensors": [
              {
                "com": 2,
                "  alert_pressure": 0,
                "mode": 1,
                "identifier": 481839967,
                "alert_battery": true,
                "alert_temp": false,
                "temperature": 29,
                "index": 0,
                "conf": 3,
                "pressure": 955260,
                "status": "activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 1,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 2,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840354,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": 27,
                "index": 3,
                "conf": 3,
                "pressure": 900360,
                "status": "activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840226,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": 28,
                "index": 4,
                "conf": 3,
                "pressure": 878400,
                "status": "activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 5,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 6,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840218,
                "alert_battery": true,
                "alert_temp": false,
                "temperature": 29,
                "index": 7,
                "conf": 3,
                "pressure": 938790,
                "status": "activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840299,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": 30,
                "index": 8,
                "conf": 3,
                "pressure": 993690,
                "status": "activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 9,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 0,
                "alert_pressure": 0,
                "mode": 0,
                "identifier": 0,
                "alert_battery": false,
                "alert_temp": false,
                "temperature": -50,
                "index": 10,
                "conf": 0,
                "pressure": 0,
                "status": "not_activated"
              },
              {
                "com": 2,
                "alert_pressure": 0,
                "mode": 1,
                "identifier": 481840223,
                "alert_battery": true,
                "alert_temp": false,
                "temperature": 26,
                "index": 11,
                "conf": 3,
                "pressure": 905850,
                "status": "activated"
              }
            ],
            "low_pressure_warning_thr": 80,
            "device_position": 2,
            "high_temperature_thr": 100,
            "system_pairing": 1
          }
        }
      ],
      "payload_reefer": [],
      "velocidade": 87,
      "payload_ebs": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 3,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "payload_cause": 1,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "brake_lining_counter": 1,
            "brake_lining_min": 102,
            "wheel_based_speed_towing_mean": 85.9921875,
            "axle_load_sum_mean": 19834,
            "rop_system_counter": 1,
            "supply_line_braking_counter": 0,
            "auto_towed_VEH_brake_counter": 0,
            "service_brake_demand_min": 0,
            "brake_temperature_mean": 0,
            "brake_temperature_status_counter": 1,
            "brake_lining_mean": 0,
            "abs_off_road_counter": 0,
            "abs_active_towing_counter": 0,
            "amber_warning_signal_counter": 0,
            "brake_temperature_max": 0,
            "axle_load_sum_min": 19574,
            "22_msg_counter": 1199,
            "11_msg_counter": 11994,
            "yc_system_counter": 1,
            "21_msg_counter": 11991,
            "service_brake_demand_max": 0,
            "wheel_based_speed_towing_min": 76.79296875,
            "vdc_active_towing_counter": 0,
            "12_msg_counter": 1199,
            "23_msg_counter": 1199,
            "wheel_based_speed_towed_min": 74.96875,
            "veh_pneumatic_supply_counter": 1,
            "lateral_acc_max": 0.9000000000000004,
            "brake_temperature_min": 2550,
            "loading_ramp_counter": 0,
            "axle_load_sum_max": 20250,
            "red_warning_signal_counter": 0,
            "wheel_based_speed_towing_max": 90.99609375,
            "lateral_acc_mean": 0.20000000000000107,
            "brake_lining_max": 0,
            "vdc_active_towed_counter": 0,
            "brake_light_counter": 0,
            "service_brake_demand_mean": 0,
            "wheel_based_speed_towed_max": 89.0859375,
            "service_brake_counter": 0,
            "lateral_acc_min": -0.09999999999999964,
            "veh_electrical_supply_counter": 1,
            "abs_active_towed_counter": 0,
            "wheel_based_speed_towed_mean": 84.29296875
          }
        }
      ],
      "id_motorista": "0",
      "latitude": 46.179776000000004,
      "horimetro": 0,
      "temperatura_info": {
        "temperatura3": 0,
        "temperatura2": 0,
        "temperatura1": 0,
        "temperatura4": 0
      },
      "ibuttonHex": "0",
      "gps_valido": "1",
      "bateriaInt": -1,
      "entradas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "blockVehicle": 0,
      "software_version": "1093747492",
      "pyaload_diag": {
        "header": {
          "session_number": 1579885223,
          "payload_type": 1,
          "message_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "payload_version": 1,
          "message_arrival_date": {
            "date_time": "2020-01-24 17:00:27",
            "epoch": 1579885227
          },
          "owner_id": 333078537,
          "payload_cause": 1,
          "generation_number": 56,
          "sender_id": 333078537,
          "payload_number": 4
        },
        "content": {
          "device_temperature": 19,
          "gnss_speed": 87,
          "gnss_vdop": 1.1,
          "battery_voltage": 0,
          "vehicle_voltage": 28.349999999999998,
          "can_activity": 2,
          "can_load": 1,
          "scheduled_period": 120,
          "gnss_hdop": 0.7000000000000001,
          "battery_charge_level": 0,
          "vehicle_motion": 2,
          "battery_temperature": -50,
          "gnss_distance": 14088,
          "battery_charge_state": 3,
          "gnss_latitude": 46.179776000000004,
          "gnss_longitude": 3.251903999999996,
          "gnss_altitude": 372,
          "gnss_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "gnss_heading": 214,
          "threshold_motion": 15,
          "threshold_unmotion": 10,
          "power_source": true,
          "threshold_hdop": 10
        }
      },
      "posicao_satelital": 0,
      "saidas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "longitude": 3.251903999999996,
      "veiculo": 1212769,
      "payload_wsn": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 4,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "vid": [
              {
                "index": 0,
                "id": 2167101684,
                "tag": "ED840RN",
                "status": "active",
                "battery_status": 1
              },
              {
                "index": 1,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              },
              {
                "index": 2,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              }
            ],
            "tes": [
              {
                "temperature": -50,
                "index": 0,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 1,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 2,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 3,
                "battery_status": 0
              }
            ],
            "dos": [
              {
                "index": 0,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              },
              {
                "index": 1,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              }
            ]
          }
        }
      ],
      "ibuttonPart1": "0",
      "ibuttonPart2": "0",
      "ignicao": "0",
      "pos_memoria": "0",
      "cliente": 394342,
      "protocol_version": "1",
      "odometro": 140,
      "telemetria": {
        "embreagem_acionada": false,
        "motor_acionado": false,
        "freio_acionado": false,
        "limpador_acionado": false
      },
      "panico_acionado": false,
      "classeEq": 0,
      "data_posicao": %s
    }
  }
}''' % today.timestamp()) )

        self.msg_invalid = Message('''
{
  "summaryDevice": {
    "jsonData": {
      "bateriaExt": 1,
      "payload_reefer": [],
      "velocidade": 87,
      "payload_ebs": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 3,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "payload_cause": 1,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "brake_lining_counter": 1,
            "brake_lining_min": 102,
            "wheel_based_speed_towing_mean": 85.9921875,
            "axle_load_sum_mean": 19834,
            "rop_system_counter": 1,
            "supply_line_braking_counter": 0,
            "auto_towed_VEH_brake_counter": 0,
            "service_brake_demand_min": 0,
            "brake_temperature_mean": 0,
            "brake_temperature_status_counter": 1,
            "brake_lining_mean": 0,
            "abs_off_road_counter": 0,
            "abs_active_towing_counter": 0,
            "amber_warning_signal_counter": 0,
            "brake_temperature_max": 0,
            "axle_load_sum_min": 19574,
            "22_msg_counter": 1199,
            "11_msg_counter": 11994,
            "yc_system_counter": 1,
            "21_msg_counter": 11991,
            "service_brake_demand_max": 0,
            "wheel_based_speed_towing_min": 76.79296875,
            "vdc_active_towing_counter": 0,
            "12_msg_counter": 1199,
            "23_msg_counter": 1199,
            "wheel_based_speed_towed_min": 74.96875,
            "veh_pneumatic_supply_counter": 1,
            "lateral_acc_max": 0.9000000000000004,
            "brake_temperature_min": 2550,
            "loading_ramp_counter": 0,
            "axle_load_sum_max": 20250,
            "red_warning_signal_counter": 0,
            "wheel_based_speed_towing_max": 90.99609375,
            "lateral_acc_mean": 0.20000000000000107,
            "brake_lining_max": 0,
            "vdc_active_towed_counter": 0,
            "brake_light_counter": 0,
            "service_brake_demand_mean": 0,
            "wheel_based_speed_towed_max": 89.0859375,
            "service_brake_counter": 0,
            "lateral_acc_min": -0.09999999999999964,
            "veh_electrical_supply_counter": 1,
            "abs_active_towed_counter": 0,
            "wheel_based_speed_towed_mean": 84.29296875
          }
        }
      ],
      "id_motorista": "0",
      "latitude": 46.179776000000004,
      "horimetro": 0,
      "temperatura_info": {
        "temperatura3": 0,
        "temperatura2": 0,
        "temperatura1": 0,
        "temperatura4": 0
      },
      "ibuttonHex": "0",
      "gps_valido": "1",
      "bateriaInt": -1,
      "entradas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "blockVehicle": 0,
      "software_version": "1093747492",
      "pyaload_diag": {
        "header": {
          "session_number": 1579885223,
          "payload_type": 1,
          "message_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "payload_version": 1,
          "message_arrival_date": {
            "date_time": "2020-01-24 17:00:27",
            "epoch": 1579885227
          },
          "owner_id": 333078537,
          "payload_cause": 1,
          "generation_number": 56,
          "sender_id": 333078537,
          "payload_number": 4
        },
        "content": {
          "device_temperature": 19,
          "gnss_speed": 87,
          "gnss_vdop": 1.1,
          "battery_voltage": 0,
          "vehicle_voltage": 28.349999999999998,
          "can_activity": 2,
          "can_load": 1,
          "scheduled_period": 120,
          "gnss_hdop": 0.7000000000000001,
          "battery_charge_level": 0,
          "vehicle_motion": 2,
          "battery_temperature": -50,
          "gnss_distance": 14088,
          "battery_charge_state": 3,
          "gnss_latitude": 46.179776000000004,
          "gnss_longitude": 3.251903999999996,
          "gnss_altitude": 372,
          "gnss_date_time": {
            "date_time": "2020-01-24 17:00:17",
            "epoch": 1579885217
          },
          "gnss_heading": 214,
          "threshold_motion": 15,
          "threshold_unmotion": 10,
          "power_source": true,
          "threshold_hdop": 10
        }
      },
      "posicao_satelital": 0,
      "saidas": [
        {
          "id": 0,
          "ativa": false,
          "porta": 0
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 1
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 2
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 3
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 4
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 5
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 6
        },
        {
          "id": 0,
          "ativa": false,
          "porta": 7
        }
      ],
      "longitude": 3.251903999999996,
      "veiculo": 1212769,
      "protocolo": 83,
      "payload_wsn": [
        {
          "header": {
            "session_number": 1579885223,
            "payload_type": 4,
            "message_date_time": {
              "date_time": "2020-01-24 17:00:17",
              "epoch": 1579885217
            },
            "payload_version": 2,
            "message_arrival_date": {
              "date_time": "2020-01-24 17:00:27",
              "epoch": 1579885227
            },
            "owner_id": 333078537,
            "generation_number": 56,
            "sender_id": 333078537,
            "payload_number": 4
          },
          "content": {
            "vid": [
              {
                "index": 0,
                "id": 2167101684,
                "tag": "ED840RN",
                "status": "active",
                "battery_status": 1
              },
              {
                "index": 1,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              },
              {
                "index": 2,
                "id": 0,
                "tag": "",
                "status": "inactive",
                "battery_status": 0
              }
            ],
            "tes": [
              {
                "temperature": -50,
                "index": 0,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 1,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 2,
                "battery_status": 0
              },
              {
                "temperature": -50,
                "index": 3,
                "battery_status": 0
              }
            ],
            "dos": [
              {
                "index": 0,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              },
              {
                "index": 1,
                "counter": 255,
                "status": 0,
                "battery_status": 0
              }
            ]
          }
        }
      ],
      "ibuttonPart1": "0",
      "ibuttonPart2": "0",
      "ignicao": "0",
      "pos_memoria": "0",
      "cliente": 394342,
      "protocol_version": "1",
      "odometro": 140,
      "telemetria": {
        "embreagem_acionada": false,
        "motor_acionado": false,
        "freio_acionado": false,
        "limpador_acionado": false
      },
      "panico_acionado": false,
      "classeEq": 0,
      "data_posicao": 1579885217
    }
  }
}            
                                   ''')
        list_valid = MyList([self.msg_valid])
        list_invalid = MyList([self.msg_invalid])
        list_valid_sem_tpm = MyList([self.msg_valid_sem_tpm])
        
        self.topics = {"Valid": MyTopicNone(list_valid), 
                       "Invalid": MyTopicNone(list_invalid),
                       "ValidSemTpm": MyTopicNone(list_valid_sem_tpm)}

class MyTopic():

    def __init__(self, msg_list):
        self.msg_list = msg_list

    def get_balanced_consumer(self,
                              zookeeper_connect,
                              auto_offset_reset,
                              auto_commit_enable,
                              consumer_group,
                              use_rdkafka
                             ):
        print(type(self.msg_list))
        return self.msg_list

    def stop(self):
      return True

    def commit(self):
        return True


class MyTopicNone():

    def __init__(self, msg_list):
        self.msg_list = msg_list

    def get_balanced_consumer(self,
                              zookeeper_connect,
                              auto_offset_reset,
                              auto_commit_enable,
                              consumer_group,
                              use_rdkafka
                             ):
        print(type(self.msg_list))
        return None

class TestProcessMessage(unittest.TestCase):

    def setUp(self):
        self.con = MyConnection()
        self.sql = '''
              BEGIN
              MERGE INTO FT_CC_TEMP_PRES_PNEU_AGREGADO agg_t1 
                  USING (
                      select  
                            to_date('{0}','yyyy-mm-dd') DAT_REFERENCIA, 
                             {1} ID_VEICULO, 
                             {2} ID_CLIENTE,
                             {3} PRESSAO_MIN,  
                             {4} TEMPERATURA_MAX, 
                             {5} DISTANCIA,
                             to_date('{6}','yyyy-mm-dd hh24:mi:ss') DATAHORA_TEMP_PRES_PNEU_AGG 
                      from DUAL
                  ) agg_t2 ON (agg_t1.ID_VEICULO = agg_t2.ID_VEICULO AND agg_t1.DAT_REFERENCIA = agg_t2.DAT_REFERENCIA)
                WHEN MATCHED THEN
                  UPDATE 
                      SET agg_t1.TEMPERATURA_MAX = CASE WHEN agg_t2.TEMPERATURA_MAX > agg_t1.TEMPERATURA_MAX THEN agg_t2.TEMPERATURA_MAX ELSE agg_t1.TEMPERATURA_MAX END,
                          agg_t1.PRESSAO_MIN = CASE WHEN agg_t2.PRESSAO_MIN < agg_t1.PRESSAO_MIN THEN agg_t2.PRESSAO_MIN ELSE agg_t1.PRESSAO_MIN END
                  WHERE agg_t1.ID_VEICULO = agg_t2.id_veiculo 
                      AND agg_t1.DAT_REFERENCIA = agg_t2.DAT_REFERENCIA
                      AND (agg_t1.PRESSAO_MIN > agg_t2.PRESSAO_MIN OR agg_t1.TEMPERATURA_MAX < agg_t2.TEMPERATURA_MAX)
                WHEN NOT MATCHED THEN
                  INSERT (ID_CLIENTE, ID_VEICULO, DAT_REFERENCIA, TEMPERATURA_MAX, PRESSAO_MIN, DISTANCIA, DATAHORA_TEMP_PRES_PNEU_AGG)
                  VALUES (agg_t2.ID_CLIENTE, agg_t2.ID_VEICULO, agg_t2.DAT_REFERENCIA, agg_t2.TEMPERATURA_MAX, agg_t2.PRESSAO_MIN, agg_t2.DISTANCIA, agg_t2.DATAHORA_TEMP_PRES_PNEU_AGG);
              EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
                  UPDATE FT_CC_TEMP_PRES_PNEU_AGREGADO agg_t1
                      SET agg_t1.TEMPERATURA_MAX = CASE WHEN {4} > agg_t1.TEMPERATURA_MAX THEN {4} ELSE agg_t1.TEMPERATURA_MAX END,
                          agg_t1.PRESSAO_MIN = CASE WHEN {3} < agg_t1.PRESSAO_MIN THEN {3} ELSE agg_t1.PRESSAO_MIN END
                  WHERE agg_t1.ID_VEICULO = {1} 
                      AND agg_t1.DAT_REFERENCIA = to_date('{0}','yyyy-mm-dd')
                      AND (agg_t1.PRESSAO_MIN > {3} OR agg_t1.TEMPERATURA_MAX < {4});
              END;     
        '''
    '''
      Test case 1: Mensagem válida / processamento efetuado com sucesso
    '''
    def test_message_valid(self):
        tpms_test = TpmsD0Process(dbclass=MyOracleClass, kafka_client_class=MyKafkaConsumer, kafka_producer_class=MyKafkaProducer)
        tpms_test.set_database_connection(login='self',
                                          password='self',
                                          host='self',
                                          database='self')
        tpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_zookeeper='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        tpms_test.set_kafka_producer(bootstrap_servers='default', kafka_topic='topic')
        self.kafka_topic = tpms_test.tpms_consumer.topics['Valid']
        tpms_test.tpms_consumer = self.kafka_topic.get_balanced_consumer(zookeeper_connect='kafka_zookeeper',
            auto_offset_reset='offset_reset',
            auto_commit_enable='KAFKA_ENABLE_AUTO_COMMIT',
            consumer_group='kafka_consumer_group_id',
            use_rdkafka='KAFKA_USE_RD')

        result = tpms_test.main()
        self.assertEqual(result, True)

    '''
      Test case 2: Mensagem Invalida / processamento não executado
    '''
    def test_message_invalid(self):
        tpms_test = TpmsD0Process(dbclass=MyOracleClass, kafka_client_class=MyKafkaConsumer)
        tpms_test.set_database_connection(login='self',
                                          password='self',
                                          host='self',
                                          database='self')
        tpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_zookeeper='default',
                                     kafka_topic='Invalid',
                                     kafka_consumer_group_id='self')
        self.kafka_topic = tpms_test.tpms_consumer.topics['Invalid']
        tpms_test.tpms_consumer = self.kafka_topic.get_balanced_consumer(zookeeper_connect='kafka_zookeeper',
            auto_offset_reset='offset_reset',
            auto_commit_enable='KAFKA_ENABLE_AUTO_COMMIT',
            consumer_group='kafka_consumer_group_id',
            use_rdkafka='KAFKA_USE_RD')
        result = tpms_test.main()
        self.assertEqual(result, True)

    '''
      Test case 3: Mensagem válida / Erro de conexão com o Oracle
    '''
    def test_message_oracle_error(self):
        tpms_test = TpmsD0Process(dbclass=MyOracleClassFail, kafka_client_class=MyKafkaConsumer, kafka_producer_class=MyKafkaProducer)
        tpms_test.set_database_connection(login='self',
                                          password='self',
                                          host='self',
                                          database='self')
        tpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_zookeeper='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        tpms_test.set_kafka_producer(bootstrap_servers='default',
                                     kafka_topic='default')

                                     
        self.kafka_topic = tpms_test.tpms_consumer.topics['Valid']
        tpms_test.tpms_consumer = self.kafka_topic.get_balanced_consumer(zookeeper_connect='kafka_zookeeper',
            auto_offset_reset='offset_reset',
            auto_commit_enable='KAFKA_ENABLE_AUTO_COMMIT',
            consumer_group='kafka_consumer_group_id',
            use_rdkafka='KAFKA_USE_RD')
        result = tpms_test.main()
        self.assertEqual(result, True)

    '''
      Test case 5: Mensagem válida / Balanced Consumer retorna nulo
    '''
    def test_kafka_none(self):
        tpms_test = TpmsD0Process(dbclass=MyOracleClass, kafka_client_class=MyKafkaConsumerNone)
        tpms_test.set_database_connection(login='self',
                                          password='self',
                                          host='self',
                                          database='self')
        tpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_zookeeper='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        self.kafka_topic = tpms_test.tpms_consumer.topics['Valid']
        tpms_test.tpms_consumer = self.kafka_topic.get_balanced_consumer(zookeeper_connect='kafka_zookeeper',
            auto_offset_reset='offset_reset',
            auto_commit_enable='KAFKA_ENABLE_AUTO_COMMIT',
            consumer_group='kafka_consumer_group_id',
            use_rdkafka='KAFKA_USE_RD')
        with self.assertRaises(SystemExit) as cm:
            tpms_test.main()

        self.assertEqual(cm.exception.code, 4)

    '''
      Test case 6: Test get_tyre_position with device position 1,3 and None
    '''
    def test_tyre_position(self):
        str_result1 = TpmsD0Process.get_tyre_position(device_position= 1, tire=11)
        str_result2 = TpmsD0Process.get_tyre_position(device_position= 3, tire=8)
        str_result3 = TpmsD0Process.get_tyre_position(device_position= 4, tire=8)
        self.assertEqual(str_result1, '0x23')
        self.assertEqual(str_result2, '0xA0')
        self.assertEqual(str_result3, None)

    '''
      Test case 7: Test MSG 
    '''
    def test_message_valid_sem_tpm(self):
        tpms_test = TpmsD0Process(dbclass=MyOracleClass, kafka_client_class=MyKafkaConsumer)
        tpms_test.set_database_connection(login='self',
                                          password='self',
                                          host='self',
                                          database='self')
        tpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_zookeeper='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        self.kafka_topic = tpms_test.tpms_consumer.topics['ValidSemTpm']
        tpms_test.tpms_consumer = self.kafka_topic.get_balanced_consumer(zookeeper_connect='kafka_zookeeper',
            auto_offset_reset='offset_reset',
            auto_commit_enable='KAFKA_ENABLE_AUTO_COMMIT',
            consumer_group='kafka_consumer_group_id',
            use_rdkafka='KAFKA_USE_RD')

        result = tpms_test.main()
        self.assertEqual(result, True)

    '''
      Test case 8: Test calc of difference between truck and trailer speeds 
    '''
    def test_calc_diff_towing_towed_pct(self):
        result = PosicaoCRCUProcess.calc_diff_towing_towed_pct(0,80)
        self.assertEqual(result, 0)
