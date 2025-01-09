import unittest
from datetime import datetime, timezone
from python_con_cc_rep_temp_pres_d0 import TpmsD0Process
from pykafka.protocol.message import Message
import cx_Oracle
from pykafka.exceptions import ConsumerStoppedException
from tpms_msg_test import MyConnection, MyOracleClass
from tpms_msg_test import MyCursor, MyOracleClassFail
from tpms_msg_test import MyConnectionFail, MyConnectionFailKafka
from tpms_msg_test import MyKafkaConsumer, MyTopic, MyList


class MyKafkaConsumerTTU(MyKafkaConsumer):

    def set_topic_content(self):

        today = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.msg_valid = Message(('''
{
    "summaryDevice": {
        "jsonData": {
            "veiculo": 968925,
            "ibuttonPart1": "0",
            "bateriaExt": 1,
            "velocidade": 0,
            "id_motorista": "0",
            "ibuttonPart2": "0",
            "latitude": -22.6235968,
            "ignicao": "0",
            "horimetro": 0,
            "pos_memoria": "1",
            "ibuttonHex": "0",
            "protocolo": 82,
            "cliente": 140249,
            "gps_valido": "0",
            "bateriaInt": 4,
            "odometro": 601666,
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
            "classeEq": 178,
            "data_posicao": %s,
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
            "longitude": -43.219441
        }
    }
}    
        ''' % today.timestamp()))
        list_valid = MyList([self.msg_valid])

        self.topics = {"Valid": MyTopic(list_valid)}

class TestProcessMessageTTU(unittest.TestCase):

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
        tpms_test = TpmsD0Process(dbclass=MyOracleClass, kafka_client_class=MyKafkaConsumerTTU)
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

        result = tpms_test.main()
        self.assertEqual(result, True)


    
