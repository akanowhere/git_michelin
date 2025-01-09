import unittest
import cx_Oracle
import pandas as pd

from unittest.mock import patch
from pykafka.protocol.message import Message

from python_parser_ebpms import EbpmsProcess

class MyOracleClass():

    def connect(self, *args):
        return MyConnection()

class MyConnection():

    def cursor(self):
        return MyCursor()

    def commit(self):
        return True

class MyCursor():

    def execute(self, sql):
        return True

class MyOracleClassFail():

    def connect(self, *args):
        return MyConnectionFail()

class MyConnectionFail():

    def cursor(self):
        return MyCursorFail()


class MyCursorFail():

    def execute(self, *args):
        raise cx_Oracle.DatabaseError


class MyKafkaConsumer():

    def __init__(self, topics, **configs):
      self.set_topic_content()


    def set_topic_content(self):

        
        self.msg_valid = Message('''{"SoftwareVersion":"41603828",
                                     "UnitIdentifier":"8DD1E300",
                                     "SessionKey":"5EDE7187",
                                     "Payload":"01FD016E067F71DE5E4542504D533D30302F35454445373130322D314442342F30352F2B30312F30462D30312F3030394200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F986",
                                     "ProtocolVersion":"1",
                                     "ReceptionDateTime":"2020-06-08T17:12:45.650Z"}''')


        self.msg_invalid = Message('''{"SoftwareVersion":"41314324",
                                       "UnitIdentifier":"1402600E",
                                       "SessionKey":"5EDE7181",
                                       "Payload":"0202012D037971DE5E0E60021402FEFFFFFF0000700000000000000000000000000000000000FEFFFFFF00007000FEFFFFFF0000700000000000000000000000000000000000FEFFFFFF00007000FEFFFFFF0000700000000000000000000000000000000000FEFFFFFF00007000A4C8960179B2",
                                       "ProtocolVersion":"1",
                                       "ReceptionDateTime":"2020-06-08T17:12:40.159Z"} ''')

        list_valid = MyList([self.msg_valid])
        list_invalid = MyList([self.msg_invalid])

        self.topics = {"Valid": MyTopic(list_valid), 
                       "Invalid": MyTopic(list_invalid)}
        
class MyList(list):

    def __init__(self, liste):
      list.__init__(self,liste)  

    def commit(self):
      return True        


class MyTopic():

    def __init__(self, msg_list):
        self.msg_list = msg_list

    def get_balanced_consumer(self):
        print(type(self.msg_list))
        return self.msg_list

class TestProcessMessage(unittest.TestCase):


    def setUp(self) -> None:
        self.oracleclass = MyOracleClass
        self.kafka = MyKafkaConsumer
        self.ebpms_test = EbpmsProcess(dbclass=self.oracleclass, kafka_client_class=self.kafka)
        self.message_fail = {"SoftwareVersion": "41603828",
                       "UnitIdentifier": "8DD1E300",
                       "SessionKey": "5EDE7187",
                       "Payload": "01FD016E067F71DE5E4542504D53!D30302F35454445373130322D314442!!!F30352F2B30312F30462D3031!!!!!!!!!94200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F986",
                       "ProtocolVersion": "1",
                       "ReceptionDateTime": "2020-06-08T17:12:45.650Z"}
        self.message_sucess = {"SoftwareVersion":"41603828",
                                     "UnitIdentifier":"8DD1E300",
                                     "SessionKey":"5EDE7187",
                                     "Payload":"01FD016E067F71DE5E4542504D533D30302F35454445373130322D314442342F30352F2B30312F30462D30312F3030394200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F986",
                                     "ProtocolVersion":"1",
                                     "ReceptionDateTime":"2020-06-08T17:12:45.650Z"}
        self.ebpms_test.set_database_connection(login='self',
                                          password='self',
                                          host='self',
                                          database='self')

    def test_sucess_parser_ebpms(self):
        result = self.ebpms_test.parser_ebpms(position=self.message_sucess)
        self.assertEqual(result.empty, False)

    def test_fail_parser_ebpms(self):
        result = self.ebpms_test.parser_ebpms(position=self.message_fail)
        self.assertEqual(result.empty, True)

    def test_sucess_extract_payload_ebpms(self):
        result = self.ebpms_test.extract_payload_ebpms(position=self.message_sucess)
        self.assertEqual(result, self.message_sucess)

    def test_fail_extract_payload_ebpms(self):
        result = self.ebpms_test.extract_payload_ebpms(position=self.message_fail)
        self.assertEqual(result, False)

    def test_sucess_execute_merge(self):
        sql_param = """INSERT INTO FT_PAYLOAD_EBPMS"""
        result = self.ebpms_test.execute_merge(sql_param)
        self.assertEqual(result, True)

    def test_fail_execute_merge(self):
        ebpms_test = EbpmsProcess(dbclass=MyOracleClassFail, kafka_client_class=self.kafka)
        ebpms_test.set_database_connection(login='self',
                                                password='self',
                                                host='self',
                                                database='self')
        sql_param = ""
        with self.assertRaises(cx_Oracle.DatabaseError):
            ebpms_test.execute_merge(sql_param)

    @patch("python_parser_ebpms.EbpmsProcess.parser_ebpms")
    @patch("python_parser_ebpms.EbpmsProcess.extract_payload_ebpms")
    def test_sucess_process_message(self, mock_extract_payload_ebpms, mock_parser_ebpms):
        mock_extract_payload_ebpms.return_value  = True
        mock_parser_ebpms.return_value = pd.DataFrame([[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]],
                                                      columns=['unitidentifier','sessionkey','softwareversion',
                                                               'hdr_payload_version','hdr_payload_type',
                                                               'hdr_payload_cause','hdr_generation_number',
                                                               'hdr_payload_number','hdr_message_date_time','free_ebpms_retarder',
                                                               'free_ebpms_time','free_ebpms_duration','free_ebpms_speed_average',
                                                               'free_ebpms_altitude_variation','free_ebpms_speed_begin',
                                                               'free_ebpms_speed_end','free_ebpms_brake_average','speed_variation'])
        result = self.ebpms_test.process_message(Message('''{"topic":"sascarBinaryPayloads", "partition":"17",
                                                            "offset":"61829899", "timestamp":"1624558446029",
                                                             "timestamp_type":"0", "key":"None",
                                                              "value":"b010101D303FDC9D46060BE4508D0F7E20A7004F6C9D46000008C010000080CA45100574C01010F0A640201015C"
                                                              , "headers":"[]", "checksum":"2235188284",
                                                               "serialized_key_size":"-1", "serialized_value_size":"254"
                                                               , "serialized_header_size":"-1"}'''))
        self.assertEqual(result, True)


    def test_sucess_main(self):
        self.ebpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        self.kafka_topic = self.ebpms_test.ebpms_consumer.topics['Valid']
        self.ebpms_test.ebpms_consumer = self.kafka_topic.get_balanced_consumer()
        result = self.ebpms_test.main()
        self.assertEqual(result, True)

    def test_fail_main(self):
        self.ebpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        self.kafka_topic = self.ebpms_test.ebpms_consumer.topics['Invalid']
        self.ebpms_test.ebpms_consumer = self.kafka_topic.get_balanced_consumer()
        result = self.ebpms_test.main()
        self.assertEqual(result, True)

    def test_fail_topic_main(self):
        self.ebpms_test.set_kafka_consumer(kafka_hosts='default',
                                     kafka_topic='Valid',
                                     kafka_consumer_group_id='self')
        with self.assertRaises(SystemExit):
            self.ebpms_test.main()




        
