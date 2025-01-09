import unittest
from unittest.mock import patch

import cx_Oracle
import pandas as pd

from queue_process import QueueProcessor


class MyOracleClass():
    pass

class MyConnection():
    pass

class MyCursor():

    def execute(self,sql):
        return 12356

class MyPostgreConnectionError:

    def cursor(self):
       raise TypeError("Only integers are allowed")


class MyConnection():

    def commit(self):
        return True

class MyPostgreClassComErro:
    pass


class MyConnectionErro():

    def cursor(self):
        raise cx_Oracle.DatabaseError

class MyCursorErro():

    def execute(self,sql):
        raise cx_Oracle.DatabaseError

class TestQueue(unittest.TestCase):

    def setUp(self):
        self.ora_con = MyConnection()
        self.ora_cur = MyCursor()
        self.post_con = MyConnection
        self.post_cur = MyCursor()
        self.queueprocessor = QueueProcessor(self.ora_con, self.ora_cur, self.post_con, self.post_cur)

    @patch("pandas.read_sql")
    def test_sucess_get_pending_vehicle(self, sql_mock):
        df_data = {'ID_VEICULO': [123645]}
        sql_mock.return_value = pd.DataFrame(df_data)
        result = self.queueprocessor.get_pending_vehicle()
        self.assertEqual(result, 123645)

    @patch("pandas.read_sql")
    def test_fail_none_get_pending_vehicle(self, sql_mock):
        sql_mock.return_value = pd.DataFrame()
        result = self.queueprocessor.get_pending_vehicle()
        self.assertEqual(result, None)

    def test_fail_get_pending_vehicle(self):
        queueprocessor = QueueProcessor(MyConnectionErro(), MyCursorErro())
        with self.assertRaises(SystemExit):
            queueprocessor.get_pending_vehicle()


    def test_sucess_set_end_process(self):
        result = self.queueprocessor.set_end_process(123645, "Success!")
        self.assertEqual(result, None)

    def test_fail_set_end_process(self):
        queueprocessor = QueueProcessor(MyConnectionErro(), MyCursorErro())
        with self.assertRaises(SystemExit):
            queueprocessor.set_end_process(123645, "Success!")

    @patch("pandas.read_sql")
    def test_sucess_get_vehicles_with_of(self, sql_mock):
        df_data = {'ID_VEICULO': [123645]}
        sql_mock.return_value = pd.DataFrame(df_data)
        result = self.queueprocessor.get_vehicles_with_of()
        self.assertEqual(result['ID_VEICULO'][0], 123645)

    def test_fail_get_vehicles_with_of(self):
        queueprocessor = QueueProcessor(post_con=MyPostgreConnectionError())
        with self.assertRaises(SystemExit):
            queueprocessor.get_vehicles_with_of()

    @patch("queue_process.QueueProcessor.get_vehicles_with_of")
    def test_sucess_execute_insert_queue(self, of_mock):
        df_data = {'rfsid_veiculo': [123645]}
        of_mock.return_value = pd.DataFrame(df_data)
        result = self.queueprocessor.execute_insert_queue()
        self.assertEqual(result, True)

    @patch("queue_process.QueueProcessor.get_vehicles_with_of")
    def test_fail_execute_insert_queue(self, of_mock):
        df_data = {'rfsid_veiculo': [123645]}
        of_mock.return_value = pd.DataFrame(df_data)
        queueprocessor = QueueProcessor(MyConnectionErro(), MyCursorErro())
        result = queueprocessor.execute_insert_queue()
        self.assertEqual(result, False)

