
"""
Created on 30/06
@author: ricardo.costardi.ext

"""

import unittest
from unittest.mock import patch, PropertyMock

import cx_Oracle

from datetime import datetime, timedelta
from python_ebpms_join import EbpmsProcess
from util import TIMEDELTA

class MyOracleClassFail():

    def connect(self, *args):
        return MyConnectionFail()

class MyConnectionFail():

    def cursor(self):
        return MyCursorFail()

class MyCursorFail():

    def execute(self, sql,  *args):
        raise cx_Oracle.DatabaseError


class MyOracleClass():

    def connect(self, *args):
        return MyConnection()

class MyConnection():
    pass


class MyCursor():
    pass

class MyPostgreClass():
    
    @staticmethod
    def connect(host, user, password, dbname):
        return MyConnection()

class MyConnection():

    def cursor(self):
        return MyCursor()

    def commit(self):
        return True

class MyCursor():
    
    def __init__(self):
        self.rowcount = 1

    def execute(self, sql):
        return True         
                    

class TestProcessMessage(unittest.TestCase):

    def setUp(self):
        self.dbclass = MyOracleClass()
        self.dbclass_post = MyPostgreClass()
        self.timedelta_control = timedelta(minutes=TIMEDELTA)
        self.cache_datetime = datetime.now()
        self.ebpms_process = EbpmsProcess(dbclass=self.dbclass, dbclass_post=self.dbclass_post)
        self.ebpms_process.set_database_connection_postgre(login='self',
                                                     password='self',
                                                     host='self',
                                                     database='self')
        self.ebpms_process.set_database_connection(login='self',
                                             password='self',
                                             host='self',
                                             database='self')

    def test_success_execute_update(self):
        result = self.ebpms_process.execute_update(123456)
        self.assertEqual(result, True)

    def test_fail_execute_update(self):
        ebpms_process = EbpmsProcess(dbclass=MyOracleClassFail())
        ebpms_process.set_database_connection(login='self',
                                                password='self',
                                                host='self',
                                                database='self')
        with self.assertRaises(SystemExit):
            ebpms_process.execute_update(123456)

    def test_false_check_timedelta_for_cache(self):

        result = self.ebpms_process.check_timedelta_for_cache(self.cache_datetime, self.timedelta_control)
        self.assertEqual(result, False)

    def test_true_check_timedelta_for_cache(self):

        self.cache_datetime = self.cache_datetime - self.timedelta_control
        result = self.ebpms_process.check_timedelta_for_cache(self.cache_datetime, self.timedelta_control)
        self.assertEqual(result, True)

    @patch("queue_process.QueueProcessor.set_end_process")
    @patch("queue_process.QueueProcessor.get_pending_vehicle")
    def test_success_main(self, vehicle_mock, set_end_mock):
        vehicle_mock.return_value = 123456
        set_end_mock.return_value = True
        sentinel = PropertyMock(side_effect=[1, 0])
        type(self.ebpms_process).RUNNING = sentinel
        result = self.ebpms_process.main()
        self.assertEqual(result, None)

    def test_fail_main(self):
        with self.assertRaises(SystemExit):
            self.ebpms_process.main()








      

   