import unittest
from unittest.mock import patch

from queue_process import QueueProcessor


class MyOracleClass():

    def connect(self, *args):
        return MyConnection()


class MyConnection():

    def cursor(self):
        return MyCursor()

    def commit(self):
        return True


class MyCursor():

    def execute(self,sql):
        return 12356, 10000
    def fetchone(self):
        return 12356, 10000


class MyPostgreClass:

    def connect(host, user, password, dbname):
        return MyConnection()


class MyConnection():

    def cursor(self):
        return MyCursor()

    def commit(self):
        return True

class MyPostgreClassComErro:

    def connect(host, user, password, dbname):
        return MyConnectionErro()


class MyConnectionErro():

    def cursor(self):
        return MyCursorErro()

    def commit(self):
        raise TypeError("Only integers are allowed")

class MyCursorErro():

    def execute(self,sql):
        raise TypeError("Only integers are allowed")


class MyEbpms:

    def __init__(self, dbclass=MyOracleClass, db_con=None, db_cursor=None, vehicle=None, max_load=None):
        self.sql = None
        self.dbclass = dbclass
        self.con = db_con
        self.cur = db_cursor
        self.vehicle = vehicle
        self.max_load = max_load

    def controller(self):
        return True


class TestQueue(unittest.TestCase):

    def setUp(self):
        self.cx_Oracle = MyOracleClass
        self.psycopg2 = MyPostgreClass
        self.ebpms_class = MyEbpms(dbclass=MyOracleClass, db_con=MyConnection, db_cursor=MyCursor,
                                   vehicle=None, max_load=None)

    def test_postconection(self):
        queue_select = QueueProcessor(dbclass_post=MyPostgreClass)
        queue_select.set_database_connection_postgre(login='self',
                                                     password='self',
                                                     host='self',
                                                     database='self')

    def test_select_max_load(self):
        queue_select = QueueProcessor(dbclass_post=MyPostgreClass)
        queue_select.set_database_connection_postgre(login='self',
                                             password='self',
                                             host='AAAAA',
                                             database='self')
        id_veio = '1978954'
        queue_select.select_max_load(id_veio)
