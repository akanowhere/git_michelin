#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
Created on 29/06/2020
Ricardo Costardi

"""

import sys, errno
import logging
from datetime import datetime, timedelta

import cx_Oracle
import psycopg2

from util import (
                    TBL_EBPMS_CALCULO,
                    TBL_EBPMS_PAYLOAD,
                    DT_EBPMS_PAYLOAD,
                    DT_EBPMS_CALCULO,
                    TIMEDELTA
                 )
from sql_code import soft_delete_dw, update_ebmps_positions
from queue_process import QueueProcessor

class EbpmsProcess():
  RUNNING = 1


  def __init__(self, dbclass=cx_Oracle, dbclass_post=psycopg2):
    self.sql=None
    self.dbclass = dbclass
    self.dbclass_post = dbclass_post


  def set_database_connection(self, login, password, host, database):
    """Set an Oracle database connection

    Args:
        login (str): db login username
        password (str): db user password
        host (str): ip address of db host
        database (str): database name
    """
    self.con = None
    self.con = self.dbclass.connect("{0}/{1}@{2}/{3}".format(login,password,host,database))
    self.cur = self.con.cursor()

  def set_database_connection_postgre(self, login, password, host, database):
    """create a postgres DB connection

    Args:
        login (str): db login username
        password (str): db user password
        host (str): ip address of db host
        database (str): database name
    """
    self.con_postgre = None
    self.con_postgre = self.dbclass_post.connect(host=host, user=login, password=password, dbname=database)
    self.cur_postgre = self.con_postgre.cursor()             


  def execute_update(self, id_vehicle) -> bool:
    """performs an update on the FT_PAYLOAD_EBPMS table
     putting the values of the ebs fields.
      It also performs an update by placing the delete flag
       in the ebpms payload and calculation tables.

         Args:
             id_vehicle (str): id of the vehicle that is been processed
    """
    sql_params = ''
    try:
      self.cur.execute(update_ebmps_positions.format(id_vehicle))
      self.con.commit()

      if  self.cur.rowcount > 0:
        sql_params = soft_delete_dw.format(id_vehicle,
                                             TBL_EBPMS_PAYLOAD,
                                             DT_EBPMS_PAYLOAD)                                       
        self.cur.execute(sql_params)
        sql_params = soft_delete_dw.format(id_vehicle,
                                             TBL_EBPMS_CALCULO,
                                             DT_EBPMS_CALCULO)                                       
        self.cur.execute(sql_params)
        self.con.commit()
        return True

    except cx_Oracle.DatabaseError as e:
      logging.error(str(e)+' \n '+sql_params)
      sys.exit(errno.EINTR)
    return True

  @staticmethod
  def check_timedelta_for_cache(cache_datetime, timedelta_control) -> bool:
      """
                    check if the date initialize with the pod has pass the
                    timedelta.

                    :param cache_datetime: date initialize with the pod
                    :type cache_datetime: datetime

                    :param timedelta_control: timedelta to compare to the actual datetime now
                    :type timedelta_control: timedelta

                    """

      check_datetime_now = datetime.now()

      if (cache_datetime + timedelta_control) <= check_datetime_now:
          return True
      return False

  def main(self):

      """
      Orchestrator function, calls the other functions in order and performs the necessary validation.

      """
      try:
          cache_datetime = datetime.now()
          timedelta_control = timedelta(minutes=TIMEDELTA)

          while self.RUNNING:
              queue_vehicle = QueueProcessor(self.con, self.cur, self.con_postgre, self.cur_postgre)
              if EbpmsProcess.check_timedelta_for_cache(cache_datetime, timedelta_control):
                  queue_vehicle.execute_insert_queue()
              id_vehicle = queue_vehicle.get_pending_vehicle()
              if id_vehicle == None:
                  continue
              self.execute_update(id_vehicle)
              queue_vehicle.set_end_process(id_vehicle, "Vehicle processed with sucess!")

      except (cx_Oracle.DatabaseError, AttributeError) as e:
          logging.error(str(e))
          sys.exit(errno.EINTR)
            
        


