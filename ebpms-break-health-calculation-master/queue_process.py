import cx_Oracle
from datetime import datetime, timezone
import logging
import sys, errno
from ebpms_break_calculation import EbpmsProcess
import pandas as pd
import psycopg2

from util import (PENDING, PROCESSING)

class QueueProcessor():
  RUNNING = 1

  def __init__(self,  dbclass=cx_Oracle, dbclass_post=psycopg2, ebpmsclass=EbpmsProcess):
    self.sql=None
    self.dbclass = dbclass
    self.dbclass_post = dbclass_post
    self.ebpmsclass = ebpmsclass
    self.sql_vehicle = '''     SELECT ID_VEICULO
                               FROM VEI_EBPMS_PROCESSAMENTO A
                               WHERE DAT_INICIO_PROCESSAMENTO IN (SELECT MIN(DAT_INICIO_PROCESSAMENTO) FROM VEI_EBPMS_PROCESSAMENTO B WHERE B.STATUS='{0}')
                               AND STATUS='{0}'
                               AND ROWNUM = 1
                               FOR UPDATE 
                           '''
    self.sql_update = '''
                            BEGIN
                              UPDATE VEI_EBPMS_PROCESSAMENTO
                                 SET STATUS = '{1}',
                                     DAT_INICIO_PROCESSAMENTO = SYSDATE
                              WHERE ID_VEICULO = {0};
                            END;
                          ''' 
    self.sql_end_process = '''
                               BEGIN
                                 UPDATE VEI_EBPMS_PROCESSAMENTO
                                    SET STATUS = '{2}',
                                        DAT_ULT_POS_CHEGA_PROC = TO_DATE('{1}', 'yyyy-mm-dd hh24:mi:ss'),
                                        PROC_ERROR = NULL
                                 WHERE ID_VEICULO = {0};
                               END;
                               ''' 



  def set_database_connection(self, login, password, host, database):
    self.con = None
    self.con = self.dbclass.connect("{0}/{1}@{2}/{3}".format(login,password,host,database))
    self.cur = self.con.cursor()

  def set_database_connection_postgre(self, login, password, host, database):
    self.con_postgre = None
    self.con_postgre = self.dbclass_post.connect(host=host, user=login, password=password, dbname=database)
    self.cur_postgre = self.con_postgre.cursor()    

  def get_pending_vehicle(self):
    try:
        today = datetime.utcnow().replace(tzinfo=timezone.utc)
        date_current = today.strftime("%Y-%m-%d %H:%M:%S")
        sql_params_vehicle = self.sql_vehicle.format(PENDING)
        vehicle_cursor = pd.read_sql(sql_params_vehicle,self.con)
        if vehicle_cursor.empty:
          return (None, None)
        
        vehicle = vehicle_cursor['ID_VEICULO'].iloc[0]
        sql_params = self.sql_update.format(vehicle, PROCESSING)

        
        self.cur.execute(sql_params)
        self.con.commit()
        
        return (vehicle, date_current)
    except cx_Oracle.DatabaseError as e:
        logging.error(str(e))
        sys.exit(errno.EINTR)

  def set_end_process(self, vehicle, last_date_received):
    try:
      sql_params = self.sql_end_process.format(vehicle, last_date_received, PENDING)
      self.cur.execute(sql_params)
      self.con.commit() 
                
    except cx_Oracle.DatabaseError as e:
      logging.error(str(e))
      sys.exit(errno.EINTR)



  def select_max_load(self,id_vehicle):

    query_max_load = """ SELECT peso_max, veibaixacarga FROM dispositivo.veiculo where veioid = {0};"""
    select_statment = query_max_load.format(id_vehicle) 
    
    try:
      
      self.cur_postgre.execute(select_statment)

      result = self.cur_postgre.fetchone()
      if result:
          max_load = result[0]
          veibaixacarga = result[1]
      else:
          max_load = 0
          veibaixacarga = 0
      if not veibaixacarga :
        veibaixacarga = '0'
      return(max_load, veibaixacarga)
    except (psycopg2.DatabaseError,TypeError, AttributeError) as e:
      logging.error(str(e))
      sys.exit(errno.EINTR)
      return(0)      

  def main(self):
    while self.RUNNING:
      
      vehicle, date_current = self.get_pending_vehicle()
      
      if not vehicle or vehicle == 0:
        continue

      max_load, veibaixacarga = self.select_max_load(vehicle)
      
      if not max_load or max_load == 0:
        self.set_end_process(vehicle=vehicle, last_date_received=date_current)
        continue


      ebpms_calculator = EbpmsProcess(db_con=self.con, db_cursor=self.cur, vehicle=vehicle, max_load=max_load/1000,
                                      veibaixacarga=veibaixacarga)
            
      ebpms_calculator.controller()
      
      self.set_end_process(vehicle=vehicle, last_date_received=date_current)


