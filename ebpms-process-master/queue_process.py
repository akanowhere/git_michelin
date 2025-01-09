import cx_Oracle
import logging
import socket
import sys, errno
import pandas as pd
import psycopg2

from sql_code import (sql_vehicle,
                      sql_update,
                      sql_end_process,
                      select_of_from_dominio,
                      insert_into_queue
                      )

class QueueProcessor():


  def __init__(self, ora_con=None, ora_cur=None, post_con=None, post_cur=None):
      self.ora_con = ora_con
      self.ora_cur = ora_cur
      self.post_con = post_con
      self.post_cur = post_cur


  def get_pending_vehicle(self) -> (str, None):
      """Gets the next available vehicle in the queue, via select in the VEI_EBPMS_PROCESSAMENTO table, checks if
       it brought a vehicle locks the processing of that vehicle to have exclusivity and returns the vehicle id.

           Args:
               id_vehicle (str): id of the vehicle that is been processed
      """
      try:
          vehicle_cursor = pd.read_sql(sql_vehicle,self.ora_con)
          if vehicle_cursor.empty:
              return None
        
          vehicle = vehicle_cursor['ID_VEICULO'].iloc[0]
          pod = socket.gethostname()
          sql_params = sql_update.format(vehicle, pod)

          self.ora_cur.execute(sql_params)
          self.ora_con.commit()
        
          return (vehicle)
      except cx_Oracle.DatabaseError as e:
          logging.error(str(e))
          sys.exit(errno.EINTR)

  def set_end_process(self, vehicle, log):
      """Return the vehicle to the queue, changing its status and unlocking it, it also records a small message
       informing whether the process was successful or not.

           Args:
               id_vehicle (str): id of the vehicle that is been processed
               log (str): message to be saved in the column, success or fail
      """
      try:
          sql_params = sql_end_process.format(vehicle, log)
          self.ora_cur.execute(sql_params)
          self.ora_con.commit()
                
      except cx_Oracle.DatabaseError as e:
         logging.error(str(e))
         sys.exit(errno.EINTR)


  def get_vehicles_with_of(self) -> pd.DataFrame:
      """Returns a list of vehicles that have the ebpms of is the registered weight.

      """
      try:
          df_vehicles_with_of = pd.read_sql(select_of_from_dominio, self.post_con)

          return df_vehicles_with_of

      except (psycopg2.DatabaseError, TypeError) as e:
          logging.error(str(e))
          sys.exit(errno.EINTR)

  def execute_insert_queue(self) -> bool:
      """Compares vehicles that have OF with vehicles that are in the processing queue.
       Try to insert new vehicles to be processed.

      """

      df_vehicle_of = self.get_vehicles_with_of()
      df_vehicle_of.rename(columns={"rfsid_veiculo": "ID_VEICULO"}, inplace=True)
      for i, row in  df_vehicle_of.iterrows():
          try:
              self.ora_cur.execute(insert_into_queue.format(row["ID_VEICULO"]))
              self.ora_con.commit()

          except cx_Oracle.DatabaseError as e:
              logging.error(str(e))
              return False
      return True




