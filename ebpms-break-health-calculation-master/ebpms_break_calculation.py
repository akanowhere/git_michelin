import sys, errno
import logging
import socket
import numpy as np
import pandas as pd
import statsmodels.robust.robust_linear_model
import cx_Oracle


from util import (MAX_PRESSURE,
                  CALCULATION_INTERVAL)
from sql_codes import update_insert_calculations

from ebpms_clean_data import EbpmsCleanData

RLM = statsmodels.robust.robust_linear_model.RLM


class EbpmsProcess:


    def __init__(self, db_con=None, db_cursor=None, vehicle=None, max_load=None, veibaixacarga=None):
        self.con = db_con
        self.cur = db_cursor
        self.vehicle = vehicle
        self.max_load = max_load
        self.veibaixacarga = veibaixacarga        

    def insert_calculations(self,dataframe, last_position) -> None:
        """Insert the FT_EBPMS_CALCULO table of the calculated bpv.

                  Args:
                      dataframe (pd.DataFrame): Pandas DataFrame with the bpv calculated.
               """
        df_ebpms_update = last_position.groupby("ID_VEICULO").head(1)
        df_ebpms_update = df_ebpms_update[['HDR_MESSAGE_DATE_TIME', 'ID_CLIENTE', 'ID_VEICULO', 'FLAG_CALC']]

        inc = 100 - dataframe['inc'][0]
        conf_range = 100 - inc
        upper_limit = dataframe['bpv'][0] + conf_range
        lower_limit = dataframe['bpv'][0] - conf_range
        sql_insert = update_insert_calculations.format(dataframe['ID_VEICULO'][0],
                                               dataframe['HDR_MESSAGE_DATE_TIME'][0],
                                               dataframe['EBS_LOAD'][0],
                                               dataframe['ROLRETARDER'][0],
                                               dataframe['SLOPECOMPENSATION'][0],
                                               dataframe['bpv'][0],
                                               inc,
                                               dataframe['z'][0],
                                               dataframe['RETARDERON'][0],
                                               df_ebpms_update['HDR_MESSAGE_DATE_TIME'][0],
                                               int(df_ebpms_update['ID_CLIENTE'][0]),
                                               df_ebpms_update['FLAG_CALC'][0],
                                               conf_range,
                                               upper_limit,
                                               lower_limit)
        self.cur.execute(sql_insert)
        self.con.commit()
        del dataframe
        del last_position
        del df_ebpms_update

    def variance(self,p, z, bcov_scaled, sigma2) -> float:
        """Variance.

                  Args:
                      p (float): Max pressure
                      z (float): Max weight
                      bcov_scaled (Array): Inverse matrix
                      sigma2 (int): constant value
               """
        x_tilde = np.array([p, z])
        return x_tilde.dot(bcov_scaled).dot(x_tilde) * sigma2

    def expectation(self,p, z, theta) -> tuple:
        """Expectation.

                  Args:
                      p (float): Max pressure
                      z (float): Max weight
                      theta (Any): Params of RLM
               """
        x_tilde = np.array([p, z])
        return x_tilde.dot(theta)

    def fit_model_rlm(self,x_line, y_line) -> tuple:
        """Fitting of the RLM model.

                  Args:
                      x_line (array): Array with the x values
                      y_line (array): Array with the y values.
               """
        rlm = RLM(y_line.values, x_line.values).fit()
        theta = rlm.params
        bcov_scaled = rlm.bcov_scaled
        sigma2 = 1
        return theta, bcov_scaled, sigma2

    def adjust_pressure(self, row) -> float:
        """Adjusts the maximum pressure parameter on the brake pedal.

                  Args:
                      rolretarder (float): percentage of time that the motor brake was activated during the braking event.
               """
        pressure_demand = row['FREE_EBPMS_BRAKE_AVERAGE'] #brake_demand
        rolretarder = row['ROLRETARDER']
        adjusted_pressure = pressure_demand
        if rolretarder == None:
            logging.info('No rolretarder')            
        elif rolretarder < 0.3:
            adjusted_pressure = 0.3+(pressure_demand-0.3)*(1-0.23*(1-3.3 * rolretarder))

        return adjusted_pressure

    def estimate_bpv(self,position, max_load) -> list:
        """Calculation of bpv.

                  Args:
                      position (pd.DataFrame): Pandas DataFrame with the vehicle positions.
                      max_load (float): Max weight for the vehicle.
               """
        x_line = pd.DataFrame(index=position.index)
        x_line['P'] = position.apply(self.adjust_pressure, axis=1)  #FREE_EBPMS_BRAKE_AVERAGE
        x_line['Z'] = position.EBS_LOAD
        y_line = position.BRAKE 
        try:
            bcov_scaled = np.linalg.inv(x_line.T.dot(x_line))
        except np.linalg.LinAlgError:
            return [
                len(position),
                position.index[-1],
                np.nan, np.nan,  # bpv, inc
                np.nan,  # theta
                x_line['Z'].mean()
                ]

        theta, bcov_scaled, sigma2 = self.fit_model_rlm(x_line, y_line)        
        max_pressure = MAX_PRESSURE 
        bpv = self.expectation(max_pressure, max_load, theta)
        inc = 2 * np.sqrt(self.variance(max_pressure, max_load, bcov_scaled, sigma2))

        return [
                len(position),
                position.index[-1],
                bpv, inc,
                theta, x_line['Z'].mean()
                ]

    def calculate_brake_perfomance(self,dataframe,max_load) -> None:
        """Calls the functions for calculating the brake performance (bpv).

                  Args:
                      dataframe (pd.DataFrame): Pandas DataFrame with the vehicle positions.
                      max_load (float): Max weight for the vehicle.
               """
        curve = []
        for pl in EbpmsProcess.rolling(dataframe, freq=1):
            curve.append(self.estimate_bpv(pl, max_load))
            curve = pd.DataFrame(curve, columns=['n', 'index', 'bpv', 'inc', 'a', 'z'])
            curve = curve.merge(dataframe[['HDR_MESSAGE_DATE_TIME','ID_VEICULO','EBS_LOAD','RETARDERON','ROLRETARDER','SLOPECOMPENSATION']],
                    left_on=['index'], right_on=dataframe.index, how='left')

        self.insert_calculations(curve, dataframe)
        del curve
        del dataframe

    @staticmethod
    def rolling(dataframe=None, freq=1) -> pd.DataFrame:
        """Rolling generator function.

                  Args:
                      dataframe (pd.DataFrame): Pandas DataFrame with the vehicle positions.
                      freq (int): Control.
               """
        counter = 0
        for i in range(len(dataframe) - CALCULATION_INTERVAL + 1):
            counter +=1
            if counter % freq == 0:
                yield dataframe.iloc[i:(i+CALCULATION_INTERVAL)]

    def controller(self) -> bool:
        """Responsible for function calls and necessary validations before and after these calls.

               """
        list_brake_event = None
        list_last_positions = None
        try:
            ebpmscleandata = EbpmsCleanData(self.con, self.cur, self.vehicle)
            minimum_weight = ebpmscleandata.calculate_minimum_weight(self.max_load, self.veibaixacarga)
            list_brake_event = ebpmscleandata.execute_select_ebpms(minimum_weight)
            list_last_positions = ebpmscleandata.get_the_last_positions(list_brake_event)
            if list_last_positions.empty:
                logging.info('No positions to process!')
                return False
            events_one_vehicle = ebpmscleandata.calculate_fields(list_last_positions)
            self.calculate_brake_perfomance(events_one_vehicle,self.max_load)
            del events_one_vehicle
        except (cx_Oracle.DatabaseError, pd.io.sql.DatabaseError) as e:
            logging.error(str(e))
            sys.exit(errno.EINTR)


        logging.info("Calculo realizado! Valores inseridos, Ultima posicao atualizada!")
        return True              


   

