
"""
Created on 30/06
@author: ricardo.costardi.ext

"""

import unittest
from unittest.mock import patch

import pandas as pd
from ebpms_break_calculation import EbpmsProcess
from ebpms_clean_data import EbpmsCleanData 
from brake_events_sample import brake_events


class MyConnection():

    def cursor(self):
        pass

    def commit(self):
        return True


class MyCursor():

    def execute(self, sql):
        print(sql)
        return True


def generate_df_multi():
    data = ([{'HDR_MESSAGE_DATE_TIME': '2021-06-29 17:44:03',
            'FREE_EBPMS_RETARDER': 0,
            'FREE_EBPMS_TIME': '2021-06-29 17:42:55',
            'FREE_EBPMS_DURATION': 5.003,
            'FREE_EBPMS_SPEED_AVERAGE': 53,
            'FREE_EBPMS_ALTITUDE_VARIATION': 1,
            'FREE_EBPMS_SPEED_BEGIN': 59,
            'FREE_EBPMS_SPEED_END': 48,
            'FREE_EBPMS_BRAKE_AVERAGE': 1.15,
            'SPEED_VARIATION': -3.05555555555556,
            'DECELERATION': -0.062879929761024,
            'BRAKE_DISTANCE': 73.65528,
            'RETARDERON': 0,
            'ABS_ACTIVE_TOWING_COUNTER': 0,
            'ABS_ACTIVE_TOWED_COUNTER': 0,
            'EBS_LOAD': 18.228,
            'ID_CLIENTE': 789456,
            'ID_VEICULO': 123456,
            'EBS_DIFF_TOWING_TOWED_PCT': 1.7503978176858448,
            'BRAKE': 1.15,
            'ROLRETARDER': 0.0,
            'SLOPECOMPENSATION': -0.1,
             'FLAG_CALC': 0}])
    df_data = pd.DataFrame(data, index=list(range(0, 200)))
    return df_data

def generate_df():
    data = ({'HDR_MESSAGE_DATE_TIME': '2021-06-29 17:44:03',
            'FREE_EBPMS_RETARDER': 1,
            'FREE_EBPMS_TIME': '2021-06-29 17:42:55',
            'FREE_EBPMS_DURATION': 13.206,
            'FREE_EBPMS_SPEED_AVERAGE': 51,
            'FREE_EBPMS_ALTITUDE_VARIATION': 3,
            'FREE_EBPMS_SPEED_BEGIN': 77,
            'FREE_EBPMS_SPEED_END': 26,
            'FREE_EBPMS_BRAKE_AVERAGE': 1.15,
            'SPEED_VARIATION': -14.166666666666666,
            'DECELERATION': 1,
            'BRAKE_DISTANCE': 1,
            'RETARDERON': 1,
            'ABS_ACTIVE_TOWING_COUNTER': 0,
            'ABS_ACTIVE_TOWED_COUNTER': 0,
            'EBS_LOAD': 14.4,
            'ID_CLIENTE': 789456,
            'ID_VEICULO': 123456,
            'EBS_DIFF_TOWING_TOWED_PCT': 1.7503978176858448,
            'BRAKE': 2.3,
            'ROLRETARDER': 0.0,
            'SLOPECOMPENSATION': -0.1,
             'FLAG_CALC': 0})
    df_data = pd.DataFrame(data, index=list(range(0, 200)))
    return df_data

def create_dataframe_bpv():

    data = {'HDR_MESSAGE_DATE_TIME': ['2020-09-24 17:42:52'],
                                'FREE_EBPMS_RETARDER':[2.9000000000000004],
                                'FREE_EBPMS_TIME':['2020-09-24 17:42:41'],
                                'FREE_EBPMS_DURATION':[3.009],
                                'FREE_EBPMS_SPEED_AVERAGE':[70],
                                'FREE_EBPMS_ALTITUDE_VARIATION':[0],
                                'FREE_EBPMS_SPEED_BEGIN':[74],
                                'FREE_EBPMS_SPEED_END':[67],
                                'FREE_EBPMS_BRAKE_AVERAGE':[0.65],
                                'EBS_DIFF_TOWING_TOWED_PCT':[0.9],
                                'SPEED_VARIATION':[-1.9444444444444444],
                                'DECELERATION':[-0.06587253002949847],
                                'BRAKE_DISTANCE':[58.50833333333333],
                                'RETARDERON':[0.9637753406447326],
                                'ABS_ACTIVE_TOWING_COUNTER':[0],
                                'ABS_ACTIVE_TOWED_COUNTER':[0],
                                'EBS_LOAD':[11600],
                                'ID_CLIENTE':[385560],
                                'ID_VEICULO':[1317281],
                                'ROLRETARDER':[0],
                                'SLOPECOMPENSATION':[0],
                                'BRAKE':[0.5],
                                'bpv':[0],
                                'inc':[0],
                                'z':[0]}
    df_bpv = pd.DataFrame(data)
    return df_bpv
     

class TestCalculationEbpms(unittest.TestCase):

    def setUp(self) -> None:
        self.ora_con = MyConnection()
        self.ora_cur = MyCursor()
        self.vehicle = 123456
        self.max_load = 20000
        self.veibaixacarga = 1

        self.ebpmsprocess = EbpmsProcess(self.ora_con, self.ora_cur, self.vehicle, self.max_load, self.veibaixacarga)


    def test_insert_calculations(self):
        result = self.ebpmsprocess.insert_calculations(create_dataframe_bpv(),generate_df())
        self.assertEqual(result, None)

    def test_variance(self):
        result = self.ebpmsprocess.variance(6.5, 20.0, ([[-2. ,  1. ],[ 1.5, -0.5]]),1)
        self.assertEqual(result, 40.5)

    def test_expectation(self):
        result = self.ebpmsprocess.expectation(6.5, 20.0, 1)
        self.assertEqual(result[0], (6.5))

    def test_fit_model_rlm(self):
        x = pd.DataFrame([1,2,3,6])
        y = pd.DataFrame([2,3,4,5])
        result0, result1, result3 = self.ebpmsprocess.fit_model_rlm(x,y)
        self.assertEqual(result3, 1)

    def test_adjust_brake_pressure_0(self):
        position = generate_df_multi()
        result = position.apply(self.ebpmsprocess.adjust_pressure, axis=1)
        print(result)
        self.assertEqual(round(result[0], 4), 0.9545)

    def test_estimate_bpv(self):
        df_position = generate_df()
        result = self.ebpmsprocess.estimate_bpv(df_position, 20000)
        self.assertEqual(result[0], 200)

    def test_calculate_brake_perfomance(self):
        result = self.ebpmsprocess.calculate_brake_perfomance(generate_df(), 20000)
        self.assertEqual(result, None)

    def test_rolling(self):
        result = list(EbpmsProcess.rolling(generate_df()))
        self.assertEqual(len(result), 1)

    def test_fail_controller(self):
        with self.assertRaises(SystemExit):
            self.ebpmsprocess.controller()


    @patch("ebpms_break_calculation.EbpmsProcess.calculate_brake_perfomance")
    @patch("ebpms_clean_data.EbpmsCleanData.calculate_fields")
    @patch("ebpms_clean_data.EbpmsCleanData.get_the_last_positions")
    @patch("ebpms_clean_data.EbpmsCleanData.execute_select_ebpms")
    def test_sucess_controller(self, mock_select, mock_last, mock_calculate, mock_bp):
        mock_select.return_value = True
        mock_last.return_value = generate_df()
        mock_calculate.return_value = True
        mock_bp.return_value = True
        result = self.ebpmsprocess.controller()
        self.assertEqual(result, True)


    def test_calculate_brake_perfomance_real(self):
        ebpmscleandata = EbpmsCleanData(ora_con=self.ora_con,ora_cur=self.ora_cur, vehicle=1317419)
        list_brake_event = pd.DataFrame(data=brake_events, index=list(range(0, 200))) #ebpmscleandata.execute_select_ebpms(minimum_weight)
        list_last_positions = ebpmscleandata.get_the_last_positions(list_brake_event)
        if list_last_positions.empty:
            logging.info('No positions to process!')
            return False
        events_one_vehicle = ebpmscleandata.calculate_fields(list_last_positions)

        result = self.ebpmsprocess.calculate_brake_perfomance(events_one_vehicle, (24000/1000))
        self.assertEqual(result, None)
