
import unittest

import pandas as pd

from unittest.mock import patch

from ebpms_clean_data import EbpmsCleanData


class MyConnection():

    def cursor(self):
        pass


class MyCursor():
    pass


def generate_df():
    data = ({'HDR_MESSAGE_DATE_TIME': '2021-06-29 17:44:03',
            'FREE_EBPMS_RETARDER': 0,
            'FREE_EBPMS_TIME': '2021-06-29 17:42:55',
            'FREE_EBPMS_DURATION': 13.206,
            'FREE_EBPMS_SPEED_AVERAGE': 51,
            'FREE_EBPMS_ALTITUDE_VARIATION': 3,
            'FREE_EBPMS_SPEED_BEGIN': 77,
            'FREE_EBPMS_SPEED_END': 26,
            'FREE_EBPMS_BRAKE_AVERAGE': 1.7,
            'SPEED_VARIATION': -14.166666666666666,
            'DECELERATION': 0,
            'BRAKE_DISTANCE': 0,
            'RETARDERON': 0,
            'ABS_ACTIVE_TOWING_COUNTER': 0,
            'ABS_ACTIVE_TOWED_COUNTER': 0,
            'EBS_LOAD': 14.4,
            'ID_CLIENTE': 789456,
            'ID_VEICULO': 123456,
            'EBS_DIFF_TOWING_TOWED_PCT': 1.7503978176858448})
    df_data = pd.DataFrame(data, index=list(range(0, 200)))
    return df_data



class TestEbpmsCleanData (unittest.TestCase):

    def setUp(self) -> None:
        self.ora_con = MyConnection
        self.ora_cur = MyCursor
        self.vehicle = 123456
        self.ebpmsclean = EbpmsCleanData(self.ora_con, self.ora_cur, self.vehicle)
        self.df_test = generate_df()

    def test_calculate_fields(self):
        result = self.ebpmsclean.calculate_fields(self.df_test)
        self.assertEqual(result['FREE_EBPMS_SPEED_BEGIN'][0], 78.3478063196181)

    def test_get_the_last_positions(self):
        result = self.ebpmsclean.get_the_last_positions(self.df_test)
        self.assertEqual(len(result.index), 200)

    def test_baixacarga_calculate_minimum_weight(self):
        result = self.ebpmsclean.calculate_minimum_weight(20000, 1)
        self.assertEqual(result, 10000)

    def test_sem_baixacarga_calculate_minimum_weight(self):
        result = self.ebpmsclean.calculate_minimum_weight(20000, 0)
        self.assertEqual(result, 0)

    @patch("pandas.read_sql")
    def test_execute_select_ebpms(self, sql_mock):
        sql_mock.return_value = generate_df()
        result = self.ebpmsclean.execute_select_ebpms(0)
        self.assertEqual(len(result.index), 200)
