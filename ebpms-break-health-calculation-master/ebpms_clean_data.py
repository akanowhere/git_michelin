
import pandas as pd

from util import CALCULATION_INTERVAL
from sql_codes import query_select_ebpms



class EbpmsCleanData:

    def __init__(self, ora_con=None, ora_cur=None, vehicle=None):
        self.ora_con = ora_con
        self.ora_cur = ora_cur
        self.vehicle = vehicle


    def calculate_fields(self,dataframe) -> pd.DataFrame:
        """Performs the calculations of the necessary fields for bpv.
            It also applies the percentage of wheel speed difference from the trailer to the truck.

             Args:
                 dataframe (pd.Dataframe): Pandas Dataframe with the vehicle positions.
        """
        dataframe_fields = dataframe.reset_index(drop=True)
        del dataframe

        dataframe_fields['FREE_EBPMS_SPEED_END'] = (dataframe_fields['FREE_EBPMS_SPEED_END'] *
                                                    ((dataframe_fields['EBS_DIFF_TOWING_TOWED_PCT'] /100) + 1))

        dataframe_fields['FREE_EBPMS_SPEED_BEGIN'] = (dataframe_fields['FREE_EBPMS_SPEED_BEGIN'] *
                                                    ((dataframe_fields['EBS_DIFF_TOWING_TOWED_PCT'] /100) + 1))

        dataframe_fields['SPEED_VARIATION'] = (dataframe_fields['FREE_EBPMS_SPEED_END'] -
                                               dataframe_fields['FREE_EBPMS_SPEED_BEGIN']) / 3.6

        dataframe_fields['BRAKE_DISTANCE'] = (dataframe_fields['FREE_EBPMS_SPEED_AVERAGE'] *
                                               dataframe_fields['FREE_EBPMS_DURATION']) / 3.6

        dataframe_fields['DECELERATION'] = (dataframe_fields['SPEED_VARIATION'] /
                                               dataframe_fields['FREE_EBPMS_DURATION']) / 9.81

        dataframe_fields['RETARDERON'] = (dataframe_fields['FREE_EBPMS_RETARDER'] /
                                               dataframe_fields['FREE_EBPMS_DURATION'])

        dataframe_fields['ROLRETARDER'] = dataframe_fields['RETARDERON'].rolling(CALCULATION_INTERVAL).mean()
        dataframe_fields['SLOPECOMPENSATION'] = dataframe_fields['FREE_EBPMS_ALTITUDE_VARIATION'] / dataframe_fields['BRAKE_DISTANCE']
        dataframe_fields['BRAKE'] = dataframe_fields.apply(lambda x: -x['DECELERATION'] -x['SLOPECOMPENSATION'], axis = 1)
        return(dataframe_fields)

    def get_the_last_positions(self, dataframe) -> pd.DataFrame:
        """Cuts the dataframe, taking only up to the indicated interval,
            returns the dataframe with the lines in the interval amount.

             Args:
                 dataframe (pd.Dataframe): Pandas Dataframe with the vehicle positions.
        """
        df_ebpms = dataframe.groupby("ID_VEICULO").head(CALCULATION_INTERVAL)

        df_filter = (df_ebpms.value_counts(subset='ID_VEICULO') == CALCULATION_INTERVAL)

        df_ebpms = df_ebpms.where(df_ebpms["ID_VEICULO"].isin(df_filter[df_filter == True].index))
        df_ebpms.dropna(inplace=True)
        del df_filter
        return (df_ebpms)

    @staticmethod
    def calculate_minimum_weight(max_load, veibaixacarga) -> float:
        """Checks if the vehicle has the low load flag.
            If you have, it performs the calculation of the minimum load
            that can be considered to assess whether a position is valid or not for the calculation.

                  Args:
                      max_load (float): Maximum registered weight.
                      veibaixacarga (int): Flag that indicates if the vehicle is traveling with a low load.
             """

        minimum_weight = 0
        if veibaixacarga:
            minimum_weight = max_load * 0.5
            return minimum_weight
        return minimum_weight

    def execute_select_ebpms(self, minimum_weight) -> pd.DataFrame:
        """Performs the select on the Oracle base and returns a dataframe with all valid positions.

                  Args:
                      minimum_weight (float): minimum weight valid to a position to be considered in the calculation.
             """
        df_ebpms = None
        df_ebpms = pd.read_sql(query_select_ebpms.format(self.vehicle, minimum_weight), self.ora_con)

        return (df_ebpms)