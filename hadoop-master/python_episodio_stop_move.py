# -*- coding: utf-8 -*-
"""
Created on Fri Aug 17 17:39:28 2018

@author: carlos.santanna.ext
"""

import os
import sys
if __name__ == "__main__":
    
    try:
       print ("Executando spark_con_cc_cumulative_odometer.py")
       command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_con_cc_cumulative_odometer.py")
       exitCode = os.system(command)
       
       if exitCode != 0:
          print ("Erro na execução do job spark_con_cc_cumulative_odometer.py")
          sys.exit(1)
       
       print ("Executando spark_con_cc_trip_report.py.py")
       command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_con_cc_trip_report.py")
       exitCode = os.system(command)
       
       if exitCode != 0:
          print ("Erro na execução do job spark_con_cc_trip_report.py")
          sys.exit(1)

    except Exception as e:
        sys.exit(str(e))