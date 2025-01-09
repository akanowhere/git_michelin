# -*- coding: utf-8 -*-
import os
import sys
if __name__ == "__main__":
    
    try:
       print ("Executando spark_con_cc_create_staging_by_timezone.py")
       command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_con_cc_create_staging_by_timezone.py {0}").format(sys.argv[1])
       exitCode = os.system(command)
       
       if exitCode != 0:
          print ("Erro na execução do job spark_con_cc_create_staging_by_timezone.py")
          sys.exit(1)

       print ("Executando spark_con_cc_ind_sec_infractions.py")
       command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_con_cc_ind_sec_infractions.py {0}").format(sys.argv[1])
       exitCode = os.system(command)
       
       if exitCode != 0:
          print ("Erro na execução do job spark_con_cc_ind_sec_infractions.py")
          sys.exit(1)       

       #print ("Executando spark_con_cc_rep_temp_pres.py")
       #command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_con_cc_rep_temp_pres.py {0}").format(sys.argv[1])
       #exitCode = os.system(command)
       
       if exitCode != 0:
          print ("Erro na execução do job spark_con_cc_rep_temp_pres.py")
          sys.exit(1)  

       print ("Executando spark_con_cc_drop_staging_by_timezone.py")
       command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_con_cc_drop_staging_by_timezone.py {0}").format(sys.argv[1])
       exitCode = os.system(command)
       
       if exitCode != 0:
          print ("Erro na execução do job spark_con_cc_drop_staging_by_timezone.py")
          sys.exit(1)  

    except Exception as e:
        sys.exit(str(e))