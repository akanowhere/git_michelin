# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 15:25:27 2018

@author: carlos.santanna.ext
"""

import os
import sys

if __name__ == "__main__":
    
    try:
       print ("Copiando arquivos para carga...")
       exitCode = os.system("/usr/bin/python /home/hadoop/sascar/carga_crcu_s3_hive/python_move_s3_json_to_in_process.py")
       
       if exitCode == 0:
          print ("Executando Spark...")
          command = ("/home/hadoop/sascar/start_job_spark_reports.sh spark_load_posicoes_crcu_into_hive.py")
          exitCode = os.system(command)
          
          if exitCode == 0:
             print ("Limpando diretório temporário...")
             
             exitCode = os.system("/usr/bin/python /home/hadoop/sascar/carga_crcu_s3_hive/python_move_s3_json_to_done.py")
             
             if exitCode == 0:
                print ("Carga completa!!!")
                
                sys.exit(os.EX_OK)
                
             else:
                print ("Falha ao mover arquivos para o diretório de 'arquivos processados'.")
                print ("Necessária ação manual para mover os arquivos antes do próximo processamento.")
                print ("Mover arquivos do S3 do diretório 'POSICOES_CRCU/IN_PROCESS' para o diretório 'POSICOES_CRCU/POSICOES_PROCESSED'.")
                
                sys.exit(os.EX_SOFTWARE)
                
          else:
             sys.exit(os.EX_SOFTWARE)

       else:
          sys.exit(os.EX_SOFTWARE)
          
    except Exception as e:
        sys.exit(str(e))
