# Teste
10 23 * * * /usr/bin/python /home/hadoop/sascar/carga_crcu_s3_hive/python_main_load_posicoes_crcu_into_hive.py > /home/hadoop/logs/python_main_load_posicoes_crcu_into_hive_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
35 23 * * * /home/hadoop/sascar/start_job_spark.sh spark_con_cc_load_positions_to_redshift.py > /home/hadoop/logs/spark_con_cc_load_positions_to_redshift_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
15 00 * * * /usr/bin/python /home/hadoop/sascar/python_episodio_stop_move.py > /home/hadoop/logs/exec_EPISODIO_STOP_MOVE_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
