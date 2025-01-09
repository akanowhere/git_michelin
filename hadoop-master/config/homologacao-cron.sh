30 01 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MAIS_1 > /home/hadoop/logs/exec_GMT_MAIS_1_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 23 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MAIS_2 > /home/hadoop/logs/exec_GMT_MAIS_2_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 22 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MAIS_3 > /home/hadoop/logs/exec_GMT_MAIS_3_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 18 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MAIS_7 > /home/hadoop/logs/exec_GMT_MAIS_7_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 17 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MAIS_8 > /home/hadoop/logs/exec_GMT_MAIS_8_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 16 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MAIS_9 > /home/hadoop/logs/exec_GMT_MAIS_9_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 03 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MENOS_2 > /home/hadoop/logs/exec_GMT_MENOS_2_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 04 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MENOS_3 > /home/hadoop/logs/exec_GMT_MENOS_3_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 05 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MENOS_4 > /home/hadoop/logs/exec_GMT_MENOS_4_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 06 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py MENOS_5 > /home/hadoop/logs/exec_GMT_MENOS_5_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
30 02 * * * /usr/bin/python /home/hadoop/sascar/python_malha.py ZERO > /home/hadoop/logs/exec_GMT_ZERO_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1
*/5 * * * * /usr/bin/python /home/hadoop/sascar/python_jobs/python_op_execute_report.py  > /home/hadoop/logs/execute_report_`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1

# Electrum-bi
00 07 * * * /home/hadoop/sascar/electrum_bi/start_electrum.sh status.py >& /home/hadoop/sascar/electrum_bi/log/status.py.log
15 07 * * * /home/hadoop/sascar/electrum_bi/start_electrum.sh index.py >& /home/hadoop/sascar/electrum_bi/log/index.py.log
30 07 * * * /home/hadoop/sascar/electrum_bi/start_electrum.sh monitoring.py >& /home/hadoop/sascar/electrum_bi/log/monitoring.py.log
