#!/bin/bash
#
# 27/12/2019 - Limitacao de concorrencia!
#

PID="$(ps -ef | grep 'python -u .*python_op_execute' | grep -v -e sh -e grep | awk '{print $2}')"

if [ -z "$PID" ]; then
	/usr/bin/python -u /home/hadoop/sascar/python_jobs/python_op_execute_report.py
else
	echo "STOP - Ja existe processamento rodando com pid $PID"
fi

