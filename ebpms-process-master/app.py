#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
Ricardo Costardi 29/06/2020
"""
import os
import sys, errno
import logging
from time import sleep
from flask import Flask, redirect, url_for
import threading
import psutil
from python_ebpms_join import EbpmsProcess
from prometheus_flask_exporter import PrometheusMetrics
from datetime import timedelta, datetime
app = Flask(__name__)
metrics = PrometheusMetrics(app)


# static information as metric
metrics.info('app_info', 'Application info', version='1.0', major=1, minor=0)
cpu_usage = metrics.info('ebpms_cpu_usage',description='ebpms % of cpu usage')
cpu_usage.set(psutil.cpu_percent())

memory_dict = dict(psutil.virtual_memory()._asdict())
ebpms_memory_usage_pct = metrics.info('ebpms_memory_usage_pct', 'Memory usage %')
ebpms_memory_usage_pct.set(memory_dict['percent'])
ebpms_memory_usage = metrics.info('ebpms_memory_usage', 'Memory usage total')
ebpms_memory_usage.set(memory_dict['used'])

app_start = datetime.now()
ebpms_up_time = metrics.info('ebpms_up_time', 'Time since the start of the app')

print(memory_dict)

#metrics.register_default(metrics.gauge('cpu_usage',description='% of cpu usage', labels={'cpu_percent': psutil.cpu_percent()}))
metrics.register_default(metrics.gauge('memory_usage', description='data about memory usage', labels={'memory_usage': dict(psutil.virtual_memory()._asdict())}))

@app.route("/")
def main():
    return "Welcome!"

@app.route("/actuator/health")
def health():
    enum = threading.enumerate()
    is_ebpms_alive = False
    for th in enum:
        if th.name == "Thread-ebpms":
            is_ebpms_alive = th.is_alive()
    if is_ebpms_alive:
        return "Health"
    else:
        sys.exit(errno.EINTR)

@app.route("/actuator/prometheus")
def prometheus():
    cpu_usage.set(psutil.cpu_percent())
    memory_dict = dict(psutil.virtual_memory()._asdict())
    print(memory_dict['percent'])
    ebpms_memory_usage_pct.set(memory_dict['percent'])
    diff_time = datetime.now() - app_start
    ebpms_up_time.set((diff_time.seconds*1000))

    return redirect(url_for('prometheus_metrics'), code=302)

def call_script():
    ebpms = EbpmsProcess()
    
    
    ebpms.set_database_connection(login=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_LOGIN'],
                                   password=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_SENHA'],
                                   host=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_IP'],
                                   database=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_NOME'])

    
    ebpms.set_database_connection_postgre(login=os.environ['SERVIDOR_BD_POSTGRE_DOM_SASCAR_LOGIN'],
                                   password=os.environ['SERVIDOR_BD_POSTGRE_DOM_SASCAR_SENHA'],
                                   host=os.environ['SERVIDOR_BD_POSTGRE_DOM_SASCAR_IP'],
                                   database=os.environ['SERVIDOR_BD_POSTGRE_DOM_SASCAR_NOME'])
 
    if not ebpms.main():
        print("thread encerrada!")
        sys.exit(errno.EINTR)
        return False
    else:
        return True
    

if __name__ == '__main__':
    app_start = datetime.now()
    th = threading.Thread(target=call_script, name="Thread-ebpms")
    th.start()
    print(th.name)
    app.run(host='0.0.0.0', port=8080)
