#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
Created on Wed Jan 29 13:54:28 2020
@author: igor.seibel.ext
"""
import os
import sys, errno
import logging
from time import sleep
from flask import Flask, redirect, url_for
import threading
import psutil
from python_con_cc_rep_temp_pres_d0 import TpmsD0Process
from prometheus_flask_exporter import PrometheusMetrics
from datetime import timedelta, datetime

app = Flask(__name__)
metrics = PrometheusMetrics(app)

# static information as metric
metrics.info('app_info', 'Application info', version='1.0', major=1, minor=0)
cpu_usage = metrics.info('tpmsd0_cpu_usage',description='tpms % of cpu usage')
cpu_usage.set(psutil.cpu_percent())

memory_dict = dict(psutil.virtual_memory()._asdict())
tpms_memory_usage_pct = metrics.info('tpms_memory_usage_pct', 'Memory usage %')
tpms_memory_usage_pct.set(memory_dict['percent'])
tpms_memory_usage = metrics.info('tpms_memory_usage', 'Memory usage total')
tpms_memory_usage.set(memory_dict['used'])

app_start = datetime.now()
tpms_up_time = metrics.info('tpms_up_time', 'Time since the start of the app')

print(memory_dict)

#metrics.register_default(metrics.gauge('cpu_usage',description='% of cpu usage', labels={'cpu_percent': psutil.cpu_percent()}))
metrics.register_default(metrics.gauge('memory_usage', description='data about memory usage', labels={'memory_usage': dict(psutil.virtual_memory()._asdict())}))

@app.route("/")
def main():
    return "Welcome!"

@app.route("/actuator/health")
def health():
    enum = threading.enumerate()
    is_tpms_alive = False
    for th in enum:
        if th.name == "Thread-tpms":
            is_tpms_alive = th.is_alive()
    if is_tpms_alive:
        return "Health"
    else:
        sys.exit(errno.EINTR)

@app.route("/actuator/prometheus")
def prometheus():
    cpu_usage.set(psutil.cpu_percent())
    memory_dict = dict(psutil.virtual_memory()._asdict())
    logging.info(memory_dict['percent'])
    tpms_memory_usage_pct.set(memory_dict['percent'])
    diff_time = datetime.now() - app_start
    tpms_up_time.set((diff_time.seconds*1000))

    return redirect(url_for('prometheus_metrics'), code=302)

def call_script(): 
    tpmsd0 = TpmsD0Process()

    '''
    ORACLE DW CONNECTION DATA
    '''    
    logging.info(os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_IP'])
    
    tpmsd0.set_database_connection(login=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_LOGIN'],
                                   password=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_SENHA'],
                                   host=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_IP'],
                                   database=os.environ['SERVIDOR_BD_ORACLE_DW_SASCAR_NOME'])

    '''
    KAFKA HOST CONNECTION DATA
    '''
    brokers_list = os.environ['NEW_KAFKA_HOSTS']
    tpmsd0.set_kafka_consumer(kafka_hosts=brokers_list,
                              kafka_zookeeper=os.environ['KAFKA_ZOOKEEPER'],
                              kafka_topic=os.environ['KAFKA_TOPIC'],
                              kafka_consumer_group_id=os.environ['KAFKA_CONSUMER_GROUP_ID']
                              )

    new_brokers_list = os.environ['NEW_KAFKA_HOSTS']
    tpmsd0.set_kafka_producer(bootstrap_servers=new_brokers_list, kafka_topic=os.environ['KAFKA_TOPIC_TEMPERATURE_REPORT'])

    if not tpmsd0.main():
        print("thread encerrada!")
        sys.exit(errno.EINTR)
        return False
    else:
        return True

def set_log_level():
    try:
        LOG_LEVEL = int(os.environ['LOG_LEVEL'])        
    except:
        LOG_LEVEL = 10
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level=LOG_LEVEL)
     

if __name__ == '__main__':
    set_log_level()
    app_start = datetime.now()
    th = threading.Thread(target=call_script, name="Thread-tpms")
    th.start()
    print(th.name)
    app.run(host='0.0.0.0', port=8080)
