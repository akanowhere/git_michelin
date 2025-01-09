#!/bin/bash

#cria pasta de log se nao houver
[ ! -d ~/sascar/logs ] && mkdir ~/sascar/logs/
(
        if hdfs dfs -ls / >& /dev/null ; then 
                hdfs dfs -du -s -h /var/log/spark/apps/ 
                hadoop fs -rmr -r /var/log/spark/apps/application_*
                hdfs dfs -du -s -h /var/log/spark/apps/
        else
                echo "falha ao acessar hdfs"
        fi
) >& ~/sascar/logs/apps_log_clear_$(date +%Y%m%d-%H%M).log