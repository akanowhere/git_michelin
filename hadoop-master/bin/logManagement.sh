#!/bin/bash
#
#  30/03/2020 Marco Andrade
#	Gerenciamento de descarte de logs:
#	- Aplicacao - local
#	- Application dentro do HDFS ** Se nao for expurgado, pode derrubar o cluster!
#
#	Caso necessite dos logs do HADOOP verifique onde eh armazenado no S3, pelo console
#
LOCAL=30
DELETE=120

ARCHIVE_DIR=/home/hadoop/logs/old
LOGDIR=/home/hadoop/logs

function log() {
  printf "%s %s\n" "$(date)" "$*"
}

(

  if [ ! -d ${ARCHIVE_DIR} ]; then
	mkdir -p ${ARCHIVE_DIR}
  fi

  log "Tratamento de logs de aplicacao"

  log "Expurgo de $( find "${ARCHIVE_DIR}" -maxdepth 1 -type f -mtime +${DELETE} -size +0c -delete -ls | wc -l ) logs, com mais de ${DELETE} dias"

  log "Expurgo de $( find "${ARCHIVE_DIR}" -maxdepth 1 -type f -mtime +${LOCAL}  -size 0c -delete -ls | wc -l ) logs com zero bytes, com mais de ${LOCAL} dias"

  log "Archive de $( find ${LOGDIR} -maxdepth 1 -type f -mtime +${LOCAL} -size +0c -exec mv "{}" ${ARCHIVE_DIR} \;  -ls | wc -l ) logs, com mais de ${LOCAL} dias"

  usage=$( hdfs dfs -du -s -h /var/log/spark/apps/ | awk '{print $1}' | sed 's/\..*//g' )
  if [ "$usage" -ge 50 ]; then
	log "Expurgo de logs do HADOOP com $usage GB"
	hadoop fs -rm -r /var/log/spark/apps/application_* &> /dev/null

	log "> Ocupacao atual"
	hdfs dfs -df -h
  fi

) &>> $LOGDIR/logManagement.log
