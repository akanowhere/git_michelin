#!/bin/bash
#

BUCKET=s3://emr-rebuild-sascar/hive_prod/posicoes_crcu
LIMIT=15
SIMULTANEOS=8
Delay=1800
#-- Replace permite exportar dados ate a data atual
REPLACE=

DEBUG=3
DRYRUN=
LOGDIR=$(dirname $0)/logs
LOGFILE=${LOGDIR}/$(basename $0 .sh).$(date +%Y%m%d-%H%M).log


if [ ! -d $LOGDIR ]; then
	mkdir -p $LOGDIR
fi

#-- Controle com base em hdfs://export/posicoes_crcu
#Previous=$( hdfs dfs -ls /export/posicoes_crcu | sort -k 8 | awk '{print $8}' | tail -1 | cut -d= -f 2 )

#------------
function log() {
  level=$1; shift
  _data="$(date)"
  if [ -z "$QUIET" ]; then
	if [ "$level" -le "$DEBUG" ]; then
		printf "%s - %s\n" "$_data" "$*"
	fi
  fi
  if [ "$level" -le "$DEBUG" ]; then
	printf "%s - %s\n" "$_data" "$*" >> $LOGFILE
  fi
}
#------------

MinReplace=$(date -d '-7 days' +%Y%m%d)

#-- Controle com base nos dados do Bucket
Previous=$( aws s3 ls ${BUCKET}/ 2> /dev/null | sort -r | head -n 1 | cut -d= -f 2 | cut -d/ -f 1 )

if [ -z "$Previous" ]; then
	log 1 " *** ERRO: Dados de inicio nao disponiveis no Bucket! ***"
	exit 1
fi

PrevN=$(echo $Previous | sed 's/-//g')
MaxAuto=$(date -d '-1 days' +%Y%m%d)

if [ "$PrevN" -ge "$MinReplace" ]; then
	Previous=$MinReplace
	PrevN=$(echo $Previous | sed 's/-//g')
	log 1 "* Replace automatico - tratando ultimos 6+1 dias"
	REPLACE=1
fi

_run=0
LOGBASE=/home/hadoop/logs/Backup_Incremental_$(date +%Y%m%d-%H%M%S)_

#-- Rotina em Background
function BackupDate() {

	_workdir="${BUCKET}/data_posicao_short=${Next}"

	if [ ! -z "${REPLACE}" ]; then
		log 2 "Removendo $_workdir para sobrescrita"
		(
			[ -z "${DRYRUN}" ] && aws s3 rm --recursive --quiet $_workdir
		) >& /dev/null
	fi

	#printf "EXPORT TABLE posicoes_crcu partition (data_posicao_short='%s') TO '/export/posicoes_crcu/data_posicao_short=%s';\n" $Next $Next
	hql="$(printf "EXPORT TABLE posicoes_crcu partition (data_posicao_short='%s') TO '%s';\n" $Next $_workdir)"
	log 3 "Executando no HIVE: $hql"
	if [ -z "${DRYRUN}" ]; then
		echo $hql | hive --silent >& $LOGBASE.$Next
	fi
	
	let DURACAO=$(date +%s)-$START
	log 1 ": Tratamento de $Next: durou $DURACAO segundos"
	kill -18 $PID
	exit 0
}


pids=
function CheckPids() {
        newPids=
        active=0
	signal=0
        for pid in $pids; do
                kill -0 $pid 2> /dev/null
                if [ $? = 0 ]; then
                        newPids="$newPids $pid"
                        let active=$active+1
                else
                        echo " >> Retirando $pid <<"
			signal=1
                fi
        done
        export pids=$newPids
        export active
	[ "signal" = 1 ] && kill -18 $$

}

#-- Sinalizacao para ser mais rapido
PID=$$
trap "export LOOP=0; echo Trap: NEXT LOOP" SIGCONT
function DelayLoop() {
        echo -- "$*"
	LOOP=1
        for I in $(seq 1 $Delay); do
                printf "            \r- Aguardando %s/%s   (%s) " $I $Delay "$pids"
                sleep 1
		[ "$LOOP" != 1 ] && break
        done
}


log 1 "Iniciando processamento. Ultimo dado armazenado: $Previous - Limite: $MaxAuto - Replace: $REPLACE"
active=0
pids=
while [ "$PrevN" -lt "$MaxAuto" ]; do

	START=$(date +%s)

	let _run=$_run+1
	if [ "${_run}" -gt "$LIMIT" ]; then
		break
	fi

	Next=$(date -d "$Previous +1 day" +%Y-%m-%d)

	log 1 "> Tratando $Next"


	BackupDate &
	pid=$!
	pids="$pids $pid"
	let active=$active+1


	#-- Proxima data
	Previous=$Next
	PrevN=$(echo $Previous | sed 's/-//g')

	[ "$active" -lt $SIMULTANEOS ] && continue


	CheckPids


	if [ "$active" -gt $SIMULTANEOS ]; then
		DelayLoop "Aguardando finalizar $pids - $active ativos"
	fi

done

CheckPids

echo ">> Aguardando finalizar $pids"
while [ ! -z "$pids" ]; do

	CheckPids
	if [ ! -z "$pids" ]; then
		DelayLoop "Aguardando finalizar $pids - $active ativos"
	fi
done
