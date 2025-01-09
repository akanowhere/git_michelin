#!/bin/bash
#

BUCKET=s3://emr-rebuild-sascar/hive_prod/posicoes_crcu/
LOGBASE=/home/hadoop/setup/delta

if [ $# -lt 2 -o "$1" = "-?" -o "$1" = '--help' ]; then
	echo "
$0 - Carga Batch de dados retroativos

  Modos de operacao
	--periodo <Inicio> <Final>
		Carrega todos os dias dentro do periodo
	--file <File>
		Carrega todos os dias listados em um arquivo, no formato YYYY-MM-DD
	--month <YYYY> <MM>

"
	exit 1
fi


export YEAR= MONTH=
export INICIO= FINAL=
export FILE=

export DAYS=

modo=
while [ ! -z "$1" ]; do
	if [ ! -z "$modo" ]; then
		printf "** Redefinicao de modo! ABORT"
		exit 1
	fi

	case $1 in
		--periodo)
			modo=$1
			INICIO=$2; shift
			FINAL=$2;  shift
			( date -d "$INICIO" && date -d "$FINAL" ) >& /dev/null
			if [ $? != 0 -o -z "$INICIO" -o -z "$FINAL" ]; then
				printf "** ERRO Periodo invalido!\n\n"
				exit 1
			fi
			DAYS=
			Cur=$(date -d "$INICIO" +%Y%m%d)
			Max=$(date -d "$FINAL"  +%Y%m%d)
			loopLimit=0
			while [ "$Cur" -le "$Max" -a "$loopLimit" -lt 1000 ]; do
				DAYS="$DAYS $(date -d $Cur +%Y-%m-%d)"
				Cur=$(date -d "$Cur +1 day" +%Y%m%d)
				let loopLimit=$loopLimit+1
			done
			;;
		--file)
			modo=$1
			FILE=$2; shift
			if [ ! -f "$FILE" -o ! -r "$FILE" ]; then
				printf "** ERRO Arquivo invalido!\n\n"
				exit 1
			fi
			DAYS=
			for D in $(cat $FILE); do
				if [ $D != "$(date -d "$D" +%Y-%m-%d)" ]; then
					printf "*** ERRO Data invalida no arquivo: $D"
					exit 1
				fi
				DAYS="$DAYS $D"
			done
			;;
		--month)
			modo=$1
			YEAR=$2;  shift
			MONTH=$2; shift

			( date -d "$YEAR-$MONTH-01" ) >& /dev/null
			if [ $? != 0 ]; then
				printf "** ERRO Periodo invalido!\n\n"
				exit 1
			fi

			dia=1
			while true; do
				DAY=$(date -d "$YEAR-$MONTH-$dia" +%Y-%m-%d)
				chk=$(echo $DAY | cut -d- -f 1-2)
				if [ "$chk" != "$YEAR-$MONTH" ]; then
					break
				fi
				DAYS="$DAYS $DAY"
				let dia=$dia+1
			done
			
			;;
		*)
			printf "** Argumentos invalidos! [$*]"
			exit 1 ;;
	esac
	shift
done

if [ -z "$DAYS" ]; then
	echo " ** ERRO Dias nao possui nenhuma informacao para carregar! **"
	exit 1
fi

echo Carregando dias: $DAYS

maxActive=30
Delay=300

pids=
function CheckPids() {
	newPids=
	active=0
	for pid in $pids; do
		kill -0 $pid 2> /dev/null
		if [ $? = 0 ]; then
			newPids="$newPids $pid"
			let active=$active+1
		else
			echo " >> Retirando $pid <<"
		fi
	done
	export pids=$newPids
	export active
	
}

function DelayLoop() {
	echo -- "$*"
	export LOOP=1
	for I in $(seq 1 $Delay); do
		printf "            \r- Aguardando %s/%s   (%s) " $I $Delay "$pids"
		sleep 1
		[ "$LOOP" != 1 ] && break
	done
}
PID=$$
trap "export LOOP=0; echo Um processo finalizou." SIGCONT

function hive_sleep() {
  (
	DATE=$1
	echo "IMPORT $DATE"
	Start=$(date +%s)
	#** ATENCAO ***
	# TAB vai travar o processo!
     (
	printf "%s\n" \
	  "ALTER TABLE posicoes_crcu DROP PARTITION(data_posicao_short='<DATE>');" \
	  "IMPORT TABLE posicoes_crcu FROM 's3://emr-rebuild-sascar/hive_prod/posicoes_crcu//data_posicao_short=<DATE>';" \
	  "SELECT 'Carregados:', data_posicao_short, count(*) FROM posicoes_crcu WHERE data_posicao_short='<DATE>' GROUP BY data_posicao_short ORDER BY data_posicao_short;" \
	 | sed "s/<DATE>/$1/g" | hive
     ) 2> $LOGBASE.$DATE.hive
	rc=$?
	let Duracao=$(date +%s)-$Start
	if [ $rc != 0 ]; then
	   echo "*** ERRO *** $DATE apos $Duracao ***"
	   exit 1
	fi
	echo "ENCERRANDO $* $Duracao - $rc"
  ) |& tee $LOGBASE.$DATE 
	exit 0
}


for DATE in $DAYS; do

	printf -- "- Iniciando o dia %s\n" $DATE

	hive_sleep $DATE &
	pids="$pids $!"
	
	active=0
	while true; do
		
		CheckPids

		printf -- "\n- Temos %s ativos!\t" $active
		if [ $active -lt $maxActive ]; then
			echo "Iniciando o proximo"
			break
		fi

		DelayLoop " * Delay para aguardar processamento * "

	done

done

echo "

*** TODOS JA INICIADOS ***

"

while true; do
	#echo ">> PIDS $pids x $newPids"
	CheckPids
	if [ -z "$pids" ]; then
		break
	fi

	DelayLoop " * Aguardando termino de $active ativos [ $pis ]"

done

echo "Termino normal."

exit 0

