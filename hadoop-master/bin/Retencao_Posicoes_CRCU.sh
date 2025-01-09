#!/bin/bash
#


ANO=$1
MES=$2

DEBUG=1
DATA=$(date +%Y%m%d-%H%M)

LOGFILE=posicoes_crcu_retencao.${DATA}.log

BucketSource=s3://sasweb2-prod/POSICOES_CRCU/POSICOES_PROCESSED
BucketTarget=s3://sasweb2-prod/Retencao/POSICOES_CRCU


function log() {
  Level=$1; shift
  if [ "$Level" -le "$DEBUG" ]; then
     printf "%s - %s\n" "$(date)" "$*" >> $LOGFILE
     [ -z "$QUIET" ] && printf "%s - %s\n" "$(date)" "$*"
  fi
}

if [ -z "$ANO" -o -z "$MES" ]; then
     log 1 "* ERRO Parametros invalidos!"
     exit 1
fi

#-- Validar origem de dados
aws s3 ls ${BucketSource}/${ANO}/${MES} >& /dev/null
if [ $? != 0 ]; then
	log 1 "* ERRO Ano/mes informados inexistentes no S3!"
	echo "aws s3 ls ${BucketSource}/${ANO}/${MES} >& /dev/null"
	exit 1
fi

log 1 "Fazendo processamento diario de ${ANO}/${MES}"
Result=
for dia in $(seq -w 01 31); do
    aws s3 ls ${BucketSource}/${ANO}/${MES}/${dia}/ >& /dev/null
    if [ $? == 0 ]; then

	_TARFile=_${ANO}${MES}_${dia}.tgz
	_WorkDir=${ANO}/${MES}/${dia}/

	log 1 "- Processando ${BucketSource}/${ANO}/${MES}/${dia}/"
	aws s3 sync --quiet --delete ${BucketSource}/${ANO}/${MES}/${dia}/ $_WorkDir
	if [ $? != 0 ]; then
		log 1 " * ERRO: Falha ao transferir: $?"
		continue
	fi
	_SourceSize=$(du -shc ${_WorkDir}/ | tail -1 | awk '{print $1}')
	log 1 " - Consolidar ${_SourceSize} em ${_TARFile}"
	tar zcf ${_TARFile} ${_WorkDir}

	_FinalSize=$(du -h $_TARFile | awk '{print $1}')
	log 1 " - Subir dados consolidados ${BucketTarget}/${_TARFile} com ${_FinalSize}"
	aws s3 mv --quiet ${_TARFile} ${BucketTarget}/${ANO}${MES}/
	if [ $? != 0 ]; then
		log 1 " * ERRO: Falha ao fazer upload: $?"
		continue
	fi

	log 1 " - Limpando WorkDir e SourceBucket"
	rm -rf ${ANO}/${MES}/${dia}/
	aws s3 rm --recursive --quiet ${BucketSource}/${ANO}/${MES}/${dia}/
	log 1 " - Concluido - ${ANO}/${MES}/${dia} - comprimidos ${_SourceSize} em ${_FinalSize}!"
    fi
    continue

done
log 1 "Concluido!"
if [ -z "$Result" ]; then
    rm -rf $ANO/$MES
fi


