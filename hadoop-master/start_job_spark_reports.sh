################################################################################################
## Job generico para chamada de programas Spark - PySpark
## Executa controle para buscar a ultima data final
## Passa ultima data final como data inicial e a data atual como data final para o job
## Mantem os parametros passados na chamada do JOB
## Não aceita comandos de memoria ou distribuição de jobs
## Ex.: Chamada ./spart_job.sh programa_python.py param1 param2 etc...
## Os parametros passados podem ser acessados de dentro do pypark a partir do sys.argv[3] 
###############################################################################################
## LIMPA CONSOLE
clear
echo "............Inicio do Script............"

## BUSCA DATA ATUAL DO SISTEMA
cur_date=`date +%Y-%m-%d\ %H:%M:%S`

echo "Data inicio job: $cur_date"
echo "Job a ser executado: $1"

## MONTA COMANDO DE CHAMADA DO CONTROLE INICIAL
comando1="python /home/hadoop/sascar/python_jobs/python_controle_inicial.py $1"
## monta lista de parametros
count_par=1
parametros=''
for par in "$@"
do
    if test $count_par -eq 1; then
	    count_par=`expr $count_par + $count_par` 
    else
      if test $count_par -eq 2; then
          parametros=" '["
          parametros+="$par"
          count_par=`expr $count_par + $count_par` 
      else 
          parametros+=",$par"
      fi
    fi
done
if test $count_par -gt 2; then
  parametros+="]'"
fi
comando1+=" $parametros" 
echo "Chama Controle Inicial: $comando1"
## EXECUTA COMANDO E PEGA O RESULTADO NA VARIAVEL RESULT
result=`$comando1`
EXIT_CODE=$?
## TESTA EXIT CODE PARA TRATAMENTO DE ERRO E SAI DO PROGRAMA SE EXECUÇÃO TERMINOU COM ERROS
if [ $EXIT_CODE -ne 0 ] ; then
	echo "ERRO"
	echo "Erro ao executar comando: $comando1"
	exit 1
fi


## carrega bibliotecas java
JARS=$(echo "/home/hadoop/lib"/*.jar | tr ' ' ',')
## carrega arquivos de configuração python
CONFIG=$(echo "/home/hadoop/sascar/config"/*.py | tr ' ' ',')
## MONTA COMANDO DE CHAMADA DO SPARK COM TODOS OS PARAMETROS
comando2="spark-submit --queue default --deploy-mode cluster --master yarn --driver-memory 3G --num-executors 2 --executor-cores 2 --executor-memory 2G --py-files /home/hadoop/sascar/spark_jobs/$1,$CONFIG --jars $JARS --packages com.databricks:spark-xml_2.11:0.4.1,org.postgresql:postgresql:42.1.1"
count_par=1 
comando2+=" /home/hadoop/sascar/spark_jobs/$1"
comando2+=" '$result' '$cur_date'"
for par in "$@"
do
    if test $count_par -eq 1; then
	count_par=`expr $count_par + $count_par` 
    else
	comando2+=" '$par'"
    fi
done
echo "Chama Programa Principal: $comando2"

## EXECUTA O COMANDO SPARK E FAZ TRATAMENTO DE ERRO
eval $comando2
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ] ; then
        echo "ERRO "
        echo "ERRO Para uma descrição detalhada execute o comando yarn logs -applicationId APPLICATIONID"
        echo "ERRO O APPLICATIONID pode ser obtido no sysout logo acima"
        echo "ERRO ao executar comando: $comando2"
        exit 1
fi

## BUSCA DATA DE FIM DE EXECUÇÃO SPARK
end_date=`date +%Y-%m-%d\ %H:%M:%S`

## CHAMA CONTROLE FINAL COM OS PARAMENTROS UTILIZADOS NAS EXECUCOES
comando3="python /home/hadoop/sascar/python_jobs/python_controle_final.py $1 $parametros  '$result' '$cur_date' '$cur_date' '$end_date'"
echo "Chama Programa Final: $comando3"
eval $comando3
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ] ; then
        echo "ERRO"
        echo "Erro ao executar comando: $comando3"
        exit 1
fi

end_date2=`date +%Y-%m-%d\ %H:%M:%S`
echo "Data fim job: $end_date2"

echo "............Fim do Script............"
