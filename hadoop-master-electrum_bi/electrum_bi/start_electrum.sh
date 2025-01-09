# $1 = script a ser executado
# $2 = id do cliente
# $3 = 

## LIMPA CONSOLE
clear
echo "............Inicio do Script............"

## BUSCA DATA ATUAL DO SISTEMA
cur_date=`date +%Y-%m-%d\ %H:%M:%S`

echo "Data inicio job: $cur_date"
echo "Job a ser executado: $1"


## carrega bibliotecas java
JARS=$(echo "/home/hadoop/lib"/*.jar | tr ' ' ',')
## carrega arquivos de configuração python
CONFIG=$(echo "/home/hadoop/sascar/config"/*.py | tr ' ' ',')
## MONTA COMANDO DE CHAMADA DO SPARK COM TODOS OS PARAMETROS
comando2="spark-submit --queue default --deploy-mode cluster --master yarn --driver-memory 3G --num-executors 2 --executor-cores 2 --executor-memory 4G --py-files /home/hadoop/sascar/electrum_bi/$1,$CONFIG --jars $JARS --packages com.databricks:spark-xml_2.11:0.4.1,org.postgresql:postgresql:42.1.1"
count_par=1 
comando2+=" /home/hadoop/sascar/electrum_bi/$1"
#comando2+=" '$result' '$cur_date'"
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

end_date2=`date +%Y-%m-%d\ %H:%M:%S`
echo "Data fim job: $end_date2"

echo "............Fim do Script............"
