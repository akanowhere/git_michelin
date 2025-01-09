# -*- coding: utf-8 -*-
##############################################################################
#### spark_con_cc_create_staging_by_timezone.py                           ####
#### Cria Tabela de Staging para processos usando timezone                ####
#### -------------------------------------------------------------------- ####
#### Versoes                                                              ####
#### 1 - Fabio Schultz - 02-02-2018                                       ####
####     Criacao do Processo                                              ####
##############################################################################


### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
import logging
import sys
import config_database as dbconf
import config_aws as awsconf


if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    #Get the root logger
    logger = logging.getLogger()
    
    #Have to set the root logger level, it defaults to logging.WARNING
    logger.setLevel(logging.NOTSET)
    
    #Config log to show msgs in stdout
    logging_handler_out = logging.StreamHandler(sys.stdout)
    logging_handler_out.setLevel(logging.ERROR)
    logger.addHandler(logging_handler_out)
    
    ### Validate Timezone Parameter
    if len(sys.argv) < 4:
        logger.error("ERRO: Quantidade de parametros incorreta, verifique o parametro de GMT")
        raise
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ## Generate temp and Stage table names
    stg_table_name = 'STG_GMT_'+sys.argv[3].replace('/','-').replace(' ','-')
    
    ## Generate filter using parameter
    gmtFilter = sys.argv[3].replace('/','_').replace(' ','_')
    
    dateProcessBegin = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S") - timedelta(days = 5)
    dateProcessEnd = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
    
    dateProcessShortBegin = dateProcessBegin.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    dateProcessShortEnd = dateProcessEnd.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    
    redshiftSsql = "SELECT '''' || REPLACE(TIMEZONE, '/', '-') || '''' AS TIMEZONE \
                    FROM  TIMEZONE_GMT \
                    WHERE GMT = '{0}'".format(gmtFilter)
    
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    
    ### Get timezones to process to the current GMT ###
    redshiftDf = sqlContext.read \
                  .format("com.databricks.spark.redshift") \
                  .option("url", redshiftURL) \
                  .option("tempdir", s3TempDir) \
                  .option("query", redshiftSsql) \
                  .load()
    
    timezoneArray = [str(i.TIMEZONE) for i in redshiftDf.select("TIMEZONE").distinct().collect()]
    timezoneArrayList = ",".join(str(x) for x in timezoneArray)
    
    ### Create temporary table to select all vehicle and days who have process ###)
    hiveSql = "SELECT A.DATA_POSICAO_SHORT AS data_posicao_short_process, \
                      A.VEICULO AS  veiculo_process \
               FROM POSICOES_CRCU A  \
               WHERE A.VEITIMEZONE IN ({0}) \
                AND  A.DATA_POSICAO_SHORT >= '{1}' \
                AND  A.DATA_POSICAO_SHORT <= '{2}' \
                AND  A.DATA_INSERT > '{3}' \
                AND  A.DATA_INSERT <= '{4}'".format(timezoneArrayList, dateProcessShortBegin, dateProcessShortEnd, dateProcessBegin, dateProcessEnd)
    
    positionsDataShort = sqlContext.sql(hiveSql)
    positionsDataShortDistinct = positionsDataShort.distinct()
    
    ## Create a Stage Table with all positions using days and vehicles to process ##
    hiveSql = "SELECT CRCU.*, \
                      CASE \
                        WHEN CRCU.DATA_INSERT > '{0}' AND CRCU.DATA_INSERT <= '{1}' THEN 1 \
                        ELSE 0 \
                      END AS flag_new \
               FROM POSICOES_CRCU AS CRCU \
               WHERE CRCU.DATA_POSICAO_SHORT >= '{2}' \
                AND  CRCU.DATA_POSICAO_SHORT <= '{3}' \
                AND  VEITIMEZONE IN ({4})".format(dateProcessBegin, dateProcessEnd, dateProcessShortBegin, dateProcessShortEnd, timezoneArrayList)
    
    positions = sqlContext.sql(hiveSql)
    positionsDistinct = positions.distinct()
    
    positionsToProcess = positionsDistinct.join(positionsDataShortDistinct, (positionsDistinct.veiculo == positionsDataShortDistinct.veiculo_process) & (positionsDistinct.data_posicao_short == positionsDataShortDistinct.data_posicao_short_process))
    positionsFinal = positionsToProcess.drop('data_posicao_short_process').drop('veiculo_process')
    
    ## Record stage to Hive ##
    try:
        positionsFinal.write.format("orc").mode("overwrite").saveAsTable(stg_table_name)
    except Exception as ex:
        if 'already exists' in str(ex):
            logger.error("ERRO: A tabela de Staging ainda existe para o parametro utilizado")
            logger.error("ERRO: Verifique se toda a cadeia de processos foi executada com sucesso")
            logger.error("ERRO: Garanta que o processo final spark_con_cc_drop_staging_by_timezone.py  foi executado com o parametro "+ sys.argv[3])
            logger.error("ERRO: Apos isso execute o job novamente")
            raise
        else:
            raise
    
    redshiftSsql = "SELECT TABELA \
                    FROM CONTROLE_TABELAS_STG \
                    WHERE GMT ='{0}'".format(gmtFilter)
    
    redshiftDf = sqlContext.read \
                  .format("com.databricks.spark.redshift") \
                  .option("url", redshiftURL) \
                  .option("tempdir", s3TempDir) \
                  .option("query", redshiftSsql) \
                  .load()
    
    ## Verify if the generated stage table is the unic to be used by other process in the registers
    if (redshiftDf.count() > 1):
        logger.error("ERRO: Atenção cadastro incorreto, existe mais de uma tabela de STG para a timezone")
        logger.error("ERRO: Necessidade de tratamento manual")
        raise
    
    else:
        if (redshiftDf.count() == 0):
            stageDict = {}
            stageList = []
            stageDict['tabela'] = stg_table_name
            stageDict['gmt'] = gmtFilter
            stageList.append(stageDict)
            
            stageToInsert = sc.parallelize(stageList).toDF()
            
            stageToInsert.write \
                   .format("com.databricks.spark.redshift") \
                   .option("url", redshiftURL) \
                   .option("tempdir", s3TempDir) \
                   .option("dbtable", "CONTROLE_TABELAS_STG") \
                   .mode("append") \
                   .save()
                   
        else:
            if str(redshiftDf.first()['tabela']) != stg_table_name:
                logger.error("ERRO: a Tabela gerada para o GMT " + gmtFilter + " é a " + stg_table_name + ".")
                logger.error("ERRO: e a tabela cadastrada na tabela controle_tabelas_stg é a " + str(redshiftDf.first()['tabela']))
                logger.error("ERRO: Ação: Corrigir a tabela controle_tabelas_stg no Redshift e reinicar o processo")
                logger.error("ERRO: Necessidade de tratamento manual")
                raise
                 
    ### Close spark session (current context) ###
    sc.stop()
