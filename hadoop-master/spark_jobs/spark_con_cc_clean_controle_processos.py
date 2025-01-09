# -*- coding: utf-8 -*-
##############################################################################
#### spark_con_cc_clean_controle_processos.py                             ####
#### Limpa a tabela controle de processos deixando as ultimas 30 execucoes####
#### -------------------------------------------------------------------- ####
#### Versoes                                                              ####
#### 1 - Fabio Schultz - 06-02-2018                                       ####
####     Criacao do Processo                                              ####
##############################################################################


### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys
import logging
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
  
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)

    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ### Create temporary table having the last 30 lines
    createTempTable = "create table tmp_controle_processos as (select " \
              "nome_processo, " \
              "parametros as parametros," \
              "data_inicio_filtro, " \
              "data_fim_filtro, " \
              "data_inicio_execucao, " \
              "data_fim_execucao " \
              "from ( " \
              "select  " \
              "controle_processos.*, " \
              "rank() over (partition by nome_processo, parametros order by data_inicio_execucao desc) as ranking " \
              "from controle_processos ) x " \
              "where ranking <= 30 )" 
   
    dropTable = "drop table controle_processos purge"

    createTable = "CREATE TABLE controle_processos ( " \
                  "nome_processo STRING COMMENT 'Nome do processo/job', " \
                  "parametros STRING COMMENT 'Demais parametros utilizados para chamada do job', " \
                  "data_inicio_filtro TIMESTAMP  COMMENT 'Data de inicio para filtro de Data', " \
                  "data_fim_filtro TIMESTAMP COMMENT 'Data de fim para filtro de Data', " \
                  "data_inicio_execucao TIMESTAMP COMMENT 'Data de inicio da execução', " \
                  "data_fim_execucao TIMESTAMP COMMENT 'Data de fim da execução' " \
                  ") COMMENT 'Tabela para controle de extrações de jobs pyspark'" 

    insertFromTemp = "insert into controle_processos (select * from tmp_controle_processos) "

    dropTempTable = "drop table tmp_controle_processos purge"

    ## Execute SQL ##
    try:
        sqlContext.sql(createTempTable)
        sqlContext.sql(dropTable)
        sqlContext.sql(createTable)
        sqlContext.sql(insertFromTemp)
        sqlContext.sql(dropTempTable)
    except Exception as ex:
        logger.error("ERRO durante a criação da tabela temporaria tmp_controle_processos")
        raise Exception(ex)

    ### Close spark session (current context) ###
    sc.stop()