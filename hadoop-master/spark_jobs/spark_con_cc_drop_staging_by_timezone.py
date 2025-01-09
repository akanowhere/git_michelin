# -*- coding: utf-8 -*-
##############################################################################
#### spark_con_cc_drop_staging_by_timezone.py                             ####
#### Deleta Tabela de Staging para processos usando timezone              ####
#### -------------------------------------------------------------------- ####
#### Versoes                                                              ####
#### 1 - Fabio Schultz - 02-02-2018                                       ####
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

    
    ## Validate Timezone Parameter
    if len(sys.argv) < 4:
        logger.error("ERRO: Quantidade de parametros incorreta, verifique o parametro de timezone")
        raise
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)

    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    ## Generate temp and Stage table names
    stg_table_name = 'STG_GMT_'+sys.argv[3].replace('/','-').replace(' ','-')
    
    ### Create temporary table to select all vehicle and days who have process ###)
    hiveSql = "DROP TABLE {0} ".format(stg_table_name)
   
    ## Execute SQL ##
    try:
        sqlContext.sql(hiveSql)
    except Exception as ex:
        if 'not found in database ' in str(ex):
            logger.error("ERRO "+str(ex)+" ao executar o comando "+hiveSql)
            logger.error("ERRO A tabela a ser deletada não existe")
            logger.error("ERRO Talvez o parametro de timezone esta incorreto, verifique")
            logger.error("ERRO Talvez alguma ação manual tenha sido tomada e esse job executou sem necessidade")
            raise Exception(ex)
        else:
            logger.error("ERRO não conhecido")
            raise Exception(ex)
        
    ### Close spark session (current context) ###
    sc.stop()
