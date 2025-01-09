# -*- coding: utf-8 -*-
##############################################################################
#### spark_op_con_last_position_from_wdl.py                               ####
#### Job para consilidar e armazenar a ultima posicao de equipamentos     ####
#### que estao sendo enviados pela WDL                                    ####
#### -------------------------------------------------------------------- ####
#### Versões                                                              ####
#### 1 - Fabio Schultz - 12-06-2018                                       ####
####     Criação do Processo                                              ####
#### 2 - Kadu - 12-09-2018                                                ####
####     Alteração do Processo                                            ####
##############################################################################


### Import libraries ###
from pyspark import SparkContext  
import sys 
from pyspark.sql import HiveContext
from pyspark.sql.functions import max
from datetime import datetime, timedelta


if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    dateToProcess = datetime.now() - timedelta(days = 1)
    dateToProcessShort = dateToProcess.strftime("%Y-%m-%d")
    
    ### get max position from stage table ###
    #df1 = sqlContext.sql("select roname, max(date_position_gmt0) as date_position_gmt0 from stg_last_position_wdl group by roname")
    allPositions = sqlContext.sql("select roname, date_position_gmt0 as date_position_gmt0 from stg_last_position_wdl where date_insert_short = '{0}'".format(dateToProcessShort))
    
    ### get actual last position from ft ###
    ftPositions = sqlContext.sql("select roname, date_position_gmt0 as date_position_gmt0 from ft_last_position_wdl")
    
    ### union all stg and ft ###
    joinPositions = allPositions.unionAll(ftPositions)
    
    ### get last position from the union all  ###
    #maxPosition = joinPositions.select("roname", "date_position_gmt0").groupBy("roname").agg(max("date_position_gmt0").alias("date_position_gmt0"))
    maxPosition = joinPositions.groupBy("roname").agg(max("date_position_gmt0").alias("date_position_gmt0"))
    
    ### Save the ft last positions into a temp table ###
    maxPosition.write.mode("overwrite").saveAsTable("ft_last_position_wdl_tmp")
    
    ### Get the ft temp last positions ###
    tempMaxPositions = sqlContext.table("ft_last_position_wdl_tmp")
    
    ### overwrite ft table using a temp table  ###
    tempMaxPositions.write.mode("overwrite").saveAsTable("ft_last_position_wdl")
        
        
        
        