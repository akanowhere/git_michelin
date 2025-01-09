# -*- coding: utf-8 -*-
##############################################################################
#### spark_streaming_op_load_positions_from_wdl_to_hive.py                ####
#### Job para Carga das posições da WDL no Hive com o objetivo de gravar  ####
#### o ROName(identificador do veiculo) e a data da posicao               ####
#### -------------------------------------------------------------------- ####
#### Versões                                                              ####
#### 1 - Fabio Schultz - 12-06-2018                                       ####
####     Criação do Processo                                              ####
##############################################################################


### Import libraries ###
import sys  
import json 
from pyspark.sql.functions import lit, col, unix_timestamp, from_unixtime
import datetime as dt
from pyspark import SparkContext  
from pyspark.sql import HiveContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


### function to save rdd into hive ###
def SaveRecord(rdd):  
    if rdd.count() > 0:
        df=rdd.toDF()
        date_insert_short = str(dt.datetime.now().strftime("%Y-%m-%d"))
        
        newDF = df.select(col("_1").alias("roname"), from_unixtime(unix_timestamp(col("_2"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).alias('date_position_gmt0').cast("timestamp"))
        
        dateInsertDF = newDF.withColumn("date_insert", lit(dt.datetime.now()))
        
        finalDF = dateInsertDF.withColumn("date_insert_short", lit(date_insert_short))
        
        finalDF.write.mode("append").format("orc").insertInto("stg_last_position_wdl")

if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.optimize.sort.dynamic.partition", "true")
    
    ### Configure and get the Sreaming context###
    ssc = StreamingContext(sc, 10)
    
    ### Get Kafka Configuration ### 
    #brokers = "10.142.0.11:9092,10.142.0.46:9092,10.142.0.19:9092"
    #brokers = "172.16.32.12:9092,172.16.32.19:9092,172.16.32.20:9092"
    brokers = "172.19.24.12:9092,172.19.24.19:9092,172.19.24.20:9092"
    groupId = "spark-streming"
    topic = 'topicWDLDataToAWS'
    
    kafkaParams = {"metadata.broker.list": brokers}
    kafkaParams["auto.offset.reset"] = "smallest"
    kafkaParams["enable.auto.commit"] = "false"
    
    
    ### start streaming ###
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    #directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)
    
    ### parse message ###
    parsed = directKafkaStream.map(lambda v: json.loads(v[1]))
    
    ### parse result with ROName and EvtDateTime ###
    lineResult = parsed.map(lambda m: (m['ROName'], m['EvtDateTime']) )
    
    ### call function to save positions in hive ###
    lineResult.foreachRDD(SaveRecord)
    
    ### Start Streaming ###
    ssc.start()
    ssc.awaitTermination()
