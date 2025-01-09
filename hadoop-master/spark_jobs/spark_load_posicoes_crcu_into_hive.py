# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 09:10:20 2017 - @author: carlos.santanna.ext
Updated on Tue Feb  5 14:46:23 2019 - @author: thomas.lima.ext

"""

### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit
import sys, os
import config_database as dbconf
import config_aws as awsconf

if __name__ == "__main__":
   try:
       ### Configure spark with an APP_NAME and get the spark session ###
       sc = SparkContext(appName="Load posicoes CRCU into Hive")
    
       ### Configure and get the SQL context ###
       sqlContext = HiveContext(sc)
       
       ### Set global hive configurations for Spark to insert data into partitions in a dynamically way ###
       sqlContext.setConf("hive.exec.dynamic.partition", "true")
       sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
       sqlContext.setConf("hive.optimize.sort.dynamic.partition", "true")
       
       #paths = ["s3://sasweb2-arquitetura/POSICOES_CRCU/POSICOES/2017/*/*/*.mng05node", "s3://sasweb2-arquitetura/params_scripts/jsonParamSchema.json"]
       
       ### Configure the path input of the Json parameter and json files to be loaded
       pathJson = awsconf.AWS_S3_POSICOES
       pathS3Json = awsconf.AWS_S3_JSON_SCHEMA
       paths = [pathS3Json, pathJson]
       
       ### Get vehicle positions from S3 files ###
       posicoes = sqlContext.read.json(paths)
       
       ### Select all vechiles for the current timezone ###
       sql = "(SELECT distinct veigmtveioid as veiculogmtid, veigmt as veiculoTimezone FROM dispositivo.veiculo_gmt WHERE veigmtdt_exclusao IS NULL) a"
       
       ### Get vehicles GMT and put in a Spark dataframe ###
       veiculosGmt = sqlContext.read.format("jdbc"). \
         option("url", dbconf.DOMINIO). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", sql). \
         load()
       
       ### Get the respective vechile current timezone ###
       posicoesTimeZone = posicoes.join(veiculosGmt, posicoes.veiculo == veiculosGmt.veiculogmtid, "left_outer")
       
       ### Fill the vehicles without time zone with 'America/Sao_Paulo' (default Sascar) ###
       adjustedVehicles = posicoesTimeZone.fillna({'veiculoTimezone':'without-gmt'})
       
       ### Create a temporary view of the current Spark dataframe. In this case, that was necessary since Hadoop do not wokr very well with Unix Time (milisecs) ###
       adjustedVehicles.createOrReplaceTempView("posicoesTimeZoneTable")
       
       unixTimePosicoes = sqlContext.sql("SELECT bateriaExt, bateriaInt, blockVehicle, classeEq, cliente, from_utc_timestamp(from_unixtime(data_posicao,'yyyy-MM-dd HH:mm:ss'), veiculoTimezone) as data_posicao, from_unixtime(data_posicao,'yyyy-MM-dd HH:mm:ss') as data_posicao_gmt0, gps_valido, horimetro, ibuttonHex, ibuttonPart1, ibuttonPart2, id_motorista, ignicao, latitude, longitude, odometro, case when size(payload_ebs) > 0 then payload_ebs else null end as payload_ebs, case when size(payload_reefer) > 0 then payload_reefer else null end as payload_reefer, case when size(payload_tpm) > 0 then payload_tpm else null end as payload_tpm, case when size(payload_wsn) > 0 then payload_wsn else null end as payload_wsn, pos_memoria, case when pyaload_diag.header.session_number > 0 then pyaload_diag else null end as payload_diag, veiculo, velocidade, software_version, protocol_version, date_format(from_utc_timestamp(from_unixtime(data_posicao,'yyyy-MM-dd HH:mm:ss'), veiculoTimezone), 'yyyy-MM-dd') as data_posicao_short, regexp_replace(veiculoTimezone, '/', '-') as veitimezone FROM posicoesTimeZoneTable")
       
       unixTimePosicoes.write.mode("overwrite").saveAsTable("stageJson")
       
       sqlInsert = "FROM stageJson INSERT INTO TABLE posicoes_crcu PARTITION (data_posicao_short, veitimezone) SELECT bateriaExt, bateriaInt, blockVehicle, classeEq, cliente, data_posicao, data_posicao_gmt0, gps_valido, horimetro, ibuttonHex, ibuttonPart1, ibuttonPart2, id_motorista, ignicao, latitude, longitude, odometro, payload_ebs, payload_reefer, payload_tpm, payload_wsn, pos_memoria, payload_diag, veiculo, velocidade, CURRENT_TIMESTAMP as data_insert, software_version, protocol_version, data_posicao_short, veitimezone"
       
       sqlContext.sql(sqlInsert)
       
       sc.stop()
       
       sys.exit(os.EX_OK)
       
   except Exception as e:
       sys.exit(str(e))
