# -*- coding: utf-8 -*-
"""
Created on Wed Feb 20 17:22:33 2019

@author: carlos.santanna.ext
"""

from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys


if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Get all the actived Truck and Assets IDs from the day before untinl now
    hiveSql = """SELECT CLIENTE AS ID_CLIENTE,
                        B.VID.ID AS TRUCK_ASSET_ID,
                        B.VID.TAG AS APELIDO
                 FROM POSICOES_CRCU
                 LATERAL VIEW OUTER EXPLODE(PAYLOAD_WSN) A AS PAYLOAD_WSN
                 LATERAL VIEW OUTER EXPLODE(A.PAYLOAD_WSN.CONTENT.VID) B AS VID
                 WHERE DATA_POSICAO_SHORT >= CAST(DATE_SUB(CURRENT_DATE, 1) AS STRING)
                  AND A.PAYLOAD_WSN IS NOT NULL
                  AND B.VID.STATUS = 'active'"""
    
    truckAssets = sqlContext.sql(hiveSql)
    
    # Remove duplicated rows
    truckAssetsDist = truckAssets.distinct()
    
    truckAssetsDist.write.mode("overwrite").saveAsTable("stg_tag_truck_asset")

    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()
    






