# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 11:39:06 2018

@author: carlos.santanna.ext
"""

from datetime import datetime, timedelta
from boto.s3.connection import S3Connection
import sys, os
sys.path.append('/home/hadoop/sascar/config')
import config_aws as awsconf

try:
    currentDate = datetime.now()
    
    aws_access_key_id = awsconf.AWS_ACCESS_KEY_ID
    aws_secret_access_key = awsconf.AWS_SECRET_ACCESS_KEY
    
    #Connect to S3 with access credentials
    conn = S3Connection(aws_access_key_id,aws_secret_access_key, host=awsconf.AWS_HOST)
    
    bucketName = awsconf.AWS_S3_BUCKET_NAME
    bucket = conn.get_bucket(bucketName)
    
    ### Get date to Hive table partition
    startDate = currentDate.date()
    endDate = startDate - timedelta(days=5)
    
    delta = startDate - endDate
    
    for i in range(delta.days + 1):
        dateProcessing = (startDate - timedelta(days=i)).strftime("%Y-%m-%d")
        dateSplited = dateProcessing.split("-")
        
        ### Get date parts separedly ###
        dateYear = dateSplited[0]
        dateMounth = dateSplited[1]
        dateDay = dateSplited[2]
        
        prefixIn = "POSICOES_CRCU/POSICOES/{0}/{1}/{2}".format(dateYear, dateMounth, dateDay)
        
        for key in bucket.list(prefix=prefixIn):
            filePath = key.key.split("/")
            fileName = filePath[len(filePath) - 1]
            prefixOut = "POSICOES_CRCU/POSICOES_IN_PROCESS/{0}/{1}/{2}/{3}".format(dateYear, dateMounth, dateDay, fileName)
            
            #keyDateTimeTuple = time.strptime(key.last_modified[:19], "%Y-%m-%dT%H:%M:%S")
            keyCreatedDate = key.last_modified.replace('T', ' ')
            keyCreatedDate = keyCreatedDate[:keyCreatedDate.find('.')]
            
            dataPosicao = dateYear + "-" + dateMounth + "-" + dateDay
            
            bucket.copy_key(prefixOut, bucketName, key.key)
            
            key.delete()
    
    sys.exit(os.EX_OK)
    
except Exception as e:
    sys.exit(str(e))
    
    
    
    
    
    
    