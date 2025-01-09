# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 11:42:03 2018

@author: carlos.santanna.ext
"""

from boto.s3.connection import S3Connection
from datetime import datetime
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
    
    prefixIn = "POSICOES_CRCU/POSICOES_IN_PROCESS"
    
    for key in bucket.list(prefix=prefixIn):
        #filePath = key.key.split("/")
        #fileName = filePath[len(filePath) - 1]
        filePath = key.key
        
        #prefixOut = "POSICOES_CRCU/POSICOES_PROCESSED/" + fileName
        prefixOut = filePath.replace('POSICOES_IN_PROCESS', 'POSICOES_PROCESSED')
        
        if(prefixOut.find('node') >= 0):
           bucket.copy_key(prefixOut, bucketName, key.key)
           
           #key.delete()
           
    for key in bucket.list(prefix=prefixIn):
        key.delete()
        
    sys.exit(os.EX_OK)
        
except Exception as e:
    print (str(e))
    