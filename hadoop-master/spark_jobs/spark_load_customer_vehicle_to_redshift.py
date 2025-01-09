# -*- coding: utf-8 -*-
##############################################################################
#### Job para Carga dos veículos dos clientes e de toda a hierarquia      ####
##############################################################################
"""
Created on 2019-02-26
@author: thomas.lima.ext

Updated on 2019-12-19
@author: thomas.lima.ext
@description: Alteração da query queryGetCustomerVehicleSql para buscar das novas tabelas 
              vehicle.vehicle_technical_solution e vehicle.vehicle_product_list
"""

### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
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
    
    #dateProcessBegin = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
    #dateProcessEnd = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
    
    #dateProcessShortBegin = (dateProcessBegin - timedelta(days=5)).strftime("%Y-%m-%d")
    #dateProcessShortEnd = (dateProcessEnd + timedelta(days=1)).strftime("%Y-%m-%d")
    
    #dateBegin = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S") - timedelta(days = 5)
    #dateEnd = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S") + timedelta(days = 1)
    #dateProcessShortBegin = dateBegin.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    #dateProcessShortEnd = dateEnd.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    
    #dateProcessShortBegin = dateProcessBegin.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    #dateProcessShortEnd = dateProcessEnd.strftime("%Y-%m-%d %H:%M:%S").split(' ')[0]
    
    
    ### get all  vehicles from a customer  ###
    queryGetCustomerVehicleSql = """(
    select 
        cus_p.cusoid as id_cliente,
        vts.vtsinfoveioid as id_veiculo
    from 
        customer.customer cus_p
        inner join lateral (
            with recursive customers(cusoid) as (
                select cust.cusoid from customer.customer cust where cust.cusoid = cus_p.cusoid
                union all
                select cus.cusoid as identifier
                from customers cust, customer.customer cus
                where cus.cusparentoid = cust.cusoid
            )
            select cusoid from customers
        ) as cus_h on true
        inner join vehicle.vehicle vei on vei.veicusoid = cus_h.cusoid
        inner join vehicle.vehicle_technical_solution vts on vts.vtsveioid = vei.veioid
        inner join vehicle.vehicle_product_list vpl on vpl.vplvtsoid = vts.vtsoid
        inner join "subscription".product p on p.prdoid = vpl.vplprdoid and p.prdclassification = 'EQ'
        inner join vehicle.equipment_status eqs on eqs.eqsoid = vpl.vpleqsoid
    where 
        1=1
        and vei.veideletedate is null
        and vts.vtsdeletedate is null
        and eqs.eqscondition = 'IN_VEHICLE'
    order by 1
    ) as tableGetCustomerVehicle"""

    opDbURL = 'jdbc:postgresql://{0}:5432/{1}?user={2}&password={3}'.format(dbconf.OP_HOST, dbconf.OP_DATABASE, dbconf.OP_USERNAME, dbconf.OP_PASSWORD)
    tableGetCustomerVehicle = sqlContext.read.format("jdbc"). \
         option("url", opDbURL). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", queryGetCustomerVehicleSql). \
         load()
    
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL

    ### Make a connection to Redshift and write the data into the table 'DM_CLIENTE_VEICULO' ###
    tableGetCustomerVehicle.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("dbtable", "DM_CLIENTE_VEICULO") \
        .option("tempdir", s3TempDir) \
        .option('sortkeyspec', 'SORTKEY(id_cliente)')\
        .option('postactions', 'GRANT SELECT ON  dm_cliente_veiculo TO sasweb_redshift_user')\
        .mode("overwrite") \
        .save()

    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()


