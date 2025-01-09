### Import libraries ###
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
import sys
import config_database as dbconf
import config_aws as awsconf

if __name__ == "__main__":
    ### Configure spark with an APP_NAME and get the spark session ###
    sc = SparkContext(appName=sys.argv[0])
    
    ### Configure and get the SQL context ###
    sqlContext = HiveContext(sc)
    
    ### Configure the spark session with the Amazon S3 Keys ###
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", awsconf.AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", awsconf.AWS_SECRET_ACCESS_KEY)
    
    
    sql = "(SELECT distinct RFSID_VEICULO as veiculoObgFinan FROM manutencao.cliente_obg_financeira WHERE RFSTAG_FUNCIONALIDADE LIKE 'SAS_WEB_CCA%') obrigacao"
    
    veiculosObrigacao = sqlContext.read.format("jdbc"). \
         option("url", dbconf.DOMINIO). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", sql). \
         load()
    
    ### Select all vechiles' political usage ###
    sql = "(select distinct coalesce(agrveigru.agrveiveioid,agrveisub.agrveiveioid) as veiculoPolUso \
    				        ,pouval.pousvalvalor as politicaVelo \
    				from dispositivo.politica_uso pou \
    				inner join dispositivo.politica_uso_restricao poures \
    				        on pou.pousoid = poures.pousrepousoid \
    				        and poures.pousrepoustipoid = 1 \
    				inner join dispositivo.politica_uso_valor pouval \
    				        on poures.pousrepousvaloid = pouval.pousvaloid \
    				left join dispositivo.veiculo_grupo veigru \
    				        on veigru.veigrupousoid = pou.pousoid \
    				        and veigru.veigrudt_exclusao is null \
    				left join dispositivo.veiculo_subgrupo veisub \
    				        on veisub.veisubgrupousoid = pou.pousoid \
    				        and veisub.veisubgrudt_exclusao is null \
    				left join dispositivo.agrupamento_veiculo agrveigru \
    				        on agrveigru.agrveiveigruoid = veigru.veigruoid \
    				left join dispositivo.agrupamento_veiculo agrveisub \
    				        on agrveisub.agrveiveisubgruoid = veisub.veisubgruoid \
    				where pou.pousdt_exclusao is null \
    				and (veigru.veigruoid is not null or veisub.veisubgruoid is not null)) politica"
    
    veiculosPoliticaUso = sqlContext.read.format("jdbc"). \
         option("url", dbconf.DOMINIO). \
         option("driver", "org.postgresql.Driver"). \
         option("dbtable", sql). \
         load()
    
    ### Generate Stage dynamic table name
    stg_table_name = 'STG_GMT_'+sys.argv[3].replace('/','_').replace(' ','_')
    
    
    ### Read and put all data from files into a Spark dataframe ###
    hiveSql = "SELECT  b.cliente, \
                       b.veiculo as veiculoId, \
                       b.data_posicao_gmt0, \
                       b.data_posicao_short, \
                       b.latitude, \
                       b.longitude, \
                       CASE \
                	      WHEN c.payload_wsn.content.vid[0].status = 'active' THEN c.payload_wsn.content.vid[0].id \
                	         ELSE CASE WHEN c.payload_wsn.content.vid[1].status = 'active' THEN c.payload_wsn.content.vid[1].id \
                	            ELSE CASE WHEN c.payload_wsn.content.vid[2].status = 'active' THEN c.payload_wsn.content.vid[2].id \
                	               ELSE 0 \
                	            END \
                	         END \
                	      END AS vid, \
                       b.velocidade, \
                       CASE \
                         WHEN \
                              b.vdc_active_towed_counter > 0 AND \
                         THEN 1 ELSE 0 \
                       END as aceleracao_lateral, \
                       lateral_acc_max, \
                       lateral_acc_min, \
                       CASE \
                         WHEN \
                              b.abs_active_towed_counter >= 2 \
                         THEN 1 ELSE 0 \
                       END as frenagem_brusca, \
                       b.flag_new \
                FROM   (SELECT p.cliente, \
                               p.veiculo, \
                               p.data_posicao_gmt0, \
                               p.data_posicao_short, \
                               p.latitude, \
                               p.longitude, \
                               p.velocidade, \
                               p.payload_wsn as payload_wsn, \
                               a.payload_ebs.content.abs_active_towed_counter as abs_active_towed_counter, \
                               a.payload_ebs.content.vdc_active_towed_counter as vdc_active_towed_counter, \
                               a.payload_ebs.content.service_brake_counter as service_brake_counter, \
                               a.payload_ebs.content.lateral_acc_max as lateral_acc_max, \
                               a.payload_ebs.content.lateral_acc_min as lateral_acc_min, \
                               p.flag_new \
                        FROM   {0} p  \
                        LATERAL VIEW OUTER explode(payload_ebs) a AS payload_ebs) b \
                LATERAL VIEW OUTER explode(b.payload_wsn) c as payload_wsn".format(stg_table_name)
    
    veiculos = sqlContext.sql(hiveSql)
    
    ### Create a temporary view of the Spark dataframes for join ###
    veiculos.createOrReplaceTempView("veiculos")
    veiculosObrigacao.createOrReplaceTempView("obrigacao")
    veiculosPoliticaUso.createOrReplaceTempView("politica")
    
    sql = "SELECT v.cliente, \
                  v.veiculoId, \
                  v.vid as truckId, \
                  v.data_posicao_gmt0, \
                  v.data_posicao_short, \
                  v.latitude, \
                  v.longitude, \
                  v.aceleracao_lateral, \
                  v.lateral_acc_max, \
                  v.lateral_acc_min, \
                  v.frenagem_brusca, \
                  CASE \
                     WHEN v.velocidade > p.politicaVelo THEN 1 ELSE 0 END as velocidade, \
                  v.flag_new \
           FROM   veiculos  v, \
                  obrigacao o, \
                  politica p \
           WHERE  v.veiculoId = o.veiculoObgFinan \
            AND   v.veiculoId = p.veiculoPolUso"
    
    report = sqlContext.sql(sql)
    
    ### Create a temporary view of the current Spark dataframe. In this case, that was necesary to transpose the vechiles infractions from columns to lines ###
    report.createOrReplaceTempView("report")
    
    ### Make a query to transpose vechiles infractions from columns to lines ###
    transposedReport = sqlContext.sql ("SELECT cliente as ID_CLIENTE, veiculoId as ID_VEICULO, data_posicao_gmt0 as DAT_POSICAO, latitude as LATITUDE, longitude as LONGITUDE, truckId as TRUCK_ID, '1' as ID_TIPO_INFRACOES, lateral_acc_max as LATERAL_ACC_MAX, lateral_acc_min as LATERAL_ACC_MIN, flag_new FROM report WHERE aceleracao_lateral = 1 \
                                       UNION ALL \
                                       SELECT cliente as ID_CLIENTE, veiculoId as ID_VEICULO, data_posicao_gmt0 as DAT_POSICAO, latitude as LATITUDE, longitude as LONGITUDE, truckId as TRUCK_ID, '2' as ID_TIPO_INFRACOES, 0 as LATERAL_ACC_MAX, 0 as LATERAL_ACC_MIN, flag_new FROM report WHERE frenagem_brusca = 1 \
                                       UNION ALL \
                                       SELECT cliente as ID_CLIENTE, veiculoId as ID_VEICULO, data_posicao_gmt0 as DAT_POSICAO, latitude as LATITUDE, longitude as LONGITUDE, truckId as TRUCK_ID, '3' as ID_TIPO_INFRACOES, 0 as LATERAL_ACC_MAX, 0 as LATERAL_ACC_MIN, flag_new FROM report WHERE velocidade = 1")
    
    ### Configure the Redshift parameters ###
    s3TempDir = awsconf.REDSHIFT_S3_TEMP_OUTPUT
    redshiftURL = dbconf.REDSHIFT_DW_SASCAR_URL
    
    ## Create Detailed Report
    detailedReport = transposedReport.filter("flag_new = 1").drop("flag_new")

    ### Make a connection to Redshift and write the data into the table 'FT_CC_INFRACOES_DETALHADO' ###
    detailedReport.write \
        .format("com.databricks.spark.redshift") \
        .option("url", redshiftURL) \
        .option("tempdir", s3TempDir) \
        .option("dbtable", "FT_CC_INFRACOES_DETALHADO") \
        .mode("append") \
        .save()
    
    ### Configure the date to format 'YYYY-MM-dd' ###
    finalReport = transposedReport.select("ID_CLIENTE", "ID_VEICULO", F.date_format("DAT_POSICAO", "YYYY-MM-dd").alias("DAT_POSICAO"), "ID_TIPO_INFRACOES")
    
    ### Get the total vechile infractions by type ###
    consolidatedReport = finalReport.groupBy("ID_CLIENTE", "ID_VEICULO", "DAT_POSICAO", "ID_TIPO_INFRACOES").agg(F.count("ID_TIPO_INFRACOES").alias("TOT_INFRACOES"))
    
    ### Write the dataframe into Hive ###
    consolidatedReport.write.mode("overwrite").saveAsTable("stageInfracoes")
    
    ### Close connection from Dominios and the spark session (current context) ###
    sc.stop()
    
    
    
    
    
    
    
    
    
    
    