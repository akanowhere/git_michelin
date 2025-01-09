#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import psycopg2
from boto.s3.connection import S3Connection
sys.path.append('/home/hadoop/sascar/config')
import config_database as chd
import config_aws as awsconf
import general_config as genconf
import subprocess
from datetime import datetime, timedelta
from glob import glob
import shutil, exceptions


def Log( *data ):
    """ Log de execucao """
    for line in data:
        print("%s %s" % ( datetime.now(), line ) )

def switch_report(typeReport):
    """ Retorna o script Spark a ser executado """
    reports = {
                'INDEX' : 'spark_op_rel_index.py',
                'MONITORING' : 'spark_op_rel_monitoring.py',
                'STATUS' : 'spark_op_rel_status.py',
                'INTERVENTION' : 'spark_op_rel_intervention.py',
                'ALERT' : 'spark_op_rel_alerts.py'
              }
    return reports.get(typeReport, "ERROR")

def switch_report_tag(typeReport):
    """ Retorna a TAG a utilizar no relatorio """
    reports = {
                'INDEX' : 'dataTrailersStatus',
                'MONITORING' : 'dataTrailersMonitoring',
                'STATUS' : 'dataTrailersStatus',
                'INTERVENTION' : 'dataInterventionReport',
                'ALERT' : 'alertsData'
              }
    return reports.get(typeReport, "ERROR")

def consolidate_files( reportId, reportName ):
    """ Executa a consolidacao dos arquivos gerados pelo Spark e encerra relatorio """

    Log( "- Consolidando reportId %s" % reportId )

    global conn
    cur = conn.cursor()

    try:

        ### Rename generated file ###
        sql = """
            SELECT rpsoid, rpsreportfullpath, rpsfilename, rpsfiletype, rpsfilecompressiontype
              FROM report.report_partner_solicitation
             WHERE rpsoid = {0}""".format( reportId )
        cur.execute(sql)

        # Fetch the report solicitation row
        files = cur.fetchall()

        cur.close()

        if not files:
            Log( "  Nenhum registro encontrado!" )
            return False

        # Set the report type
        fileType = files[0][3]

        # Set the begin name of the report temporary file
        tempFileName = str(files[0][2])

        part = tempFileName.split('.')
        if len(part) > 1:
            tempFileName = part[0]

        # Set the compress type if it is the case
        fileCompressType = str(files[0][4])

        # Set the main folder in the S3 bucket for Michelin report files integration
        dstFileName = 'MichelinSolutions/'

        # Set the folder with the files from the report generation. In this case the 'tempFileName' is only a directory in S3 with those files
        prefixIn = 'MichelinSolutions/' + tempFileName

        # Path to create a temporary folder to precesso the files of the currente report
        tempFilePath = "%s/%s/" % ( genconf.PATH_ELECTRUM_TEMP_FILE, reportId )
        filePath = ''
        Log( "  fileType:%s tempFileName:%s fileCompressType:%s tempFilePath:%s" % ( fileType, tempFileName, fileCompressType, tempFilePath ) )

        command = "hdfs dfs -get %s/%s %s" % ( genconf.PATH_ELECTRUM_HDFS, reportId, genconf.PATH_ELECTRUM_TEMP_FILE ) 
        Log("  Recuperando dados: %s" % command )
        exitCode = os.system( command )
        if exitCode != 0:
            Log("* ERRO: Falha ao transferir dados do HDFS!")
            os.exit( 1 )

        fileParts = glob('%s/part-0*' % tempFilePath )
        fileParts.sort()

        #-- Limitando o tamanho do arquivo em 04/06/2020 alterado p/ 6G ---  2G (1.8GB por seguranca)
        SizeFile=0
        for fname in fileParts:
            SizeFile += os.path.getsize( fname )

        if SizeFile > 6.0 * 1024 ** 3:
            Log( "* ERRO: rpsoid=%s Arquivo excedeu tamanho maximo (%.6fGB) - Reduzir o periodo!" % ( reportId, SizeFile / 1024 ** 3 ) )
            # Desvia o fluxo pra nao repetir limpeza ;)
            raise exceptions.StopIteration

        Log( "  Consolidacao dos dados de %s arquivos" % len(fileParts) )
        if (fileType == 'XML'):
            # Update the report file name with the chosen format
            tempFileName = tempFileName + '.xml'

            # Open the temporary file in the folder
            filePath = tempFilePath + tempFileName
            F = open(filePath,'w')

            # Set the XML main header for the file report
            strHeader = '<?xml version="1.0" encoding="UTF-8"?>\n'
            F.write(strHeader)

            # Open the main tag for the file report
            reportTag = switch_report_tag(reportName)
            F.write('<' + reportTag + '>\n')

            # Process all the files from the generated report
            #for key in bucket.list(prefix=prefixIn):
            for fname in fileParts:
                Log("  - %s" % fname )
                fr = open(fname, 'r')
                file = fr.read()
                fr.close()

                # Check if there is any content in the file
                if (len(file) > 1):
                   file = file.replace('<ROWS>', '')
                   file = file.replace('</ROWS>', '')

                   file = file.replace('<NotificationElement>', '')
                   file = file.replace('</NotificationElement>', '')
                   F.write(file)

            # Close the main tag for the file report
            F.write('</' + reportTag + '>\n')

            # Close file report
            F.close()

        else: # JSON format
            # Update the report file name with the chosen format
            tempFileName = tempFileName + '.json'

            # Open the temporary file in the folder
            filePath = tempFilePath + tempFileName
            F = open(filePath,'w')

            # Start the file with '[' character to transform the object to a json list
            F.write('[\n')

            # Variables to control the firts row to be record in the file to control the ',' character
            isFirstRow = 1

            for fname in fileParts:
                # Get file's all content
                Log("  - %s" % fname )
                fr = open(fname, 'r')
                file = fr.read()
                fr.close()

                # Check if there is any content in the file
                if (len(file) > 1):

                   # Read the file line by line
                   for line in file.splitlines():
                       # Check if it is the first row. In this case, do not put the ',' character in the beginning of the line
                       if (isFirstRow == 1):
                           F.write(line + '\n')
                           isFirstRow = 0

                       else:
                           F.write(',' + line + '\n')

            # Finish the file with ']' character to close the json list
            F.write(']')
            F.close()

        # Check for compressed file
        if (fileCompressType == 'GZIP'):
            Log( "  Iniciando compressao" )
            # Compress the file
            compressCommand = "gzip -c {0} > {0}.gzip".format(filePath)

            exitCode = os.system(compressCommand)

            if (exitCode != 0):
                raise Exception("Error on compressing to gzip. Check if the file '{0}' was correct generated.").format(filePath)

            else:
                #print('No Compress')
                # Change the file name
                tempFileName = tempFileName + ".gzip"
                filePath = filePath + ".gzip"

        # Copy file to S3 bucket
        s3PathToCopy = awsconf.AWS_S3_BUCKET_MSOL + tempFileName
        copyCommand = "aws s3 cp {0} {1} >& /dev/null".format(filePath, s3PathToCopy)
        Log("  Upload do consolidado: %s" % copyCommand )
        exitCode = os.system(copyCommand)

        # Copy a BKP file in S3
        s3PathToCopyBkp = awsconf.AWS_S3_BUCKET_MSOL + "BACKUP/" + tempFileName
        copyCommand2 = "aws s3 cp {0} {1} >& /dev/null".format(s3PathToCopy,s3PathToCopyBkp)
        Log("  Backup do arquivo consolidado: %s" % copyCommand2 )
        exitCode2 = os.system(copyCommand2)


        # Get s3 file md5
        md5 = ''
        if (exitCode != 0):
            #Log('File NOT copied')
            raise Exception("It was not possible to get the md5. Check if the file '{0}' on S3 was correct copied.").format(s3PathToCopy)

        md5Command = 'md5sum {0}'.format(filePath)
        Log('  Gerando hash MD5: %s' % md5Command)
        resultString = subprocess.check_output(md5Command, shell=True)
        md5 = resultString.split(' ')[0]
        Log('  -> %s' % md5 )

        # Query to update the last information about the report solicitation
        sql = """UPDATE report.report_partner_solicitation
               SET rpsreportstatus = 'DONE',
                   rpsreportmd5 = '{0}',
                   rpsfilename = '{1}'
               WHERE rpsoid = {2}""".format(md5, tempFileName, reportId)

        # Execute and commit the update query
        RunSQL( sql )

        return True

    #-- Excessao por tamanho do arquivo
    except exceptions.StopIteration:
        # Query to update the last information about the report solicitation
        sql = """UPDATE report.report_partner_solicitation
               SET rpsreportstatus = 'ERROR-TOO_BIG'
               WHERE rpsoid = {0}""".format(reportId)

        # Execute and commit the update query
        RunSQL( sql )
        return False

    except Exception as e:
        Log("* ERRO: consolidate_files( %s ): %s" % ( reportId, str(e) ) )
        return False

    finally:
        Log("  Expurgo de dados temporarios:" )
        ### delete old files and folder ### - Executado em qq situacao
        purgeHDFS = 'hdfs dfs -rm -r %s/%s' % ( genconf.PATH_ELECTRUM_HDFS, reportId )
        Log("  - Local: %s" % tempFilePath)
        Log("  -  HDFS: %s" % purgeHDFS)
        shutil.rmtree( tempFilePath, ignore_errors=True )
        exitCode = os.system( purgeHDFS )
        if exitCode != 0:
            Log("    ** ERRO: Falha ao fazer expurgo no HDFS!")

    Log("  Concluido!")

def NewRequest():
    """ Recupera novos relatorios, caso existam e trata duracao """

    global conn
    ### sql to get all report solicitations ###
    if len(sys.argv) == 1:
        sql = """
            SELECT rpsoid, rpsreporttype, rpsdatetimefrom, rpsdatetimeto
              FROM report.report_partner_solicitation
             WHERE rpsreportstatus = '{0}'
             ORDER BY rpsoid
             LIMIT 1
            """.format("REQUESTED")
    else:
        if len(sys.argv) != 3:
                print("Utilize a chamada com o nome do REPORT e o IDs separados por virgulas (,)\n")
                sys.exit( 1 )

        ReportStatus, ReportId = sys.argv[1:]
        sql = """
                SELECT rpsoid, rpsreporttype, rpsdatetimefrom, rpsdatetimeto
                  FROM report.report_partner_solicitation
                 WHERE rpsreportstatus = '{0}' AND rpsoid IN ( {1} )
                 ORDER BY rpsoid
            """.format( ReportStatus, ReportId )

    Log(" Recuperanco requisicoes: %s" % sql )

    ### execute sql ###
    cur = conn.cursor()
    cur.execute(sql)

    ### Fetch the result row ###
    rows = cur.fetchall()
    cur.close()

    return rows

def RunSQL( sql ):
    """ Executa as consultas e faz commit """
    global conn
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def ExecuteSpark( reportId, reportName ):
    """ Faz chamada ao processo Spark que gera os dados """
    global conn
    try:
        # Criar o caminho
        exitCode = os.system('hdfs dfs -mkdir -p %s/%s' % ( genconf.PATH_ELECTRUM_HDFS, reportId ) )
        if exitCode != 0:
            Log("** ERRO: Falha ao criar pasta de processamento!" )
            os.exit( 1 )

        ### get report script name ###
        sparkProgram = switch_report(reportName)

        ### validate report type
        if (sparkProgram == "ERROR"):
            Log("* ERRO: Erro na selecao de sparkProgram!")
            sql = "update report.report_partner_solicitation  \
                   set rpsreportstatus = '{0}' \
                      ,rpsreportfailuremessage = '{1}' \
                   where rpsoid = {2}".format("ERRO", "Invalid Report Type" ,row[0])

            RunSQL( sql )
        else:
            ### execute report spark process ###
            command = ("/home/hadoop/sascar/start_job_spark_michelin.sh {0} {1} >& /home/hadoop/logs/{2}.{1}.log").format(sparkProgram, reportId, reportName)

            Log("- Executando %s" % command )
            exitCode = os.system(command)

            if (exitCode != 0):
                Log("* ERRO: Processamento retornou %s para %s" % ( exitCode, command ) )
                sql = "update report.report_partner_solicitation  \
                       set rpsreportstatus = '{0}' \
                          ,rpsreportfailuremessage = '{1}' \
                       where rpsoid = {2}".format("ERRO", "Error calling program to execute report", reportId)

                RunSQL( sql )
                return False
    except Exception as e:
        Log("* ERRO: ExecuteSpark %s - %s: %s\n" % ( reportId, reportName, str(e)) )

    Log("- ExecuteSpark encerrou")

    return True


def main():
    """ Faz validacoes de path, chaves e dispara chamadas necessarias """

    global conn

    # Validacao de caminho para consolidacao de arquivos
    if not os.path.isdir( genconf.PATH_ELECTRUM_TEMP_FILE ):
        try:
            os.mkdir( genconf.PATH_ELECTRUM_TEMP_FILE )
        except Exception as e:
            Log("** ERRO: Pasta temporaria '%s' inexistente, e nao foi possivel criar!" % genconf.PATH_ELECTRUM_TEMP_FILE )
            os.exit( 1 )

    try:
        ### Set the variables for access S3 ###
        aws_access_key_id = awsconf.AWS_ACCESS_KEY_ID
        aws_secret_access_key = awsconf.AWS_SECRET_ACCESS_KEY
        aws_report_bucket = awsconf.AWS_S3_REPORT_BUCKET
        aws_repor_host = 's3-eu-west-1.amazonaws.com'

        # Open connection with S3 using the configured keys
        try:
            connS3 = S3Connection(aws_access_key_id,aws_secret_access_key, host=aws_repor_host)
            bucket = connS3.get_bucket(aws_report_bucket)

        except:
            Log("*** ERRO: Falha ao conectar no S3!")
            os.exit( 1 )

        ### Create conection to operational portal ###
        try:
            conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(chd.OP_DATABASE, chd.OP_USERNAME, chd.OP_HOST, chd.OP_PASSWORD))
        except Exception as e:
            Log("*** ERRO: Falha ao conectar na base de dados: %s" % str(e) )
            os.exit( 1 )

        rows = NewRequest()
        #    result, reportId, reportName = NewRequest()

        if not rows:
            #Log("- Nenhum registro para processamento")
            sys.exit( 0 )

        for row in rows:
            # Get report type name
            reportName = row[1]
            # Get report solicitation ID
            reportId = row[0]
            datetimeFrom = row[2]
            datetimeTo   = row[3]

            # LIMITE somente para MONITORING - objetivo garantir arquivos menores que 2 GB
            # Depende da Michelin enviar a cada hora!!
            #
            deltaDatetimeFrom = datetime.now() - datetimeFrom
            if reportName == 'MONITORING' and datetimeTo == None:
                datetimeTo = datetimeFrom + timedelta(hours = 2)
                #deltaDatetimeFrom = datetime.now() - datetimeFrom
                # se o (now - datetimeFrom) < 6:00:00 entao datetimeTo = datetimeFrom
                Log("DEBUG  now - datetimeFrom: %s " % deltaDatetimeFrom)

                if deltaDatetimeFrom < timedelta(hours = 6):
                    datetimeTo = datetimeFrom
                    Log(" Forcando datetimeTo = datetimeFrom pois o delta do datetimeFrom em relacao a agora eh: %s " % deltaDatetimeFrom)

                sql = "update report.report_partner_solicitation  \
                           set rpsreportstatus = '{0}',rpsdatetimeto='{1}' \
                           where rpsoid = {2} \
                          ".format("IN_PROCESS", datetimeTo, reportId)
                Log("- rpsoid %s - Data/hora limite ajustada para %s" % ( reportId, datetimeTo ) )
            elif reportName == 'STATUS' and deltaDatetimeFrom < timedelta(hours = 150):
                datetimeTo = datetimeFrom
                Log(" Forcando datetimeTo = datetimeFrom para o STATUS pois o delta do datetimeFrom em relacao a agora eh: %s " % deltaDatetimeFrom)
                sql = "update report.report_partner_solicitation  \
                           set rpsreportstatus = '{0}',rpsdatetimeto='{1}' \
                           where rpsoid = {2} \
                          ".format("IN_PROCESS", datetimeTo, reportId)
                Log("- rpsoid %s - Data/hora limite ajustada para %s" % ( reportId, datetimeTo ) )
            elif datetimeTo == None and deltaDatetimeFrom > timedelta(hours = 6):
                datetimeTo = datetimeFrom + timedelta(hours = 6)
                Log(" Forcando datetimeTo para +6h pois o delta do datetimeFrom em relacao a agora eh: %s " % deltaDatetimeFrom)

                sql = "update report.report_partner_solicitation  \
                           set rpsreportstatus = '{0}',rpsdatetimeto='{1}' \
                           where rpsoid = {2} \
                          ".format("IN_PROCESS", datetimeTo, reportId)
                Log("- rpsoid %s - Data/hora limite ajustada para %s" % ( reportId, datetimeTo ) )
            else:
                Log("- Iniciando processamento do reportId %s" % reportId )
                ### update reports to in process ###
                sql = "update report.report_partner_solicitation  \
                           set rpsreportstatus = '{0}' \
                           where rpsoid = {1} \
                          ".format("IN_PROCESS", reportId)

            # Execute and commit the update query
            RunSQL(sql)

            if ExecuteSpark( reportId, reportName ):
                if consolidate_files( reportId, reportName ):
                    Log("- Concluido com sucesso!")

    except Exception as e:
        Log("* ERRO: Falha na execucao: %s" % str(e) )
        sys.exit( 1 )




if __name__ == "__main__":

    main()
