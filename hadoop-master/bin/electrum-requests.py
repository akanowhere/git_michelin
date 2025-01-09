#!/usr/bin/python -u
#
import sys, os, getopt
from datetime import datetime, timedelta
import exceptions
import psycopg2
import json
sys.path.append('/home/hadoop/sascar/config')
import config_database as chd

def Log( *data ):
    """ Log de execucao """
    for line in data:
        print("%s %s" % ( datetime.now(), line ) )

def Param2Date( dt ):
    """ Identifica o formato de data, e atribui 00:00 caso nao seja informado horario"""
    try:
        result = datetime.strptime(dt, "%Y-%m-%d %H:%M")
    except exceptions.ValueError:
        result = datetime.strptime(dt, "%Y-%m-%d")
    except Exception as e:
        print("Deu ruim: %s = %s" % ( type(e), str(e) ) )
        result = None

    return result

def ConnectPg():
    global conn

    try:
        conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(chd.OP_DATABASE, chd.OP_USERNAME, chd.OP_HOST, chd.OP_PASSWORD))

    except Exception as e:
        Log("* ERRO: Falha ao conectar: %s" % str(e))
        sys.exit(1)

def FileType(typeReport):
    reports = {
                'INDEX' : 'XML',
                'MONITORING' : 'XML',
                'STATUS' : 'XML',
                'INTERVENTION' : 'JSON'
              }
    return reports.get(typeReport, "ERROR")

def CreateRequest(ReportType):
    """ Inserir a requisicao no banco de dados """
    global conn
    global PartnerOID, DatetimeFrom, DatetimeTo, ReportStatus

    sql = """
    INSERT INTO report.report_partner_solicitation (
            rpspartneroid, rpsdatetimefrom, rpsdatetimeto,
            rpsinsertedby,
            rpsreporttype,
            rpssolicitationdate,
            rpsfiletype, rpsfilecompressiontype,
            rpsreportstatus,
            rpsfullrequest
        )
        VALUES ( %(partner)s, %(datefrom)s, %(dateto)s,
                 95,
                %(reporttype)s,
                %(solicitationdate)s,
                %(filetype)s, 'GZIP',
                %(reportstatus)s,
                %(fullrequest)s )

        RETURNING rpsoid

        """

    FullRequest  = '{"callBackEndPoint": "N3.Troubleshooting","compressingMethod": "NONE","days": 5,"fileFormat": "XML","identifierSender": "ShortData","messageType": "Empty","resultCollection": [],"scopeParams": [],"startTimestamp": "2019-03-21T11:00:00.000Z","startTimestampLong": 1553166000000,"todayTimestampLong": 1553605750535}'
    try:
        cur = conn.cursor()
        cur.execute( sql, {
                "partner": PartnerOID, "datefrom": DatetimeFrom, "dateto": DatetimeTo,
                "reporttype": ReportType,
                "solicitationdate": datetime.now(),
                "filetype": FileType( ReportType ),
                "reportstatus": ReportStatus,
                "fullrequest": FullRequest
            })

        id = cur.fetchone()
        Log("- Requisicao de '%s' registrada com rpsoid %s" % ( ReportType, id[0] ) )

        OIDs.append(str(id[0]))

        conn.commit()
        cur.close()
    except Exception as e:
        Log("* ERRO: Ao criar o registro: %s" % str(e))
        conn.rollback()
        sys.exit( 1 )


def Usage():
    print("""

        --intervention | -i Criar report INTERVENTION
        --monitoring | -m   Criar report MONITORING
        --status | -s       Criar report STATUS
        --index | -x        Criar report INDEX

        --from DATA         Data de inicio da requisicao
        --to DATA           Data de termino da requisicao

        --progress          Cria report com status IN_PROGRESS
        --rs STATUS         Indica um status desejado

        --partner ID        Partner a ser criado (Default: 3)

        """)

def main(argv):
    """ Execucao das rotinas """
    global conn
    global PartnerOID, DatetimeFrom, DatetimeTo, ReportStatus
    global OIDs

    OIDs = list()

    PartnerOID = 3
    DatetimeFrom = datetime.now() - timedelta( hours = 1 )   
    DatetimeTo   = datetime.now()
    ReportStatus = 'IN_PROGRESS'

    try:
        opts, arg = getopt.getopt(argv,"xsmi",
                    [
                        "intervention","status", "monitoring","index",
                        'from=', 'to=',
                        'partner=',
                        'progress', 'reportstatus=', 'rs='
                    ])
    except getopt.GetoptError:
        Usage()
        sys.exit(2)

    Monitoring = None
    Intervention = None
    Index = None
    Status = None
    BadArgs = False
    for opt,val in opts:
        if opt in [ '--intervention', '-i' ]:
            Intervention = True

        elif opt in [ '--monitoring', '-m' ]:
            Monitoring = True

        elif opt in [ '--index', '-x' ]:
            Index = True

        elif opt in [ '--status', '-s' ]:
            Status = True

        elif opt in [ '--progress', '--rs' ]:
            ReportStatus = 'IN_PROGRESS'

            if opt == '--rs' and val != '':
                ReportStatus = val

        elif opt in [ '--from' ]:
            DatetimeFrom = Param2Date( val )

        elif opt in [ '--to' ]:
            DatetimeTo = Param2Date( val )

        else:
            BadArgs = True
            if val != '':
                print "  opt=%s  val=%s" % ( opt, val )
            else:
                print "  opt=%s" % ( opt )

    if BadArgs:
        Log("Argumentos invalidos informados...")
        Usage()
        sys.exit( 1 )
    if not ( Index or Monitoring or Status or Intervention ):
        Log("* ERRO: Nenhum relatorio indicado!")
        sys.exit( 1 )

    Log("Criando requisicoes com os parametros:")
    Log("- DateFrom: %s" % DatetimeFrom )
    Log("-   DateTo: %s" % DatetimeTo )
    Log("-   Status: %s" % ReportStatus )
    Log("-  Partner: %s" % PartnerOID )

    ConnectPg()
    if Index:
        CreateRequest( 'INDEX' )

    if Monitoring:
        CreateRequest( 'MONITORING' )

    if Status:
        CreateRequest( 'STATUS' )

    if Intervention:
        CreateRequest( 'INTERVENTION' )


    Log("Requisicoes criadas:")
    Log("  time python -u ~/sascar/python_jobs/python_op_execute_report.py %s %s" % ( ReportStatus, ",".join(OIDs) ) )

    sys.exit( 0 )


if __name__ == "__main__":
   main(sys.argv[1:])

