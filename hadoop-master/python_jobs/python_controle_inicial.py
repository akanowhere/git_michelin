# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 13:48:22 2018
Updated on Thu Oct 18 12:32:00 2018

@author: carlos.santanna.ext
"""

import psycopg2
from datetime import datetime, timedelta
import sys
sys.path.append('/home/hadoop/sascar/config')
import config_database as chd


if __name__ == "__main__":
    ## verifica se o job utiliza parametros para chamada
    if len(sys.argv) > 2:
        if (sys.argv[2].find("'") >= 0):
           sql = "select max(a.data_fim_filtro) as data_fim_filtro from controle_processos as a where a.nome_processo = '{0}' and a.parametros = {1}".format(sys.argv[1], sys.argv[2])
        else:
           sql = "select max(a.data_fim_filtro) as data_fim_filtro from controle_processos as a where a.nome_processo = '{0}' and a.parametros = '{1}'".format(sys.argv[1], sys.argv[2])
    else:
        sql = "select max(a.data_fim_filtro) as data_fim_filtro from controle_processos as a where a.nome_processo = '{0}' and a.parametros is null".format(sys.argv[1])

    ## Query Redshift para retornar parametro

    ## Abre conexão Redshift
    conn = psycopg2.connect(host=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_IP,
                           user=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_LOGIN,
                           port=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_PORTA,
                           password=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_SENHA,
                           dbname=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_NOME)

    ## Cria Cursor a partir da conexão
    cur = conn.cursor()

    ## Executa SQL
    cur.execute(sql)

    ## recupera primeira linha do cursor
    linha = cur.fetchone()

    ## Pega data do sitema
    dataAuxiliar = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    ## verifica se não retornou nadenva e retorna data correta
    if linha[0] is not None:
       dataAuxiliar = linha[0]

    ## Formata data de retorno
    dataRetorno = dataAuxiliar.strftime('%Y-%m-%d %H:%M:%S')

    conn.commit()

    ## Fecha Cursor
    conn.close()

    ## Imprime data de retorno para Shell recuperar
    print(dataRetorno)
