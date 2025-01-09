# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 14:37:45 2018

@author: carlos.santanna.ext
"""

import psycopg2
import sys
sys.path.append('/home/hadoop/sascar/config')
import config_database as chd


if __name__ == "__main__":
    ## Query hive para inserir resultado
    if len(sys.argv) > 6:
        sql = "insert into controle_processos \
               (nome_processo, parametros, data_inicio_filtro, data_fim_filtro, data_inicio_execucao, data_fim_execucao) \
                values  ('{0}','{1}', '{2}', '{3}', '{4}', '{5}' )".format(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    else:	
        sql = "insert into controle_processos \
               (nome_processo, data_inicio_filtro, data_fim_filtro, data_inicio_execucao, data_fim_execucao) \
                values  ('{0}','{1}', '{2}', '{3}', '{4}')".format(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    
    ## abre conexão hive
    conn = psycopg2.connect(host=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_IP,
                            user=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_LOGIN,
                            port=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_PORTA,
                            password=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_SENHA,
                            dbname=chd.SERVIDOR_BD_REDSHIFT_DW_SASCAR_NOME)
    
    ## Cria Cursor a partir da conexão
    cur = conn.cursor()
    
    ## Executa SQL
    cur.execute(sql)
    
    conn.commit()
    
    ## Fecha Cursor
    conn.close()
    sys.exit()















