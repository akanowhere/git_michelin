soft_delete_dw = """UPDATE {1}
                       SET FLAG_CHANGE_LOAD = 1
                     WHERE ID_VEICULO = {0}
                       AND FLAG_CHANGE_LOAD != 1
                       AND {2} <=  (SELECT MAX(DATE_UP) AS DATA_SOFT_DELETE FROM ( SELECT DATE_UP, DIFF 
                                      FROM
                                      (
                                      SELECT DATE_FROM DATE_UP, 
                                             PESO_MAX - (LEAD(PESO_MAX) OVER (PARTITION BY ID_VEICULO ORDER BY dv.VERSAO DESC)) DIFF,
                                             NVL(dv.BAIXA_CARGA, '0') BAIXA_CARGA,
                                             NVL(LEAD(BAIXA_CARGA) OVER (PARTITION BY ID_VEICULO ORDER BY dv.VERSAO DESC), '0') BAIXA_CARGA_PREV,
                                             rank() OVER (PARTITION BY ID_VEICULO ORDER BY dv.VERSAO DESC) ORD
                                        FROM DM_VEICULO dv 
                                        WHERE dv.ID_VEICULO = {0}
                                      ) WHERE ORD = 1
                                         AND (DIFF != 0 OR (BAIXA_CARGA <> BAIXA_CARGA_PREV))))"""


select_of_from_dominio = """ SELECT distinct rfsid_veiculo
                                FROM manutencao.cliente_obg_financeira o, dispositivo.veiculo v
                                where o.rfsid_veiculo = v.veioid
                                and rfstag_funcionalidade = 'SAS_WEB_OPC_BPSPRC'
                                and peso_max > 0 """



update_ebmps_positions = """UPDATE  FT_PAYLOAD_EBPMS ebpms 
                      SET (ABS_ACTIVE_TOWING_COUNTER,
                           ABS_ACTIVE_TOWED_COUNTER,
                           EBS_LOAD,
                           ID_CLIENTE,
                           ID_VEICULO,
                           EBS_DIFF_TOWING_TOWED_PCT) = 
                                                    (SELECT DISTINCT ebs.ABS_ACTIVE_TOWING_COUNTER,
                                                     ebs.ABS_ACTIVE_TOWED_COUNTER,
                                                     ebs.PESO, ebs.ID_CLIENTE,
                                                     ebs.ID_VEICULO,
                                                     ebs.DIFF_TOWING_TOWED_PCT 
                                                     FROM FT_CC_POSICAO ebs 
                                                     WHERE ebpms.UNITIDENTIFIER = ebs.OWNER_ID 
                                                     AND ebpms.SESSIONKEY  = ebs.SESSION_NUMBER 
                                                     AND ebpms.HDR_MESSAGE_DATE_TIME = ebs.DAT_POSICAO 
                                                     AND ebpms.HDR_MESSAGE_DATE_TIME >= trunc(SYSDATE) 
                                                     AND ebs.DAT_RECEBIDO = (SELECT MAX(ebs2.DAT_RECEBIDO)
                                                     							FROM FT_CC_POSICAO ebs2
                                                     							WHERE ebs.DAT_POSICAO = ebs2.DAT_POSICAO
                                                     							AND ebs.ID_VEICULO = ebs2.ID_VEICULO)
                                                     AND ebs.ID_VEICULO = {0} 
                                                     AND ebs.DAT_POSICAO >= trunc(SYSDATE) 
                                                     AND ebs.PESO > 0) 
                      WHERE EXISTS (SELECT 1 FROM FT_CC_POSICAO ebs  
                                                    WHERE ebpms.UNITIDENTIFIER = ebs.OWNER_ID 
                                                    AND ebpms.SESSIONKEY  = ebs.SESSION_NUMBER 
                                                    AND ebpms.HDR_MESSAGE_DATE_TIME = ebs.DAT_POSICAO 
                                                    AND ebpms.HDR_MESSAGE_DATE_TIME >= trunc(SYSDATE) 
                                                    AND ebs.ID_VEICULO = {0} 
                                                    AND ebs.DAT_POSICAO >= trunc(SYSDATE) 
                                                    AND ebs.PESO > 0) 
                                                    AND ebpms.EBS_LOAD IS NULL 
                                                    AND ebpms.HDR_MESSAGE_DATE_TIME >= trunc(SYSDATE)"""

insert_into_queue = """ BEGIN
                      INSERT INTO VEI_EBPMS_PROCESSAMENTO(ID_VEICULO, DAT_INICIO_PROCESSAMENTO) 
                                                  values ({0}, TO_DATE('1970-01-01 00:00:00','YYYY-MM-DD hh24:mi:ss'));
                      EXCEPTION
                      WHEN DUP_VAL_ON_INDEX THEN
                        UPDATE VEI_EBPMS_PROCESSAMENTO SET
                         STATUS = 'PENDING',
                         ID_POD = NULL
                         WHERE ID_VEICULO in ({0})
                              AND ((SYSDATE - DAT_INICIO_PROCESSAMENTO)  * 24 * 60) > 30;           
                    END; """




sql_vehicle =  """ SELECT ID_VEICULO
                               FROM VEI_EBPMS_PROCESSAMENTO A
                               WHERE DAT_INICIO_PROCESSAMENTO IN (SELECT MIN(DAT_INICIO_PROCESSAMENTO) 
                               FROM VEI_EBPMS_PROCESSAMENTO B WHERE B.STATUS='PENDING')
                               AND STATUS='PENDING'
                               AND ROWNUM = 1
                               FOR UPDATE """

sql_update = """
                            BEGIN
                              UPDATE VEI_EBPMS_PROCESSAMENTO
                                 SET STATUS = 'PROCESSING',
                                     DAT_INICIO_PROCESSAMENTO = SYSDATE,
                                     ID_POD = '{1}'
                              WHERE ID_VEICULO = {0};
                            END;"""

sql_end_process =""" 
                               BEGIN
                                 UPDATE VEI_EBPMS_PROCESSAMENTO
                                    SET STATUS = 'PENDING',
                                        PROC_ERROR = '{1}',
                                        DAT_INICIO_PROCESSAMENTO = SYSDATE,
                                        ID_POD = NULL
                                 WHERE ID_VEICULO = {0};
                               END;"""
