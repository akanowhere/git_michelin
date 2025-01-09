merge_sensor = '''
               BEGIN
	             MERGE
                 INTO
	             FT_CC_SENSOR_PNEU_AGREGADO agg_t1
	             	USING (
	             SELECT
	                {0} ID_CLIENTE,
	                {1} ID_VEICULO,
	                {2} ID_SENSOR,
	                '{3}' POSICAO_PNEU,
	                TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS') DAT_ULT_POSICAO_SENSOR,
	                TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS') DAT_ULT_POSICAO_VEICULO,
	                TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS') DAT_PRIM_POSICAO_SENSOR,
	                {5} DAT_PRIM_ALERTA_BATERIA
	             FROM
	             	DUAL
                 ) agg_t2 ON (agg_t1.ID_CLIENTE = agg_t2.ID_CLIENTE
	                          AND agg_t1.ID_VEICULO = agg_t2.ID_VEICULO
	                          AND agg_t1.ID_SENSOR = agg_t2.ID_SENSOR)
	            WHEN MATCHED THEN
                  UPDATE
                  SET
                  	agg_t1.DAT_ULT_POSICAO_SENSOR =
                         CASE
                  		 WHEN agg_t2.DAT_ULT_POSICAO_SENSOR > agg_t1.DAT_ULT_POSICAO_SENSOR THEN agg_t2.DAT_ULT_POSICAO_SENSOR
                  		  ELSE agg_t1.DAT_ULT_POSICAO_SENSOR
                           END,
                      agg_t1.DAT_PRIM_ALERTA_BATERIA = CASE 
                                                       WHEN (agg_t1.DAT_PRIM_ALERTA_BATERIA IS NULL AND agg_t2.DAT_PRIM_ALERTA_BATERIA IS NOT NULL) THEN 
                                                         agg_t2.DAT_PRIM_ALERTA_BATERIA
                                                       ELSE 
                                                         agg_t1.DAT_PRIM_ALERTA_BATERIA
                                                       END,
                    agg_t1.POSICAO_PNEU = agg_t2.POSICAO_PNEU
                  WHERE
                  	agg_t1.ID_CLIENTE = agg_t2.ID_CLIENTE
                  	AND agg_t1.ID_VEICULO = agg_t2.ID_VEICULO
                  	AND agg_t1.ID_SENSOR = agg_t2.ID_SENSOR
                  WHEN NOT MATCHED THEN
                    INSERT
                    	(ID_CLIENTE,
                    	 ID_VEICULO,
                    	 ID_SENSOR,
                    	 POSICAO_PNEU,
                    	 DAT_ULT_POSICAO_SENSOR,
                    	 DAT_ULT_POSICAO_VEICULO,
                    	 DAT_PRIM_POSICAO_SENSOR,
                    	 DAT_PRIM_ALERTA_BATERIA)
                    VALUES(agg_t2.ID_CLIENTE,
                           agg_t2.ID_VEICULO,
                           agg_t2.ID_SENSOR,
                           agg_t2.POSICAO_PNEU,
                           agg_t2.DAT_ULT_POSICAO_SENSOR,
                           agg_t2.DAT_ULT_POSICAO_VEICULO,
                           agg_t2.DAT_PRIM_POSICAO_SENSOR,
                           agg_t2.DAT_PRIM_ALERTA_BATERIA);
             EXCEPTION 
             WHEN DUP_VAL_ON_INDEX THEN
               UPDATE FT_CC_SENSOR_PNEU_AGREGADO agg_t1
                  SET
               	  agg_t1.DAT_ULT_POSICAO_SENSOR =  CASE
               		WHEN TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS') > agg_t1.DAT_ULT_POSICAO_SENSOR THEN 
               		  TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS')
               		ELSE 
               		  agg_t1.DAT_ULT_POSICAO_SENSOR
                       END,
                    agg_t1.DAT_PRIM_ALERTA_BATERIA = CASE 
                                                    WHEN (agg_t1.DAT_PRIM_ALERTA_BATERIA IS NULL AND {5} IS NOT NULL) THEN 
                                                      {5}
                                                    ELSE 
                                                      agg_t1.DAT_PRIM_ALERTA_BATERIA
                                                    END,
                    agg_t1.POSICAO_PNEU = '{3}'
                WHERE
               	    agg_t1.ID_CLIENTE = {0}
               	AND agg_t1.ID_VEICULO = {1}
               	AND agg_t1.ID_SENSOR = {2};
             END;
   '''

update_position_sql = '''
                   BEGIN
                    UPDATE FT_CC_SENSOR_PNEU_AGREGADO sa
                       SET sa.DAT_ULT_POSICAO_VEICULO = CASE
  		                                                WHEN TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS') > sa.DAT_ULT_POSICAO_VEICULO THEN 
  		                                                  TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS')
  		                                                ELSE 
  		                                                  sa.DAT_ULT_POSICAO_VEICULO
                                                        END
                       WHERE ID_CLIENTE = {2}
                         AND ID_VEICULO = {1};
                EXCEPTION WHEN OTHERS THEN
                  NULL;
                END;
'''