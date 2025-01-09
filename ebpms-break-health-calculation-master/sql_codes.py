update_insert_calculations = """BEGIN
                                    BEGIN
                                    DELETE FROM FT_EBPMS_CALCULO
                                            WHERE ID_VEICULO = {0}
                                            AND DAT_POSICAO = TO_DATE('{1}', 'YYYY-MM-DD HH24:MI:SS');
                                            
                                        INSERT INTO FT_EBPMS_CALCULO
                                           (ID_VEICULO,
                                            DAT_POSICAO,
                                            PESO,
                                            ROLRET,
                                            SINT,
                                            BRAKEPERFOMANCE,
                                            INC,
                                            PESO_MED,
                                            RETARDERON,                                            
                                            DT_FIRST_ROW_DF,
                                            DT_LAST_ROW_DF,
                                            FLG_CALC_FIRST_ROW_DF,
                                            CONFIDENCE_RANGE,
                                            UPPER_LIMIT,
                                            LOWER_LIMIT)
                                         VALUES({0},
                                                TO_DATE('{1}', 'YYYY-MM-DD HH24:MI:SS'),
                                                {2},
                                                {3},
                                                {4},
                                                {5},
                                                {6},
                                                {7},
                                                {8},
                                                TO_DATE('{9}', 'YYYY-MM-DD HH24:MI:SS'),
                                                TO_DATE('{1}', 'YYYY-MM-DD HH24:MI:SS'),
                                                {11},
                                                {12},
                                                {13},
                                                {14});
                                                
                                        UPDATE FT_PAYLOAD_EBPMS ebpms
                                        SET FLAG_CALC = 1 
                                        WHERE HDR_MESSAGE_DATE_TIME = TO_DATE('{9}', 'YYYY-MM-DD HH24:MI:SS')
                                        AND ID_CLIENTE = {10}
                                        AND ID_VEICULO = {0};
                                        
                                    EXCEPTION WHEN OTHERS THEN
                                            NULL;
                                    END;
                                END;                       
                                    """

query_select_ebpms = """SELECT
                                HDR_MESSAGE_DATE_TIME,
                                FREE_EBPMS_RETARDER,
                                FREE_EBPMS_TIME,
                                FREE_EBPMS_DURATION,
                                FREE_EBPMS_SPEED_AVERAGE,
                                FREE_EBPMS_ALTITUDE_VARIATION,
                                FREE_EBPMS_SPEED_BEGIN,
                                FREE_EBPMS_SPEED_END,
                                FREE_EBPMS_BRAKE_AVERAGE,
                                SPEED_VARIATION, 
                                NVL(DECELERATION,0) as DECELERATION,
                                NVL(BRAKE_DISTANCE,0) as BRAKE_DISTANCE,
                                NVL(RETARDERON,0) as RETARDERON,
                                ABS_ACTIVE_TOWING_COUNTER,
                                ABS_ACTIVE_TOWED_COUNTER, 
                                EBS_LOAD /1000 AS EBS_LOAD,
                                ID_CLIENTE,
                                ID_VEICULO, 
                                NVL(EBS_DIFF_TOWING_TOWED_PCT,0) as EBS_DIFF_TOWING_TOWED_PCT,
                                FLAG_CALC
                            FROM FT_PAYLOAD_EBPMS
                            WHERE FREE_EBPMS_DURATION < 25
                            AND FREE_EBPMS_SPEED_BEGIN > 10
                            AND EBS_LOAD > 0 AND EBS_LOAD < 40000
                            AND ABS_ACTIVE_TOWING_COUNTER = 0
                            AND SPEED_VARIATION != 0
                            AND FLAG_CHANGE_LOAD = 0
                            AND FLAG_CALC = 0
                            AND ID_VEICULO = {0}
                            AND EBS_LOAD >= {1}
                            ORDER BY HDR_MESSAGE_DATE_TIME
                        """

delete_equal_calculations_ebpms = """DELETE FROM FT_EBPMS_CALCULO
                                            WHERE ID_VEICULO = {0}
                                            AND DAT_POSICAO = TO_DATE('{1}', 'YYYY-MM-DD HH24:MI:SS')"""