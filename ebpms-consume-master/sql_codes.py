insert_postion_ebpms = """ BEGIN
                             BEGIN
                               INSERT INTO FT_PAYLOAD_EBPMS(unitidentifier,
                                                       sessionkey,
                                                       softwareversion,
                                                       HDR_PAYLOAD_VERSION,
                                                       HDR_PAYLOAD_TYPE,
                                                       HDR_PAYLOAD_CAUSE,
                                                       HDR_GENERATION_NUMBER,
                                                       HDR_PAYLOAD_NUMBER,
                                                       HDR_MESSAGE_DATE_TIME,
                                                       FREE_EBPMS_RETARDER,
                                                       FREE_EBPMS_TIME,
                                                       FREE_EBPMS_DURATION,
                                                       FREE_EBPMS_SPEED_AVERAGE,
                                                       FREE_EBPMS_ALTITUDE_VARIATION,
                                                       FREE_EBPMS_SPEED_BEGIN,
                                                       FREE_EBPMS_SPEED_END,
                                                       FREE_EBPMS_BRAKE_AVERAGE,
                                                       speed_variation)
                                                VALUES ('{0}',
                                                        '{1}',
                                                        '{2}',
                                                        {3},
                                                        {4},
                                                        {5},
                                                        {6},
                                                        {7},
                                                       TO_DATE('{8}', 'YYYY-MM-DD HH24:MI:SS'),
                                                        {9},
                                                       TO_DATE('{10}', 'YYYY-MM-DD HH24:MI:SS'),
                                                        {11},
                                                        {12},
                                                        {13},
                                                        {14},
                                                        {15},
                                                        {16},
                                                        {17} );
                            EXCEPTION WHEN OTHERS THEN
                                        NULL;
                                END;
                            END; """
