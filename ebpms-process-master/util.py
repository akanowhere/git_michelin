TBL_EBPMS_PAYLOAD = 'FT_PAYLOAD_EBPMS'
DT_EBPMS_PAYLOAD  = 'HDR_MESSAGE_DATE_TIME'
TBL_EBPMS_CALCULO = 'FT_EBPMS_CALCULO'
DT_EBPMS_CALCULO  = 'DAT_POSICAO'
TIMEDELTA = 20 #minutes


def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]
