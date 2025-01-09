"""
    Unit for generic variables and methods
"""

def treat_db_null(value=None, is_str=False):
    if not value and is_str:
        return ''
    elif not value:
        return 'NULL'
    
    return value