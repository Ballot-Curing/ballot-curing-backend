'''
Functions to return the current states and elections we have in our ballot database.
'''

cur_states = ['GA', 'NC']

def get_elections(cursor):
    query = '''
    SELECT election_dt
    FROM elections;
    '''
    
    cursor.execute(query)
    dates = cursor.fetchall()
    elections = [date['election_dt'].strftime('%m_%d_%Y') for date in dates]

    return elections

