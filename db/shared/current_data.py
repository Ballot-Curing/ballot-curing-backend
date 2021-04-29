'''
Functions to return the current states and elections we have in our ballot database.
'''

import MySQLdb
from config import load_config

config = load_config()

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


def mysql_connect(state):
    mydb = MySQLdb.connect(
        host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'],
        local_infile=1)

    return mydb
