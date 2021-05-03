'''
Functions to return the current states and elections we have in our ballot database.
'''

import MySQLdb
from config import load_config

config = load_config()

cur_states = ['GA', 'NC']

active_elections = {'GA' : ['01_05_2021',], 'NC' : ['11_03_2020']}

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

def stats_has_date(cursor, proc_dt, elec_dt, county=None):
    table = 'county_stats' if county else 'state_stats'
    additional_where = 'AND county = "{county}"' if county else ''

    query = f'''
    SELECT COUNT(*)
    FROM {table} 
    WHERE election_dt = '{elec_dt}' AND
        proc_date = '{proc_dt}'
        {additional_where}
    ;
    '''

    cursor.execute(query)
    count = cursor.fetchone()['COUNT(*)']
    return count > 0
    

def time_series_has_date(cursor, proc_dt, elec_dt, county=None):
    table = 'county_time_series' if county else 'state_time_series'
    additional_where = 'AND county = "{county}"' if county else ''

    query = f'''
    SELECT COUNT(*)
    FROM {table} 
    WHERE election_dt = '{elec_dt}' AND
        proc_date = '{proc_dt}'
        {additional_where}
    ;
    '''

    cursor.execute(query)
    count = cursor.fetchone()['COUNT(*)']
    return count > 0
    

