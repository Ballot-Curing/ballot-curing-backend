
'''
Queries to gather statistics of interest.
'''
import MySQLdb
import json
from datetime import datetime 

import schema
from current_data import mysql_connect

class Election:

    def __init__(self, cursor, state, proc_date=None):
        self.cursor = cursor
        self.state = state

        self.county = None

        if proc_date:
            self.set_proc_date(proc_date)

    def set_county(self, county):
        self.county = county

    def set_proc_date(self, proc_dt):
        global proc_date 
        proc_date = proc_dt

    def set_elec_dt(self, elec_dt):
        self.elec_dt = elec_dt

        self.elec_str = datetime.strftime(elec_dt, '%m_%d_%Y')
        self.cured_table = f'cured_{self.elec_str}'
        self.rej_table = f'rejected_{self.elec_str}'

    def get_processed(self):
        self.cursor.execute(get_processed_count(self.elec_str, self.county))
        return self.cursor.fetchall()[0]['num_processed']
        
    def get_cured(self):
        self.cursor.execute(get_cured_count(self.elec_str, self.county))
        return self.cursor.fetchall()[0]['num_cured']

    def get_rejected(self):
        self.cursor.execute(get_rej_count(self.elec_str, self.county))
        return self.cursor.fetchall()[0]['num_rejected']

    def get_rej_reasons(self):
        self.cursor.execute(get_rej_reasons(self.elec_str, self.county))
        return json.dumps(str(self.cursor.fetchall()))

    def get_gender_counts(self):
        counts = {}

        self.cursor.execute(get_gender_count(self.elec_str, self.county))
        counts['total'] = json.dumps(str(self.cursor.fetchall()))

        self.cursor.execute(get_gender_count(self.cured_table, self.county))
        counts['cured'] = json.dumps(str(self.cursor.fetchall()))

        self.cursor.execute(get_gender_count(self.rej_table, self.county))
        counts['rejected'] = json.dumps(str(self.cursor.fetchall()))

        return counts

    def get_race_counts(self):
        counts = {}

        self.cursor.execute(get_race_count(self.elec_str, self.county))
        counts['total'] = json.dumps(str(self.cursor.fetchall()))

        self.cursor.execute(get_race_count(self.cured_table, self.county))
        counts['cured'] = json.dumps(str(self.cursor.fetchall()))

        self.cursor.execute(get_race_count(self.rej_table, self.county))
        counts['rejected'] = json.dumps(str(self.cursor.fetchall()))

        return counts

    def get_age_counts(self):
        counts = {}

        self.cursor.execute(get_age_count(self.elec_str, self.county))
        counts['total'] = json.dumps(str(self.cursor.fetchall()))

        self.cursor.execute(get_age_count(self.cured_table, self.county))
        counts['cured'] = json.dumps(str(self.cursor.fetchall()))

        self.cursor.execute(get_age_count(self.rej_table, self.county))
        counts['rejected'] = json.dumps(str(self.cursor.fetchall()))

        return counts
    
    def get_unique_processed(self):
        self.cursor.execute(get_unique_per_day(self.elec_str, self.county))
        return self.cursor.fetchall()

    def get_unique_rej(self):
        self.cursor.execute(get_unique_rej_per_day(self.elec_str, self.county))
        return self.cursor.fetchall()

    def get_unique_cured(self):
        self.cursor.execute(get_unique_cured_per_day(self.elec_str, self.county))
        return self.cursor.fetchall()

def get_counties():
    return f'''
    SELECT county
    FROM counties;
    '''

def get_rejected(table, cured_db):
    return f'''
    SELECT today.*
    FROM {table} as today
    LEFT JOIN {cured_db} as cured
    ON today.voter_reg_id = cured.voter_reg_id
    WHERE 
    cured.voter_reg_id IS NULL 
    AND today.ballot_rtn_status = "R";
    '''

def get_cured(table):
    return f'''
    SELECT acc.*
    FROM {table} AS rej
    INNER JOIN ( 
            SELECT *
            FROM {table}
            WHERE ballot_rtn_status='A') acc
    ON acc.voter_reg_id = rej.voter_reg_id 
    WHERE rej.ballot_rtn_status = 'R';
    '''

def get_count(table, field_name, county, where=[]):
    where_clause = f'WHERE proc_date = "{proc_date}"' if proc_date else ''

    if where_clause:
        where_clause += f' AND county = "{county}"' if county else ''
    else:
        where_clause = f'WHERE county = "{county}"' if county else ''
    
    for additional_clause in where:
        where_clause += f' AND {additional_clause}'

    query = f'''
    SELECT COUNT(*) AS {field_name}
    FROM {table}
    {where_clause};
    '''

    return query

def get_processed_count(election, county=None):
    field_name = 'num_processed'
    table = election
    where_clause = f'WHERE proc_date = "{proc_date}"' if proc_date else ''

    query =  f'''
    SELECT COUNT(DISTINCT voter_reg_id) AS {field_name}
    FROM {table}
    {where_clause};
    '''

    print(query)
    return query

def get_cured_count(election, county=None):
    table = f'cured_{election}'
    return get_count(table, 'num_cured', county)

def get_rej_count(election, county=None):
    table = f'rejected_{election}'
    return get_count(table, 'num_rejected', county)

def get_multi_count(table, col, county, where=[]):
    where_clause = f'WHERE proc_date = "{proc_date}"' if proc_date else ''
    
    if where_clause:
        where_clause += f' AND county = "{county}"' if county else ''
    else:
        where_clause = f'WHERE county = "{county}"' if county else ''
    
    for additional_clause in where:
        where_clause += f' AND {additional_clause}'
   
    return f'''
    SELECT {col}, COUNT({col}) AS {col}_count
    FROM (
            SELECT voter_reg_id, {col}
            FROM {table} 
            {where_clause}
            GROUP BY voter_reg_id, {col}
         ) temp
    GROUP BY {col};    
    ''' 
    
def get_gender_count(election, county=None):
    return get_multi_count(election, 'gender', county)

def get_race_count(election, county=None):
    return get_multi_count(election, 'race', county)

def get_age_count(election, county=None):
    where_clause = f'WHERE proc_date = "{proc_date}"' if proc_date else ''

    if where_clause:
        where_clause += f' AND county = "{county}"' if county else ''
    else:
        where_clause = f'WHERE county = "{county}"' if county else ''

    return f'''
    SELECT
        CONVERT(SUM(IF(age BETWEEN 18 AND 29,1,0)), CHAR) AS '18-29',
        CONVERT(SUM(IF(age BETWEEN 30 AND 44,1,0)), CHAR) AS '30-44',
        CONVERT(SUM(IF(age BETWEEN 45 AND 64,1,0)), CHAR) AS '45-64',
        CONVERT(SUM(IF(age BETWEEN 65 and 119,1,0)), CHAR) AS '65-119'
    FROM (
        SELECT voter_reg_id, age
        FROM {election}
        {where_clause}
        GROUP BY voter_reg_id, age
    ) temp; 
    '''

  #  return get_multi_count(election, 'age', county) # if want each individual age

def get_rej_reasons(election, county=None):
    where = ['ballot_rtn_status = "R"'];

    return get_multi_count(election, 'ballot_issue', county, where)

def get_unique_per_day(table, county=None):
    return f'''
    SELECT
        proc_date,
        COUNT(proc_date) AS count,
        GREATEST(COUNT(proc_date) - LAG(COUNT(proc_date), 1) OVER (ORDER BY proc_date), 0) AS diff
    FROM {table}
    GROUP BY proc_date ORDER BY proc_date;
    '''

def get_unique_rej_per_day(election, county=None):
    table = f'rejected_{election}'
    return get_unique_per_day(table, county)

def get_unique_cured_per_day(election, county=None):
    table = f'cured_{election}'
    return get_unique_per_day(table, county)

# State specific queries

def get_cured_NC(table):
    return f'''
    SELECT *
    FROM {table}
    WHERE ballot_issue = 'ACCEPTED - CURED';
    '''

def get_rejected_NC(table):
    return f'''
    SELECT *
    FROM {table}
    WHERE ballot_rtn_status = 'R';
    '''
