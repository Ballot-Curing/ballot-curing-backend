'''
Queries to gather statistics of interest.
'''

def get_counties():
    return f'''
    SELECT county
    FROM counties;
    '''


def get_today_rejected(table, today_datetime, cured_db):
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
    WHERE rej.ballot_rtn_status='R';
    '''

def get_count(table, field_name, county):
    where_clause = f'WHERE county="{county}"' if county else ''
    
    return f'''
    SELECT COUNT(*) AS {field_name}
    FROM {table}
    {where_clause};
    '''

def get_processed_count(election, county=None):
    return get_count(election, 'num_processed', county)

def get_cured_count(election, county=None):
    table = f'cured_{election}'
    return get_count(table, 'num_cured', county)

def get_rej_count(election, county=None):
    table = f'rejected_{election}'
    return get_count(table, 'num_rejected', county)

def get_multi_count(table, col, county, where=[]):
    where_clause = f'WHERE county="{county}"' if county else ''
    
    for additional_clause in where:
        where_clause += f' AND {additional_clause}' if where_clause else f'WHERE {additional_clause}'
   
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
    # consolidate age ranges
    where = f'WHERE county="{county}"' if county else ''

    return f'''
    SELECT
    SUM(IF(age BETWEEN 18 AND 29,1,0)) as '18-29',
    SUM(IF(age BETWEEN 30 AND 44,1,0)) as '30-44',
    SUM(IF(age BETWEEN 45 AND 64,1,0)) as '45-64',
    SUM(IF(age BETWEEN 65 and 119,1,0)) as '65-119'
    FROM (
        SELECT voter_reg_id, age
        FROM {election}
        {where}
        GROUP BY voter_reg_id, age
    ) temp; 
    '''

  #  return get_multi_count(election, 'age', county) # if want each individual age

def get_rej_reasons(election, county=None):
    where = ['ballot_rtn_status="R"'];

    return get_multi_count(election, 'ballot_issue', county, where)

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
