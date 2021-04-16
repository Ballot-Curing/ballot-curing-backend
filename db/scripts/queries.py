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
    FROM 01_04_2021 AS rej
    INNER JOIN ( 
        SELECT *
        FROM 01_04_2021
        WHERE ballot_rtn_status='A') acc
    ON acc.voter_reg_id = rej.voter_reg_id 
    WHERE rej.ballot_rtn_status='R';
    '''

# Specific queries for NC for snapshot
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

def get_cured_count(election, county=None):
    table = f'cured_{election}'
    
    where_clause = f'WHERE county="{county}"' if county else ''

    return f'''
    SELECT COUNT(*) 
    FROM {table} 
    {where_clause};
    '''


def get_rej_count(election, county=None):
    table = f'rejected_{election}'

    where_clause = f'WHERE county="{county}"' if county else ''
    
    return f'''
    SELECT COUNT(*)
    FROM {table}
    {where_clause};
    '''

