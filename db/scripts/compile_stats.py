import MySQLdb
import sys
import json
import logging, logging.config

from datetime import datetime, timedelta, date
from time import time

import schema
import queries

from current_data import cur_states as states, get_elections
from config import load_config

config = load_config()

logging.config.fileConfig(fname='log_config.ini')

logger = logging.getLogger('dev')

'''
State-level statistics
'''
for state in states:
    mydb = MySQLdb.connect(
        host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'],
        local_infile=1)
 
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute(schema.create_state_stats_table())
    
    elections = get_elections(cursor)
  
    for election in elections:
        logger.info(f'Computing {state} state-level statistics for {election}.')
        t0 = time()
        
        cured_table = f'cured_{election}'
        rej_table = f'rejected_{election}'

        cursor.execute(queries.get_processed_count(election))
        processed_tot = cursor.fetchall()[0]['num_processed']

        cursor.execute(queries.get_cured_count(election))
        cured_tot = cursor.fetchall()[0]['num_cured']

        cursor.execute(queries.get_rej_count(election))
        rej_tot = cursor.fetchall()[0]['num_rejected']

        logger.debug(f'proc, cured, rej done @ {time() - t0:.2f}s')

        cursor.execute(queries.get_rej_reasons(election))
        rej_reasons = json.dumps(str(cursor.fetchall()))

        logger.debug(f'reasons done @ {time() - t0:.2f}s')

        cursor.execute(queries.get_gender_count(election))
        gender_tot = json.dumps(str(cursor.fetchall()))

        cursor.execute(queries.get_gender_count(cured_table))
        gender_cured = json.dumps(str(cursor.fetchall()))

        cursor.execute(queries.get_gender_count(rej_table))
        gender_rej = json.dumps(str(cursor.fetchall()))

        logger.debug(f'gender done @ {time() - t0:.2f}s')

        cursor.execute(queries.get_race_count(election))
        race_tot = json.dumps(str(cursor.fetchall()))

        cursor.execute(queries.get_race_count(cured_table))
        race_cured = json.dumps(str(cursor.fetchall()))

        cursor.execute(queries.get_race_count(rej_table))
        race_rej = json.dumps(str(cursor.fetchall()))

        logger.debug(f'race done @ {time() - t0:.2f}s')
        
        cursor.execute(queries.get_age_count(election))
        age_tot = json.dumps(str(cursor.fetchall()))

        cursor.execute(queries.get_age_count(cured_table))
        age_cured = json.dumps(str(cursor.fetchall()))

        cursor.execute(queries.get_age_count(rej_table))
        age_rej = json.dumps(str(cursor.fetchall()))

        logger.debug(f'age done @ {time() - t0:.2f}s')
        
        # add entry in state_stats
        proc_date = date.today()
        election_dt = datetime.strptime(election, '%m_%d_%Y')

        query = schema.add_state_stat()
        cursor.execute(query, (proc_date, election_dt, 
            processed_tot, cured_tot, rej_tot,
            rej_reasons, 
            gender_tot, gender_cured, gender_rej, 
            race_tot, race_cured, race_rej, 
            age_tot, age_cured, age_rej))

        mydb.commit()
        logger.debug(f'Total time for {state} state-level statistics: {time() - t0:.2f}s')

    mydb.close()

'''
County-level statistics
'''
for state in states:
    mydb = MySQLdb.connect(
        host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'],
        local_infile=1)
 
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute(schema.create_county_stats_table())

    cursor.execute(queries.get_counties())
    counties = cursor.fetchall()

    elections = get_elections(cursor)
  
    for election in elections:
        logger.info(f'Computing {state} county-level statistics for {election}.')
        t0 = time()

        cured_table = f'cured_{election}'
        rej_table = f'rejected_{election}'

        for entry in counties:
            county = entry['county']

            cursor.execute(queries.get_processed_count(election, county))
            processed_tot = cursor.fetchall()[0]['num_processed']

            cursor.execute(queries.get_cured_count(election, county))
            cured_tot = cursor.fetchall()[0]['num_cured']

            cursor.execute(queries.get_rej_count(election, county))
            rej_tot = cursor.fetchall()[0]['num_rejected']

            cursor.execute(queries.get_rej_reasons(rej_table, county))
            reasons = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_gender_count(election, county))
            gender_tot = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_gender_count(cured_table, county))
            gender_cured = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_gender_count(rej_table, county))
            gender_rej = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_race_count(election, county))
            race_tot = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_race_count(cured_table, county))
            race_cured = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_race_count(rej_table, county))
            race_rej = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_age_count(election, county))
            age_tot = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_age_count(cured_table, county))
            age_cured = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_age_count(rej_table, county))
            age_rej = json.dumps(str(cursor.fetchall()))

            proc_date = date.today()
            election_dt = datetime.strptime(election, '%m_%d_%Y') 

            query = schema.add_county_stat()
            cursor.execute(query, (county, proc_date, election_dt, 
                processed_tot, cured_tot, rej_tot,
                rej_reasons, 
                gender_tot, gender_cured, gender_rej, 
                race_tot, race_cured, race_rej, 
                age_tot, age_cured, age_rej))
          
        mydb.commit()
        logger.debug(f'Total time for county-level statistics: {time() - t0:.2f}s')

    mydb.close()

