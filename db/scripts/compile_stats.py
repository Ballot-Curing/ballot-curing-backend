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

def compute_state_stats(proc_date):
    '''
    State-level statistics
    
    proc_date : datetime
        The date to process stats up until
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

            print(queries.get_processed_count(election, proc_date))
            processed_tot = cursor.fetchall()[0]['num_processed']

            cursor.execute(queries.get_cured_count(election, proc_date))
            cured_tot = cursor.fetchall()[0]['num_cured']

            cursor.execute(queries.get_rej_count(election, proc_date))
            rej_tot = cursor.fetchall()[0]['num_rejected']

            logger.debug(f'proc, cured, rej done @ {time() - t0:.2f}s')

            cursor.execute(queries.get_rej_reasons(election, proc_date))
            rej_reasons = json.dumps(str(cursor.fetchall()))

            logger.debug(f'reasons done @ {time() - t0:.2f}s')

            cursor.execute(queries.get_gender_count(election, proc_date))
            gender_tot = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_gender_count(cured_table, proc_date))
            gender_cured = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_gender_count(rej_table, proc_date))
            gender_rej = json.dumps(str(cursor.fetchall()))

            logger.debug(f'gender done @ {time() - t0:.2f}s')

            cursor.execute(queries.get_race_count(election, proc_date))
            race_tot = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_race_count(cured_table, proc_date))
            race_cured = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_race_count(rej_table, proc_date))
            race_rej = json.dumps(str(cursor.fetchall()))

            logger.debug(f'race done @ {time() - t0:.2f}s')
            
            cursor.execute(queries.get_age_count(election, proc_date))
            age_tot = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_age_count(cured_table, proc_date))
            age_cured = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_age_count(rej_table, proc_date))
            age_rej = json.dumps(str(cursor.fetchall()))

            logger.debug(f'age done @ {time() - t0:.2f}s')
            
            # add entry in state_stats
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


def compute_county_stats(proc_date):
    '''
    County-level statistics

    proc_date : datetime
        The date to process stats up until
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

                cursor.execute(queries.get_processed_count(election, county, proc_date))
                processed_tot = cursor.fetchall()[0]['num_processed']

                cursor.execute(queries.get_cured_count(election, county, proc_date))
                cured_tot = cursor.fetchall()[0]['num_cured']

                cursor.execute(queries.get_rej_count(election, county, proc_date))
                rej_tot = cursor.fetchall()[0]['num_rejected']

                cursor.execute(queries.get_rej_reasons(rej_table, county, proc_date))
                reasons = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_gender_count(election, county, proc_date))
                gender_tot = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_gender_count(cured_table, county, proc_date))
                gender_cured = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_gender_count(rej_table, county, proc_date))
                gender_rej = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_race_count(election, county, proc_date))
                race_tot = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_race_count(cured_table, county, proc_date))
                race_cured = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_race_count(rej_table, county, proc_date))
                race_rej = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_age_count(election, county, proc_date))
                age_tot = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_age_count(cured_table, county, proc_date))
                age_cured = json.dumps(str(cursor.fetchall()))

                cursor.execute(queries.get_age_count(rej_table, county, proc_date))
                age_rej = json.dumps(str(cursor.fetchall()))

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


if __name__ == '__main__':
    proc_date = datetime.now()

    compute_state_stats(proc_date)
    compute_county_stats(proc_date)
