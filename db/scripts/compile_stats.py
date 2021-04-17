import MySQLdb
import configparser
import sys
import json
import logging, logging.config

from datetime import datetime, timedelta, date
from time import time

import schema
import queries
from current_data import cur_states as states, get_elections

config = configparser.ConfigParser()
if not config.read('config.ini'):
    raise Exception('config.ini not in current directory. Please run again from top-level directory.')

logging.config.fileConfig(fname='log_config.ini')

logger = logging.getLogger('dev')

for state in states:
    # connect to DB
    mydb = MySQLdb.connect(
        host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'],
        local_infile=1)
 
    logger.info(f'Connected to {state} state database.')
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
  
    # create state_stats DB if not created
    cursor.execute(schema.create_state_stats_table())

    # create county_stats DB if not created
    cursor.execute(schema.create_county_stats_table())

    # query for list of counties
    cursor.execute(queries.get_counties())
    counties = cursor.fetchall()
  
    elections = get_elections(cursor)
  
    for election in elections:
        '''
        State-level statistics
        '''
        logger.info(f'Computing state-level statistics for {election}.')
        t0 = time()

        cursor.execute(queries.get_processed_count(election))
        tot_processed = cursor.fetchall()[0]['num_processed']

        cursor.execute(queries.get_cured_count(election))
        tot_cured = cursor.fetchall()[0]['num_cured']

        cursor.execute(queries.get_rej_count(election))
        tot_rejected = cursor.fetchall()[0]['num_rejected']

        logger.debug(f'proc, cured, rej done @ {time() - t0:.2f}s')

        cursor.execute(queries.get_gender_count(election))
        gender_rej = json.dumps(str(cursor.fetchall()))

        logger.debug(f'gender done @ {time() - t0:.2f}s')

        cursor.execute(queries.get_race_count(election))
        race_rej = json.dumps(str(cursor.fetchall()))

        logger.debug(f'race done @ {time() - t0:.2f}s')
        
        cursor.execute(queries.get_age_count(election))
        age_rej = json.dumps(str(cursor.fetchall()))

        logger.debug(f'age done @ {time() - t0:.2f}s')
        
        cursor.execute(queries.get_rej_reasons(election))
        reasons = json.dumps(str(cursor.fetchall()))

        logger.debug(f'reasons done @ {time() - t0:.2f}s')
        
        # add entry in state_stats
        proc_date = date.today()
        election_dt = datetime.strptime(election, '%m_%d_%Y')

        query = schema.add_state_stat()
        cursor.execute(query, (proc_date, election_dt, tot_processed, tot_cured, tot_rejected,
            gender_rej, race_rej, age_rej, reasons))

        logger.debug(f'Total time for state-level statistics: {time() - t0:.2f}s')
        
        '''
        County-level statistics
        '''
        logger.info(f'Computing county-level statistics for {election}.')
        t0 = time()

        for entry in counties:
            county = entry['county']

            cursor.execute(queries.get_processed_count(election, county))
            tot_processed = cursor.fetchall()[0]['num_processed']

            cursor.execute(queries.get_cured_count(election, county))
            tot_cured = cursor.fetchall()[0]['num_cured']

            cursor.execute(queries.get_rej_count(election, county))
            tot_rejected = cursor.fetchall()[0]['num_rejected']

            cursor.execute(queries.get_gender_count(election, county))
            gender_rej = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_race_count(election, county))
            race_rej = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_age_count(election, county))
            age_rej = json.dumps(str(cursor.fetchall()))

            cursor.execute(queries.get_rej_reasons(election, county))
            reasons = json.dumps(str(cursor.fetchall()))

            proc_date = date.today()
            election_dt = datetime.strptime(election, '%m_%d_%Y') 

            query = schema.add_county_stat()
            cursor.execute(query, (county, proc_date, election_dt, tot_processed, tot_cured, tot_rejected,
                gender_rej, race_rej, age_rej, reasons))
          
        logger.debug(f'Total time for county-level statistics: {time() - t0:.2f}s')
    
    # done with state - commit changes to db and close
    mydb.commit()
    mydb.close()

