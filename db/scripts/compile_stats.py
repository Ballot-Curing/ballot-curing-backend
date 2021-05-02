import MySQLdb
import os
import sys
import json
import itertools
from datetime import datetime, timedelta, date
from time import time

import schema
import queries
import current_data as data
from config import load_logger

logger = load_logger()

def compute_state_stats(proc_date):
    '''
    State-level statistics
    
    proc_date : datetime
        The date to process stats up until
    '''
    states = data.cur_states

    for state in states:
        mydb = data.mysql_connect(state)
        cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(schema.create_state_stats_table())
        cursor.execute(schema.create_state_time_series_table())
        
        elections = data.active_elections[state]

        elec = queries.Election(cursor, state, proc_date) 

        for election in elections:
            elec_dt = datetime.strptime(election, '%m_%d_%Y')
            elec.set_elec_dt(elec_dt)
            
            logger.info(f'Computing {state} state-level statistics for {election} on {proc_date}.')
            t0 = time()
            

            if not data.time_series_has_date(cursor, proc_date, elec_dt):
                proc_ts = elec.get_unique_processed()
                rej_ts = elec.get_unique_rej()
                cured_ts = elec.get_unique_cured()

                logger.debug(f'Time series queries retrieved from db @ {time() - t0:.2f}s')

                for proc, rej, cur in itertools.zip_longest(proc_ts, rej_ts, cured_ts):
                    proc_total = proc['count'] if proc else None
                    proc_uniq = proc['diff'] if proc else None

                    rej_total = rej['count'] if rej else None
                    rej_uniq = rej['diff'] if rej else None

                    cur_total = cur['count'] if cur else None
                    cur_uniq = cur['diff'] if cur else None

                    query = schema.insert_time_series()
                    cursor.execute(query, (proc['proc_date'], elec_dt,
                        proc_total, proc_uniq,
                        rej_total, rej_uniq,
                        cur_total, cur_uniq))

                mydb.commit()

            logger.debug(f'Time series data entry finished @ {time() - t0:.2f}s')

            if not data.stats_has_date(cursor, proc_date, elec_dt):
                processed_tot = elec.get_processed()
                cured_tot = elec.get_cured()
                rej_tot = elec.get_rejected()
                logger.debug(f'proc, cured, rej done @ {time() - t0:.2f}s')

                rej_reasons = elec.get_rej_reasons()
                logger.debug(f'reasons done @ {time() - t0:.2f}s')

                gender = elec.get_gender_counts()
                logger.debug(f'gender done @ {time() - t0:.2f}s')

                race = elec.get_race_counts()
                logger.debug(f'race done @ {time() - t0:.2f}s')
                
                age = elec.get_age_counts()
                logger.debug(f'age done @ {time() - t0:.2f}s')

                query = schema.insert_stat()
                cursor.execute(query, (proc_date, elec_dt, 
                    processed_tot, cured_tot, rej_tot,
                    rej_reasons, 
                    gender['total'], gender['cured'], gender['rejected'], 
                    race['total'], race['cured'], race['rejected'], 
                    age['total'], age['cured'], age['rejected']))

                mydb.commit()

            logger.debug(f'Total time for {state} state-level statistics: {time() - t0:.2f}s')

        mydb.close()


def compute_county_stats(proc_date):
    '''
    County-level statistics

    proc_date : datetime
        The date to process stats up until
    '''
    states = data.cur_states

    for state in states:
        mydb = data.mysql_connect(state)
        cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(schema.create_county_stats_table())
        cursor.execute(schema.create_county_time_series_table())

        elections = data.active_elections[state] 
        cursor.execute(queries.get_counties())
        counties = cursor.fetchall()

        elec = queries.Election(cursor, state, proc_date)
        
        for election in elections:
            elec_dt = datetime.strptime(election, '%m_%d_%Y')
            elec.set_elec_dt(elec_dt)

            logger.info(f'Computing {state} county-level statistics for {election} on {proc_date}.')
            t0 = time()
                
            done = 0
            for entry in counties:
                county = entry['county']
                elec.set_county(county)

                logger.debug(f'{state} county #{done + 1} / {len(counties)}')

                if not data.time_series_has_date(cursor, proc_date, elec_dt, county):
                    proc_ts = elec.get_unique_processed()
                    rej_ts = elec.get_unique_rej()
                    cured_ts = elec.get_unique_cured()

                    logger.debug(f'Time series queries retrieved from db @ {time() - t0:.2f}s')

                    for proc, rej, cur in itertools.zip_longest(proc_ts, rej_ts, cured_ts):
                        proc_total = proc['count'] if proc else None
                        proc_uniq = proc['diff'] if proc else None

                        rej_total = rej['count'] if rej else None
                        rej_uniq = rej['diff'] if rej else None

                        cur_total = cur['count'] if cur else None
                        cur_uniq = cur['diff'] if cur else None

                        query = schema.insert_time_series(county)
                        cursor.execute(query, (proc['proc_date'], elec_dt,
                            proc_total, proc_uniq,
                            rej_total, rej_uniq,
                            cur_total, cur_uniq))

                    mydb.commit()

                logger.debug(f'Time series data entry finished @ {time() - t0:.2f}s')

                if not data.stats_has_date(cursor, proc_date, elec_dt, county):
                    processed_tot = elec.get_processed()
                    cured_tot = elec.get_cured()
                    rej_tot = elec.get_rejected()
                    logger.debug(f'proc, cured, rej done @ {time() - t0:.2f}s')

                    rej_reasons = elec.get_rej_reasons()
                    logger.debug(f'reasons done @ {time() - t0:.2f}s')

                    gender = elec.get_gender_counts()
                    logger.debug(f'gender done @ {time() - t0:.2f}s')

                    race = elec.get_race_counts()
                    logger.debug(f'race done @ {time() - t0:.2f}s')
                    
                    age = elec.get_age_counts()
                    logger.debug(f'age done @ {time() - t0:.2f}s')

                    query = schema.insert_stat(county)
                    cursor.execute(query, (proc_date, elec_dt, 
                        processed_tot, cured_tot, rej_tot,
                        rej_reasons, 
                        gender['total'], gender['cured'], gender['rejected'],
                        race['total'], race['cured'], race['rejected'], 
                        age['total'], age['cured'], age['rejected']))
                  

                    mydb.commit()

                done += 1

            logger.debug(f'Total time for county-level statistics: {time() - t0:.2f}s')

        mydb.close()

def compute_2021_runoff_election():
    start_date = datetime.strptime('2020-11-29', '%Y-%m-%d').date()
    end_date = datetime.strptime('2021-01-23', '%Y-%m-%d').date()
    
    proc_date = start_date

    while proc_date <= end_date:
        compute_state_stats(proc_date)
        compute_county_stats(proc_date)
        proc_date += timedelta(days=1)

if __name__ == '__main__':
    start_time = time()

    '''
    # for use while in a real election
    proc_date = datetime.today().date()
    compute_state_stats(proc_date)
    compute_county_stats(proc_date)
    '''

    compute_2021_runoff_election()
    
    print(f'Total time taken: {time() - start_time:.2f}s')
