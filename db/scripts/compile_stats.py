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
from util import find, safe_min_max

logger = load_logger()

def compute_time_series(elec):
    state = elec.get_state() 
    proc_date = elec.get_proc_date()
    elec_dt = elec.get_elec_dt()
    county = elec.get_county()
    mydb = elec.get_db()

    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute(schema.create_state_time_series_table())
    cursor.execute(schema.create_county_time_series_table())
    
    t0 = time()

    if not data.time_series_has_date(cursor, proc_date, elec_dt, county):
        # empty list for county temp for time-saving, don't really need proc for counties
        proc_ts = elec.get_unique_processed()
        rej_ts = elec.get_unique_rej()
        cur_ts = elec.get_unique_cured()

        logger.debug(f'Time series queries retrieved from db @ {time() - t0:.2f}s')

        proc_ts_begin = safe_min_max(min, [row['proc_date'] for row in proc_ts])
        proc_ts_end = safe_min_max(max, [row['proc_date'] for row in proc_ts]) 
    
        rej_ts_begin = safe_min_max(min, [row['proc_date'] for row in rej_ts]) 
        rej_ts_end = safe_min_max(max, [row['proc_date'] for row in rej_ts]) 
    
        cur_ts_begin = safe_min_max(min, [row['proc_date'] for row in cur_ts]) 
        cur_ts_end = safe_min_max(max, [row['proc_date'] for row in cur_ts]) 
    
        proc_date_start = safe_min_max(min, [proc_ts_begin, rej_ts_begin, cur_ts_begin])
        proc_date_end = safe_min_max(max, [proc_ts_end, rej_ts_end, cur_ts_end])

        prev_proc_tot = 0
        prev_rej_tot = 0
        prev_cur_tot = 0
        
        proc_date_cur = proc_date_start
        
        while proc_date_cur and proc_date_cur <= proc_date_end:
            proc_idx = find(proc_ts, 'proc_date', proc_date_cur)
            rej_idx = find(rej_ts, 'proc_date', proc_date_cur)
            cur_idx = find(cur_ts, 'proc_date', proc_date_cur)

            proc_tot = proc_ts[proc_idx]['count'] if proc_idx else prev_proc_tot
            proc_uniq = proc_ts[proc_idx]['diff'] if proc_idx else 0 

            rej_tot = rej_ts[rej_idx]['count'] if rej_idx else prev_rej_tot 
            rej_uniq = rej_ts[rej_idx]['diff'] if rej_idx else 0

            cur_tot = cur_ts[cur_idx]['count'] if cur_idx else prev_cur_tot
            cur_uniq = cur_ts[cur_idx]['diff'] if cur_idx else 0

            proc_tot = max(proc_tot, prev_proc_tot)
            rej_tot = max(rej_tot, prev_rej_tot)
            cur_tot = max(cur_tot, prev_cur_tot)
            
            prev_proc_tot = proc_tot
            prev_rej_tot = rej_tot
            prev_cur_tot = cur_tot

            query = schema.insert_time_series(county)
            cursor.execute(query, (proc_date_cur, elec_dt,
                proc_tot, proc_uniq,
                rej_tot, rej_uniq,
                cur_tot, cur_uniq))

            proc_date_cur += timedelta(days=1)

        mydb.commit()
    
    logger.debug(f'Time series data entry finished @ {time() - t0:.2f}s')

def compute_state_stats(state, proc_date, election):
    '''
    State-level statistics
    
    proc_date : datetime
        The date to process stats up until
    '''
    mydb = data.mysql_connect(state)
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(schema.create_state_stats_table())
    
    elections = data.active_elections[state]

    elec = queries.Election(cursor, state, proc_date, mydb) 

    elec_dt = datetime.strptime(election, '%m_%d_%Y')
    elec.set_elec_dt(elec_dt)
    
    logger.info(f'Computing {state} state-level statistics for {election} on {proc_date}.')
    t0 = time()
    
    compute_time_series(elec)
    
    # TODO: remove proc rej cured here for future
    if state != 'GA' and not data.stats_has_date(cursor, proc_date, elec_dt):
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


def compute_county_stats(state, proc_date, election):
    '''
    County-level statistics

    proc_date : datetime
        The date to process stats up until
    '''
    mydb = data.mysql_connect(state)
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(schema.create_county_stats_table())

    elections = data.active_elections[state] 
    cursor.execute(queries.get_counties())
    counties = cursor.fetchall()

    elec = queries.Election(cursor, state, proc_date, mydb)
    
    elec_dt = datetime.strptime(election, '%m_%d_%Y')
    elec.set_elec_dt(elec_dt)

    logger.info(f'Computing {state} county-level statistics for {election} on {proc_date}.')
    t0 = time()
        
    done = 0
    for entry in counties:
        county = entry['county']
        elec.set_county(county)

        logger.debug(f'{state} county {county}, #{done + 1} / {len(counties)}')

        compute_time_series(elec)

        #TODO: Remove proc, cur, rej from here stats tables (?). Then can re-run daily. Commented out for time-saving purposes
        '''
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
        '''

        done += 1

    logger.debug(f'Total time for county-level statistics: {time() - t0:.2f}s')

    mydb.close()

def compute_2021_runoff_election():
    start_date = datetime.strptime('2020-11-29', '%Y-%m-%d').date()
    end_date = datetime.strptime('2021-01-23', '%Y-%m-%d').date()

    election = '01_05_2021'
    
    proc_date = start_date

    compute_county_stats('GA', proc_date, election) 
    quit()

    while proc_date <= end_date:
        compute_state_stats('GA', proc_date, election)
        compute_county_stats('GA', proc_date, election) 
        proc_date += timedelta(days=1)

if __name__ == '__main__':
    start_time = time()

    compute_2021_runoff_election()
    '''
    # for use while in a real election
    proc_date = datetime.today().date()
    compute_state_stats(proc_date)
    compute_county_stats(proc_date)
    '''
    '''
    proc_date = datetime.strptime('2021-04-17', '%Y-%m-%d').date()
    compute_state_stats('NC', proc_date)
    compute_county_stats('NC', proc_date)
    '''
    
    print(f'Total time taken: {time() - start_time:.2f}s')
