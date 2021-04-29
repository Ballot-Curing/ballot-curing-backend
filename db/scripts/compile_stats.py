import MySQLdb
import os
import sys
import json
from datetime import datetime, timedelta, date
from time import time

import schema
import queries
from current_data import cur_states as states, get_elections, mysql_connect
from config import load_logger

logger = load_logger()

def compute_state_stats(proc_date):
    '''
    State-level statistics
    
    proc_date : datetime
        The date to process stats up until
    '''
    logger.debug(f'len states:  {len(states)}')
    for state in states:
        mydb = mysql_connect(state)
        cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(schema.create_state_stats_table())
        cursor.execute(schema.create_state_time_series_table())
        
        elections = get_elections(cursor)
        logger.debug(f'Length of elections: {len(elections)}')

        elec = queries.Election(cursor, state, proc_date) 

        for election in elections:
            elec_dt = datetime.strptime(election, '%m_%d_%Y')
            elec.set_elec_dt(elec_dt)

            logger.info(f'Computing {state} state-level statistics for {election}.')
            t0 = time()
            
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

            query = schema.add_state_stat()
            cursor.execute(query, (proc_date, elec_dt, 
                processed_tot, cured_tot, rej_tot,
                rej_reasons, 
                gender['total'], gender['cured'], gender['rejected'], 
                race['total'], race['cured'], race['rejected'], 
                age['total'], age['cured'], age['rejected']))

            mydb.commit()
            logger.debug(f'general stats done @ {time() - t0:.2f}s')

            proc_ts = elec.get_unique_processed()
            rej_ts = elec.get_unique_rej()
            cured_ts = elec.get_unique_cured()

            print(proc_ts)
            print(rej_ts)
            logger.debug(f'\nLengths of TS data: {len(proc_ts)} {len(rej_ts)} {len(cured_ts)}\n')

            for p_row, r_row, c_row in zip(proc_ts, rej_ts, cured_ts):
                print(p_row)
                p_proc_date, p_total, p_unique = p_row
                r_proc_date, r_total, r_unique = r_row
                c_proc_date, c_total, c_unique = c_row

                query = schema.insert_state_time_series()
                cursor.execute(query, (p_proc_date, elec_dt,
                    p_total, p_unique,
                    r_total, r_unique,
                    c_total, c_unique))

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
        mydb = mysql_connect(state)
        cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(schema.create_county_stats_table())
        cursor.execute(schema.create_county_time_series_table())

        elections = get_elections(cursor)
        cursor.execute(queries.get_counties())
        counties = cursor.fetchall()

        elec = queries.Election(cursor, state, proc_date)
        
        for election in elections:
            elec_dt = datetime.strptime(election, '%m_%d_%Y')
            elec.set_elec_dt(elec_dt)

            logger.info(f'Computing {state} county-level statistics for {election}.')
            t0 = time()
                
            done = 0
            for entry in counties:
                county = entry['county']
                elec.set_county(county)

                logger.debug(f'{state} county #{done + 1} / {len(counties)}')

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

                query = schema.add_county_stat()
                cursor.execute(query, (county, proc_date, elec_dt, 
                    processed_tot, cured_tot, rej_tot,
                    rej_reasons, 
                    gender['total'], gender['cured'], gender['rejected'],
                    race['total'], race['cured'], race['rejected'], 
                    age['total'], age['cured'], age['rejected']))
              
                done += 1

            mydb.commit()
            logger.debug(f'Total time for county-level statistics: {time() - t0:.2f}s')

        mydb.close()

def compute_2020_general_election():
    start_date = datetime.strptime('2020-11-29', '%Y-%m-%d').date()
    end_date = datetime.strptime('2021-01-23', '%Y-%m-%d').date()
    
    proc_date = start_date
    print(proc_date)

    while proc_date <= end_date:
        compute_state_stats(proc_date)
        # compute_county_stats(proc_date)
        proc_date += datetime.timedelt(days=1)

if __name__ == '__main__':
    compute_2020_general_election()

    '''
    # for use while in a real election
    proc_date = datetime.today().date()
    compute_state_stats(proc_date)
    compute_county_stats(proc_date)
    '''
