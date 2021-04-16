import MySQLdb
import configparser
import sys

from datetime import datetime, timedelta, date

import schema
import queries
from current_data import cur_states as states, get_elections

config = configparser.ConfigParser()
if not config.read('config.ini'):
    raise Exception('config.ini not in current directory. Please run again from top-level directory.')

for state in states:
  # connect to DB
  mydb = MySQLdb.connect(
    host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config[state]['db'],
    local_infile=1)
 
  print(f'\nConnected to {state} state database.')
  cursor = mydb.cursor()
  
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
    print(f'Computing state-level statistics for\t{election}.')
    # query size of cured table, if empty set to 0
    cursor.execute(queries.get_cured_count(election))
    output = cursor.fetchall()
    
    tot_cured = output[0][0]

    # query size of rejected table, if empty set to 0
    cursor.execute(queries.get_rej_count(election))
    output = cursor.fetchall()
    
    tot_rejected = output[0][0]

    # add entry in state_stats
    proc_date = date.today().strftime("%m/%d/%Y")

    cursor.execute(schema.add_state_stat(proc_date, election, tot_rejected, tot_cured))

    '''
    County-level statistics
    '''
    print(f'Computing county-level statistics for\t{election}.')
    for entry in counties:
      county = entry[0]

      # get num cured for the county
      cursor.execute(queries.get_cured_count(election, county))
      output = cursor.fetchall()
      num_cured = output[0][0]
      

      # get num rejected for the county
      cursor.execute(queries.get_rej_count(election, county))
      output = cursor.fetchall()
      num_rej = output[0][0]
  
      proc_date = date.today().strftime("%m/%d/%Y")

      cursor.execute(schema.add_county_stat(county, proc_date, election, num_rej, num_cured))
    
  mydb.commit()
  # close the connection
  mydb.close()

