import MySQLdb
import configparser
import queries
import sys
from datetime import datetime, timedelta, date

config = configparser.ConfigParser()
config.read('config.ini')

states = ['GA', 'NC']

# TODO: this only works for a single election listed in config, 
#       how will we do this for multiple elections at the same time?
def get_state_stats(state):
  # connect to DB
  table = config[state]['table']
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
              user=config['DATABASE']['user'],
              passwd=config['DATABASE']['passwd'],
              db=config[state]['db'],
              local_infile=1)
  print("Connected to db of " + state)
  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  # create state_stats DB if not created
  state_stats_table = state + '_state_stats'
  cursor.execute(queries.create_state_stats_table(state_stats_table))

  # query size of cured table, if empty set to 0
  print("Querying cured table")
  cured_table = "cured_"+table
  cursor.execute(queries.get_cured_count(cured_table))
  output = cursor.fetchall()

  tot_cured = output[0]['num_cured']

  # query size of rejected table, if empty set to 0
  print("Querying rejected table")
  rej_table = "rejected_"+table
  cursor.execute(queries.get_rej_count(rej_table))
  output = cursor.fetchall()

  tot_rejected = output[0]['num_rej']

  # add entry in state_stats
  election_dt = config[state]['table']
  proc_date = date.today().strftime("%m/%d/%Y")

  cursor.execute(queries.add_state_stat(state_stats_table, proc_date, election_dt, tot_rejected, tot_cured))
  mydb.commit()

  # close the connection
  mydb.close()



def get_county_stats(state):
  table = config[state]['table']
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
              user=config['DATABASE']['user'],
              passwd=config['DATABASE']['passwd'],
              db=config[state]['db'],
              local_infile=1)
  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  # create county_stats DB if not created

  # query for list of all counties for state

  # for each county query size of cured table for given county

  # for each county query size of rejected table for given county

  # add entry in county_stats




# TODO: implement for counties
# Driver Code
if __name__ == "__main__":
  # for each state, get state stats
  for state in states:
    get_state_stats(state)
  
  # for each state, find all of its counties, and do county query

