import MySQLdb
import configparser
import queries
import sys
from datetime import datetime, timedelta

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
  cursor.execute(queries.create_state_stats_table(table))

  # query size of cured table, if empty set to 0
  cured_db = "cured_"+table

  # query size of rejected table, if empty set to 0
  rejected_db = "rejected_"+table

  # add entry in state_stats


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

  # for each state, find all of its counties, and do county query

