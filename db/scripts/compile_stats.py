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
  print("Connected to db of " + state)
  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  # create county_stats DB if not created
  county_stats_table = state + '_county_stats'
  cursor.execute(queries.create_county_stats_table(county_stats_table))

  # query for list of counties
  cursor.execute(queries.get_counties(table))
  output = cursor.fetchall()

  for entry in output:
    county = entry['county']

    # get num cured for the county
    cured_table = "cured_" + table
    cursor.execute(queries.get_cured_ballots_from_county(cured_table, county))
    output = cursor.fetchall()
    num_cured = output[0]['num_cured']

    # get num rejected for the county
    rej_table = "rejected_" + table
    cursor.execute(queries.get_rej_ballots_from_county(rej_table, county))
    output = cursor.fetchall()
    num_rej = output[0]['num_rej']
  
    # add entry in county_stats
    election_dt = config[state]['table']
    proc_date = date.today().strftime("%m/%d/%Y")

    cursor.execute(queries.add_county_stat(county_stats_table, county, proc_date, election_dt, num_rej, num_cured))
    mydb.commit()

  # close the connection
  mydb.close()



# TODO: if ran more than once on same day, it still adds to DB. fix this
# Driver Code
if __name__ == "__main__":
  for state in states:
    get_state_stats(state)
    get_county_stats(state)


