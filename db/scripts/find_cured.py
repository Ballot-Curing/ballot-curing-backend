
import MySQLdb
import configparser
import queries
import sys

from datetime import datetime, timedelta


config = configparser.ConfigParser()
config.read('config.ini')


# Special find_cured way for NC, since we don't have multiple days of snapshots to use classic find_cured
def find_cured_NC(state):
  table = config[state]['table']
  rejected_db = "rejected_"+table
  cured_db = "cured_"+table
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
              user=config['DATABASE']['user'],
              passwd=config['DATABASE']['passwd'],
              db=config[state]['db'],
              local_infile=1)
  print("Connected to db")

  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  # make cured table if not made
  cursor.execute(queries.create_cured_table(cured_db))

  # make rejected table if not made
  cursor.execute(queries.create_rejected_table(rejected_db))

  # get all cured ballots as of today
  print("Getting all cured ballots for NC")
  cursor.execute(queries.get_cured_NC(table))
  output = cursor.fetchall()
  
  # for each cured entry, add to cured_db
  for entry in output:
    cursor.execute(queries.add_to_cured_NC(cured_db, entry))
    mydb.commit()

  # get all of the rejected ballots
  print("Getting rejected ballots from main table")
  cursor.execute(queries.get_rejected_NC(table))
  output = cursor.fetchall()

  # for each rejected entry, add to rejected table
  for entry in output:
    cursor.execute(queries.add_to_rejected_NC(rejected_db, entry))
    mydb.commit()

  # To close the connection
  mydb.close()


# Adds entries to cured and rejected tables
def find_cured(today_datetime, state):
  table = config[state]['table']
  rejected_db = "rejected_"+table
  cured_db = "cured_"+table
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
              user=config['DATABASE']['user'],
              passwd=config['DATABASE']['passwd'],
              db=config[state]['db'],
              local_infile=1)
  print("Connected to db")

  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  # make cured table if not made
  cursor.execute(queries.create_cured_table(cured_db))

  # make rejected table if not made
  cursor.execute(queries.create_rejected_table(rejected_db))
  
  # get all cured ballots as of today
  print("Get all cured ballots")
  cursor.execute(queries.get_cured(table))
  output = cursor.fetchall()

  # for each cured entry, add to cured_db
  for entry in output:
    cursor.execute(queries.add_to_cured(cured_db, entry))
    mydb.commit()

  # query the current day for any new rejected that are not cured
  print("Getting today's rejected ballots from main table")
  cursor.execute(queries.get_today_rejected(table, today_datetime, cured_db))
  output = cursor.fetchall()

  # for each rejected entry today, add to rejected table
  for entry in output:
    cursor.execute(queries.add_to_rejected(
      rejected_db, entry))
    mydb.commit()

  # To close the connection
  mydb.close()


# Driver Code
if __name__ == "__main__":

  if len(sys.argv) > 1 and sys.argv[1] == "NC":
    find_cured_NC("NC")
  elif len(sys.argv) > 1:
    # TODO: Remove this for-loop for real election
    start_date = "10/10/20" 
    start_datetime = datetime.strptime(start_date, '%m/%d/%y')
    for i in range(95):
      print("Start date: " + str(start_datetime))
      find_cured(start_datetime, sys.argv[1])
      start_datetime += timedelta(days=1)
  else: # By default just do GA
    start_date = "10/10/20" 
    start_datetime = datetime.strptime(start_date, '%m/%d/%y')
    for i in range(95):
      print("Start date: " + str(start_datetime))
      find_cured(start_datetime, "GA")
      start_datetime += timedelta(days=1)
