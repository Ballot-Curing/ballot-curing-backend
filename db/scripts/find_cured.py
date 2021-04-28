import MySQLdb
import sys

from datetime import datetime, timedelta

import queries
import schema
from current_data import get_elections
from schema import rejected_schema_table, cured_schema_table
from config import load_config

config = load_config()

# Special find_cured way for NC, since we don't have multiple days of snapshots to use classic find_cured
def find_cured_NC(state):
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config[state]['db'],
    local_infile=1)

  print("Connected to db")

  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  elections = get_elections(cursor)

  for election in elections:
    cured_db = f'cured_{election}'
    rejected_db = f'rejected_{election}'

    # make cured table if not made
    cursor.execute(schema_table(cured_db))

    # make rejected table if not made
    cursor.execute(schema_table(rejected_db))

    # get all cured ballots as of today
    print("Getting all cured ballots for NC")
    cursor.execute(queries.get_cured_NC(election))
    output = cursor.fetchall()
  
    # for each cured entry, add to cured_db
    for entry in output:
      cursor.execute(schema.add_to_cured_NC(cured_db, entry))

    # get all of the rejected ballots
    print("Getting rejected ballots from main table")
    cursor.execute(queries.get_rejected_NC(election))
    output = cursor.fetchall()

    # for each rejected entry, add to rejected table
    for entry in output:
      cursor.execute(schema.add_to_rejected_NC(rejected_db, entry))

    # commit changes to db when done with election
    mydb.commit()
  
  # To close the connection
  mydb.close()


# Adds entries to cured and rejected tables
def find_cured(state):
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
              user=config['DATABASE']['user'],
              passwd=config['DATABASE']['passwd'],
              db=config[state]['db'],
              local_infile=1)
  print("Connected to db")

  cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

  elections = get_elections(cursor)
  
  for election in elections:
    cured_db = f'cured_{election}'
    rejected_db = f'rejected_{election}'

    # make cured table if not made
    cursor.execute(cured_schema_table(cured_db))

    # make rejected table if not made
    cursor.execute(rejected_schema_table(rejected_db))

    # get all cured ballots as of today by comparing rejected_db and accepted ballots in election table
    print("Get all cured ballots")
    cursor.execute(queries.get_cured(election, rejected_db))
    output = cursor.fetchall()

    # for each cured entry, add to cured_db and remove from rejected_db
    for entry in output:
      cursor.execute(schema.add_to_cured(cured_db, entry))
      cursor.execute(schema.remove_cured_from_rejected(rejected_db, entry))
      mydb.commit()

    # query the current day for any new rejected that are not cured
    print("Getting today's rejected ballots from main table")
    cursor.execute(queries.get_rejected(election, cured_db))
    output = cursor.fetchall()

    print("Adding rejected ballots to rejected table")
    # for each rejected entry today, add to rejected table
    for entry in output:
      try:
        cursor.execute(schema.add_to_rejected(rejected_db, entry))
        mydb.commit()
      except:
        print("Error adding the following entry: ")
        print(entry)
        continue

    # commit changes to db when done with election
    mydb.commit()

  # To close the connection
  mydb.close()

# Helper function if running from another program
def run_find_cured(state):
  if state == "NC":
    find_cured_NC("NC")
  else:
    find_cured(state)

# Main code if running from terminal
if __name__ == "__main__":

  if len(sys.argv) > 1 and sys.argv[1] == "NC":
    run_find_cured("NC")
  elif len(sys.argv) > 1:
    run_find_cured(sys.argv[1])
  else: # By default just do GA
    run_find_cured("GA")

