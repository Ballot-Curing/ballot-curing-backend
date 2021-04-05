
import MySQLdb
import configparser
import queries

from datetime import datetime, timedelta

state = 'NC' # temp, TODO: change into an input

config = configparser.ConfigParser()
config.read('../config.ini')
table = config[state]['table']
rejected_db = "rejected_"+table
cured_db = "cured_"+table

# Special find_cured way for NC, since we don't have multiple days of snapshots to use classic find_cured
def find_cured_NC():
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
	cursor.execute(queries.get_cured_NC(table))
	output = cursor.fetchall()
	
	# for each cured entry, add to cured_db
	for entry in output:
		cursor.execute(queries.add_to_cured_NC(
			cured_db, entry, datetime.strptime("10/10/20", '%m/%d/%y')))
		mydb.commit()

	# get all of the rejected ballots
	print("Getting rejected ballots from main table")
	cursor.execute(queries.get_rejected_NC(table))
	output = cursor.fetchall()

	# for each rejected entry, add to rejected table
	for entry in output:
		cursor.execute(queries.add_to_rejected_NC(
			rejected_db, entry, datetime.strptime("10/10/20", '%m/%d/%y')))
		mydb.commit()

	# To close the connection
	mydb.close()


# Adds entries to cured and rejected tables
def find_cured(today_datetime):
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
		cursor.execute(queries.add_to_cured(
			cured_db, entry, today_datetime))
		mydb.commit()

	# query the current day for any new rejected that are not cured
	print("Getting today's rejected ballots from main table")
	cursor.execute(queries.get_today_rejected(table, today_datetime, cured_db))
	output = cursor.fetchall()

	# for each rejected entry today, add to rejected table
	for entry in output:
		cursor.execute(queries.add_to_rejected(
			rejected_db, entry, today_datetime))
		mydb.commit()

	# To close the connection
	mydb.close()


# Driver Code
if __name__ == "__main__":
	
  # TODO: Remove this for-loop for real election
	start_date = "10/10/20" 
	start_datetime = datetime.strptime(start_date, '%m/%d/%y')
	for i in range(95):
		print("Start date: " + str(start_datetime))
		find_cured(start_datetime)
		start_datetime += timedelta(days=1)
