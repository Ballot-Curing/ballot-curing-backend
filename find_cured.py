
import MySQLdb
import configparser
import queries

from datetime import datetime

today = "10/20/20"

config = configparser.ConfigParser()
config.read('config-sample.ini')
rejected_db = "rejected"
cured_db = "cured"
table = "nc_test"

# table = config['GA']['table']

def mysqlconnect(today): 
	today_datetime = datetime.strptime(today, '%m/%d/%y')
  
	# To connect MySQL database 
	mydb = MySQLdb.connect( 
			host='localhost', 
			user='test1',  
			passwd = 'password', 
			db='georgia_test', 
			)

	# mydb = MySQLdb.connect(host=config['DATABASE']['host'],
  #                       user=config['DATABASE']['user'],
  #                       passwd=config['DATABASE']['passwd'],
  #                       db=config['GA']['db'],
  #                       local_infile = 1)
  
	cursor = mydb.cursor(MySQLdb.cursors.DictCursor) 
 
	# make cured table if not made
	cursor.execute(queries.create_cured_table(cured_db)) 

	# make rejected table if not made
	cursor.execute(queries.create_rejected_table(rejected_db))
 
	# get rejected ballots from total rejected
	cursor.execute(queries.get_all_rejected(rejected_db))
	
	# for each rejected entry, query for today to see if they were accepted
	output = cursor.fetchall()
	for entry in output:
		cursor.execute(queries.query_for_accepted(table, today, entry))
		accepted = cursor.fetchall()
  
		# if accepted, add to cured and remove from rejected
		if len(accepted) > 0:
			print("Entry was accepted today: " + str(entry["voter_reg_num"]))

			cursor.execute(queries.add_to_cured(cured_db, entry, today_datetime))
			mydb.commit()

			cursor.execute(queries.remove_from_rejected(rejected_db, entry))
			mydb.commit()
   

  # query the current day for any new rejected
	cursor.execute(queries.get_today_rejected(table, today))
	output = cursor.fetchall()
 
	# for each rejected entry today, add to rejected table
	for entry in output:
		print("Found rejected entry: " + entry["voter_reg_num"])
		
		cursor.execute(queries.add_to_rejected(rejected_db, entry, today_datetime))
		print("Added to rejected table")
		mydb.commit()
   
	# To close the connection 
	mydb.close() 


# Driver Code 
if __name__ == "__main__" : 
	mysqlconnect(today)
