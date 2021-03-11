
import MySQLdb
import configparser

from datetime import datetime

today = "10/18/20"

config = configparser.ConfigParser()
config.read('config-sample.ini')
rejected_db = "rejected"
cured_db = "cured"
table = "nc_test"

# table = config['GA']['table']

create_cured_table = f'''
CREATE TABLE IF NOT EXISTS {cured_db} (
	voter_reg_num           INT,
	zip                     VARCHAR(10),
	county									VARCHAR(25),
	election_dt             DATETIME,
	rejection_dt            DATETIME,
	cured_dt			         	DATETIME
);
'''

create_rejected_table = f'''
CREATE TABLE IF NOT EXISTS {rejected_db} (
	voter_reg_num           INT,
	zip                     VARCHAR(10),
	county									VARCHAR(25),
	election_dt             DATETIME,
	rejection_dt            DATETIME
);
'''

get_today_rejected = f'''
SELECT DISTINCT(voter_reg_num), voter_zip as zip, county_desc as county, election_dt, ballot_rtn_dt
FROM {table}
WHERE ballot_rtn_status = "REJECTED"
AND ballot_rtn_dt = "{today}";
'''
 
get_all_rejected = f'''
SELECT *
FROM {rejected_db};
'''

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
	cursor.execute(create_cured_table) 

	# make rejected table if not made
	cursor.execute(create_rejected_table)
 
	# get rejected ballots from total rejected
	cursor.execute(get_all_rejected)
	
	# for each rejected person, if they were accepted on the current day (10/20/20), add them to the list of cured
	output = cursor.fetchall()
	
	# for each rejected entry, query for today to see if they were accepted
	for entry in output:
		query_for_accepted = f'''
		SELECT voter_reg_num, voter_zip, county_desc, ballot_rtn_dt
		FROM {table}
		WHERE ballot_rtn_status = "ACCEPTED"
		AND ballot_rtn_dt = "{today}"
  	AND voter_reg_num = "{entry["voter_reg_num"]}";
		'''
		cursor.execute(query_for_accepted)
		accepted = cursor.fetchall()
  
		# if accepted, add to cured and remove from rejected
		if len(accepted) > 0:
			print("Entry was accepted today: " + str(entry["voter_reg_num"]))

			add_to_cured = f'''
			INSERT IGNORE INTO {cured_db}(voter_reg_num, zip, county, election_dt, rejection_dt, cured_dt)
			VALUES({entry["voter_reg_num"]}, {entry["zip"]}, {entry["county"]}, "{entry["election_dt"]}", "{entry["rejection_dt"]}", "{today_datetime}");
			'''
			cursor.execute(add_to_cured)
			mydb.commit()

			remove_from_rejected = f'''
			DELETE
			FROM {rejected_db}
			WHERE voter_reg_num = "{entry["voter_reg_num"]}";
			'''
			cursor.execute(remove_from_rejected)
			mydb.commit()
   

  # query the current day for any new rejected
	cursor.execute(get_today_rejected)
	output = cursor.fetchall()
 
	# for each rejected entry today, add to rejected table
	for entry in output:
		print("Found rejected entry: " + entry["voter_reg_num"])
		add_to_rejected = f'''
			INSERT IGNORE INTO {rejected_db}(voter_reg_num, zip, county, election_dt, rejection_dt)
			VALUES({entry["voter_reg_num"]}, {entry["zip"]}, {entry["county"]}, "{entry["election_dt"]}", "{today_datetime}");
			'''
		cursor.execute(add_to_rejected)
		print("Added to rejected table")
		mydb.commit()
   
	
	# To close the connection 
	mydb.close() 
  
# Driver Code 
if __name__ == "__main__" : 
	mysqlconnect(today)
