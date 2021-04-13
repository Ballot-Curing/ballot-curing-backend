import requests
import zipfile
import os
import configparser
import MySQLdb
import time

from datetime import date

from schema import schema_table
from elections import elections_table, elections_load

config = configparser.ConfigParser()
config.read('config.ini')

# url of zip and target zip name
url = config['NC']['url']
target_file = config['NC']['zip_filename']

# get file and write it to output 
myfile = requests.get(url)
open(target_file, 'wb').write(myfile.content)

# unzips file
with zipfile.ZipFile(target_file, 'r') as zip_ref:
  print('Unzipping file: ' + target_file)
  zip_ref.extractall('./test_NC_data/')

# deletes the old zip file
print('Deleting old zip file')
os.remove(target_file)

# connect to the database
mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config['NC']['db'],
    local_infile = 1)

cursor = mydb.cursor()

query = schema_table(config['NC']['table'])

cursor.execute(query)
csv_file = os.path.join('./test_NC_data/', config['NC']['csv_name'])

# drop the function if it already exists
query = f'''
DROP FUNCTION IF EXISTS STANDARDIZE
'''
cursor.execute(query)

# define the standardization function
query = f'''
CREATE FUNCTION STANDARDIZE (return_issue VARCHAR(255))
RETURNS VARCHAR(50)
DETERMINISTIC
BEGIN
   
   DECLARE return_val VARCHAR(50);
   SET return_val = 'R';
   IF (return_issue = 'ACCEPTED' OR return_issue = 'ACCEPTED - CURED') THEN
        SET return_val = 'A';
   ELSEIF return_issue = 'CANCELLED' THEN
        SET return_val = 'C';
   ELSEIF return_issue = 'SPOILED' THEN
        SET return_val = 'S';
   ELSEIF (return_issue = 'PENDING CURE' OR return_issue = 'NOT VOTED' OR    
            return_issue = 'PENDING' OR return_issue = ' ') THEN
        SET return_val = '';
   END IF;

   RETURN (return_val);

END
'''

cursor.execute(query)

# load the data into the database
query =f'''
LOAD DATA LOCAL INFILE '{csv_file}'
INTO TABLE {config['NC']['table']}
CHARACTER SET latin1
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES (county, @ignore, voter_reg_id, last_name, first_name, middle_name, race, 
                ethnicity, gender, age, street_address, city, state, zip, @dummy, @dummy, @dummy,
                @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @elec_date, 
			   party_code, precinct, cong_dist, st_house, st_senate, @dummy, ballot_style, @dummy,
			   @req_dt, @send_dt, @ret_dt, @ballot_status, @dummy, @dummy, @dummy)
               SET proc_date = NOW(),
                   election_dt = STR_TO_DATE(@elec_date, '%m/%d/%Y'),
                   ballot_req_dt = STR_TO_DATE(@req_dt, '%m/%d/%Y'),
                   ballot_send_dt = STR_TO_DATE(@send_dt, '%m/%d/%Y'),
                   ballot_ret_dt = STR_TO_DATE(@ret_dt, '%m/%d/%Y'),
                   ballot_issue = @ballot_status,
                   ballot_rtn_status = STANDARDIZE(@ballot_status);
'''

print('Database query started.')

cursor.execute(query)
mydb.commit()

print('Database query executed.')

print('Updating processed date.')

# create the elections table
query = elections_table()
cursor.execute(query)
mydb.commit()

# update the value of election last processed date
today = date.today()
test_proc_date = today.strftime("%m/%d/%Y")

entry = {
  "election_dt": config['NC']['table'], 
  "proc_date": test_proc_date
}

query = elections_load(entry)
cursor.execute(query)
mydb.commit()
