import requests
import zipfile
import os
import configparser
import MySQLdb
import time

from datetime import date

config = configparser.ConfigParser()
config.read('config.ini')

# url of zip and target zip name
url = config['NC']['url']
target_file = config['NC']['zip_filename']

# get file and write it to output 
myfile = requests.get(url)
open(target_file, 'wb').write(myfile.content)
'''
storage_path = config['NC']['storage_dir']
today = date.today().strftime('%Y-%m-%d')
new_file_dir = os.path.join(storage_path, today)
if not os.path.exists(new_file_dir):
  os.mkdir(new_file_dir)
'''
# unzips file
with zipfile.ZipFile(target_file, 'r') as zip_ref:
  print('Unzipping file: ' + target_file)
  zip_ref.extractall('./test_NC_data/')

# deletes the old zip file
print('Deleting old zip file')
os.remove(target_file)

mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config['NC']['db'],
    local_infile = 1)

cursor = mydb.cursor()

query = f'''
CREATE TABLE IF NOT EXISTS {config['NC']['table']} (
  proc_date               DATETIME,
  county                  VARCHAR(25),
  voter_reg_num           INT,
  first_name              VARCHAR(50),
  middle_name             VARCHAR(50),
  last_name               VARCHAR(50),
  race                    VARCHAR(50),
  ethnicity               VARCHAR(50),
  gender                  VARCHAR(10),
  age                     INT,
  street_address          VARCHAR(255),
  city                    VARCHAR(50),
  state                   VARCHAR(10),
  zip                     VARCHAR(10),
  election_dt             DATETIME,
  party_code              VARCHAR(10),
  precinct                VARCHAR(50),
  cong_dist               VARCHAR(50),
  st_house                VARCHAR(50),
  st_senate               VARCHAR(50),
  ballot_style            VARCHAR(50),
  ballot_req_dt           DATETIME,
  ballot_send_dt         	DATETIME,
  ballot_ret_dt	          DATETIME,
  ballot_issue            VARCHAR(255),
  ballot_rtn_status       VARCHAR(50)
);
'''

cursor.execute(query)
csv_file = os.path.join('./test_NC_data/', config['NC']['csv_name'])

query =f'''
LOAD DATA LOCAL INFILE '{csv_file}'
INTO TABLE {config['NC']['table']}
CHARACTER SET latin1
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS (county, voter_reg_num, @ignore, last_name, first_name, middle_name, race, ethnicity,
            gender, age, street_address, city, state, zip, @dummy, @dummy, @dummy, @dummy,
               @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, election_dt, 
			   party_code, precinct, cong_dist, st_house, st_senate, @dummy, @dummy, @dummy,
			   @req_dt, @send_dt, @ret_dt, ballot_rtn_status, @dummy, @dummy, @dummy)
               SET proc_date = NOW(),
                   ballot_req_dt = STR_TO_DATE(@req_dt, '%m/%d/%Y'),
                   ballot_send_dt = STR_TO_DATE(@send_dt, '%m/%d/%Y'),
                   ballot_ret_dt = STR_TO_DATE(@ret_dt, '%m/%d/%Y');
'''

print('Database query started.')

cursor.execute(query)
mydb.commit()

print('Database query executed.')
