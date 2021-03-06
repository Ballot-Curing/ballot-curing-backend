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
IGNORE 1 ROWS (@county_st, @voter_reg_num_st, @ignore, @last_name_st, @first_name_st, @middle_name_st, @race_st, 
                @ethnicity_st, @gender_st, @age_st, @street_address_st, @city_st, @state_st, @zip_st, @dummy, @dummy, @dummy,
                @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @dummy, @elec_date, 
			   @party_code_st, @precinct_st, @cong_dist_st, @st_house_st, @st_senate_st, @dummy, @dummy, @dummy,
			   @req_dt, @send_dt, @ret_dt, @ballot_rtn_status_st, @dummy, @dummy, @dummy)
               SET proc_date = NOW(),
                   county = TRIM(BOTH '"' FROM @county_st),
                   voter_reg_num = TRIM(BOTH '"' FROM @county_reg_num_st),
                   last_name = TRIM(BOTH '"' FROM @last_name_st),
                   first_name = TRIM(BOTH '"' FROM @first_name_st),
                   middle_name = TRIM(BOTH '"' FROM @middle_name_st),
                   race = TRIM(BOTH '"' FROM @race_st),
                   ethnicity = TRIM(BOTH '"' FROM @ethnicity_st),
                   gender = TRIM(BOTH '"' FROM @gender),
                   age = TRIM(BOTH '"' FROM @age_st),
                   street_address = TRIM(BOTH '"' FROM @street_address_st),
                   city = TRIM(BOTH '"' FROM @city_st),
                   state = TRIM(BOTH '"' FROM @state_st),
                   zip = TRIM(BOTH '"' FROM @zip_st),
                   party_code = TRIM(BOTH '"' FROM @party_code_st),
                   precinct = TRIM(BOTH '"' FROM @precinct_st),
                   cong_dist = TRIM(BOTH '"' FROM @cong_dist_st),
                   st_house = TRIM(BOTH '"' FROM @st_house_st),
                   st_senate = TRIM(BOTH '"' FROM @st_senate_st),
                   ballot_rtn_status = TRIM(BOTH '"' FROM @ballot_rtn_status_st),
                   election_dt = STR_TO_DATE(@elec_date, '"%m/%d/%Y"'),
                   ballot_req_dt = STR_TO_DATE(@req_dt, '"%m/%d/%Y"'),
                   ballot_send_dt = STR_TO_DATE(@send_dt, '"%m/%d/%Y"'),
                   ballot_ret_dt = STR_TO_DATE(@ret_dt, '"%m/%d/%Y"');
'''

print('Database query started.')

cursor.execute(query)
mydb.commit()

print('Database query executed.')
