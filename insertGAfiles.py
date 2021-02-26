import os
import shutil
import sys
import configparser
import time
import zipfile
import MySQLdb

from datetime import date 
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select

config = configparser.ConfigParser()
config.read('config.ini')

mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config['GA']['db'],
    local_infile = 1)

cursor = mydb.cursor()

query = f'''
CREATE TABLE IF NOT EXISTS {config['GA']['table']} (
  id                      INT NOT NULL AUTO_INCREMENT, 
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
  ballot_rtn_status       VARCHAR(50),
  PRIMARY KEY(id)
);
'''
cursor.execute(query)

ga_dir = config['GA']['ga_files']

for entry in os.scandir(ga_dir):
  if not entry.path.endswith('.zip'):
    continue 
  date = entry.name[:10]

  new_file_dir = os.path.join(config['GA']['rain_ga_storage'], date)
  
  try:
    with zipfile.ZipFile(entry.path, 'r') as zip_ref:
      print(f'Unzipping file: {entry.name}.')
      zip_ref.extractall(new_file_dir)
  except:
    print(f'Error for {entry.name}: ', sys.exc_info()[0])
    continue

  csv_file = os.path.join(new_file_dir, config['GA']['csv_name'])

  query =f'''
  LOAD DATA LOCAL INFILE '{csv_file}'
  INTO TABLE {config['GA']['table']}
  CHARACTER SET latin1
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS (county, voter_reg_num, last_name, first_name, middle_name, @dummy, @street_no,
            @street_name, @apt_no, city, state, zip, @dummy, @dummy, @dummy, @dummy,
                @dummy, @dummy, @dummy, ballot_rtn_status, ballot_issue, @req_dt, @send_dt,
                @ret_dt, ballot_style, @dummy, @dummy, @dummy, @dummy, precinct, cong_dist, 
                st_senate, st_house, @dummy, @dummy, @dummy, @dummy, @dummy, party_code)
                SET proc_date = STR_TO_DATE('{date}', '%Y_%m_%d'),
                    street_address = CONCAT(@street_no, ' ', @street_name, ' ', @apt_no),
                    ballot_req_dt = STR_TO_DATE(@req_dt, '%m/%d/%Y'),
                    ballot_send_dt = STR_TO_DATE(@send_dt, '%m/%d/%Y'),
                    ballot_ret_dt = STR_TO_DATE(@ret_dt, '%m/%d/%Y');
  '''

  print(f'Database insertion started for {date}.')
  cursor.execute(query)
  mydb.commit()

print('Done.')
