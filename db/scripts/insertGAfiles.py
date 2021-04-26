import os
import shutil
import sys
import time
import zipfile
import MySQLdb
import find_cured

from datetime import date 
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select

from schema import schema_table, schema_index
from config import load_config

config = load_config()

mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config['GA']['db'],
    local_infile = 1)

cursor = mydb.cursor()
table = config['GA']['table']
# table = '2021_01_01'
query = schema_table(table);

cursor.execute(query)

try:
    query = schema_index(table)
    cursor.execute(query)
except:
    print('Index already exists.')

ga_dir = config['GA']['ga_files']

for entry in os.scandir(ga_dir):
  if not entry.path.endswith('.zip'):
    continue 

  new_file_dir = os.path.join(config['GA']['rain_ga_storage'], date)
  
  try:
    with zipfile.ZipFile(entry.path, 'r') as zip_ref:
      print(f'Unzipping file: {entry.name}.')
      zip_ref.extractall(new_file_dir)
  except:
    print(f'Error for {entry.name}: ', sys.exc_info()[0])
    continue

  csv_file = config['GA']['csv_name']
  
  query =f'''
  LOAD DATA LOCAL INFILE '{csv_file}' IGNORE
  INTO TABLE {table}
  CHARACTER SET latin1
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS (county, voter_reg_id, last_name, first_name, middle_name, @dummy, @street_no,
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
  start_time = time.time()
  cursor.execute(query)
  mydb.commit()
  print(f'Rows added: {cursor.rowcount} Time taken: {time.time() - start_time} seconds.')

  print("Running find_cured")
  run_find_cured("GA")

print('Done.')
