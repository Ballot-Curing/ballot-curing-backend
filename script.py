import os
import shutil
import configparser
import time
import zipfile

from datetime import date 
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select

config = configparser.ConfigParser()
config.read('config.ini')

year = config.get('GA', 'Year')
name = config.get('GA', 'Name')

# start selenium webdriver
op = ChromeOptions()
op.add_argument('--headless')
browser = Chrome(options=op)
browser.get(('https://elections.sos.ga.gov/Elections/voterabsenteefile.do'))
print('Selenium driver started...')

# select year
select_election_year = browser.find_element_by_id('nbElecYear')
for option in select_election_year.find_elements_by_tag_name('option'):
  if option.text == year:
    option.click()
    break
print('Year selected...')

# wait 10 seconds for election year data to load
WebDriverWait(browser, 10)

# select election name
select_election_name = browser.find_element_by_id('idElection')
for option in select_election_name.find_elements_by_tag_name('option'):
  if option.text == name:
    option.click()
    break
print('Election selected...')

# wait 10 seconds for election data to load
WebDriverWait(browser, 10)

# download the file
url = 'javascript:downLoadFile();'
download = browser.find_element_by_xpath('//a[@href="'+url+'"]')
download.click()
print('File download started...')

# download filename can either be set in config
# or programmed in script i.e. grab from .crdownload
# or find pattern from SOS site
filename = config.get('GA', 'Filename')

download_path = config['SYSTEM']['download_dir']

file_path = os.path.join(download_path, filename)
timeout = config.getint('GA', 'Timeout')
count = 0

while not os.path.exists(file_path):
    time.sleep(1)
    count += 1
    if count > timeout:
      print('Timeout on downloading file. Exiting program.')
      break

if not os.path.isfile(file_path):
  raise ValueError("%s isn't a file!" % file_path)

storage_path = config.get('GA', 'storage_dir')
today = date.today().strftime('%Y-%m-%d')

new_file_path = storage_path + filename
shutil.move(file_path, new_file_path)

new_file_dir = os.path.join(storage_path, today)
os.mkdir(new_file_dir)

with zipfile.ZipFile(new_file_path, 'r') as zip_ref:
    zip_ref.extractall(new_file_dir)

os.remove(new_file_path)

# TODO rm all non-STATEWIDE.csv files
print(f'Files located at: {new_file_dir}')
