from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select



# input election year and name
# TODO These should probably be in a config
# file per state so they don't have to be input every time
year = input('Election year: ')
name = input('Election name: ')

# sample name and value
sample_year = '2021'
sample_name = '01/05/2021 - JANUARY 5, 2021 FEDERAL RUNOFF ELECTION'
year = sample_year
name = sample_name

# start selenium webdriver
op = ChromeOptions()
op.add_argument('--headless')
browser = Chrome(options=op)
browser.get(('https://elections.sos.ga.gov/Elections/voterabsenteefile.do'))

# select year
select_election_year = browser.find_element_by_id('nbElecYear')
for option in select_election_year.find_elements_by_tag_name('option'):
  if option.text == year:
    option.click()
    break

# wait 10 seconds for election year data to load
WebDriverWait(browser, 10)

# select election name
select_election_name = browser.find_element_by_id('idElection')
for option in select_election_name.find_elements_by_tag_name('option'):
  if option.text == name:
    option.click()
    break

# wait 10 seconds for election data to load
WebDriverWait(browser, 10)

# download the file
url = 'javascript:downLoadFile();'
download = browser.find_element_by_xpath('//a[@href="'+url+'"]')
download.click()
