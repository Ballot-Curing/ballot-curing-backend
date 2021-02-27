import requests

# url of zip and target zip name
url = 'https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2020_11_03/absentee_20201103.zip'
target_file = 'ncdata.zip'

# get file and write it to output 
myfile = requests.get(url)
open(target_file, 'wb').write(myfile.content)