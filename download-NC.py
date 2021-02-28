import requests
import zipfile
import os

# url of zip and target zip name
url = 'https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2020_11_03/absentee_20201103.zip'
target_file = 'ncdata.zip'

# get file and write it to output 
myfile = requests.get(url)
open(target_file, 'wb').write(myfile.content)

# unzips file
with zipfile.ZipFile(target_file, 'r') as zip_ref:
  print('Unzipping file: ' + target_file)
  zip_ref.extractall()

# deletes the old zip file
print('Deleting old zip file')
os.remove(target_file)