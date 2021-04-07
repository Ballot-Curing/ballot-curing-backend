import MySQLdb
import configparser

from datetime import datetime
from flask import Blueprint
from flask import request as req

from schema import schema_col_names

ballots_bp = Blueprint('ballots', __name__)

config = configparser.ConfigParser()
# input path to config file
config.read('/home/cs310_prj3/Ballot-Curing-Project/config.ini')

@ballots_bp.route('/', methods=['GET'])
def ballots():
 
  # required parameters - throws error if not present
  state = upper(req.args['state'])
  elec_dt = datetime.strptime(req.args['election_dt'], '%m-%d-%Y')
 
  params = {} 
  # optional parameters
  for param in schema_col_names:
    if param == 'ballot_rtn_status' 

  params[ballot_rtn_status] = req.args.get('ballot_rtn_status', 'R')
  params[county] = req.args.get('county', '')
  params[voter_reg_id] = req.args.get('voter_reg_id', '')
  params[first_name] = req.args.get('first_name', '')
  params[middle_name] = req.args.get('middle_name', '')
  params[last_name] = req.args.get('last_name', '')
  params[race] = req.args.get('race', '')
  params[ethnicity] = req.args.get('ethnicity', '')
  params[gender] = req.args.get('gender', '')
  params[age] = req.args.get('age', '')
  params[street] = req.args.get('street_address', '')
  params[city] = req.args.get('city', '')
  params[zipcode] = req.args.get('zip', '')
  params[party] = req.args.get('party_code', '')
  params[precinct] = req.args.get('precinct', '')
  params[cong] = req.args.get('cong_dist', '')
  params[st_house] = req.args.get('st_house', '')
  params[st_senate] = req.args.get('st_senate', '')
  params[style] = req.args.get('ballot_style', '')
  params[req_dt] = req.args.get('ballot_req_dt', None)
  params[send_dt] = req.args.get('ballot_send_dt', None)
  params[ret_dt] = req.args.get('ballot_ret_dt', None)
  params[issue] = req.args.get('ballot_issue', '')
  params[limit] = req.args.get('limit', 10)

  # TODO support historic data requests
  historic = req.args.get('show_historic', False)

  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db=config[state]['db'],
    local_infile = 1)
  
  cursor = mydb.cursor()

  # run query with election_dt as table name
  db_table_name = elec_dt.strftime('%m_%d_%Y')
  

  # pre-construct WHERE clause from parameters for optimized SQL query speed
  where = ''

  for param, val in params.items():
    where += f''

  where += f'ballot_rtn_status = "{rtn_status}" AND'
  where += f'county = "{county}"' if county else ''
  where += f'voter_reg_id = "voter_reg_id"' if voter_reg_id else ''

  query = f'''
  SELECT *
  FROM {db_table_name}
  {where_clause}
  '''

  cursor.execute(query)
  mydb.commit()

  # if show_historic is false, then run query on `rejected` table, otherwise run on main table
  
  # if optional parameters are not null, add 'WHERE {optional parameter}' string to query 
  
  
  return "Nothing for now"
  
