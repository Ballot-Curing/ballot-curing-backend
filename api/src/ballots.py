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
  state = req.args['state'].upper()
  elec_dt = datetime.strptime(req.args['election_dt'], '%m-%d-%Y')
 
  # build WHERE clause for optional parameters on the fly for optimized SQL query times
  where_clause = ''

  # set any default values for params if needed
  rtn_status = req.args.get('ballot_rtn_status', 'R')
  where_clause += f'ballot_rtn_status = "{rtn_status}" AND '

  # optional parameters
  for param in schema_col_names:
    if param in where_clause: 
      continue
    else:
      val = req.args.get(param, None)
      where_clause += f'{param} = "{val}" AND ' if val else '' # double quotes important to prevent SQL injection

  # remove last AND
  where_clause = where_clause[:-5]


  limit = int(req.args.get('limit', 10))
  limit_clause = f'LIMIT {limit}' if limit != -1 else ''

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

  query = f'''
  SELECT *
  FROM {db_table_name}
  WHERE {where_clause}
  {limit_clause};
  '''

  print(query)
  cursor.execute(query)
  mydb.commit()

  # if show_historic is false, then run query on `rejected` table, otherwise run on main table
  
  # if optional parameters are not null, add 'WHERE {optional parameter}' string to query 
  
  
  return "Nothing for now"
  
