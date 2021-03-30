from flask import Blueprint
import MySQLdb

ballots_bp = Blueprint('ballots', __name__)

@ballots_bp.route('/ballots')
def ballots():
  
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db="vote_"+state, 
    local_infile = 1)
  
  # run query with election_dt as table name
  
  # if show_historic is false, then run query on `rejected` table, otherwise run on main table
  
  # if optional parameters are not null, add 'WHERE {optional parameter}' string to query 
  
  
  return "Last processed: today"
  