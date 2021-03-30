from flask import Blueprint
import MySQLdb

lastProcessed_bp = Blueprint('lastProcessed', __name__)

@lastProcessed_bp.route('/lastProcessed')
def lastProcessed():
  
  state = "ga" # temp
  
  mydb = MySQLdb.connect(host=config['DATABASE']['host'],
    user=config['DATABASE']['user'],
    passwd=config['DATABASE']['passwd'],
    db="vote_"+state, 
    local_infile = 1)
  
  # query `election` table for last proc_dt
  return "Last processed: today"
  