import MySQLdb
import configparser

from flask import Blueprint
from flask import abort
from flask import jsonify
from flask import request

from datetime import datetime

stats_bp = Blueprint('stats',__name__)

config = configparser.ConfigParser()
if not config.read('../../config.ini'):
    raise Exception('config.ini not in current directory. Please run again from top-level directory.')

@stats_bp.route('/', methods=['GET'])
def stats():
    # get required params
    state = request.args['state'].upper()
    elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')

    # connect to the database
    mydb = MySQLdb.connect(host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'], 
        local_infile = 1)

    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

    # query to get statewide stats
    query = f'''
    SELECT *
    FROM state_stats
    WHERE election_dt = '{elec_dt.strftime("%y/%m/%d")}';
    '''
    
    cursor.execute(query)

    rows = cursor.fetchall()

    for row in rows:
        election_dt = row['election_dt']
        tot_rej = row['tot_rejected']
        tot_cured = row['tot_cured']
        tot_processed = row['tot_processed']
        break

    query = f'''
    SELECT *
    FROM county_stats
    WHERE election_dt = '{elec_dt.strftime("%y/%m/%d")}';
    '''
    cursor.execute(query)

    rows = cursor.fetchall()

    county_reject = []
    county_cured = []
    county_processed = []

    for row in rows:
        county_reject.append({"name" : row['county'].title(), "value" : row['tot_rejected']})
        county_cured.append({"name" : row['county'].title(), "value" : row['tot_cured']})
        county_processed.append({"name" : row['county'].title(), "value" : row['tot_processed']})


    ret = {
        "state" : state,
	    "election_dt" : election_dt.strftime("%m/%d/%Y"),
        "total_rejected" : tot_rej,
        "total_cured" : tot_cured,
        "county_rejected" : county_reject,
        "county_cured" : county_cured,
        "county_processed" : county_processed
    }

    response = jsonify(ret)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response