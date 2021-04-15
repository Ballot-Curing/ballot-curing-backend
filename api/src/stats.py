import MySQLdb
import configparser

from flask import Blueprint
from flask import abort
from flask import jsonify
from flask import request

from datetime import datetime

stats_bp = Blueprint('stats',__name__)

config = configparser.ConfigParser()
# input path to config file
config.read('../../config.ini')

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

    cursor = mydb.cursor()

    query = 'SELECT * FROM test_state_stats'  
    
    cursor.execute(query)

    rows = cursor.fetchall()

    for row in rows:
        election_dt = row[1]
        tot_rej = row[2]
        tot_cured = row[3]
        tot_acc = row[4]

    query = 'SELECT * FROM test_county_stats'  
    
    cursor.execute(query)

    rows = cursor.fetchall()

    county_reject = []
    county_accepted = []
    county_cured = []

    for row in rows:
        county_reject.append({"name" : row[0], "value" : row[3]})
        county_cured.append({"name" : row[0], "value" : row[4]})
        county_accepted.append({"name" : row[0], "value" : row[5]})


    ret = {
        "state" : "GA",
	    "election_dt" : election_dt.strftime("%m/%d/%Y"),
        "total_rejected" : tot_rej,
        "total_cured" : tot_cured,
        "total_accepted" : tot_acc,
        "county_rejected" : county_reject,
        "county_cured" : county_cured,
        "county_accepted" : county_accepted
    }

    return jsonify(ret)