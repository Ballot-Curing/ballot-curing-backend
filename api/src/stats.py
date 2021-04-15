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

    query = 'SELECT * FROM ' + config[state]['state_stats_table'] + " WHERE election_dt = '" + elec_dt.strftime("%y/%m/%d") + "'; "
    
    cursor.execute(query)

    rows = cursor.fetchall()

    for row in rows:
        election_dt = row[2]
        tot_rej = row[3]
        tot_cured = row[4]
        break

    query = "SELECT * FROM " + config[state]['county_stats_table'] + " WHERE election_dt = '" + elec_dt.strftime("%y/%m/%d") + "'; "
    print("Debug: " + query)
    cursor.execute(query)

    rows = cursor.fetchall()

    county_reject = []
    county_cured = []

    for row in rows:
        county_reject.append({"name" : row[1].title(), "value" : row[4]})
        county_cured.append({"name" : row[1].title(), "value" : row[5]})


    ret = {
        "state" : "GA",
	    "election_dt" : election_dt.strftime("%m/%d/%Y"),
        "total_rejected" : tot_rej,
        "total_cured" : tot_cured,
        "county_rejected" : county_reject,
        "county_cured" : county_cured,
    }

    response = jsonify(ret)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response