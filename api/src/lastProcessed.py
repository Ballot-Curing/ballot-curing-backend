from flask import Blueprint
from flask import request
from flask import jsonify
import MySQLdb
import configparser
from datetime import datetime

lastProcessed_bp = Blueprint('lastProcessed', __name__)

@lastProcessed_bp.route('/')
def lastProcessed():

    # get state parameter
    if 'state' in request.args:
        state = request.args['state']
        state = state.upper()
    else:
        return "404 bad request"

    # get election_dt parameter
    if 'election_dt' in request.args:
        election_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')
    else:
        return "404 bad request"

    # parse the config file
    config = configparser.ConfigParser()
    if not config.read('../../config.ini'):
        raise Exception('config.ini not in current directory. Please run again from top-level directory.')

    # connect to the database
    try:
        mydb = MySQLdb.connect(host=config['DATABASE']['host'],
            user=config['DATABASE']['user'],
            passwd=config['DATABASE']['passwd'],
            db=config[state]['db'], 
            local_infile = 1)
    except:
        return "404 bad request"

    # run query to get processed date for that election
    cursor = mydb.cursor()
    query = " SELECT processed FROM processed WHERE election = ' " + election_dt.strftime("%y/%m/%d") + " '; "
    cursor.execute(query)

    # get result (note should only be one row since only one matching election)
    output = cursor.fetchall()
    # if no result, then input date was invalid
    if len(output) == 0:
        return "404 bad request"
    for row in output:
        result = row[0]

    # put results into a dictionary and return as a json
    ret_dict = {
        "state" : state,
        "election_dt" : election_dt.strftime("%m/%d/%Y"),
        "last_proc" : result.strftime("%m/%d/%Y")
    }

    return jsonify(ret_dict)

