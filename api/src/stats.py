import MySQLdb
import sys
import json
from flask import Blueprint
from flask import abort
from flask import jsonify
from flask import request
from datetime import datetime

from config import load_config
import util

from lib import util, stats_util

stats_bp = Blueprint('stats',__name__)

@stats_bp.route('/', methods=['GET'])
def state_stats():
    try:
        # get required params
        state = request.args['state'].upper()
        elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')
    except:
        abort(400, description = 'Bad Request')

    # connect to the database
    mydb = util.mysql_connect(state)
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

    # query to get statewide stats
    query = f'''
    SELECT *
    FROM state_stats
    WHERE election_dt = '{elec_dt}'
    ORDER BY proc_date ASC;
    '''
    
    cursor.execute(query)

    row = cursor.fetchone()

    if not row:
        print(query)
        abort(404, description = 'Not Found')

    # string parsing to convert rej_reason into the right form
    rej_reason = json.loads(row['rej_reason'])
    rej_reason = stats_util.process_json(rej_reason) 

    # get demographic stats
    demo_stats = stats_util.get_demographics(state, row)

    # builds dictionary manually due to the processing that was needed
    ret = {
        "state" : state,
	"election_dt" : row['election_dt'],
        "proc_date" : row['proc_date'],
        "total_rejected": row['tot_rejected'],
        "total_cured" : row['tot_cured'],
        "total_processed" : row['tot_processed'],
        "rejected_gender" : demo_stats['gender_rej'],
        "cured_gender" : demo_stats['gender_cur'],
        "total_gender" : demo_stats['gender_tot'],
        "rejected_race" : demo_stats['race_rej'],
        "cured_race" : demo_stats['race_cur'],
        "total_race" : demo_stats['race_tot'],
        "rejected_age_group" : demo_stats['age_rej'],
        "cured_age_group" : demo_stats['age_cur'],
        "total_age_group" : demo_stats['age_tot'],
        "ballot_issue_count" : rej_reason,
    }

    response = jsonify(ret)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

@stats_bp.route('/county_stats/', methods=['GET'])
def county_stats():
    try:
        # get required params
        state = request.args['state'].upper()
        elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')

        mydb = util.mysql_connect(state)
        cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
    except:
        abort(400, 'Bad Request')

    # run query
    query = f'''
    SELECT *
    FROM county_stats
    WHERE election_dt = '{elec_date}'
    ORDER BY proc_date ASC;
    '''

    response = stats_util.get_county_data(cursor, query, state, elec_dt)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

@stats_bp.route('/county_stats/<county>', methods=['GET'])
def single_county_stats(county):
    # get required params
    state = request.args['state'].upper()
    elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')

    # connect to the database
    mydb = util.mysql_connect(state)
    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)

    # run query
    query = f'''
    SELECT *
    FROM county_stats
    WHERE (election_dt = '{elec_dt}'
    AND county = "{county}"
    ORDER BY proc_date ASC);
    '''

    response = stats_util.get_county_data(cursor, query, state, elec_dt)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

