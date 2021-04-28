import MySQLdb
import json

from flask import Blueprint
from flask import abort
from flask import jsonify
from flask import request
from datetime import datetime

import queries

from lib import util, stats_util

stats_bp = Blueprint('stats',__name__)

@stats_bp.route('/', methods=['GET'])
def state_stats():
    # get required params
    state = request.args['state'].upper()
    elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')

    # connect to the database
    cursor = util.mysql_connect(state)

    # TODO: query for max proc_date for election, use in where clause to grab latest statistics

    # query to get statewide stats
    query = f'''
    SELECT *
    FROM state_stats
    WHERE election_dt = '{elec_dt.strftime("%y/%m/%d")}';
    '''
    
    cursor.execute(query)

    row = cursor.fetchone()

    # gets the fields
    election_dt = row['election_dt']
    tot_rej = row['tot_rejected']
    tot_cured = row['tot_cured']
    tot_processed = row['tot_processed']

    # string parsing to convert rej_reason into the right form
    rej_reason = json.loads(row['rej_reason'])
    rej_reason = process_json(rej_reason) 

    # get demographic stats
    demo_stats = get_demographics(state, row)

    # builds dictionary manually due to the processing that was needed
    ret = {
        "state" : state,
	"election_dt" : election_dt.strftime("%m/%d/%Y"),
        "total_rejected" : tot_rej,
        "total_cured" : tot_cured,
        "total_processed" : tot_processed,
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

    # get required params
    state = request.args['state'].upper()
    elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')

    cursor = util.mysql_connect(state)

    # run query
    query = f'''
    SELECT *
    FROM county_stats
    WHERE election_dt = '{elec_dt.strftime("%y/%m/%d")}';
    '''

    response = get_county_data(cursor, query, state, elec_dt)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

@stats_bp.route('/county_stats/<county>', methods=['GET'])
def single_county_stats(county):
    # get required params
    state = request.args['state'].upper()
    elec_dt = datetime.strptime(request.args['election_dt'], '%m-%d-%Y')

    # connect to the database
    cursor = util.mysql_connect(state)

    # run query
    query = f'''
    SELECT *
    FROM county_stats
    WHERE (election_dt = '{elec_dt.strftime("%y/%m/%d")}'
    AND county = "{county}");
    '''

    response = get_county_data(cursor, query, state, elec_dt)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@stats_bp.route('time_series/', methods=['GET'])
def time_series():
    state = request.args['state'].upper()
    county = request.args.get('county', None)

    cur = util.mysql_connect(state)
        
    query = queries.get_unique_rej_per_day(election_dt)
    
    try:
        cur.execute(query)
    except:
        # if valid, then election_dt not valid
        abort(500, description="internal service failure")

    
    row_headers = [x[0] for x in cur.description]
    id_idx = row_headers.index('id')
    row_headers.pop(id_idx)

    rows = cur.fetchall()

    data = []

    for row in rows:
        mod_row = list(row)
        mod_row.pop(id_idx)
        data.append(dict(zip(row_headers, mod_row)))


    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


