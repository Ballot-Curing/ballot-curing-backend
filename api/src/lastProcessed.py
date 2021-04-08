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
		state = state.lower()
	else:
		return "ERROR: No state specified"

	# get election_dt parameter
	if 'election_dt' in request.args:
		election_dt = datetime.strptime(request.args['election_dt'], '%m/%d/%y')
	else:
		return "ERROR: No election date provided"
		
	# parse the config file
	config = configparser.ConfigParser()
        if not config.read('config.ini'):
            raise Exception('config.ini not in current directory. Please run again from top-level directory.')
  
	# connect to the database
	mydb = MySQLdb.connect(host=config['DATABASE']['host'],
		user=config['DATABASE']['user'],
		passwd=config['DATABASE']['passwd'],
		db="vote_"+state, 
		local_infile = 1)

	# run query to get processed date for that election
	cursor = mydb.cursor()
	query = " SELECT processed FROM processed WHERE election = ' " + election_dt.strftime("%y/%m/%d") + " '; "
	cursor.execute(query)

	# get result (note should only be one row since only one matching election)
	output = cursor.fetchall()
	for row in output:
		result = row[0]
	
	# put results into a dictionary and return as a json
	ret_dict = {
		"state" : state,
		"election_dt" : election_dt.strftime("%m/%d/%Y"),
		"last_proc" : result.strftime("%m/%d/%Y")
	}

	return jsonify(ret_dict)
	
