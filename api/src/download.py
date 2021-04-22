import MySQLdb
import csv

from flask import Blueprint
from flask import request as req
from flask import abort
from flask import send_file

from datetime import datetime

from schema import schema_col_names
from config import load_config

download_bp = Blueprint('download', __name__)

config = load_config()

@download_bp.route('/')
def download():
    # required parameters - throws error if not present
    try:
        state = req.args['state'].upper()
        elec_dt = datetime.strptime(req.args['election_dt'], '%m-%d-%Y')
    except:
        abort(404, description="Resource not found")
    
    # build WHERE clause for optional parameters on the fly for optimized SQL query times
    where_clause = ''

    # set any default values for params if needed
    rtn_status = req.args.get('ballot_rtn_status', 'R')
    where_clause += f'ballot_rtn_status = "{rtn_status}" AND '

    # optional parameters
    for param in schema_col_names:
        if param in where_clause or param == 'state' or param == 'election_dt': 
            continue
        else:
            val = req.args.get(param, None)
            where_clause += f'{param} = "{val}" AND ' if val else '' # double quotes important to prevent SQL injection

    # remove last AND
    where_clause = where_clause[:-5]


    limit = int(req.args.get('limit', 10))
    limit_clause = f'LIMIT {limit}' if limit != -1 else ''

    # TODO support historic data requests - run query on `rejected` table, otherwise run on main table
    historic = req.args.get('show_historic', False)

    try:
        mydb = MySQLdb.connect(host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'],
        local_infile = 1)
    except:
        # if connection failed, then input state was not valid
        abort(500, description="internal service failure")
    
    cur = mydb.cursor()

    db_table_name = elec_dt.strftime('%m_%d_%Y')

    query = f'''
    SELECT *
    FROM {db_table_name}
    WHERE {where_clause}
    {limit_clause};
    '''

    print(f'DEBUG:\n{query}')

    try:
        cur.execute(query)
    except:
        # if valid, then election_dt not valid
        abort(500, description="internal service failure")

    # get the row headers
    row_headers = [x[0] for x in cur.description]
    id_idx = row_headers.index('id')
    row_headers.pop(id_idx)    

    # get the file name
    filename = f"./output/votes_{datetime.now().strftime('%d_%m_%y_%H_%M_%S')}.csv"
    with open(filename, 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(row_headers) # write the row headers
        rows = cur.fetchall()
        for row in rows:
            # deletes id from row then writes it to the csv
            mod_row = list(row)
            mod_row.pop(id_idx)
            writer.writerow(mod_row)

    response = send_file(filename, mimetype = filename, attachment_filename = filename, as_attachment=True)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response