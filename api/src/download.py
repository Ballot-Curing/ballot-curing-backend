import MySQLdb
import csv

from flask import Blueprint
from flask import request as req
from flask import abort
from flask import send_file

from datetime import datetime

from schema import schema_col_names
from config import load_config
from ballots import perform_query

download_bp = Blueprint('download', __name__)

config = load_config()

@download_bp.route('/')
def download():
    # perform the query to get the cursor
    cur = perform_query()

    # get the row headers
    row_headers = [x[0] for x in cur.description]
    id_idx = row_headers.index('id')
    row_headers.pop(id_idx)    

    # END : end of similarity with ballots.py endpoint

    # get the file name
    filename = f"vote_{datetime.now().strftime('%d_%m_%y_%H_%M_%S')}.csv"
    with open(f"./output/{filename}", 'w', newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(row_headers) # write the row headers
        rows = cur.fetchall()
        for row in rows:
            # deletes id from row then writes it to the csv
            mod_row = list(row)
            mod_row.pop(id_idx)
            writer.writerow(mod_row)

    response = send_file(f"./output/{filename}", mimetype = 'text/csv', attachment_filename = filename, as_attachment=True)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Disposition'

    return response
