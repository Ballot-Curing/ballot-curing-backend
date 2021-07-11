import MySQLdb
import csv
import os

from flask import Blueprint
from flask import request as req
from flask import abort
from flask import send_file
from flask import after_this_request

from datetime import datetime
from zipfile import ZipFile

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

    date_idxs = [
            row_headers.index('proc_date'), 
            row_headers.index('ballot_ret_dt'), 
            row_headers.index('ballot_req_dt'), 
            row_headers.index('ballot_send_dt')
            ]

    # get the file name
    filename = None
    file_type = None
    if ('vol_count' in req.args):
        num_vol = int(req.args['vol_count'])

        file_type = 'application/zip'
        # update to add functionality
        filename = f"vote_{datetime.now().strftime('%d_%m_%y_%H_%M_%S')}.zip"

        rows = cur.fetchall()
        num_voters = len(rows)

        num_voters_per_vol = int(num_voters / num_vol)

        with ZipFile(f"./output/{filename}", 'w') as zip_file:
            
            for i in range(num_vol - 1):
                with open(f"./output/volunteer{i+1}.csv", 'w', newline = '') as file:
                    writer = csv.writer(file)
                    writer.writerow(row_headers) # write the row headers
                    for j in range(i * num_voters_per_vol, (i + 1) * num_voters_per_vol):
                        # deletes id from row then writes it to the csv
                        mod_row = list(rows[j])
                        mod_row.pop(id_idx)
                        
                        for idx in date_idxs:
                            if mod_row[idx]:
                                mod_row[idx] = datetime.strftime(mod_row[idx], '%m-%d-%Y') 
                        writer.writerow(mod_row)

                zip_file.write(f"./output/volunteer{i+1}.csv")
                os.remove(f"./output/volunteer{i+1}.csv")

            with open(f"./output/volunteer{num_vol}.csv", 'w', newline = '') as file:
                writer = csv.writer(file)
                writer.writerow(row_headers) # write the row headers
                for j in range((num_vol - 1) * num_voters_per_vol, num_voters):
                    # deletes id from row then writes it to the csv
                    mod_row = list(rows[j])
                    mod_row.pop(id_idx)
                        
                    for idx in date_idxs:
                        if mod_row[idx]:
                            mod_row[idx] = datetime.strftime(mod_row[idx], '%m-%d-%Y') 
                    writer.writerow(mod_row)

            zip_file.write(f"./output/volunteer{num_vol}.csv")  
            os.remove(f"./output/volunteer{num_vol}.csv")
            
        file_response = send_file(f"./output/{filename}", mimetype = file_type,
            attachment_filename = f"vote_{req.args['state'].upper()}_{datetime.now().strftime('%m_%d_%Y')}.zip", as_attachment=True) 
    else:
        file_type = 'text/csv'
        filename = f"vote_{datetime.now().strftime('%d_%m_%y_%H_%M_%S')}.csv"
        with open(f"./output/{filename}", 'w', newline = '') as file:
            writer = csv.writer(file)
            writer.writerow(row_headers) # write the row headers
            rows = cur.fetchall()
            for row in rows:
                # deletes id from row then writes it to the csv
                mod_row = list(row)
                mod_row.pop(id_idx)
                
                for idx in date_idxs:
                    if mod_row[idx]:
                        mod_row[idx] = datetime.strftime(mod_row[idx], '%m-%d-%Y') 
                writer.writerow(mod_row)
        file_response = send_file(f"./output/{filename}", mimetype = file_type,
            attachment_filename = f"vote_{req.args['state'].upper()}_{datetime.now().strftime('%m_%d_%Y')}.csv", as_attachment=True)


    file_response.headers['Access-Control-Allow-Origin'] = '*'
    file_response.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'
    file_response.headers['Access-Control-Allow-Headers'] = 'Content-Disposition'

    # this runs after the request and deletes the old file
    @after_this_request
    def delete_file(response):
        os.remove(f"./output/{filename}")
        return response

    return file_response
