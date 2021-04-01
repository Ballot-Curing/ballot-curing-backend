# General schema creation functions

def schema_table(table):
    return f'''
    CREATE TABLE IF NOT EXISTS {table} (
      id                      INT NOT NULL AUTO_INCREMENT, 
      proc_date               DATETIME,
      county                  VARCHAR(25),
      voter_reg_id            VARCHAR(25),
      first_name              VARCHAR(50),
      middle_name             VARCHAR(50),
      last_name               VARCHAR(50),
      race                    VARCHAR(50),
      ethnicity               VARCHAR(50),
      gender                  VARCHAR(10),
      age                     INT,
      street_address          VARCHAR(255),
      city                    VARCHAR(50),
      state                   VARCHAR(10),
      zip                     VARCHAR(10),
      election_dt             DATETIME,
      party_code              VARCHAR(10),
      precinct                VARCHAR(50),
      cong_dist               VARCHAR(50),
      st_house                VARCHAR(50),
      st_senate               VARCHAR(50),
      ballot_style            VARCHAR(50),
      ballot_req_dt           DATETIME,
      ballot_send_dt          DATETIME,
      ballot_ret_dt	      DATETIME,
      ballot_issue            VARCHAR(255),
      ballot_rtn_status       VARCHAR(50),
      PRIMARY KEY(id)
    );
    '''

def schema_index(table):
   return f'''
    CREATE UNIQUE INDEX IF NOT EXISTS voter_idx ON {table} (county, voter_reg_num, ballot_style, ballot_req_dt, ballot_ret_dt, ballot_issue, ballot_rtn_status);
    ''' 


# State-specific functions

def ga_load(csv, table):
    return f'''
    LOAD DATA LOCAL INFILE '{csv}'
    INTO TABLE {table}
    CHARACTER SET latin1
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS (county, voter_reg_id, last_name, first_name, middle_name, @dummy, @street_no,
               @street_name, @apt_no, city, state, zip, @dummy, @dummy, @dummy, @dummy,
                   @dummy, @dummy, @dummy, ballot_rtn_status, ballot_issue, @req_dt, @send_dt,
                   @ret_dt, ballot_style, @dummy, @dummy, @dummy, @dummy, precinct, cong_dist, 
                   st_senate, st_house, @dummy, @dummy, @dummy, @dummy, @dummy, party_code)
                   SET proc_date = NOW(),
                       street_address = CONCAT(@street_no, ' ', @street_name, ' ', @apt_no),
                       ballot_req_dt = STR_TO_DATE(@req_dt, '%m/%d/%Y'),
                       ballot_send_dt = STR_TO_DATE(@send_dt, '%m/%d/%Y'),
                       ballot_ret_dt = STR_TO_DATE(@ret_dt, '%m/%d/%Y');
    '''

