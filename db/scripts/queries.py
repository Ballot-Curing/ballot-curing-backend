def add_to_cured(cured_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {cured_db}(proc_date, county, voter_reg_id, first_name, middle_name, last_name, race, ethnicity, gender, age, street_address, city, state, zip, election_dt,
				party_code, precinct, cong_dist, st_house, st_senate, ballot_style, ballot_req_dt, ballot_send_dt, ballot_ret_dt, ballot_issue, ballot_rtn_status)
			VALUES("{entry["proc_date"]}", "{entry["county"]}", "{entry["voter_reg_id"]}", "{entry["first_name"]}", "{entry["middle_name"]}", "{entry["last_name"]}", "{entry["race"]}",
			"{entry["ethnicity"]}", "{entry["gender"]}", "{entry["age"]}", "{entry["street_address"]}", "{entry["city"]}", "{entry["state"]}", "{entry["zip"]}", "{entry["election_dt"]}",
			"{entry["party_code"]}", "{entry["precinct"]}", "{entry["cong_dist"]}", "{entry["st_house"]}", "{entry["st_senate"]}", "{entry["ballot_style"]}", "{entry["ballot_req_dt"]}",
			"{entry["ballot_send_dt"]}", "{entry["ballot_ret_dt"]}", "{entry["ballot_issue"]}", "{entry["ballot_rtn_status"]}");
			'''

def add_to_cured_NC(cured_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {cured_db}(proc_date, county, voter_reg_id, first_name, middle_name, last_name, race, ethnicity, gender, age, street_address, city, state, zip, election_dt,
				party_code, precinct, cong_dist, st_house, st_senate, ballot_style, ballot_req_dt, ballot_send_dt, ballot_ret_dt, ballot_issue, ballot_rtn_status)
			VALUES("{entry["proc_date"]}", "{entry["county"]}", "{entry["voter_reg_id"]}", "{entry["first_name"]}", "{entry["middle_name"]}", "{entry["last_name"]}", "{entry["race"]}",
			"{entry["ethnicity"]}", "{entry["gender"]}", "{entry["age"]}", "{entry["street_address"]}", "{entry["city"]}", "{entry["state"]}", "{entry["zip"]}", "{entry["election_dt"]}",
			"{entry["party_code"]}", "{entry["precinct"]}", "{entry["cong_dist"]}", "{entry["st_house"]}", "{entry["st_senate"]}", "{entry["ballot_style"]}", "{entry["ballot_req_dt"]}",
			"{entry["ballot_send_dt"]}", "{entry["ballot_ret_dt"]}", "{entry["ballot_issue"]}", "{entry["ballot_rtn_status"]}");
			'''

def add_to_rejected(rejected_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {rejected_db}(proc_date, county, voter_reg_id, first_name, middle_name, last_name, race, ethnicity, gender, age, street_address, city, state, zip, election_dt,
				party_code, precinct, cong_dist, st_house, st_senate, ballot_style, ballot_req_dt, ballot_send_dt, ballot_ret_dt, ballot_issue, ballot_rtn_status)
			VALUES("{entry["proc_date"]}", "{entry["county"]}", "{entry["voter_reg_id"]}", "{entry["first_name"]}", "{entry["middle_name"]}", "{entry["last_name"]}", "{entry["race"]}",
			"{entry["ethnicity"]}", "{entry["gender"]}", "{entry["age"]}", "{entry["street_address"]}", "{entry["city"]}", "{entry["state"]}", "{entry["zip"]}", "{entry["election_dt"]}",
			"{entry["party_code"]}", "{entry["precinct"]}", "{entry["cong_dist"]}", "{entry["st_house"]}", "{entry["st_senate"]}", "{entry["ballot_style"]}", "{entry["ballot_req_dt"]}",
			"{entry["ballot_send_dt"]}", "{entry["ballot_ret_dt"]}", "{entry["ballot_issue"]}", "{entry["ballot_rtn_status"]}");
			'''
   
def add_to_rejected_NC(rejected_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {rejected_db}(proc_date, county, voter_reg_id, first_name, middle_name, last_name, race, ethnicity, gender, age, street_address, city, state, zip, election_dt,
				party_code, precinct, cong_dist, st_house, st_senate, ballot_style, ballot_req_dt, ballot_send_dt, ballot_ret_dt, ballot_issue, ballot_rtn_status)
			VALUES("{entry["proc_date"]}", "{entry["county"]}", "{entry["voter_reg_id"]}", "{entry["first_name"]}", "{entry["middle_name"]}", "{entry["last_name"]}", "{entry["race"]}",
			"{entry["ethnicity"]}", "{entry["gender"]}", "{entry["age"]}", "{entry["street_address"]}", "{entry["city"]}", "{entry["state"]}", "{entry["zip"]}", "{entry["election_dt"]}",
			"{entry["party_code"]}", "{entry["precinct"]}", "{entry["cong_dist"]}", "{entry["st_house"]}", "{entry["st_senate"]}", "{entry["ballot_style"]}", "{entry["ballot_req_dt"]}",
			"{entry["ballot_send_dt"]}", "{entry["ballot_ret_dt"]}", "{entry["ballot_issue"]}", "{entry["ballot_rtn_status"]}");
			'''

def create_cured_table(cured_db):
    return f'''
		CREATE TABLE IF NOT EXISTS {cured_db} (
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
			ballot_send_dt         	DATETIME,
			ballot_ret_dt	          DATETIME,
			ballot_issue            VARCHAR(255),
			ballot_rtn_status       VARCHAR(50)
		);
		'''

def create_rejected_table(rejected_db):
    return f'''
		CREATE TABLE IF NOT EXISTS {rejected_db} (
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
			ballot_send_dt         	DATETIME,
			ballot_ret_dt	          DATETIME,
			ballot_issue            VARCHAR(255),
			ballot_rtn_status       VARCHAR(50)
		);
		'''


def get_today_rejected(table, today_datetime, cured_db):
    return f'''
    SELECT DISTINCT(today.voter_reg_id), today.zip, today.county, today.election_dt, ballot_ret_dt
    FROM {table} as today
    LEFT JOIN {cured_db} as cured
    ON today.voter_reg_id = cured.voter_reg_id
    WHERE 
    cured.voter_reg_id IS NULL 
    AND today.ballot_rtn_status = "R"
    AND ballot_ret_dt = "{today_datetime}";
    '''

def get_cured_GA(table):
	return f'''
	SELECT rej.voter_reg_id, accepted.zip, accepted.county, accepted.election_dt, rej.ballot_ret_dt as rejection_dt, accepted.ballot_ret_dt as cured_dt, rej.ballot_issue
	FROM {table} as accepted
	INNER JOIN ( 
	SELECT voter_reg_id, ballot_rtn_status as status, ballot_ret_dt,
	ballot_issue
	FROM {table} 
	WHERE ballot_rtn_status='R' 
	AND ballot_ret_dt IS NOT NULL) rej
	ON accepted.voter_reg_id = rej.voter_reg_id 
	WHERE ballot_rtn_status='A'
	AND accepted.ballot_ret_dt IS NOT NULL;
	'''

def get_cured_NC(table, today_datetime):
	return f'''
	SELECT *
	FROM {table}
	WHERE ballot_issue = 'ACCEPTED - CURED' AND 
	voter_reg_id IN (SELECT distinct(voter_reg_id) FROM {table});
	'''

def get_rejected_NC(table, today_datetime):
	return f'''
	SELECT *
	FROM {table}
	WHERE ballot_rtn_status = 'R' AND 
	voter_reg_id IN (SELECT distinct(voter_reg_id) FROM {table});
	'''