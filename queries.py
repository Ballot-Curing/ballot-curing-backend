
def query_for_accepted(table, today_datetime, entry):
    return f'''
			SELECT voter_reg_num, zip, county, ballot_ret_dt
			FROM {table}
			WHERE ballot_rtn_status = "A"
			AND ballot_ret_dt = "{today_datetime}"
			AND voter_reg_num = "{entry["voter_reg_num"]}";
			'''


def add_to_cured(cured_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {cured_db}(voter_reg_num, zip, county, election_dt, rejection_dt, cured_dt, ballot_issue)
			VALUES({entry["voter_reg_num"]}, {entry["zip"]}, "{entry["county"]}", "{entry["election_dt"]}", "{entry["rejection_dt"]}", "{entry["cured_dt"]}", "{entry["ballot_issue"]}");
			'''


def remove_from_rejected(rejected_db, entry):
    return f'''
			DELETE
			FROM {rejected_db}
			WHERE voter_reg_num = "{entry["voter_reg_num"]}";
			'''


def add_to_rejected(rejected_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {rejected_db}(voter_reg_num, zip, county, election_dt, rejection_dt)
			VALUES({entry["voter_reg_num"]}, {entry["zip"]}, "{entry["county"]}", "{entry["election_dt"]}", "{today_datetime}");
			'''


def create_cured_table(cured_db):
    return f'''
		CREATE TABLE IF NOT EXISTS {cured_db} (
			voter_reg_num           INT NOT NULL PRIMARY KEY,
			zip                     VARCHAR(10),
			county									VARCHAR(25),
			election_dt             DATETIME,
			rejection_dt            DATETIME,
			cured_dt			         	DATETIME,
      ballot_issue            VARCHAR(255)
		);
		'''


def create_rejected_table(rejected_db):
    return f'''
		CREATE TABLE IF NOT EXISTS {rejected_db} (
			voter_reg_num           INT,
			zip                     VARCHAR(10),
			county									VARCHAR(25),
			election_dt             DATETIME,
			rejection_dt            DATETIME
		);
		'''


def get_today_rejected(table, today_datetime):
    return f'''
		SELECT DISTINCT(voter_reg_num), zip, county, election_dt, ballot_ret_dt
		FROM {table}
		WHERE ballot_rtn_status = "R"
		AND ballot_ret_dt = "{today_datetime}";
		'''


def get_all_rejected(rejected_db):
    return f'''
		SELECT *
		FROM {rejected_db};
		'''

def get_cured(table):
  return f'''
  SELECT rej.voter_reg_num, accepted.zip, accepted.county, accepted.election_dt, rej.ballot_ret_dt as rejection_dt, accepted.ballot_ret_dt as cured_dt, rej.ballot_issue
  FROM {table} as accepted
  INNER JOIN ( 
  SELECT voter_reg_num, ballot_rtn_status as status, ballot_ret_dt,
  ballot_issue
  FROM {table} 
  WHERE ballot_rtn_status='R' 
  AND ballot_ret_dt IS NOT NULL) rej
  ON accepted.voter_reg_num = rej.voter_reg_num 
  WHERE ballot_rtn_status='A'
  AND accepted.ballot_ret_dt IS NOT NULL;

    '''