
def query_for_accepted(table, today_datetime, entry):
    return f'''
			SELECT voter_reg_num, voter_zip, county_desc, ballot_rtn_dt
			FROM {table}
			WHERE ballot_rtn_status = "ACCEPTED"
			AND ballot_rtn_dt = "{today_datetime}"
			AND voter_reg_num = "{entry["voter_reg_num"]}";
			'''


def add_to_cured(cured_db, entry, today_datetime):
    return f'''
			INSERT IGNORE INTO {cured_db}(voter_reg_num, zip, county, election_dt, rejection_dt, cured_dt)
			VALUES({entry["voter_reg_num"]}, {entry["zip"]}, {entry["county"]}, "{entry["election_dt"]}", "{entry["rejection_dt"]}", "{today_datetime}");
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
			VALUES({entry["voter_reg_num"]}, {entry["zip"]}, {entry["county"]}, "{entry["election_dt"]}", "{today_datetime}");
			'''


def create_cured_table(cured_db):
    return f'''
		CREATE TABLE IF NOT EXISTS {cured_db} (
			voter_reg_num           INT,
			zip                     VARCHAR(10),
			county									VARCHAR(25),
			election_dt             DATETIME,
			rejection_dt            DATETIME,
			cured_dt			         	DATETIME
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
		SELECT DISTINCT(voter_reg_num), zip, county, election_dt, ballot_rtn_dt
		FROM {table}
		WHERE ballot_rtn_status = "REJECTED"
		AND ballot_rtn_dt = "{today_datetime}";
		'''


def get_all_rejected(rejected_db):
    return f'''
		SELECT *
		FROM {rejected_db};
		'''
