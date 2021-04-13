
# creates election table
def elections_table():
    return f'''
    CREATE TABLE IF NOT EXISTS elections (
      election_dt             DATETIME,
      proc_date               DATETIME,
      PRIMARY KEY(election_dt)
    );
    '''

# adds new entry for processed time
def elections_load(entry):
  return f'''
  INSERT INTO elections (election_dt, proc_date) 
  VALUES(STR_TO_DATE('{entry["election_dt"]}','%m_%d_%Y'),STR_TO_DATE("{entry["proc_date"]}",'%m/%d/%Y')) 
  ON DUPLICATE KEY UPDATE proc_date = STR_TO_DATE("{entry["proc_date"]}",'%m/%d/%Y');
  '''
