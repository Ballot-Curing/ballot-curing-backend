def mysql_connect(state):
    import MySQLdb
    from config import load_config
    
    config = load_config()

    mydb = MySQLdb.connect(host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'], 
        local_infile = 1)

    return mydb

def find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return None

def safe_min_max(func, lst):
    try:
        return func([i for i in lst if i])
    except:
        return None
