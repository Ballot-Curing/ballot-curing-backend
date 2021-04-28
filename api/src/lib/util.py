def mysql_connect(state):
    import MySQLdb
    from config import load_config
    
    config = load_config()

    mydb = MySQLdb.connect(host=config['DATABASE']['host'],
        user=config['DATABASE']['user'],
        passwd=config['DATABASE']['passwd'],
        db=config[state]['db'], 
        local_infile = 1)

    cursor = mydb.cursor(MySQLdb.cursors.DictCursor)
    return cursor

