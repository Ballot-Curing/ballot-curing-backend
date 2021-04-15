import MySQLdb
import configparser

from flask import Blueprint

stats_bp = Blueprint('stats',__name__)

config = configparser.ConfigParser()
# input path to config file
config.read('/home/cs310_prj3/Ballot-Curing-Project/config.ini')

@stats_bp.route('/', methods=['GET'])
def stats():
    return "this is the stat's endpoint"