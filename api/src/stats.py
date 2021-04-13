from flask import Blueprint
from flask import request
from flask import jsonify
import MySQLdb
import configparser
from datetime import datetime

stats_bp = Blueprint('stats', __name__)

@stats_bp.route('/')
def stats():
	return "You've reached the stats endpoint temporary"
