import MySQLdb

from flask import Blueprint

from config import load_config

download_bp = Blueprint('download', __name__)

config = load_config()

@download_bp.route('/')
def download():
    return "You've reached the download page"