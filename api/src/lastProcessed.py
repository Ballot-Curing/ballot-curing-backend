from flask import Blueprint

lastProcessed_bp = Blueprint('lastProcessed', __name__)

@lastProcessed_bp.route('/lastProcessed')
def lastProcessed():
  return "Last processed: today"