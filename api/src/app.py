from flask import Flask
from lastProcessed import lastProcessed_bp
from ballots import ballots_bp
from stats import stats_bp

app = Flask(__name__)
app.register_blueprint(lastProcessed_bp, url_prefix='/api/v1/lastProcessed')
app.register_blueprint(ballots_bp, url_prefix='/api/v1/ballots')
app.register_blueprint(stats_bp, url_prefix='/api/v1/stats')

app.register_blueprint(lastProcessed_bp, url_prefix='/api/v1/lastProcessed')
app.register_blueprint(ballots_bp, url_prefix='/api/v1/ballots')

@app.route('/')
def root():
    return 'You have reached the CS310 vote-by-mail project\'s API.'
