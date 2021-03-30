from flask import Flask
from lastProcessed import lastProcessed_bp
from ballots import ballots_bp

app = Flask(__name__)
app.register_blueprint(lastProcessed_bp, url_prefix='/api/lastProcessed')
app.register_blueprint(ballots_bp, url_prefix='/api/ballots')

@app.route('/')
def hello_world():
    return 'Hello, World!'
