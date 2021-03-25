from flask import Flask
from lastProcessed import lastProcessed_bp

app = Flask(__name__)
app.register_blueprint(lastProcessed_bp)

@app.route('/')
def hello_world():
    return 'Hello, World!'
