from flask import Flask, jsonify
import threading
import time

app = Flask(__name__)

@app.route('/')
def dashboard():
    return '<h1>EV Charging System Dashboard</h1><p>Flask is working!</p>'

@app.route('/status')
def status():
    return jsonify({'status': 'online', 'stations': 5, 'active_sessions': 2})

if __name__ == '__main__':
    print('Starting Flask test server on port 5001...')
    app.run(host='0.0.0.0', port=5001, debug=True)
