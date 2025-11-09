import os
from flask import Flask, send_from_directory

app = Flask(__name__)

# Try different locations for index.html
@app.route('/')
def index():
    # Try serving from static first
    try:
        return send_from_directory('static', 'index.html')
    except:
        pass
    
    # Try serving from templates
    try:
        return send_from_directory('templates', 'index.html')
    except:
        pass
    
    # If neither works, return a simple message
    return '<h1>EV Charging System</h1><p>Dashboard is working but index.html not found in expected locations</p>'

if __name__ == '__main__':
    print('Testing Flask server...')
    print('Current directory:', os.getcwd())
    print('Static folder exists:', os.path.exists('static'))
    print('Templates folder exists:', os.path.exists('templates'))
    app.run(host='0.0.0.0', port=5000, debug=True)
