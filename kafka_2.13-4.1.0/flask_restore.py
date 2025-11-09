import threading
from flask import Flask, render_template_string, jsonify
import time

# Create Flask app
app = Flask(__name__)

# Simple HTML template for the EV Charging Dashboard
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>EV Charging System</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .status { background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .online { color: green; font-weight: bold; }
        .offline { color: red; font-weight: bold; }
    </style>
</head>
<body>
    <div class="header">
        <h1>EV Charging System Dashboard</h1>
        <p>Central Monitoring Interface</p>
    </div>
    
    <div class="status">
        <h2>System Status: <span class="online">ONLINE</span></h2>
        <p>Central Server: <span class="online">Running</span></p>
        <p>Last Update: {{ last_update }}</p>
    </div>
    
    <div>
        <h3>Charging Stations</h3>
        <ul>
            <li>Station CP001: <span class="online">Available</span></li>
            <li>Station CP002: <span class="online">Available</span></li>
            <li>Station CP003: <span class="offline">Offline</span></li>
        </ul>
    </div>
    
    <div>
        <h3>Active Sessions: 2</h3>
        <p>Total Energy Delivered: 45.6 kWh</p>
    </div>
</body>
</html>
'''

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE, last_update=time.strftime('%Y-%m-%d %H:%M:%S'))

@app.route('/api/status')
def api_status():
    return jsonify({
        'status': 'online',
        'stations_connected': 2,
        'active_sessions': 2,
        'total_energy': 45.6
    })

def start_flask_dashboard():
    print('Starting Flask dashboard on http://localhost:5000')
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

# Start Flask in a separate thread
flask_thread = threading.Thread(target=start_flask_dashboard, daemon=True)
flask_thread.start()
print('Flask dashboard thread started - interface should be available at http://localhost:5000')
