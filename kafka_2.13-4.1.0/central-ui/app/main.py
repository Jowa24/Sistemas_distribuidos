import sys
import os
import sqlite3
from flask import Flask, render_template, jsonify, request
import threading
import time

# Add the parent directory to the path to import EV_Utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from EV_Utils import DatabaseManager, ChargingPointStatus, InformationTypeMonitor
except ImportError:
    # Fallback for when running in Docker
    from EV_Utile import DatabaseManager, ChargingPointStatus, InformationTypeMonitor

app = Flask(__name__)

class DashboardManager:
    def __init__(self):
        self.db = DatabaseManager()
        self.update_interval = 2  # seconds
    
    def get_dashboard_data(self):
        """Get all data needed for the dashboard"""
        charging_points = self.db.get_charging_points()
        
        # Calculate statistics
        stats = {
            'total': len(charging_points),
            'available': len([cp for cp in charging_points if cp['status'] == ChargingPointStatus.ACTIVE.name]),
            'supplying': len([cp for cp in charging_points if cp['status'] == ChargingPointStatus.IN_USAGE.name]),
            'faulty': len([cp for cp in charging_points if cp['status'] in [
                ChargingPointStatus.OUT_OF_ORDER.name, 
                ChargingPointStatus.DEFECT.name
            ]]),
            'disconnected': len([cp for cp in charging_points if cp['status'] == ChargingPointStatus.DISCONNECTED.name])
        }
        
        # Enhance charging points data for display
        enhanced_cps = []
        for cp in charging_points:
            enhanced_cp = cp.copy()
            enhanced_cp['status_display'] = self.get_status_display(cp['status'])
            enhanced_cp['status_class'] = self.get_status_class(cp['status'])
            enhanced_cps.append(enhanced_cp)
        
        return {
            'charging_points': enhanced_cps,
            'stats': stats,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def get_status_display(self, status):
        """Convert status to display format with emojis"""
        status_map = {
            ChargingPointStatus.ACTIVE.name: "🟢 AVAILABLE",
            ChargingPointStatus.OUT_OF_ORDER.name: "🟠 OUT OF ORDER", 
            ChargingPointStatus.IN_USAGE.name: "🔵 SUPPLYING",
            ChargingPointStatus.DEFECT.name: "🔴 DEFECT",
            ChargingPointStatus.DISCONNECTED.name: "⚫ DISCONNECTED"
        }
        return status_map.get(status, "⚫ UNKNOWN")
    
    def get_status_class(self, status):
        """Convert status to CSS class"""
        status_map = {
            ChargingPointStatus.ACTIVE.name: "available",
            ChargingPointStatus.IN_USAGE.name: "supplying",
            ChargingPointStatus.OUT_OF_ORDER.name: "out-of-order",
            ChargingPointStatus.DEFECT.name: "defect",
            ChargingPointStatus.DISCONNECTED.name: "disconnected"
        }
        return status_map.get(status, "unknown")

# Global dashboard manager
dashboard_manager = DashboardManager()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/dashboard-data')
def get_dashboard_data():
    """API endpoint for dashboard data"""
    data = dashboard_manager.get_dashboard_data()
    return jsonify(data)

@app.route('/api/charging-points')
def get_charging_points():
    """API endpoint for charging points data only"""
    data = dashboard_manager.get_dashboard_data()
    return jsonify(data['charging_points'])

@app.route('/api/stats')
def get_stats():
    """API endpoint for statistics only"""
    data = dashboard_manager.get_dashboard_data()
    return jsonify(data['stats'])

@app.route('/api/control', methods=['POST'])
def control_cp():
    """API endpoint for controlling charging points"""
    data = request.json
    cp_id = data.get('cp_id')
    action = data.get('action')
    
    # Here you would integrate with your EV_Central to send control commands
    # For now, we'll just update the database
    if action == 'stop':
        dashboard_manager.db.update_cp_status(cp_id, ChargingPointStatus.OUT_OF_ORDER.name)
    elif action == 'resume':
        dashboard_manager.db.update_cp_status(cp_id, ChargingPointStatus.ACTIVE.name)
    
    return jsonify({'status': 'success', 'action': action, 'cp_id': cp_id})

def start_dashboard_server(host='0.0.0.0', port=5000):
    """Start the dashboard server"""
    print(f"Starting EV Charging Dashboard on http://{host}:{port}")
    app.run(host=host, port=port, debug=False)

if __name__ == '__main__':
    start_dashboard_server()