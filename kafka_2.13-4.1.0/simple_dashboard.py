from flask import Flask, render_template_string, jsonify
import sqlite3
import time
import os

app = Flask(__name__)

# Clean Modern HTML template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>EV Charging Monitoring Panel</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }
        
        body {
            background: #f8fafc;
            color: #1e293b;
            padding: 0;
            line-height: 1.6;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }
        
        .header {
            background: white;
            padding: 24px;
            border-radius: 12px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            border-left: 4px solid #3b82f6;
        }
        
        .header h1 {
            color: #1e293b;
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        
        .header-subtitle {
            color: #64748b;
            font-size: 16px;
            font-weight: 500;
        }
        
        .stats-bar {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }
        
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            text-align: center;
        }
        
        .stat-number {
            font-size: 32px;
            font-weight: 700;
            color: #1e293b;
            margin-bottom: 4px;
        }
        
        .stat-label {
            font-size: 14px;
            color: #64748b;
            font-weight: 500;
        }
        
        .stations-section {
            background: white;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }
        
        .section-title {
            color: #1e293b;
            font-size: 20px;
            font-weight: 600;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .stations-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 16px;
        }
        
        .station-card {
            background: #f8fafc;
            border-radius: 8px;
            padding: 16px;
            border: 1px solid #e2e8f0;
            transition: all 0.2s ease;
        }
        
        .station-card:hover {
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
            transform: translateY(-1px);
        }
        
        .station-card.available {
            border-left: 4px solid #10b981;
        }
        
        .station-card.supplying {
            border-left: 4px solid #3b82f6;
        }
        
        .station-card.out-of-order {
            border-left: 4px solid #ef4444;
        }
        
        .station-id {
            font-weight: 600;
            font-size: 16px;
            color: #1e293b;
            margin-bottom: 8px;
        }
        
        .station-location {
            color: #64748b;
            font-size: 14px;
            margin-bottom: 8px;
        }
        
        .station-price {
            color: #059669;
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 12px;
        }
        
        .driver-info {
            background: #dbeafe;
            border-radius: 6px;
            padding: 12px;
            margin-top: 8px;
        }
        
        .out-of-order-banner {
            background: #fef2f2;
            color: #dc2626;
            border-radius: 6px;
            padding: 12px;
            margin-top: 8px;
            font-weight: 600;
            font-size: 14px;
            text-align: center;
        }
        
        .driver-id {
            font-weight: 600;
            color: #1e40af;
            margin-bottom: 4px;
            font-size: 14px;
        }
        
        .consumption {
            color: #475569;
            margin-bottom: 2px;
            font-size: 13px;
        }
        
        .cost {
            font-weight: 600;
            color: #059669;
            font-size: 14px;
        }
        
        .requests-section {
            background: white;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }
        
        .requests-table {
            width: 100%;
            border-collapse: collapse;
        }
        
        .requests-table th {
            background: #f8fafc;
            color: #475569;
            padding: 12px 16px;
            text-align: left;
            font-weight: 600;
            font-size: 14px;
            border-bottom: 1px solid #e2e8f0;
        }
        
        .requests-table td {
            padding: 12px 16px;
            border-bottom: 1px solid #f1f5f9;
            color: #475569;
            font-size: 14px;
        }
        
        .requests-table tr:last-child td {
            border-bottom: none;
        }
        
        .messages-section {
            background: white;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }
        
        .messages-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .message-item {
            padding: 12px 16px;
            border-radius: 6px;
            font-size: 14px;
            display: flex;
            align-items: center;
        }
        
        .message-item:before {
            content: "•";
            margin-right: 8px;
            font-size: 18px;
        }
        
        .message-error {
            background: #fef2f2;
            color: #dc2626;
            border-left: 3px solid #dc2626;
        }
        
        .message-info {
            background: #f0f9ff;
            color: #0369a1;
            border-left: 3px solid #0ea5e9;
        }
        
        .message-success {
            background: #f0fdf4;
            color: #059669;
            border-left: 3px solid #10b981;
        }
        
        .footer {
            text-align: center;
            color: #64748b;
            font-size: 14px;
            margin-top: 32px;
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
        }
        
        .refresh-btn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
            font-size: 14px;
            margin-left: 12px;
            transition: background 0.2s ease;
        }
        
        .refresh-btn:hover {
            background: #2563eb;
        }
        
        .status-indicator {
            display: inline-flex;
            align-items: center;
            background: #dcfce7;
            color: #166534;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
            margin-left: 12px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>EV Charging Network</h1>
            <div class="header-subtitle">Real-time Monitoring Panel</div>
        </div>
        
        <!-- Statistics Bar -->
        <div class="stats-bar">
            <div class="stat-card">
                <div class="stat-number" id="total-stations">0</div>
                <div class="stat-label">Total Stations</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="available-stations">0</div>
                <div class="stat-label">Available</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="supplying-stations">0</div>
                <div class="stat-label">In Use</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="faulty-stations">0</div>
                <div class="stat-label">Out of Order</div>
            </div>
        </div>
        
        <!-- Charging Stations -->
        <div class="stations-section">
            <div class="section-title">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M13 2L3 14h6l-1 8 10-12h-6l1-8z"></path>
                </svg>
                Charging Stations
            </div>
            <div class="stations-grid" id="stations-grid">
                <!-- Stations will be dynamically inserted here -->
            </div>
        </div>
        
        <!-- Ongoing Requests -->
        <div class="requests-section">
            <div class="section-title">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                </svg>
                Active Charging Sessions
            </div>
            <table class="requests-table" id="requests-table">
                <thead>
                    <tr>
                        <th>DATE</th>
                        <th>START TIME</th>
                        <th>USER ID</th>
                        <th>STATION</th>
                    </tr>
                </thead>
                <tbody id="requests-body">
                    <!-- Requests will be dynamically inserted here -->
                </tbody>
            </table>
        </div>
        
        <!-- System Messages -->
        <div class="messages-section">
            <div class="section-title">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"></path>
                </svg>
                System Messages
            </div>
            <div class="messages-list" id="messages-list">
                <!-- Messages will be dynamically inserted here -->
            </div>
        </div>
        
        <div class="footer">
            Last updated: <span id="last-update">Never</span> 
            <button class="refresh-btn" onclick="refreshData()">Refresh</button>
            <span class="status-indicator">System Online</span>
        </div>
    </div>

    <script>
        function refreshData() {
            fetch('/api/dashboard-data')
                .then(response => response.json())
                .then(data => {
                    updateDashboard(data);
                })
                .catch(error => {
                    console.error('Error:', error);
                    document.getElementById('messages-list').innerHTML = 
                        '<div class="message-item message-error">Error loading data from central system</div>';
                });
        }

        function updateDashboard(data) {
            updateStats(data.stats);
            updateStationsGrid(data.charging_points);
            updateRequestsTable(data.ongoing_requests);
            updateMessages(data.messages);
            document.getElementById('last-update').textContent = data.timestamp;
        }

        function updateStats(stats) {
            document.getElementById('total-stations').textContent = stats.total || 0;
            document.getElementById('available-stations').textContent = stats.available || 0;
            document.getElementById('supplying-stations').textContent = stats.supplying || 0;
            document.getElementById('faulty-stations').textContent = stats.faulty || 0;
        }

        function updateStationsGrid(stations) {
            const grid = document.getElementById('stations-grid');
            grid.innerHTML = '';
            
            stations.forEach(station => {
                const card = document.createElement('div');
                
                // Determine card class based on status
                let statusClass = 'available';
                if (station.status === 'IN_USAGE') {
                    statusClass = 'supplying';
                } else if (station.status === 'OUT_OF_ORDER' || station.status === 'DEFECT') {
                    statusClass = 'out-of-order';
                }
                
                card.className = `station-card ${statusClass}`;
                
                let statusHtml = '';
                if (station.status === 'IN_USAGE' && station.current_driver_id) {
                    statusHtml = `
                        <div class="driver-info">
                            <div class="driver-id">${station.current_driver_id}</div>
                            <div class="consumption">${station.current_power || 0} kWh</div>
                            <div class="cost">${station.current_amount || 0} €</div>
                        </div>
                    `;
                } else if (station.status === 'OUT_OF_ORDER' || station.status === 'DEFECT') {
                    statusHtml = `
                        <div class="out-of-order-banner">
                            OUT OF ORDER
                        </div>
                    `;
                }
                
                card.innerHTML = `
                    <div class="station-id">${station.id}</div>
                    <div class="station-location">${station.location}</div>
                    <div class="station-price">${station.price_per_kwh} €/kWh</div>
                    ${statusHtml}
                `;
                
                grid.appendChild(card);
            });
        }

        function updateRequestsTable(requests) {
            const tbody = document.getElementById('requests-body');
            tbody.innerHTML = '';
            
            if (requests.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="4" style="text-align: center; color: #94a3b8; font-style: italic; padding: 32px;">
                            No active charging sessions
                        </td>
                    </tr>
                `;
                return;
            }
            
            requests.forEach(request => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${request.date}</td>
                    <td>${request.start_time}</td>
                    <td><strong>${request.user_id}</strong></td>
                    <td><strong>${request.cp_id}</strong></td>
                `;
                tbody.appendChild(row);
            });
        }

        function updateMessages(messages) {
            const messagesContainer = document.getElementById('messages-list');
            messagesContainer.innerHTML = '';
            
            if (messages.length === 0) {
                messagesContainer.innerHTML = `
                    <div class="message-item message-info">
                        No new system messages
                    </div>
                `;
                return;
            }
            
            messages.forEach(message => {
                const messageDiv = document.createElement('div');
                let messageClass = 'message-info';
                if (message.type === 'error') {
                    messageClass = 'message-error';
                } else if (message.type === 'success') {
                    messageClass = 'message-success';
                }
                
                messageDiv.className = `message-item ${messageClass}`;
                messageDiv.textContent = message.text;
                messagesContainer.appendChild(messageDiv);
            });
        }

        // Load data immediately and then every 3 seconds
        refreshData();
        setInterval(refreshData, 3000);
    </script>
</body>
</html>
'''

def get_dashboard_data():
    """Get data from SQLite database and format it for the dashboard"""
    try:
        conn = sqlite3.connect('charging_network.db')
        cursor = conn.cursor()
        
        # Get charging points
        cursor.execute('''
            SELECT id, location, status, price_per_kwh, current_power, current_amount, current_driver_id
            FROM charging_points
        ''')
        charging_points = cursor.fetchall()
        
        # Convert to list of dictionaries
        cp_list = []
        for cp in charging_points:
            cp_list.append({
                'id': cp[0],
                'location': cp[1],
                'status': cp[2],
                'price_per_kwh': cp[3],
                'current_power': cp[4],
                'current_amount': cp[5],
                'current_driver_id': cp[6]
            })
        
        # Calculate statistics
        stats = {
            'total': len(cp_list),
            'available': len([cp for cp in cp_list if cp['status'] == 'ACTIVE']),
            'supplying': len([cp for cp in cp_list if cp['status'] == 'IN_USAGE']),
            'faulty': len([cp for cp in cp_list if cp['status'] in ['OUT_OF_ORDER', 'DEFECT']]),
            'disconnected': len([cp for cp in cp_list if cp['status'] == 'DISCONNECTED'])
        }
        
        # Generate sample ongoing requests
        ongoing_requests = [
            {'date': '12/9/25', 'start_time': '10:58', 'user_id': '5', 'cp_id': 'MAD2'},
            {'date': '12/9/25', 'start_time': '9:00', 'user_id': '23', 'cp_id': 'SEV3'},
            {'date': '12/9/25', 'start_time': '9:05', 'user_id': '234', 'cp_id': 'COR1'}
        ]
        
        # Generate application messages
        messages = [
            {'text': 'ALC3 out of order', 'type': 'error'},
            {'text': 'SEV2 out of order', 'type': 'error'},
            {'text': 'All systems operational', 'type': 'success'}
        ]
        
        conn.close()
        return {
            'charging_points': cp_list,
            'ongoing_requests': ongoing_requests,
            'messages': messages,
            'stats': stats,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        print(f"Database error: {e}")
        # Return sample data matching your image exactly
        sample_stations = [
            {'id': 'ALC1', 'location': 'C/failis 5', 'status': 'ACTIVE', 'price_per_kwh': '0,546', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'ALC3', 'location': 'Gran Via 2', 'status': 'OUT_OF_ORDER', 'price_per_kwh': '0,546', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'MAD2', 'location': 'O/Serrano 10', 'status': 'IN_USAGE', 'price_per_kwh': '0,664', 'current_power': '10,3', 'current_amount': '6,18', 'current_driver_id': 'Driver 5'},
            {'id': 'MAD3', 'location': 'C/Per 23', 'status': 'ACTIVE', 'price_per_kwh': '0,486', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'MAD1', 'location': 'C/Agquelles', 'status': 'ACTIVE', 'price_per_kwh': '0,467', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'SEV3', 'location': 'C/Sevilla 4', 'status': 'IN_USAGE', 'price_per_kwh': '0,546', 'current_power': '50', 'current_amount': '27', 'current_driver_id': 'Driver 24'},
            {'id': 'SEV2', 'location': 'Maestranza', 'status': 'OUT_OF_ORDER', 'price_per_kwh': '0,46', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'VAL8', 'location': 'Museo Arts', 'status': 'ACTIVE', 'price_per_kwh': '0,564', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'VAL1', 'location': 'San Javier', 'status': 'ACTIVE', 'price_per_kwh': '0,486', 'current_power': None, 'current_amount': None, 'current_driver_id': None},
            {'id': 'COR1', 'location': 'Macquita', 'status': 'IN_USAGE', 'price_per_kwh': '0,467', 'current_power': '20', 'current_amount': '8', 'current_driver_id': 'Driver 234'}
        ]
        
        stats = {
            'total': len(sample_stations),
            'available': len([s for s in sample_stations if s['status'] == 'ACTIVE']),
            'supplying': len([s for s in sample_stations if s['status'] == 'IN_USAGE']),
            'faulty': len([s for s in sample_stations if s['status'] in ['OUT_OF_ORDER', 'DEFECT']]),
            'disconnected': 0
        }
        
        return {
            'charging_points': sample_stations,
            'ongoing_requests': [
                {'date': '12/9/25', 'start_time': '10:58', 'user_id': '5', 'cp_id': 'MAD2'},
                {'date': '12/9/25', 'start_time': '9:00', 'user_id': '23', 'cp_id': 'SEV3'},
                {'date': '12/9/25', 'start_time': '9:05', 'user_id': '234', 'cp_id': 'COR1'}
            ],
            'messages': [
                {'text': 'ALC3 out of order', 'type': 'error'},
                {'text': 'SEV2 out of order', 'type': 'error'},
                {'text': 'All systems operational', 'type': 'success'}
            ],
            'stats': stats,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/dashboard-data')
def dashboard_data():
    data = get_dashboard_data()
    return jsonify(data)

if __name__ == '__main__':
    print("🚀 Starting Clean EV Charging Monitoring Panel...")
    print("📊 Dashboard will be available at: http://localhost:5000")
    print("⏹️  Press Ctrl+C to stop the server")
    app.run(host='0.0.0.0', port=5000, debug=True)