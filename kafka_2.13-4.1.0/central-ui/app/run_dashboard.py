from flask import Flask, render_template_string
import datetime

app = Flask(__name__)

# Your HTML content
HTML_CONTENT = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Charging Solution - Monitorization Panel</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Courier New', monospace;
        }
        
        body {
            background-color: #f0f0f0;
            padding: 20px;
            color: #333;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border: 1px solid #ccc;
            padding: 15px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        
        .header {
            text-align: center;
            margin-bottom: 20px;
            border-bottom: 2px solid #333;
            padding-bottom: 10px;
        }
        
        .header h1 {
            font-size: 24px;
            font-weight: bold;
        }
        
        .stations-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 10px;
            margin-bottom: 30px;
        }
        
        .station {
            border: 1px solid #999;
            padding: 10px;
            text-align: center;
            background: #fff;
        }
        
        .station-name {
            font-weight: bold;
            font-size: 18px;
            margin-bottom: 5px;
        }
        
        .station-address {
            font-size: 14px;
            margin-bottom: 5px;
        }
        
        .station-price {
            font-size: 14px;
            margin-bottom: 10px;
        }
        
        .driver-info {
            background: #e0e0e0;
            padding: 5px;
            margin-top: 10px;
            border: 1px dashed #999;
        }
        
        .driver-id {
            font-weight: bold;
        }
        
        .driver-energy, .driver-cost {
            font-size: 14px;
        }
        
        .section-title {
            font-weight: bold;
            text-align: center;
            margin: 20px 0 10px;
            font-size: 18px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        
        th, td {
            border: 1px solid #999;
            padding: 8px;
            text-align: center;
        }
        
        th {
            background-color: #e0e0e0;
            font-weight: bold;
        }
        
        .messages-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .message {
            border: 1px solid #999;
            padding: 10px;
            text-align: center;
            background: #fff;
        }
        
        .out-of-order {
            background-color: #ffcccc;
            font-weight: bold;
        }
        
        .system-ok {
            background-color: #ccffcc;
            font-weight: bold;
        }
        
        .footer {
            text-align: center;
            margin-top: 20px;
            font-style: italic;
            font-size: 14px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>SDSD BY CHARGING SOLUTION. MONITORIZATION PANEL ***</h1>
        </div>
        
        <div class="stations-grid">
            <!-- First row of stations -->
            <div class="station">
                <div class="station-name">ALC1</div>
                <div class="station-address">C/flatia 5</div>
                <div class="station-price">0,546/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">ALC3</div>
                <div class="station-address">Gran Via 2</div>
                <div class="station-price">0,546/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">MAD2</div>
                <div class="station-address">C/Serrano 10</div>
                <div class="station-price">0,66/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">MAD3</div>
                <div class="station-address">C/Per 23</div>
                <div class="station-price">0,486/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">MAD1</div>
                <div class="station-address">C/Argustles</div>
                <div class="station-price">0,46/kWh</div>
            </div>
            
            <!-- Second row of stations -->
            <div class="station">
                <div class="station-name">SEV3</div>
                <div class="station-address">C/Savilla 4</div>
                <div class="station-price">0,546/kWh</div>
                <div class="driver-info">
                    <div class="driver-id">Driver 24</div>
                    <div class="driver-energy">50kWh</div>
                    <div class="driver-cost">27 €</div>
                </div>
            </div>
            
            <div class="station">
                <div class="station-name">SEV2</div>
                <div class="station-address">Maestranza</div>
                <div class="station-price">0,46/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">VAL8</div>
                <div class="station-address">Museo Arts</div>
                <div class="station-price">0,56/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">VAL1</div>
                <div class="station-address">San Javier</div>
                <div class="station-price">0,486/kWh</div>
            </div>
            
            <div class="station">
                <div class="station-name">COR1</div>
                <div class="station-address">Mezquita</div>
                <div class="station-price">0,46/kWh</div>
                <div class="driver-info">
                    <div class="driver-id">Driver 234</div>
                    <div class="driver-energy">20kWh</div>
                    <div class="driver-cost">8 €</div>
                </div>
            </div>
        </div>
        
        <div class="section-title">** ON_GOING DRIVERS REQUESTS ***</div>
        
        <table>
            <tr>
                <th>DATE</th>
                <th>START TIME</th>
                <th>User ID</th>
                <th>CP</th>
            </tr>
            <tr>
                <td id="current-date">12/9/25</td>
                <td id="current-time">10:58</td>
                <td>5</td>
                <td>MAD2</td>
            </tr>
            <tr>
                <td>12/9/25</td>
                <td>9:00</td>
                <td>23</td>
                <td>SEV3</td>
            </tr>
            <tr>
                <td>12/9/25</td>
                <td>9:05</td>
                <td>234</td>
                <td>COR1</td>
            </tr>
        </table>
        
        <div class="section-title">** APLICATION MESSAGES ***</div>
        
        <div class="messages-grid">
            <div class="message out-of-order">ALC3 out of order</div>
            <div class="message out-of-order">SEV2 out of order</div>
            <div class="message system-ok">CENTRAL system status OK</div>
        </div>
        
        <div class="footer">
            vre 1: Example of a central system control panel
        </div>
    </div>

    <script>
        // Update time function
        function updateTime() {
            const now = new Date();
            
            // Format date as DD/MM/YY
            const day = String(now.getDate()).padStart(2, '0');
            const month = String(now.getMonth() + 1).padStart(2, '0');
            const year = String(now.getFullYear()).slice(-2);
            const currentDate = ${day}//;
            
            // Format time as HH:MM
            const hours = String(now.getHours()).padStart(2, '0');
            const minutes = String(now.getMinutes()).padStart(2, '0');
            const currentTime = ${hours}:;
            
            // Update the table
            document.getElementById('current-date').textContent = currentDate;
            document.getElementById('current-time').textContent = currentTime;
        }
        
        // Update time immediately and every minute
        updateTime();
        setInterval(updateTime, 60000);
    </script>
</body>
</html>'''

@app.route('/')
def dashboard():
    return render_template_string(HTML_CONTENT)

if __name__ == '__main__':
    print('🚀 Starting EV Charging Dashboard...')
    print('📊 Open your browser and go to: http://localhost:5000')
    print('⏹️  Press Ctrl+C to stop the server')
    app.run(host='0.0.0.0', port=5000, debug=True)
