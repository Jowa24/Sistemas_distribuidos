from flask import Flask
app = Flask(__name__)

@app.route('/')
def home():
    return """
    <h1>EV Charging System Test</h1>
    <p>If you can see this, Flask is working!</p>
    <p>Now we need to find your actual UI code.</p>
    """

if __name__ == '__main__':
    print("Starting test server on http://localhost:5000")
    print("Press Ctrl+C to stop")
    app.run(host='0.0.0.0', port=5000, debug=True)
