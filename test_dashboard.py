from flask import Flask, jsonify
import time
import socket

app = Flask(__name__)

@app.route('/')
def index():
    return """
    <html>
        <head><title>Dashboard Test</title></head>
        <body style="font-family: Arial; padding: 20px;">
            <h1>Dashboard is Running!</h1>
            <p>Time: {}</p>
            <p>Hostname: {}</p>
            <p>If you can see this, Flask is working properly.</p>
            <hr>
            <p><a href="/api/test">Test API</a></p>
        </body>
    </html>
    """.format(time.strftime('%Y-%m-%d %H:%M:%S'), socket.gethostname())

@app.route('/api/test')
def test():
    return jsonify({
        'status': 'ok',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'hostname': socket.gethostname()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
