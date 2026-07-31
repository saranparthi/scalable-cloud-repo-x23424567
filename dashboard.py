
# dashboard.py
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import boto3
import time
import os
from datetime import datetime
import pytz

app = Flask(__name__)
CORS(app)

# AWS Configuration
athena_client = boto3.client('athena', region_name='us-east-1')
BUCKET_NAME = 's3-bucket-x23424567'
ATHENA_OUTPUT = f's3://{BUCKET_NAME}/athena-results/'

# Timezone configuration
LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')  # Change to your timezone

def get_local_timestamp():
    """Get current timestamp in local timezone"""
    return datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')

def run_athena_query(query):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'default'},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        query_id = response['QueryExecutionId']
        
        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)
        
        if status == 'SUCCEEDED':
            response = athena_client.get_query_results(QueryExecutionId=query_id)
            rows = response['ResultSet']['Rows']
            if len(rows) > 1:
                columns = [col['VarCharValue'] for col in rows[0]['Data']]
                data = []
                for row in rows[1:]:
                    values = [col.get('VarCharValue', '') for col in row['Data']]
                    data.append(dict(zip(columns, values)))
                return data
        return []
    except Exception as e:
        print(f'Query failed: {e}')
        return []

@app.route('/api/batch', methods=['GET'])
def get_batch():
    query = """
    SELECT 
        metric_type,
        metric_name,
        count,
        percentage,
        hour
    FROM batch_results
    ORDER BY metric_type, count DESC
    """
    results = run_athena_query(query)
    return jsonify(results)

@app.route('/api/speed', methods=['GET'])
def get_speed():
    query = """
    SELECT 
        window_start,
        window_end,
        metric_type,
        metric_name,
        count,
        rank
    FROM speed_results
    WHERE window_start = (SELECT MAX(window_start) FROM speed_results)
    ORDER BY metric_type, count DESC
    """
    results = run_athena_query(query)
    return jsonify(results)

@app.route('/api/speed-history', methods=['GET'])
def get_speed_history():
    query = """
    SELECT 
        window_start,
        metric_type,
        metric_name,
        count
    FROM speed_results
    ORDER BY window_start DESC
    LIMIT 50
    """
    results = run_athena_query(query)
    return jsonify(results)

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    batch = run_athena_query("SELECT count FROM batch_results")
    speed = run_athena_query("SELECT count FROM speed_results WHERE window_start = (SELECT MAX(window_start) FROM speed_results)")
    
    batch_total = sum(int(row['count']) for row in batch) if batch else 0
    speed_total = sum(int(row['count']) for row in speed) if speed else 0
    
    metrics = {
        'batch_total': batch_total,
        'speed_total': speed_total,
        'timestamp': get_local_timestamp()
    }
    return jsonify(metrics)

@app.route('/api/entities', methods=['GET'])
def get_entities():
    """Get NER results from batch layer"""
    query = """
    SELECT 
        metric_name,
        count,
        percentage
    FROM batch_results
    WHERE metric_type = 'entity'
    ORDER BY count DESC
    """
    results = run_athena_query(query)
    return jsonify(results)

@app.route('/api/batch-topics', methods=['GET'])
def get_batch_topics():
    """Get topic distribution from batch layer"""
    query = """
    SELECT 
        metric_name,
        count,
        percentage
    FROM batch_results
    WHERE metric_type = 'topic'
    ORDER BY count DESC
    """
    results = run_athena_query(query)
    return jsonify(results)

@app.route('/')
def index():
    return send_from_directory('.', 'templates/index.html')

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy', 
        'timestamp': get_local_timestamp()
    })

if __name__ == '__main__':
    print("Starting Dashboard Server...")
    print("Access dashboard at: http://localhost:5000")
    print("Or use: Run -> Preview Running Application in Cloud9")
    app.run(host='0.0.0.0', port=5000, debug=False)
    
