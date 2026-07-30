# kinesis_setup.py
import boto3
import json
import time
from datetime import datetime

# Create Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# Create stream
stream_name = 'kinesis-x23424567'
try:
    response = kinesis_client.create_stream(
        StreamName=stream_name,
        ShardCount=2,
        StreamModeDetails={'StreamMode': 'ON_DEMAND'}
    )
    print(f"Created Kinesis stream: {stream_name}")
except Exception as e:
    print(f"Stream may already exist: {e}")

# Wait for stream to become active
def wait_for_stream_active(stream_name):
    while True:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        status = response['StreamDescription']['StreamStatus']
        if status == 'ACTIVE':
            print(f"Stream {stream_name} is ACTIVE")
            break
        print(f"Stream status: {status}, waiting...")
        time.sleep(5)

wait_for_stream_active(stream_name)