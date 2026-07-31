# # kinesis_producer.py - Real-time Bluesky data only

# import boto3
# import json
# import time
# import asyncio
# import websockets
# from datetime import datetime

# kinesis_client = boto3.client('kinesis', region_name='us-east-1')
# stream_name = 'kinesis-x23424567'

# async def bluesky_to_kinesis():
#     """Send real-time Bluesky data to Kinesis"""
    
#     print("Connecting to Bluesky Jetstream...")
#     record_count = 0
#     start_time = time.time()
    
#     try:
#         async with websockets.connect(
#             "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
#             max_size=None
#         ) as websocket:
#             print("Connected to Bluesky Jetstream")
#             print("Sending to Kinesis stream: " + stream_name)
#             print("Press Ctrl+C to stop")
#             print("")
            
#             while True:
#                 try:
#                     message = await websocket.recv()
#                     data = json.loads(message)
                    
#                     commit = data.get('commit', {})
#                     record = commit.get('record', {})
#                     text = record.get('text', '')
#                     langs = record.get('langs', [])
                    
#                     if not text or 'en' not in langs:
#                         continue
                    
#                     kinesis_record = {
#                         'timestamp': datetime.now().isoformat(),
#                         'text': text,
#                         'langs': langs,
#                         'did': data.get('did'),
#                         'created_at': record.get('createdAt'),
#                         'record_id': record_count
#                     }
                    
#                     kinesis_client.put_record(
#                         StreamName=stream_name,
#                         Data=json.dumps(kinesis_record).encode('utf-8'),
#                         PartitionKey=data.get('did', 'default')
#                     )
                    
#                     record_count += 1
                    
#                     if record_count % 10 == 0:
#                         elapsed = time.time() - start_time
#                         print("Sent " + str(record_count) + " records (" + str(record_count/elapsed) + " rec/sec)")
                    
#                 except json.JSONDecodeError as e:
#                     print("JSON decode error: " + str(e))
#                     continue
#                 except Exception as e:
#                     print("Error processing message: " + str(e))
#                     continue
                    
#     except websockets.exceptions.ConnectionClosed:
#         print("Connection to Bluesky closed. Reconnecting...")
#         await asyncio.sleep(2)
#         await bluesky_to_kinesis()
#     except KeyboardInterrupt:
#         elapsed = time.time() - start_time
#         print("")
#         print("Stopped after " + str(record_count) + " records in " + str(elapsed) + " seconds")
#         print("Average throughput: " + str(record_count/elapsed) + " rec/sec")
#     except Exception as e:
#         print("Fatal error: " + str(e))

# def create_kinesis_stream():
#     """Create Kinesis stream if it doesn't exist"""
#     try:
#         response = kinesis_client.describe_stream(StreamName=stream_name)
#         print("Stream '" + stream_name + "' already exists")
#         return True
#     except kinesis_client.exceptions.ResourceNotFoundException:
#         print("Creating stream '" + stream_name + "'...")
#         kinesis_client.create_stream(
#             StreamName=stream_name,
#             ShardCount=1,
#             StreamModeDetails={'StreamMode': 'ON_DEMAND'}
#         )
        
#         print("Waiting for stream to become active...")
#         while True:
#             try:
#                 response = kinesis_client.describe_stream(StreamName=stream_name)
#                 status = response['StreamDescription']['StreamStatus']
#                 if status == 'ACTIVE':
#                     print("Stream '" + stream_name + "' is ACTIVE")
#                     return True
#                 print("Stream status: " + status + ", waiting...")
#                 time.sleep(5)
#             except Exception as e:
#                 print("Error checking stream: " + str(e))
#                 time.sleep(5)

# if __name__ == '__main__':
#     print("Kinesis Producer - Bluesky Real-time Stream")
#     print("=" * 50)
    
#     create_kinesis_stream()
    
#     try:
#         asyncio.run(bluesky_to_kinesis())
#     except KeyboardInterrupt:
#         print("")
#         print("Producer stopped")




# kinesis_producer.py

import boto3
import json
import asyncio
import websockets
from datetime import datetime
import time

s3_client = boto3.client('s3')
BUCKET_NAME = 's3-bucket-x23424567'

async def bluesky_to_s3():
    print("Connecting to Bluesky Jetstream...")
    record_count = 0
    start_time = time.time()
    batch_records = []
    
    try:
        async with websockets.connect(
            "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
            max_size=None
        ) as websocket:
            print("Connected to Bluesky Jetstream")
            print("Writing to S3: s3://" + BUCKET_NAME + "/kinesis-data/")
            print("Press Ctrl+C to stop")
            print("")
            
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    commit = data.get('commit', {})
                    record = commit.get('record', {})
                    text = record.get('text', '')
                    langs = record.get('langs', [])
                    
                    if not text or 'en' not in langs:
                        continue
                    
                    kinesis_record = {
                        'timestamp': datetime.now().isoformat(),
                        'text': text,
                        'langs': langs,
                        'did': data.get('did'),
                        'created_at': record.get('createdAt'),
                        'record_id': record_count
                    }
                    
                    batch_records.append(kinesis_record)
                    record_count += 1
                    
                    if len(batch_records) >= 10:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        content = '\n'.join([json.dumps(r) for r in batch_records])
                        
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=f'kinesis-data/data_{timestamp}_{record_count}.json',
                            Body=content.encode('utf-8')
                        )
                        
                        elapsed = time.time() - start_time
                        print(f"Sent {record_count} records to S3 ({record_count/elapsed:.1f} rec/sec)")
                        batch_records = []
                    
                except json.JSONDecodeError as e:
                    continue
                except Exception as e:
                    print("Error: " + str(e))
                    continue
                    
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed. Reconnecting...")
        await asyncio.sleep(2)
        await bluesky_to_s3()
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print("")
        print("Stopped after " + str(record_count) + " records in " + str(elapsed) + " seconds")
        print("Average throughput: " + str(record_count/elapsed) + " rec/sec")

if __name__ == '__main__':
    print("Bluesky to S3 Producer")
    print("=" * 50)
    asyncio.run(bluesky_to_s3())
