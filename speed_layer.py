

# speed_layer.py - PySpark Streaming with MapReduce Pattern

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from textblob import TextBlob
import time
import boto3
import json
from datetime import datetime
import pytz
import traceback

print("=" * 60)
print("SPEED LAYER STARTING (PySpark MapReduce)")
print("=" * 60)

LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')

def get_local_timestamp():
    return datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M%S")

def get_local_datetime():
    return datetime.now(LOCAL_TIMEZONE)

spark = SparkSession.builder \
    .appName("SpeedLayer") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============ MAPPER FUNCTIONS ============

def map_sentiment(text):
    """MAP: Extract sentiment from text"""
    try:
        if not text:
            return ("Neutral", 1)
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return ("Positive", 1)
        elif polarity < -0.1:
            return ("Negative", 1)
        else:
            return ("Neutral", 1)
    except:
        return ("Neutral", 1)

def map_keywords(text):
    """MAP: Extract keywords from text"""
    if not text:
        return []
    words = str(text).lower().split()
    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'for', 
                 'with', 'without', 'of', 'to', 'is', 'i', 'you', 'we', 'they', 
                 'he', 'she', 'it', 'my', 'your', 'our', 'their', 'from', 'this',
                 'that', 'these', 'those', 'then', 'than', 'so', 'too', 'very'}
    return [(w, 1) for w in words if w not in stopwords and len(w) > 3]

def reduce_counts(rdd):
    """REDUCE: Sum up counts"""
    return rdd.reduceByKey(lambda a, b: a + b)

def save_json_to_s3(data, prefix, filename):
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    
    if not data:
        return
    
    content = '\n'.join([json.dumps(row) for row in data])
    s3_client.put_object(
        Bucket=bucket,
        Key=f'{prefix}/{filename}',
        Body=content.encode('utf-8')
    )
    print(f"Saved to s3://{bucket}/{prefix}/{filename}")

def process_batch(df, batch_id):
    if df.count() == 0:
        return
    
    print(f"Processing {df.count()} records...")
    
    # Convert to RDD for MapReduce
    rdd = df.rdd.map(lambda row: row.asDict())
    
    # Extract text field
    text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
    total_records = text_rdd.count()
    
    if total_records == 0:
        print("No valid text records")
        return
    
    print(f"Valid records: {total_records}")
    timestamp = get_local_timestamp()
    results = []
    
    # ============ MAPREDUCE: SENTIMENT ============
    sentiment_rdd = text_rdd.flatMap(lambda text: [map_sentiment(text)])
    sentiment_counts = reduce_counts(sentiment_rdd).collect()
    
    for sentiment, count in sentiment_counts:
        results.append({
            'window_start': timestamp,
            'window_end': timestamp,
            'metric_type': 'sentiment',
            'metric_name': sentiment,
            'count': count,
            'rank': None
        })
    
    # ============ MAPREDUCE: KEYWORDS ============
    keyword_rdd = text_rdd.flatMap(lambda text: map_keywords(text))
    keyword_counts = reduce_counts(keyword_rdd) \
        .sortBy(lambda x: x[1], ascending=False) \
        .take(5)
    
    for i, (keyword, count) in enumerate(keyword_counts):
        results.append({
            'window_start': timestamp,
            'window_end': timestamp,
            'metric_type': 'trending',
            'metric_name': keyword,
            'count': count,
            'rank': i + 1
        })
    
    if results:
        save_json_to_s3(results, 'results/speed', f'speed_results_{timestamp}.json')
        print(f"Processed {total_records} records, saved {len(results)} results")

def read_and_process():
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    prefix = 'kinesis-data/'
    processed_keys = set()
    
    print("Speed Layer started. Checking for new files every 10 seconds...")
    print(f"Reading from: s3://{bucket}/{prefix}")
    print(f"Local timezone: {LOCAL_TIMEZONE.zone}")
    
    while True:
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    
                    if key in processed_keys or not key.endswith('.json'):
                        continue
                    
                    print(f"Processing new file: {key}")
                    
                    try:
                        file_response = s3_client.get_object(Bucket=bucket, Key=key)
                        content = file_response['Body'].read().decode('utf-8')
                    except Exception as e:
                        print(f"Error reading file: {e}")
                        continue
                    
                    records = []
                    for line in content.strip().split('\n'):
                        if line.strip():
                            try:
                                record = json.loads(line)
                                if record.get('text'):
                                    records.append(record)
                            except:
                                continue
                    
                    if records:
                        if len(records) > 500:
                            records = records[:500]
                        
                        try:
                            df = spark.createDataFrame(records)
                            process_batch(df, key)
                            processed_keys.add(key)
                        except Exception as e:
                            print(f"Error processing: {e}")
                            continue
            
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\nStopping Speed Layer...")
            break
        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()
            time.sleep(10)

if __name__ == "__main__":
    try:
        read_and_process()
    except KeyboardInterrupt:
        print("Speed Layer stopped")
    finally:
        spark.stop()
        print("Speed Layer finished")
