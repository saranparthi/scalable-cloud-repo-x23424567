



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
import time
import boto3
import json
from datetime import datetime

print("Starting Speed Layer...")

spark = SparkSession.builder \
    .appName("SpeedLayer") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def sentiment_analysis(text):
    try:
        if not text:
            return "Neutral"
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return "Positive"
        elif polarity < -0.1:
            return "Negative"
        else:
            return "Neutral"
    except:
        return "Neutral"

def extract_keywords(text):
    if not text:
        return []
    words = str(text).lower().split()
    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'for', 
                 'with', 'without', 'of', 'to', 'is', 'i', 'you', 'we', 'they', 
                 'he', 'she', 'it', 'my', 'your', 'our', 'their'}
    words = [w for w in words if w not in stopwords and len(w) > 3]
    return words[:5]

sentiment_udf = udf(sentiment_analysis, StringType())
keywords_udf = udf(extract_keywords, ArrayType(StringType()))

def save_json_to_s3(data, prefix, filename):
    """Save JSON data to S3 using boto3"""
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
    
    processed = df \
        .withColumn("sentiment", sentiment_udf(col("text"))) \
        .withColumn("keywords", keywords_udf(col("text")))
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Collect results
    sentiment_counts = processed.groupBy("sentiment") \
        .agg(count("*").alias("count")) \
        .collect()
    
    keyword_counts = processed.select(explode(col("keywords")).alias("keyword")) \
        .groupBy("keyword") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(5) \
        .collect()
    
    results = []
    
    for row in sentiment_counts:
        results.append({
            'window_start': timestamp,
            'window_end': timestamp,
            'metric_type': 'sentiment',
            'metric_name': row['sentiment'],
            'count': row['count'],
            'rank': None
        })
    
    for i, row in enumerate(keyword_counts):
        results.append({
            'window_start': timestamp,
            'window_end': timestamp,
            'metric_type': 'trending',
            'metric_name': row['keyword'],
            'count': row['count'],
            'rank': i + 1
        })
    
    # Save to S3 using boto3
    save_json_to_s3(results, 'results/speed', f'speed_results_{timestamp}.json')
    print(f"Processed {df.count()} records, saved to S3")

def read_and_process():
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    prefix = 'kinesis-data/'
    processed_keys = set()
    
    print("Speed Layer started. Checking for new files every 10 seconds...")
    print(f"Reading from: s3://{bucket}/{prefix}")
    
    while True:
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    
                    if key in processed_keys or not key.endswith('.json'):
                        continue
                    
                    print(f"Processing new file: {key}")
                    
                    file_response = s3_client.get_object(Bucket=bucket, Key=key)
                    content = file_response['Body'].read().decode('utf-8')
                    
                    records = []
                    for line in content.strip().split('\n'):
                        if line.strip():
                            try:
                                records.append(json.loads(line))
                            except:
                                continue
                    
                    if records:
                        # Limit batch size
                        if len(records) > 500:
                            records = records[:500]
                        df = spark.createDataFrame(records)
                        process_batch(df, key)
                        processed_keys.add(key)
            
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\nStopping Speed Layer...")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("=" * 50)
    print("Speed Layer Started")
    print("=" * 50)
    try:
        read_and_process()
    except KeyboardInterrupt:
        print("Speed Layer stopped")
    finally:
        spark.stop()
