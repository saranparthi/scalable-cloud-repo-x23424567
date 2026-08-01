# batch_layer.py

#!/usr/bin/env python3
"""
Batch Layer - PySpark with MapReduce Pattern
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, lit, when, explode, split, regexp_replace,
    lower, trim, size, count, sum, avg, max, min, desc
)
from pyspark.sql.types import *
from textblob import TextBlob
from datetime import datetime
import pytz
import json
import boto3
import re
import io
import csv
import traceback

print("=" * 60)
print("BATCH LAYER STARTING (PySpark MapReduce)")
print("=" * 60)

# ============ INCREASE THESE VALUES ============
MAX_RECORDS_TO_PROCESS = 500   # Increased from 500 to 50,000
MAX_FILES_TO_READ = 50         # Increased from 50 to 1,000
# =============================================

LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')

def get_local_timestamp():
    return datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M%S")

def get_local_datetime():
    return datetime.now(LOCAL_TIMEZONE)

# Create Spark session
spark = SparkSession.builder \
    .appName("BatchLayer") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============ MAPPER FUNCTIONS ============

def map_sentiment(text):
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



def map_topic(text):
    topics = {
        'technology': ['tech', 'software', 'code', 'computer', 'ai', 'data', 'digital', 'app', 'web', 
                      'algorithm', 'programming', 'developer', 'cloud', 'aws', 'python', 'java'],
        'politics': ['government', 'election', 'policy', 'vote', 'political', 'president', 'minister', 
                     'democracy', 'senate', 'congress', 'bill', 'law', 'parliament'],
        'sports': ['game', 'team', 'score', 'win', 'sport', 'player', 'football', 'cricket', 'basketball', 
                  'soccer', 'nfl', 'nba', 'match', 'tournament', 'championship'],
        'business': ['business', 'company', 'market', 'profit', 'stock', 'finance', 'economy', 'investment', 
                     'startup', 'ceo', 'revenue', 'earnings', 'quarterly'],
        'entertainment': ['movie', 'music', 'film', 'celebrity', 'entertainment', 'show', 'hollywood', 
                          'netflix', 'spotify', 'concert', 'actor', 'actress'],
        'health': ['health', 'doctor', 'hospital', 'medical', 'disease', 'covid', 'vaccine', 'wellness', 
                  'fitness', 'therapy', 'mental health'],
        'education': ['school', 'college', 'university', 'student', 'teacher', 'education', 'learn', 'study', 
                      'degree', 'classroom', 'professor', 'lecture'],
        'travel': ['travel', 'trip', 'vacation', 'flight', 'hotel', 'tourist', 'beach', 'mountain', 
                  'adventure', 'destination', 'airline', 'airport']
    }
    text_lower = str(text).lower()
    for topic, keywords in topics.items():
        for kw in keywords:
            if kw in text_lower:
                return (topic, 1)
    return ("general", 1)



def map_keywords(text):
    if not text:
        return []
    words = str(text).lower().split()
    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'for', 
                 'with', 'without', 'of', 'to', 'is', 'i', 'you', 'we', 'they', 
                 'he', 'she', 'it', 'my', 'your', 'our', 'their', 'from', 'this',
                 'that', 'these', 'those', 'then', 'than', 'so', 'too', 'very',
                 'just', 'like', 'get', 'got', 'can', 'will', 'would', 'could',
                 'should', 'may', 'might', 'must', 'shall', 'has', 'have', 'had'}
    return [(w, 1) for w in words if w not in stopwords and len(w) > 3]


def map_entities(text):
    entities = []
    text_lower = str(text).lower()
    
    person_keywords = ['president', 'prime minister', 'mr.', 'mrs.', 'dr.', 'prof.', 
                      'elon', 'musk', 'modi', 'trump', 'biden', 'putin', 'zelensky']
    org_keywords = ['google', 'amazon', 'microsoft', 'apple', 'facebook', 'meta', 'twitter', 
                    'tesla', 'spacex', 'nasa', 'united nations', 'world bank', 'imf']
    location_keywords = ['india', 'usa', 'united states', 'uk', 'united kingdom', 'china', 
                         'russia', 'europe', 'asia', 'africa', 'australia', 'canada']
    
    for keyword in person_keywords:
        if keyword in text_lower:
            entities.append(('PERSON', 1))
            break
    for keyword in org_keywords:
        if keyword in text_lower:
            entities.append(('ORGANIZATION', 1))
            break
    for keyword in location_keywords:
        if keyword in text_lower:
            entities.append(('LOCATION', 1))
            break
    if re.search(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', text_lower):
        entities.append(('DATE', 1))
    
    return entities



def read_data_from_s3():
    print(f"Reading data from S3 (max {MAX_FILES_TO_READ} files)...")
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    prefix = 'kinesis-data/'
    
    records = []
    file_count = 0
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=100)
        
        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    file_count += 1
                    try:
                        file_response = s3_client.get_object(Bucket=bucket, Key=key)
                        content = file_response['Body'].read().decode('utf-8')
                        for line in content.strip().split('\n'):
                            if line.strip():
                                try:
                                    record = json.loads(line)
                                    if record.get('text'):
                                        records.append(record)
                                        if len(records) >= MAX_RECORDS_TO_PROCESS:
                                            print(f"Reached max records ({MAX_RECORDS_TO_PROCESS})")
                                            return records
                                except:
                                    pass
                    except:
                        continue
                    if file_count >= MAX_FILES_TO_READ:
                        print(f"Reached max files ({MAX_FILES_TO_READ})")
                        return records
        
        # print(f"Read {len(records)} records from {file_count} files")
        print("="*60)
        print(f"FILES READ      : {file_count}")
        print(f"RECORDS FOUND   : {len(records)}")
        print(f"MAX RECORDS     : {MAX_RECORDS_TO_PROCESS}")
        print("="*60)
        return records
        
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return []


def reduce_counts(rdd):
    return rdd.reduceByKey(lambda a, b: a + b)

def save_to_s3(data, prefix, filename):
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    
    if not data:
        return False
    
    try:
        output = io.StringIO()
        fieldnames = data[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        
        s3_client.put_object(
            Bucket=bucket,
            Key=f'{prefix}/{filename}',
            Body=output.getvalue().encode('utf-8')
        )
        print(f"Saved to s3://{bucket}/{prefix}/{filename}")
        return True
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return False

def process_counts(counts_list, total_valid, metric_type):
    result = []
    for item in counts_list:
        if not isinstance(item, tuple) or len(item) != 2:
            continue
        key, count = item
        if total_valid > 0:
            percentage = round((float(count) / float(total_valid)) * 100.0, 2)
        else:
            percentage = 0.0
        result.append({
            "metric_type": metric_type,
            "metric_name": str(key),
            "count": int(count),
            "percentage": percentage,
            "hour": ""
        })
    return result

def main():
    try:
        records = read_data_from_s3()
        print(f"Records returned to Spark: {len(records)}")
        if not records:
            print("No data available")
            return
        
        rdd = spark.sparkContext.parallelize(records)
        total_records = rdd.count()
        print(f"Processing {total_records} records with PySpark MapReduce...")
        
        text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
        total_valid = text_rdd.count()
        print(f"Valid records: {total_valid}")
        
        if total_valid == 0:
            print("No valid records with text")
            return
        
        # Sentiment Analysis
        print("\n--- MAPREDUCE: Sentiment Analysis ---")
        sentiment_rdd = text_rdd.flatMap(lambda text: [map_sentiment(text)])
        sentiment_counts = reduce_counts(sentiment_rdd).collect()
        sentiment_data = process_counts(sentiment_counts, total_valid, 'sentiment')
        print(f"Sentiment results: {sentiment_counts}")
        
        # Keyword Extraction
        print("\n--- MAPREDUCE: Keyword Extraction ---")
        keyword_rdd = text_rdd.flatMap(lambda text: map_keywords(text))
        keyword_counts = reduce_counts(keyword_rdd).sortBy(lambda x: x[1], ascending=False).take(20)
        keyword_data = process_counts(keyword_counts, total_valid, 'keywords')
        for item in keyword_data:
            item['percentage'] = ''
        print(f"Top 5 keywords: {keyword_counts[:5] if keyword_counts else 'None'}")
        
        # Topic Classification
        print("\n--- MAPREDUCE: Topic Classification ---")
        topic_rdd = text_rdd.map(lambda text: map_topic(text))
        topic_counts = reduce_counts(topic_rdd).collect()
        topic_data = process_counts(topic_counts, total_valid, 'topic')
        print(f"Topic results: {topic_counts}")
        
        # NER
        print("\n--- MAPREDUCE: Named Entity Recognition (NER) ---")
        entity_rdd = text_rdd.flatMap(lambda text: map_entities(text))
        entity_counts = reduce_counts(entity_rdd).collect()
        entity_data = process_counts(entity_counts, total_valid, 'entity')
        print(f"Entity results: {entity_counts}")
        
        all_data = sentiment_data + keyword_data + topic_data + entity_data
        
        timestamp = get_local_timestamp()
        filename = f'batch_results_{timestamp}/part-00000.csv'
        success = save_to_s3(all_data, 'results/batch', filename)
        
        if success:
            print("\n" + "=" * 60)
            print("BATCH LAYER COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print(f"Total records processed: {total_valid}")
            print(f"Timestamp: {get_local_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            if sentiment_data:
                print("\nSentiment Distribution:")
                for item in sentiment_data:
                    print(f"  {item['metric_name']}: {item['count']} ({item['percentage']}%)")
            
            if keyword_data:
                print("\nTop 10 Keywords:")
                for i, item in enumerate(keyword_data[:10]):
                    print(f"  {i+1}. {item['metric_name']}: {item['count']}")
            
            if topic_data:
                print("\nTopic Distribution:")
                for item in topic_data:
                    print(f"  {item['metric_name']}: {item['count']} ({item['percentage']}%)")
            
            if entity_data:
                print("\nEntity Distribution (NER):")
                for item in entity_data:
                    print(f"  {item['metric_name']}: {item['count']} ({item['percentage']}%)")
            print("=" * 60)
        else:
            print("Failed to save results")
            
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
    spark.stop()
    print("Batch Layer finished")
