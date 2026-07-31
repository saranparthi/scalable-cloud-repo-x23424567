
# #!/usr/bin/env python3
# """
# Batch Layer - PySpark with MapReduce Pattern
# """
# from pyspark.sql import SparkSession
# # from pyspark.sql.functions import *


# import pyspark.sql.functions as F
# from pyspark.sql.functions import (
#     col,
#     lit,
#     when,
#     explode,
#     split,
#     regexp_replace,
#     lower,
#     trim,
#     size,
#     count,
#     sum,
#     avg,
#     max,
#     min,
#     desc
# )


# from pyspark.sql.types import *
# from textblob import TextBlob
# from datetime import datetime
# import pytz
# import json
# import boto3
# import re
# import io
# import csv
# import traceback

# print("=" * 60)
# print("BATCH LAYER STARTING (PySpark MapReduce)")
# print("=" * 60)

# MAX_RECORDS_TO_PROCESS = 5000
# LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')

# def get_local_timestamp():
#     return datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M%S")

# def get_local_datetime():
#     return datetime.now(LOCAL_TIMEZONE)

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("BatchLayer") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # ============ MAPPER FUNCTIONS ============

# def map_sentiment(text):
#     """MAP: Extract sentiment from text"""
#     try:
#         if not text:
#             return ("Neutral", 1)
#         blob = TextBlob(str(text))
#         polarity = blob.sentiment.polarity
#         if polarity > 0.1:
#             return ("Positive", 1)
#         elif polarity < -0.1:
#             return ("Negative", 1)
#         else:
#             return ("Neutral", 1)
#     except:
#         return ("Neutral", 1)

# def map_keywords(text):
#     """MAP: Extract keywords from text"""
#     if not text:
#         return []
#     words = str(text).lower().split()
#     stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'for', 
#                  'with', 'without', 'of', 'to', 'is', 'i', 'you', 'we', 'they', 
#                  'he', 'she', 'it', 'my', 'your', 'our', 'their', 'from', 'this',
#                  'that', 'these', 'those', 'then', 'than', 'so', 'too', 'very',
#                  'just', 'like', 'get', 'got', 'can', 'will', 'would', 'could',
#                  'should', 'may', 'might', 'must', 'shall', 'has', 'have', 'had'}
#     return [(w, 1) for w in words if w not in stopwords and len(w) > 3]

# def map_topic(text):
#     """MAP: Extract topic from text"""
#     topics = {
#         'technology': ['tech', 'software', 'code', 'computer', 'ai', 'data', 'digital', 'app', 'web', 
#                       'algorithm', 'programming', 'developer', 'cloud', 'aws', 'python', 'java'],
#         'politics': ['government', 'election', 'policy', 'vote', 'political', 'president', 'minister', 
#                      'democracy', 'senate', 'congress', 'bill', 'law', 'parliament'],
#         'sports': ['game', 'team', 'score', 'win', 'sport', 'player', 'football', 'cricket', 'basketball', 
#                   'soccer', 'nfl', 'nba', 'match', 'tournament', 'championship'],
#         'business': ['business', 'company', 'market', 'profit', 'stock', 'finance', 'economy', 'investment', 
#                      'startup', 'ceo', 'revenue', 'earnings', 'quarterly'],
#         'entertainment': ['movie', 'music', 'film', 'celebrity', 'entertainment', 'show', 'hollywood', 
#                           'netflix', 'spotify', 'concert', 'actor', 'actress'],
#         'health': ['health', 'doctor', 'hospital', 'medical', 'disease', 'covid', 'vaccine', 'wellness', 
#                   'fitness', 'therapy', 'mental health'],
#         'education': ['school', 'college', 'university', 'student', 'teacher', 'education', 'learn', 'study', 
#                       'degree', 'classroom', 'professor', 'lecture'],
#         'travel': ['travel', 'trip', 'vacation', 'flight', 'hotel', 'tourist', 'beach', 'mountain', 
#                   'adventure', 'destination', 'airline', 'airport']
#     }
#     text_lower = str(text).lower()
#     for topic, keywords in topics.items():
#         for kw in keywords:
#             if kw in text_lower:
#                 return (topic, 1)
#     return ("general", 1)

# def map_entities(text):
#     """MAP: Extract named entities from text"""
#     entities = []
#     text_lower = str(text).lower()
    
#     person_keywords = ['president', 'prime minister', 'mr.', 'mrs.', 'dr.', 'prof.', 
#                       'elon', 'musk', 'modi', 'trump', 'biden', 'putin', 'zelensky']
#     org_keywords = ['google', 'amazon', 'microsoft', 'apple', 'facebook', 'meta', 'twitter', 
#                     'tesla', 'spacex', 'nasa', 'united nations', 'world bank', 'imf']
#     location_keywords = ['india', 'usa', 'united states', 'uk', 'united kingdom', 'china', 
#                          'russia', 'europe', 'asia', 'africa', 'australia', 'canada']
    
#     for keyword in person_keywords:
#         if keyword in text_lower:
#             entities.append(('PERSON', 1))
#             break
#     for keyword in org_keywords:
#         if keyword in text_lower:
#             entities.append(('ORGANIZATION', 1))
#             break
#     for keyword in location_keywords:
#         if keyword in text_lower:
#             entities.append(('LOCATION', 1))
#             break
#     if re.search(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', text_lower):
#         entities.append(('DATE', 1))
    
#     return entities

# def reduce_counts(rdd):
#     """REDUCE: Sum up counts for key-value pairs"""
#     return rdd.reduceByKey(lambda a, b: a + b)

# def read_data_from_s3():
#     print("Reading data from S3...")
#     s3_client = boto3.client('s3')
#     bucket = 's3-bucket-x23424567'
#     prefix = 'kinesis-data/'
    
#     records = []
#     file_count = 0
    
#     try:
#         paginator = s3_client.get_paginator('list_objects_v2')
#         pages = paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=50)
        
#         for page in pages:
#             if 'Contents' not in page:
#                 continue
#             for obj in page['Contents']:
#                 key = obj['Key']
#                 if key.endswith('.json'):
#                     file_count += 1
#                     try:
#                         file_response = s3_client.get_object(Bucket=bucket, Key=key)
#                         content = file_response['Body'].read().decode('utf-8')
#                         for line in content.strip().split('\n'):
#                             if line.strip():
#                                 try:
#                                     record = json.loads(line)
#                                     if record.get('text'):
#                                         records.append(record)
#                                         if len(records) >= MAX_RECORDS_TO_PROCESS:
#                                             print(f"Reached max records ({MAX_RECORDS_TO_PROCESS})")
#                                             return records
#                                 except:
#                                     pass
#                     except:
#                         continue
#                     if file_count >= 100:
#                         print(f"Reached max files (100)")
#                         return records
        
#         print(f"Read {len(records)} records from {file_count} files")
#         return records
        
#     except Exception as e:
#         print(f"Error reading from S3: {e}")
#         return []

# def save_to_s3(data, prefix, filename):
#     s3_client = boto3.client('s3')
#     bucket = 's3-bucket-x23424567'
    
#     if not data:
#         return False
    
#     try:
#         output = io.StringIO()
#         fieldnames = data[0].keys()
#         writer = csv.DictWriter(output, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(data)
        
#         s3_client.put_object(
#             Bucket=bucket,
#             Key=f'{prefix}/{filename}',
#             Body=output.getvalue().encode('utf-8')
#         )
#         print(f"Saved to s3://{bucket}/{prefix}/{filename}")
#         return True
#     except Exception as e:
#         print(f"Error saving to S3: {e}")
#         return False

# # def process_counts(counts_list, total_valid, metric_type):
# #     """Process counts and return formatted data"""
# #     result = []
# #     for item in counts_list:
# #         # item is tuple (key, count) from RDD collect()
# #         if isinstance(item, tuple) and len(item) == 2:
# #             key, count = item
# #             percentage = round((count / total_valid) * 100, 2) if total_valid > 0 else 0.0
# #             result.append({
# #                 'metric_type': metric_type,
# #                 'metric_name': key,
# #                 'count': count,
# #                 'percentage': percentage,
# #                 'hour': ''
# #             })
# #     return result




# def process_counts(counts_list, total_valid, metric_type):
#     """Process counts and return formatted data"""
#     result = []

#     for item in counts_list:

#         if not isinstance(item, tuple) or len(item) != 2:
#             continue

#         key, count = item

#         if total_valid > 0:
#             percentage = round((float(count) / float(total_valid)) * 100.0, 2)
#         else:
#             percentage = 0.0

#         result.append({
#             "metric_type": metric_type,
#             "metric_name": str(key),
#             "count": int(count),
#             "percentage": percentage,
#             "hour": ""
#         })

#     return result


# def main():
#     try:
#         records = read_data_from_s3()
#         if not records:
#             print("No data available")
#             return
        
#         # Create RDD from records
#         rdd = spark.sparkContext.parallelize(records)
#         total_records = rdd.count()
#         print(f"Processing {total_records} records with PySpark MapReduce...")
        
#         # Extract text field
#         text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
#         total_valid = text_rdd.count()
#         print(f"Valid records: {total_valid}")
        
#         if total_valid == 0:
#             print("No valid records with text")
#             return
        
#         # ============ MAPREDUCE: SENTIMENT ANALYSIS ============
#         print("\n--- MAPREDUCE: Sentiment Analysis ---")
#         sentiment_rdd = text_rdd.flatMap(lambda text: [map_sentiment(text)])
#         sentiment_counts = reduce_counts(sentiment_rdd).collect()
#         sentiment_data = process_counts(sentiment_counts, total_valid, 'sentiment')
#         print(f"Sentiment results: {sentiment_counts}")
        
#         # ============ MAPREDUCE: KEYWORD EXTRACTION ============
#         print("\n--- MAPREDUCE: Keyword Extraction ---")
#         keyword_rdd = text_rdd.flatMap(lambda text: map_keywords(text))
#         keyword_counts = reduce_counts(keyword_rdd) \
#             .sortBy(lambda x: x[1], ascending=False) \
#             .take(20)
#         keyword_data = process_counts(keyword_counts, total_valid, 'keywords')
#         # Remove percentage for keywords
#         for item in keyword_data:
#             item['percentage'] = ''
#         print(f"Top 5 keywords: {keyword_counts[:5] if keyword_counts else 'None'}")
        
#         # ============ MAPREDUCE: TOPIC CLASSIFICATION ============
#         print("\n--- MAPREDUCE: Topic Classification ---")
#         topic_rdd = text_rdd.map(lambda text: map_topic(text))
#         topic_counts = reduce_counts(topic_rdd).collect()
#         topic_data = process_counts(topic_counts, total_valid, 'topic')
#         print(f"Topic results: {topic_counts}")
        
#         # ============ MAPREDUCE: NAMED ENTITY RECOGNITION ============
#         print("\n--- MAPREDUCE: Named Entity Recognition (NER) ---")
#         entity_rdd = text_rdd.flatMap(lambda text: map_entities(text))
#         entity_counts = reduce_counts(entity_rdd).collect()
#         entity_data = process_counts(entity_counts, total_valid, 'entity')
#         print(f"Entity results: {entity_counts}")
        
#         # ============ COMBINE ALL RESULTS ============
#         all_data = sentiment_data + keyword_data + topic_data + entity_data
        
#         # ============ SAVE TO S3 ============
#         timestamp = get_local_timestamp()
#         filename = f'batch_results_{timestamp}/part-00000.csv'
#         success = save_to_s3(all_data, 'results/batch', filename)
        
#         if success:
#             print("\n" + "=" * 60)
#             print("BATCH LAYER COMPLETED SUCCESSFULLY")
#             print("=" * 60)
#             print(f"Total records processed: {total_valid}")
#             print(f"Timestamp: {get_local_datetime().strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
#             if sentiment_data:
#                 print("\nSentiment Distribution:")
#                 for item in sentiment_data:
#                     print(f"  {item['metric_name']}: {item['count']} ({item['percentage']}%)")
            
#             if keyword_data:
#                 print("\nTop 10 Keywords:")
#                 for i, item in enumerate(keyword_data[:10]):
#                     print(f"  {i+1}. {item['metric_name']}: {item['count']}")
            
#             if topic_data:
#                 print("\nTopic Distribution:")
#                 for item in topic_data:
#                     print(f"  {item['metric_name']}: {item['count']} ({item['percentage']}%)")
            
#             if entity_data:
#                 print("\nEntity Distribution (NER):")
#                 for item in entity_data:
#                     print(f"  {item['metric_name']}: {item['count']} ({item['percentage']}%)")
#             print("=" * 60)
#         else:
#             print("Failed to save results")
            
#     except Exception as e:
#         print(f"Error: {e}")
#         traceback.print_exc()

# if __name__ == "__main__":
#     main()
#     spark.stop()
#     print("Batch Layer finished")











# #!/usr/bin/env python3
# """
# Performance Benchmark for Lambda Architecture
# Measures throughput, latency, and speedup under different loads
# """
# from pyspark.sql import SparkSession
# # from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from textblob import TextBlob
# import time
# import boto3
# import json
# import io
# import csv
# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib
# matplotlib.use('Agg')
# from datetime import datetime
# import pytz
# import os
# import traceback
# import re
# import builtins




# print("=" * 60)
# print("PERFORMANCE BENCHMARK STARTING")
# print("=" * 60)

# LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')
# BUCKET_NAME = 's3-bucket-x23424567'

# def get_local_timestamp():
#     return datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M%S")

# # def get_local_datetime():
# #     return datetime.now(LOCAL_TIMEZONE)

# # ============ MAPPER FUNCTIONS ============

# def map_sentiment(text):
#     try:
#         if not text:
#             return ("Neutral", 1)
#         blob = TextBlob(str(text))
#         polarity = blob.sentiment.polarity
#         if polarity > 0.1:
#             return ("Positive", 1)
#         elif polarity < -0.1:
#             return ("Negative", 1)
#         else:
#             return ("Neutral", 1)
#     except:
#         return ("Neutral", 1)


# def map_keywords(text):
#     if not text:
#         return []

#     stopwords = {
#         'the','a','an','and','or','but','in','on','at','for',
#         'with','without','of','to','is','i','you','we','they',
#         'he','she','it','my','your','our','their','from','this',
#         'that','these','those','then','than','so','too','very',
#         'just','like','get','got','can','will','would','could',
#         'should','may','might','must','shall','has','have','had'
#     }

#     words = re.findall(r"\b[a-zA-Z]{4,}\b", str(text).lower())

#     return [(w, 1) for w in words if w not in stopwords]
    
    
    

# def map_topic(text):
#     topics = {
#         'technology': ['tech', 'software', 'code', 'computer', 'ai', 'data', 'digital'],
#         'politics': ['government', 'election', 'policy', 'vote', 'political', 'president'],
#         'sports': ['game', 'team', 'score', 'win', 'sport', 'player', 'football'],
#         'business': ['business', 'company', 'market', 'profit', 'stock', 'finance'],
#         'entertainment': ['movie', 'music', 'film', 'celebrity', 'entertainment', 'show']
#     }
#     text_lower = str(text).lower()
#     for topic, keywords in topics.items():
#         for kw in keywords:
#             if kw in text_lower:
#                 return (topic, 1)
#     return ("general", 1)

# def reduce_counts(rdd):
#     return rdd.reduceByKey(lambda a, b: a + b)

# def read_sample_data(records_count):
#     """Read sample data from S3"""
#     print(f"Reading {records_count} records from S3...")
#     s3_client = boto3.client('s3')
#     bucket = BUCKET_NAME
#     prefix = 'kinesis-data/'
    
#     records = []
    
#     try:
#         paginator = s3_client.get_paginator('list_objects_v2')
#         pages = paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=50)
        
#         for page in pages:
#             if 'Contents' not in page:
#                 continue
#             for obj in page['Contents']:
#                 key = obj['Key']
#                 if key.endswith('.json'):
#                     try:
#                         file_response = s3_client.get_object(Bucket=bucket, Key=key)
#                         content = file_response['Body'].read().decode('utf-8')
#                         for line in content.strip().split('\n'):
#                             if line.strip():
#                                 try:
#                                     record = json.loads(line)
#                                     if record.get('text'):
#                                         records.append(record)
#                                         if len(records) >= records_count:
#                                             print(f"Collected {len(records)} records")
#                                             return records
#                                 except:
#                                     pass
#                     except:
#                         continue
#         print(f"Collected {len(records)} records")
#         return records
        
#     except Exception as e:
#         print(f"Error reading from S3: {e}")
#         return []

# def run_sequential(records, total_valid, spark_session):
#     """Run sequential processing (1 core)"""
#     print(f"\n--- SEQUENTIAL MODE (1 core) ---")
#     start_time = time.time()
    
#     sc = spark_session.sparkContext
#     rdd = sc.parallelize(records)
#     text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
    
#     sentiment_counts = reduce_counts(text_rdd.flatMap(lambda text: [map_sentiment(text)])).collect()
#     keyword_counts = reduce_counts(text_rdd.flatMap(lambda text: map_keywords(text))).sortBy(lambda x: x[1], ascending=False).take(20)
#     topic_counts = reduce_counts(text_rdd.map(lambda text: map_topic(text))).collect()
    
#     end_time = time.time()
#     total_time = end_time - start_time
    
#     print(f"Sequential processing completed in {total_time:.2f} seconds")
#     print(f"Processed {total_valid} records")
#     print(f"Throughput: {total_valid/total_time:.2f} rec/sec")
    
#     return total_time, total_valid/total_time

# def run_parallel(records, total_valid, workers, spark_session):
#     """Run parallel processing"""

#     print(f"\n--- PARALLEL MODE ({workers} cores) ---")

#     start_time = time.time()

#     sc = spark_session.sparkContext

#     rdd = sc.parallelize(records, workers)

#     text_rdd = (
#         rdd.map(lambda x: x.get("text", ""))
#           .filter(lambda x: x and len(x) > 0)
#     )

#     reduce_counts(
#         text_rdd.flatMap(lambda text: [map_sentiment(text)])
#     ).collect()

#     reduce_counts(
#         text_rdd.flatMap(map_keywords)
#     ).sortBy(lambda x: x[1], ascending=False).take(20)

#     reduce_counts(
#         text_rdd.map(map_topic)
#     ).collect()

#     end_time = time.time()

#     total_time = end_time - start_time
#     throughput = total_valid / total_time

#     print(f"Parallel processing completed in {total_time:.2f} seconds")
#     print(f"Processed {total_valid} records")
#     print(f"Throughput: {throughput:.2f} rec/sec")

#     return total_time, throughput


# def measure_latency(records, ingestion_rate, spark_session):
#     """Measure latency at different ingestion rates"""
#     print(f"\n--- LATENCY TEST (Ingestion Rate: {ingestion_rate} rec/sec) ---")
    
#     delay = 1.0 / ingestion_rate if ingestion_rate > 0 else 0
#     latencies = []
    
#     sc = spark_session.sparkContext
#     rdd = sc.parallelize(records[:500])
#     text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
    
#     for text in text_rdd.collect():
#         record_start = time.time()
#         sentiment = map_sentiment(text)
#         keywords = map_keywords(text)
#         record_end = time.time()
#         latencies.append((record_end - record_start) * 1000)
        
#         if delay > 0:
#             time.sleep(delay)
    
#     avg_latency = sum(latencies) / len(latencies) if latencies else 0
#     p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
#     max_latency = max(latencies) if latencies else 0
    
#     print(f"  Avg Latency: {avg_latency:.2f} ms")
#     print(f"  P95 Latency: {p95_latency:.2f} ms")
#     print(f"  Max Latency: {max_latency:.2f} ms")
#     print(f"  Records processed: {len(latencies)}")
    
#     return avg_latency, p95_latency, max_latency

# def generate_graphs(results_df, output_dir):
#     """Generate all benchmark graphs"""
#     print("\n--- Generating Graphs ---")
    
#     # Graph 1: Speedup vs Worker Count
#     plt.figure(figsize=(10, 6))
#     for workload in results_df['workload'].unique():
#         subset = results_df[results_df['workload'] == workload]
#         plt.plot(subset['workers'], subset['speedup'], marker='o', label=f"{workload:,} records")
#     plt.xlabel('Number of Workers (Cores)')
#     plt.ylabel('Speedup')
#     plt.title('Speedup vs Worker Count')
#     plt.legend()
#     plt.grid(True)
#     plt.savefig(f'{output_dir}/speedup_vs_workers.png', dpi=150, bbox_inches='tight')
#     plt.close()
#     print("  Saved: speedup_vs_workers.png")
    
#     # Graph 2: Processing Time vs Records
#     plt.figure(figsize=(10, 6))
#     seq_data = results_df[results_df['mode'] == 'sequential']
#     par_data = results_df[results_df['mode'] == 'parallel']
    
#     if not seq_data.empty:
#         plt.plot(seq_data['workload'], seq_data['time_seconds'], 'bo-', label='Sequential')
    
#     for workers in par_data['workers'].unique():
#         subset = par_data[par_data['workers'] == workers]
#         if not subset.empty:
#             plt.plot(subset['workload'], subset['time_seconds'], 'o-', label=f'Parallel ({workers} cores)')
    
#     plt.xlabel('Records Count')
#     plt.ylabel('Processing Time (seconds)')
#     plt.title('Processing Time vs Records Count')
#     plt.legend()
#     plt.grid(True)
#     plt.savefig(f'{output_dir}/processing_time_vs_records.png', dpi=150, bbox_inches='tight')
#     plt.close()
#     print("  Saved: processing_time_vs_records.png")
    
#     # Graph 3: Throughput Comparison
#     plt.figure(figsize=(10, 6))
#     if not seq_data.empty:
#         plt.plot(seq_data['workload'], seq_data['throughput'], 'bo-', label='Sequential')
    
#     for workers in par_data['workers'].unique():
#         subset = par_data[par_data['workers'] == workers]
#         if not subset.empty:
#             plt.plot(subset['workload'], subset['throughput'], 'o-', label=f'Parallel ({workers} cores)')
    
#     plt.xlabel('Records Count')
#     plt.ylabel('Throughput (records/sec)')
#     plt.title('Throughput Comparison')
#     plt.legend()
#     plt.grid(True)
#     plt.savefig(f'{output_dir}/throughput_comparison.png', dpi=150, bbox_inches='tight')
#     plt.close()
#     print("  Saved: throughput_comparison.png")

# def save_results_to_s3(data, filename):
#     """Save results to S3"""
#     s3_client = boto3.client('s3')
#     bucket = BUCKET_NAME
#     prefix = 'benchmark-results/'
    
#     try:
#         output = io.StringIO()
#         if isinstance(data, pd.DataFrame):
#             data.to_csv(output, index=False)
#         else:
#             writer = csv.DictWriter(output, fieldnames=data[0].keys())
#             writer.writeheader()
#             writer.writerows(data)
        
#         s3_client.put_object(
#             Bucket=bucket,
#             Key=f'{prefix}{filename}',
#             Body=output.getvalue().encode('utf-8')
#         )
#         print(f"Saved to s3://{bucket}/{prefix}{filename}")
#         return True
#     except Exception as e:
#         print(f"Error saving to S3: {e}")
#         return False


# def format_number(val, decimals=2):
#     if val is None:
#         return 0.0

#     return builtins.round(float(val), decimals)

# def main():

    
#     spark = SparkSession.builder \
#         .appName("Benchmark") \
#         .master("local[4]") \
#         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#         .getOrCreate()
    
#     spark.sparkContext.setLogLevel("WARN")
    
#     try:
#         workloads = [1000, 5000, 10000]
#         worker_counts = [1, 2, 4]
        
#         output_dir = f"benchmark_results_{get_local_timestamp()}"
#         os.makedirs(output_dir, exist_ok=True)
        
#         results = []
#         latency_results = []
        
#         print("\n" + "=" * 60)
#         print("RUNNING BENCHMARKS")
#         print("=" * 60)
        
#         for workload in workloads:
#             print(f"\n\n========== WORKLOAD: {workload} RECORDS ==========")
            
#             records = read_sample_data(workload)
#             if not records:
#                 print(f"No data available for workload {workload}, skipping...")
#                 continue
            
#             total_valid = len([r for r in records if r.get('text')])
#             print(f"Valid records: {total_valid}")
            
#             if total_valid == 0:
#                 print("No valid records, skipping...")
#                 continue
            
#             # Sequential run
#             seq_time, seq_throughput = run_sequential(records, total_valid, spark)

#             results.append({
#                 'workload': workload,
#                 'mode': 'sequential',
#                 'workers': 1,
#                 'time_seconds': format_number(seq_time, 2),
#                 'throughput': format_number(seq_throughput, 2),
#                 'speedup': 1.0,
#                 'efficiency': 1.0,
#                 'records_processed': total_valid
#             })
            
#             # Parallel runs
#             for workers in worker_counts:
#                 if workers == 1:
#                     continue
#                 try:
#                     par_time, par_throughput = run_parallel(records, total_valid, workers, spark)
#                     speedup = seq_time / par_time if par_time > 0 else 0
#                     efficiency = speedup / workers if workers > 0 else 0
                    
#                     results.append({
#                         'workload': workload,
#                         'mode': 'parallel',
#                         'workers': workers,
#                         'time_seconds': format_number(par_time, 2),
#                         'throughput': format_number(par_throughput, 2),
#                         'speedup': format_number(speedup, 2),
#                         'efficiency': format_number(efficiency, 2),
#                         'records_processed': total_valid
#                     })
#                 except Exception as e:
#                     print(f"Error in parallel run with {workers} workers: {e}")
#                     continue
        
#         # Latency tests
#         if results:
#             test_records = read_sample_data(500)
#             if test_records:
#                 ingestion_rates = [10, 50, 100, 200]
#                 for rate in ingestion_rates:
#                     avg_lat, p95_lat, max_lat = measure_latency(test_records, rate, spark)
#                     latency_results.append({
#                         'ingestion_rate': rate,
#                         'avg_latency': format_number(avg_lat, 2),
#                         'p95_latency': format_number(p95_lat, 2),
#                         'max_latency': format_number(max_lat, 2)
#                     })
        
#         # Save results
#         if results:
#             results_df = pd.DataFrame(results)
#             latency_df = pd.DataFrame(latency_results) if latency_results else pd.DataFrame()
            
#             save_results_to_s3(results_df, 'benchmark_results.csv')
#             if not latency_df.empty:
#                 save_results_to_s3(latency_df, 'latency_results.csv')
            
#             generate_graphs(results_df, output_dir)
            
#             s3_client = boto3.client('s3')
#             for file in os.listdir(output_dir):
#                 if file.endswith('.png'):
#                     s3_client.upload_file(
#                         f'{output_dir}/{file}',
#                         BUCKET_NAME,
#                         f'benchmark-results/graphs/{file}'
#                     )
            
#             print("\n" + "=" * 60)
#             print("BENCHMARK SUMMARY")
#             print("=" * 60)
#             print("\n" + results_df.to_string(index=False))
            
#             if not latency_df.empty:
#                 print("\nLATENCY RESULTS:")
#                 print(latency_df.to_string(index=False))
            
#             with open(f'{output_dir}/summary.txt', 'w') as f:
#                 f.write("=" * 60 + "\n")
#                 f.write("BENCHMARK SUMMARY\n")
#                 f.write("=" * 60 + "\n\n")
#                 f.write(results_df.to_string(index=False))
#                 if not latency_df.empty:
#                     f.write("\n\nLATENCY RESULTS:\n")
#                     f.write(latency_df.to_string(index=False))
            
#             print(f"\nResults saved to: {output_dir}/")
#             print(f"Uploaded to s3://{BUCKET_NAME}/benchmark-results/")
#         else:
#             print("No benchmark results generated")
        
#     except Exception as e:
#         print(f"Error in benchmark: {e}")
#         traceback.print_exc()
#     finally:
#         spark.stop()
#         print("Benchmark finished")

# if __name__ == "__main__":
#     main()









#!/usr/bin/env python3
"""
Batch Layer - PySpark with MapReduce Pattern
"""
from pyspark.sql import SparkSession
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

MAX_RECORDS_TO_PROCESS = 5000
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

def reduce_counts(rdd):
    return rdd.reduceByKey(lambda a, b: a + b)

def read_data_from_s3():
    print("Reading data from S3...")
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    prefix = 'kinesis-data/'
    
    records = []
    file_count = 0
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=50)
        
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
                    if file_count >= 100:
                        print(f"Reached max files (100)")
                        return records
        
        print(f"Read {len(records)} records from {file_count} files")
        return records
        
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return []

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
