
#!/usr/bin/env python3
"""
Performance Benchmark for Lambda Architecture
Measures throughput, latency, and speedup under different loads
"""
from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import builtins
import re
from pyspark.sql.types import *
from textblob import TextBlob
import time
import boto3
import json
import io
import csv
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from datetime import datetime
import pytz
import os
import traceback

print("=" * 60)
print("PERFORMANCE BENCHMARK STARTING")
print("=" * 60)

LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')
BUCKET_NAME = 's3-bucket-x23424567'

def get_local_timestamp():
    return datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M%S")

def get_local_datetime():
    return datetime.now(LOCAL_TIMEZONE)

# ============ MAPPER FUNCTIONS (Same as batch_layer.py) ============

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

    stopwords = {
        'the','a','an','and','or','but','in','on','at','for',
        'with','without','of','to','is','i','you','we','they',
        'he','she','it','my','your','our','their','from','this',
        'that','these','those','then','than','so','too','very',
        'just','like','get','got','can','will','would','could',
        'should','may','might','must','shall','has','have','had'
    }

    words = re.findall(r"\b[a-zA-Z]{4,}\b", str(text).lower())

    return [(word, 1) for word in words if word not in stopwords]
    
    

def map_topic(text):
    topics = {
        'technology': ['tech', 'software', 'code', 'computer', 'ai', 'data', 'digital'],
        'politics': ['government', 'election', 'policy', 'vote', 'political', 'president'],
        'sports': ['game', 'team', 'score', 'win', 'sport', 'player', 'football'],
        'business': ['business', 'company', 'market', 'profit', 'stock', 'finance'],
        'entertainment': ['movie', 'music', 'film', 'celebrity', 'entertainment', 'show']
    }
    text_lower = str(text).lower()
    for topic, keywords in topics.items():
        for kw in keywords:
            if kw in text_lower:
                return (topic, 1)
    return ("general", 1)

def reduce_counts(rdd):
    return rdd.reduceByKey(lambda a, b: a + b)

def read_sample_data(records_count):
    """Read sample data from S3"""
    print(f"Reading {records_count} records from S3...")
    s3_client = boto3.client('s3')
    bucket = BUCKET_NAME
    prefix = 'kinesis-data/'
    
    records = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=50)
        
        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    try:
                        file_response = s3_client.get_object(Bucket=bucket, Key=key)
                        content = file_response['Body'].read().decode('utf-8')
                        for line in content.strip().split('\n'):
                            if line.strip():
                                try:
                                    record = json.loads(line)
                                    if record.get('text'):
                                        records.append(record)
                                        if len(records) >= records_count:
                                            print(f"Collected {len(records)} records")
                                            return records
                                except:
                                    pass
                    except:
                        continue
        print(f"Collected {len(records)} records")
        return records
        
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return []

def run_sequential(records, total_valid):
    """Run sequential processing (1 core)"""
    print(f"\n--- SEQUENTIAL MODE (1 core) ---")
    start_time = time.time()
    
    # Create RDD
    rdd = spark.sparkContext.parallelize(records)
    text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
    
    # MAPREDUCE operations
    sentiment_counts = reduce_counts(text_rdd.flatMap(lambda text: [map_sentiment(text)])).collect()
    keyword_counts = reduce_counts(text_rdd.flatMap(lambda text: map_keywords(text))).sortBy(lambda x: x[1], ascending=False).take(20)
    topic_counts = reduce_counts(text_rdd.map(lambda text: map_topic(text))).collect()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Sequential processing completed in {total_time:.2f} seconds")
    print(f"Processed {total_valid} records")
    print(f"Throughput: {total_valid/total_time:.2f} rec/sec")
    
    return total_time, total_valid/total_time

def run_parallel(records, total_valid, workers):
    
    sc = spark.sparkContext
    
    rdd = sc.parallelize(records, workers)
    
    text_rdd = rdd.map(lambda x: x.get("text", "")) \
                  .filter(lambda x: x and len(x) > 0)
    
    sentiment_counts = reduce_counts(
        text_rdd.flatMap(lambda text: [map_sentiment(text)])
    ).collect()
    
    keyword_counts = reduce_counts(
        text_rdd.flatMap(map_keywords)
    ).sortBy(lambda x: x[1], ascending=False).take(20)
    
    topic_counts = reduce_counts(
        text_rdd.map(map_topic)
    ).collect()

def measure_latency(records, ingestion_rate):
    """Measure latency at different ingestion rates"""
    print(f"\n--- LATENCY TEST (Ingestion Rate: {ingestion_rate} rec/sec) ---")
    
    # Simulate ingestion rate by delaying processing
    delay = 1.0 / ingestion_rate if ingestion_rate > 0 else 0
    
    latencies = []
    start_time = time.time()
    
    rdd = spark.sparkContext.parallelize(records[:500])  # Use 500 records for latency test
    text_rdd = rdd.map(lambda x: x.get('text', '')).filter(lambda x: x and len(x) > 0)
    
    for text in text_rdd.collect():
        record_start = time.time()
        
        # Process single record
        sentiment = map_sentiment(text)
        keywords = map_keywords(text)
        
        record_end = time.time()
        latencies.append((record_end - record_start) * 1000)  # Convert to ms
        
        if delay > 0:
            time.sleep(delay)
    
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
    max_latency = max(latencies) if latencies else 0
    
    print(f"  Avg Latency: {avg_latency:.2f} ms")
    print(f"  P95 Latency: {p95_latency:.2f} ms")
    print(f"  Max Latency: {max_latency:.2f} ms")
    print(f"  Records processed: {len(latencies)}")
    
    return avg_latency, p95_latency, max_latency

def generate_graphs(results_df, output_dir):
    """Generate all benchmark graphs"""
    print("\n--- Generating Graphs ---")
    
    # Graph 1: Speedup vs Worker Count
    plt.figure(figsize=(10, 6))
    for workload in results_df['workload'].unique():
        subset = results_df[results_df['workload'] == workload]
        plt.plot(subset['workers'], subset['speedup'], marker='o', label=f"{workload:,} records")
    plt.xlabel('Number of Workers (Cores)')
    plt.ylabel('Speedup')
    plt.title('Speedup vs Worker Count')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{output_dir}/speedup_vs_workers.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: speedup_vs_workers.png")
    
    # Graph 2: Processing Time vs Records
    plt.figure(figsize=(10, 6))
    seq_data = results_df[results_df['mode'] == 'sequential']
    par_data = results_df[results_df['mode'] == 'parallel']
    
    plt.plot(seq_data['workload'], seq_data['time_seconds'], 'bo-', label='Sequential')
    for workers in par_data['workers'].unique():
        subset = par_data[par_data['workers'] == workers]
        plt.plot(subset['workload'], subset['time_seconds'], 'o-', label=f'Parallel ({workers} cores)')
    
    plt.xlabel('Records Count')
    plt.ylabel('Processing Time (seconds)')
    plt.title('Processing Time vs Records Count')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{output_dir}/processing_time_vs_records.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: processing_time_vs_records.png")
    
    # Graph 3: Throughput Comparison
    plt.figure(figsize=(10, 6))
    seq_throughput = results_df[results_df['mode'] == 'sequential']
    par_throughput = results_df[results_df['mode'] == 'parallel']
    
    plt.plot(seq_throughput['workload'], seq_throughput['throughput'], 'bo-', label='Sequential')
    for workers in par_throughput['workers'].unique():
        subset = par_throughput[par_throughput['workers'] == workers]
        plt.plot(subset['workload'], subset['throughput'], 'o-', label=f'Parallel ({workers} cores)')
    
    plt.xlabel('Records Count')
    plt.ylabel('Throughput (records/sec)')
    plt.title('Throughput Comparison')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{output_dir}/throughput_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: throughput_comparison.png")
    
    # Graph 4: Latency vs Ingestion Rate
    if 'ingestion_rate' in results_df.columns and 'avg_latency' in results_df.columns:
        plt.figure(figsize=(10, 6))
        plt.plot(results_df['ingestion_rate'], results_df['avg_latency'], 'ro-', label='Avg Latency')
        plt.plot(results_df['ingestion_rate'], results_df['p95_latency'], 'bo-', label='P95 Latency')
        plt.plot(results_df['ingestion_rate'], results_df['max_latency'], 'go-', label='Max Latency')
        
        plt.xlabel('Ingestion Rate (records/sec)')
        plt.ylabel('Latency (ms)')
        plt.title('Latency vs Ingestion Rate')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'{output_dir}/latency_vs_ingestion.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("  Saved: latency_vs_ingestion.png")

def save_results_to_s3(data, filename):
    """Save results to S3"""
    s3_client = boto3.client('s3')
    bucket = BUCKET_NAME
    prefix = 'benchmark-results/'
    
    try:
        output = io.StringIO()
        if isinstance(data, pd.DataFrame):
            data.to_csv(output, index=False)
        else:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        s3_client.put_object(
            Bucket=bucket,
            Key=f'{prefix}{filename}',
            Body=output.getvalue().encode('utf-8')
        )
        print(f"Saved to s3://{bucket}/{prefix}{filename}")
        return True
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return False



def py_round(value, decimals=2):
    return builtins.round(float(value), decimals)



def main():
    global spark
    
    spark = SparkSession.builder \
        .appName("Benchmark") \
        .master("local[4]") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Test configurations
        workloads = [1000, 5000, 10000]
        worker_counts = [1, 2, 4]
        ingestion_rates = [10, 50, 100, 200]
        
        # Create output directory
        output_dir = f"benchmark_results_{get_local_timestamp()}"
        os.makedirs(output_dir, exist_ok=True)
        
        results = []
        latency_results = []
        
        print("\n" + "=" * 60)
        print("RUNNING BENCHMARKS")
        print("=" * 60)
        
        for workload in workloads:
            print(f"\n\n========== WORKLOAD: {workload} RECORDS ==========")
            
            # Read data
            records = read_sample_data(workload)
            if not records:
                print(f"No data available for workload {workload}, skipping...")
                continue
            
            total_valid = len([r for r in records if r.get('text')])
            print(f"Valid records: {total_valid}")
            
            # Sequential run (1 core)
            seq_time, seq_throughput = run_sequential(records, total_valid)
            results.append({
                'workload': workload,
                'mode': 'sequential',
                'workers': 1,
                # 'time_seconds': round(seq_time, 2),
                # 'throughput': round(seq_throughput, 2),
                'time_seconds': py_round(seq_time),
                'throughput': py_round(seq_throughput),
                'speedup': 1.0,
                'efficiency': 1.0,
                'records_processed': total_valid
            })
            
            # Parallel runs (multiple cores)
            for workers in worker_counts:
                if workers == 1:
                    continue
                par_time, par_throughput = run_parallel(records, total_valid, workers)
                speedup = seq_time / par_time if par_time > 0 else 0
                efficiency = speedup / workers if workers > 0 else 0
                
                results.append({
                    'workload': workload,
                    'mode': 'parallel',
                    'workers': workers,
                    # 'time_seconds': round(par_time, 2),
                    # 'throughput': round(par_throughput, 2),
                    # 'speedup': round(speedup, 2),
                    # 'efficiency': round(efficiency, 2),
                    
                    'time_seconds': py_round(par_time),
                    'throughput': py_round(par_throughput),
                    'speedup': py_round(speedup),
                    'efficiency': py_round(efficiency),
                    'records_processed': total_valid
                })
        
        # Latency tests (first workload only for consistency)
        if results:
            test_records = read_sample_data(500)
            for rate in ingestion_rates:
                avg_lat, p95_lat, max_lat = measure_latency(test_records, rate)
                latency_results.append({
                    'ingestion_rate': rate,
                    # 'avg_latency': round(avg_lat, 2),
                    # 'p95_latency': round(p95_lat, 2),
                    # 'max_latency': round(max_lat, 2)
                    'avg_latency': py_round(avg_lat),
                    'p95_latency': py_round(p95_lat),
                    'max_latency': py_round(max_lat)
                })
        
        # Convert to DataFrame
        results_df = pd.DataFrame(results)
        latency_df = pd.DataFrame(latency_results)
        
        # Merge for combined results
        if not results_df.empty and not latency_df.empty:
            # Add latency data to results for graphing
            results_df['ingestion_rate'] = None
            results_df['avg_latency'] = None
            results_df['p95_latency'] = None
            results_df['max_latency'] = None
        
        # Save results
        save_results_to_s3(results_df, 'benchmark_results.csv')
        if not latency_df.empty:
            save_results_to_s3(latency_df, 'latency_results.csv')
        
        # Generate graphs
        if not results_df.empty:
            generate_graphs(results_df, output_dir)
            
            # Also save graphs to S3
            for file in os.listdir(output_dir):
                if file.endswith('.png'):
                    s3_client = boto3.client('s3')
                    s3_client.upload_file(
                        f'{output_dir}/{file}',
                        BUCKET_NAME,
                        f'benchmark-results/graphs/{file}'
                    )
        
        # Print summary
        print("\n" + "=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)
        print("\n" + results_df.to_string(index=False) if not results_df.empty else "No results")
        
        if not latency_df.empty:
            print("\nLATENCY RESULTS:")
            print(latency_df.to_string(index=False))
        
        # Save summary to file
        with open(f'{output_dir}/summary.txt', 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("BENCHMARK SUMMARY\n")
            f.write("=" * 60 + "\n\n")
            f.write(results_df.to_string(index=False))
            if not latency_df.empty:
                f.write("\n\nLATENCY RESULTS:\n")
                f.write(latency_df.to_string(index=False))
        
        print(f"\nResults saved to: {output_dir}/")
        print(f"Uploaded to s3://{BUCKET_NAME}/benchmark-results/")
        
    except Exception as e:
        print(f"Error in benchmark: {e}")
        traceback.print_exc()
    finally:
        spark.stop()
        print("Benchmark finished")

if __name__ == "__main__":
    main()
