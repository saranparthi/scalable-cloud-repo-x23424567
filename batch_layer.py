
# #!/usr/bin/env python3
# """
# Batch Layer - Configurable batch size
# """
# import boto3
# import json
# import csv
# import io
# from datetime import datetime
# from textblob import TextBlob
# import sys
# import traceback
# import gc

# print("=" * 60)
# print("BATCH LAYER STARTING")
# print("=" * 60)

# # CONFIGURATION - Adjust these values
# MAX_FILES_TO_READ = 100    # Number of JSON files to read (each has ~10 records)
# MAX_RECORDS_TO_PROCESS = 500  # Maximum records to process

# def sentiment_analysis(text):
#     try:
#         if not text:
#             return "Neutral"
#         blob = TextBlob(str(text))
#         polarity = blob.sentiment.polarity
#         if polarity > 0.1:
#             return "Positive"
#         elif polarity < -0.1:
#             return "Negative"
#         else:
#             return "Neutral"
#     except:
#         return "Neutral"

# def extract_keywords(text):
#     if not text:
#         return []
#     words = str(text).lower().split()
#     stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'for', 
#                  'with', 'without', 'of', 'to', 'is', 'i', 'you', 'we', 'they', 
#                  'he', 'she', 'it', 'my', 'your', 'our', 'their'}
#     words = [w for w in words if w not in stopwords and len(w) > 3]
#     return words[:5]

# def read_data_from_s3():
#     """Read JSON data from S3 using boto3"""
#     print(f"Reading data from S3 (max {MAX_FILES_TO_READ} files)...")
#     s3_client = boto3.client('s3')
#     bucket = 's3-bucket-x23424567'
#     prefix = 'kinesis-data/'
    
#     records = []
#     file_count = 0
    
#     try:
#         # Use paginator to get more files
#         paginator = s3_client.get_paginator('list_objects_v2')
#         pages = paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=100)
        
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
#                                         # Stop if we reach max records
#                                         if len(records) >= MAX_RECORDS_TO_PROCESS:
#                                             print(f"Reached max records ({MAX_RECORDS_TO_PROCESS})")
#                                             return records
#                                 except:
#                                     pass
#                     except Exception as e:
#                         print(f"Error reading {key}: {e}")
#                         continue
                    
#                     # Stop if we reach max files
#                     if file_count >= MAX_FILES_TO_READ:
#                         print(f"Reached max files ({MAX_FILES_TO_READ})")
#                         return records
        
#         print(f"Read {len(records)} records from {file_count} files")
#         return records
        
#     except Exception as e:
#         print(f"Error reading from S3: {e}")
#         traceback.print_exc()
#         return []

# def save_to_s3(data, prefix, filename):
#     """Save data to S3 using boto3"""
#     s3_client = boto3.client('s3')
#     bucket = 's3-bucket-x23424567'
    
#     if not data:
#         print("No data to save")
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
#         traceback.print_exc()
#         return False

# def main():
#     try:
#         records = read_data_from_s3()
        
#         if not records:
#             print("No data available for batch processing")
#             return
        
#         print(f"Processing {len(records)} records...")
        
#         sentiment_counts = {}
#         keyword_counts = {}
        
#         for record in records:
#             text = record.get('text', '')
#             if not text:
#                 continue
            
#             sentiment = sentiment_analysis(text)
#             sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
            
#             keywords = extract_keywords(text)
#             for keyword in keywords:
#                 keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1
        
#         # Free memory
#         records = None
#         gc.collect()
        
#         total_count = sum(sentiment_counts.values())
        
#         if total_count == 0:
#             print("No valid records with text")
#             return
        
#         print(f"Processed {total_count} valid records")
        
#         csv_data = []
        
#         for sentiment, count in sentiment_counts.items():
#             percentage = round(count / total_count * 100, 2)
#             csv_data.append({
#                 'metric_type': 'sentiment',
#                 'metric_name': sentiment,
#                 'count': count,
#                 'percentage': percentage,
#                 'hour': ''
#             })
        
#         sorted_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        
#         for keyword, count in sorted_keywords:
#             csv_data.append({
#                 'metric_type': 'keywords',
#                 'metric_name': keyword,
#                 'count': count,
#                 'percentage': '',
#                 'hour': ''
#             })
        
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         filename = f'batch_results_{timestamp}/part-00000.csv'
#         success = save_to_s3(csv_data, 'results/batch', filename)
        
#         if success:
#             print("\n" + "=" * 60)
#             print("BATCH LAYER COMPLETED SUCCESSFULLY")
#             print("=" * 60)
#             print(f"Total records processed: {total_count}")
#             print("\nSentiment Distribution:")
#             for sentiment, count in sentiment_counts.items():
#                 percentage = round(count / total_count * 100, 2)
#                 print(f"  {sentiment}: {count} ({percentage}%)")
#             print("\nTop 10 Keywords:")
#             for i, (keyword, count) in enumerate(sorted_keywords[:10]):
#                 print(f"  {i+1}. {keyword}: {count}")
#             print("=" * 60)
#         else:
#             print("Failed to save results")
            
#     except Exception as e:
#         print(f"Error in batch processing: {e}")
#         traceback.print_exc()

# if __name__ == "__main__":
#     main()
#     print("Batch Layer finished")





#!/usr/bin/env python3
"""
Batch Layer - Configurable batch size with Topic Classification
"""
import boto3
import json
import csv
import io
from datetime import datetime
from textblob import TextBlob
import sys
import traceback
import gc

print("=" * 60)
print("BATCH LAYER STARTING")
print("=" * 60)

# CONFIGURATION - Adjust these values
MAX_FILES_TO_READ = 5000    # Number of JSON files to read
MAX_RECORDS_TO_PROCESS = 10000  # Maximum records to process

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

def topic_classification(text):
    """Classify text into topics"""
    topics = {
        'technology': ['tech', 'software', 'code', 'computer', 'ai', 'data', 'digital', 'app', 'web', 'algorithm', 'programming', 'developer'],
        'politics': ['government', 'election', 'policy', 'vote', 'political', 'president', 'minister', 'democracy', 'senate', 'congress'],
        'sports': ['game', 'team', 'score', 'win', 'sport', 'player', 'football', 'cricket', 'basketball', 'soccer', 'nfl', 'nba'],
        'business': ['business', 'company', 'market', 'profit', 'stock', 'finance', 'economy', 'investment', 'startup', 'ceo'],
        'entertainment': ['movie', 'music', 'film', 'celebrity', 'entertainment', 'show', 'hollywood', 'netflix', 'spotify', 'concert'],
        'health': ['health', 'doctor', 'hospital', 'medical', 'disease', 'covid', 'vaccine', 'wellness', 'fitness'],
        'education': ['school', 'college', 'university', 'student', 'teacher', 'education', 'learn', 'study', 'degree'],
        'travel': ['travel', 'trip', 'vacation', 'flight', 'hotel', 'tourist', 'beach', 'mountain', 'adventure']
    }
    text_lower = str(text).lower()
    for topic, keywords in topics.items():
        if any(kw in text_lower for kw in keywords):
            return topic
    return 'general'

def read_data_from_s3():
    """Read JSON data from S3 using boto3"""
    print(f"Reading data from S3 (max {MAX_FILES_TO_READ} files)...")
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    prefix = 'kinesis-data/'
    
    records = []
    file_count = 0
    
    try:
        # Use paginator to get more files
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
                                        # Stop if we reach max records
                                        if len(records) >= MAX_RECORDS_TO_PROCESS:
                                            print(f"Reached max records ({MAX_RECORDS_TO_PROCESS})")
                                            return records
                                except:
                                    pass
                    except Exception as e:
                        print(f"Error reading {key}: {e}")
                        continue
                    
                    # Stop if we reach max files
                    if file_count >= MAX_FILES_TO_READ:
                        print(f"Reached max files ({MAX_FILES_TO_READ})")
                        return records
        
        print(f"Read {len(records)} records from {file_count} files")
        return records
        
    except Exception as e:
        print(f"Error reading from S3: {e}")
        traceback.print_exc()
        return []

def save_to_s3(data, prefix, filename):
    """Save data to S3 using boto3"""
    s3_client = boto3.client('s3')
    bucket = 's3-bucket-x23424567'
    
    if not data:
        print("No data to save")
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
        traceback.print_exc()
        return False

def main():
    try:
        records = read_data_from_s3()
        
        if not records:
            print("No data available for batch processing")
            return
        
        print(f"Processing {len(records)} records...")
        
        sentiment_counts = {}
        keyword_counts = {}
        topic_counts = {}
        
        for record in records:
            text = record.get('text', '')
            if not text:
                continue
            
            # Sentiment analysis
            sentiment = sentiment_analysis(text)
            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
            
            # Keyword extraction
            keywords = extract_keywords(text)
            for keyword in keywords:
                keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1
            
            # Topic classification
            topic = topic_classification(text)
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
        
        # Free memory
        records = None
        gc.collect()
        
        total_count = sum(sentiment_counts.values())
        
        if total_count == 0:
            print("No valid records with text")
            return
        
        print(f"Processed {total_count} valid records")
        
        csv_data = []
        
        # Sentiment data
        for sentiment, count in sentiment_counts.items():
            percentage = round(count / total_count * 100, 2)
            csv_data.append({
                'metric_type': 'sentiment',
                'metric_name': sentiment,
                'count': count,
                'percentage': percentage,
                'hour': ''
            })
        
        # Keywords data
        sorted_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        for keyword, count in sorted_keywords:
            csv_data.append({
                'metric_type': 'keywords',
                'metric_name': keyword,
                'count': count,
                'percentage': '',
                'hour': ''
            })
        
        # Topic data
        sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
        for topic, count in sorted_topics:
            percentage = round(count / total_count * 100, 2)
            csv_data.append({
                'metric_type': 'topic',
                'metric_name': topic,
                'count': count,
                'percentage': percentage,
                'hour': ''
            })
        
        # Save to S3
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'batch_results_{timestamp}/part-00000.csv'
        success = save_to_s3(csv_data, 'results/batch', filename)
        
        if success:
            print("\n" + "=" * 60)
            print("BATCH LAYER COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print(f"Total records processed: {total_count}")
            
            print("\nSentiment Distribution:")
            for sentiment, count in sentiment_counts.items():
                percentage = round(count / total_count * 100, 2)
                print(f"  {sentiment}: {count} ({percentage}%)")
            
            print("\nTop 10 Keywords:")
            for i, (keyword, count) in enumerate(sorted_keywords[:10]):
                print(f"  {i+1}. {keyword}: {count}")
            
            print("\nTopic Distribution:")
            for topic, count in sorted_topics:
                percentage = round(count / total_count * 100, 2)
                print(f"  {topic}: {count} ({percentage}%)")
            print("=" * 60)
        else:
            print("Failed to save results")
            
    except Exception as e:
        print(f"Error in batch processing: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
    print("Batch Layer finished")