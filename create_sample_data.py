# create_sample_data.py
import boto3
import json
import csv
import io
from datetime import datetime, timedelta

s3_client = boto3.client('s3')
BUCKET_NAME = 's3-bucket-x23424567'

def create_speed_data():
    """Create sample speed data"""
    records = []
    
    for i in range(10):
        window_time = datetime.now() - timedelta(minutes=i*2)
        window_start = window_time.strftime('%Y-%m-%d %H:%M:%S')
        window_end = (window_time + timedelta(seconds=30)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Sentiment
        records.append({'window_start': window_start, 'window_end': window_end, 
                       'metric_type': 'sentiment', 'metric_name': 'Positive', 
                       'count': str(15 + i * 2), 'rank': ''})
        records.append({'window_start': window_start, 'window_end': window_end, 
                       'metric_type': 'sentiment', 'metric_name': 'Negative', 
                       'count': str(8 + i), 'rank': ''})
        records.append({'window_start': window_start, 'window_end': window_end, 
                       'metric_type': 'sentiment', 'metric_name': 'Neutral', 
                       'count': str(20 + i * 3), 'rank': ''})
        
        # Trending keywords
        keywords = ['ai', 'cloud', 'data', 'python', 'aws']
        for idx, keyword in enumerate(keywords):
            records.append({'window_start': window_start, 'window_end': window_end, 
                           'metric_type': 'trending', 'metric_name': keyword, 
                           'count': str(30 - idx * 3 + i), 'rank': ''})
        
        # Top 5
        for idx, keyword in enumerate(keywords):
            records.append({'window_start': window_start, 'window_end': window_end, 
                           'metric_type': 'top5', 'metric_name': keyword, 
                           'count': str(40 - idx * 3 + i), 'rank': str(idx + 1)})
        
        # Topics
        topics = ['technology', 'politics', 'sports', 'business', 'entertainment']
        for idx, topic in enumerate(topics):
            records.append({'window_start': window_start, 'window_end': window_end, 
                           'metric_type': 'topic', 'metric_name': topic, 
                           'count': str(10 + idx * 2 + i), 'rank': ''})
        
        # NER
        entities = ['PERSON', 'ORGANIZATION', 'LOCATION']
        for idx, entity in enumerate(entities):
            records.append({'window_start': window_start, 'window_end': window_end, 
                           'metric_type': 'ner', 'metric_name': entity, 
                           'count': str(5 + idx * 2 + i), 'rank': ''})
    
    # Write as JSON Lines
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    content = '\n'.join([json.dumps(r) for r in records])
    
    # Upload to both possible locations
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f'speed_results/speed_{timestamp}.json',
        Body=content.encode('utf-8')
    )
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f'results/speed/speed_{timestamp}.json',
        Body=content.encode('utf-8')
    )
    
    print(f"Created {len(records)} speed records")
    return records

def create_batch_data():
    """Create sample batch data"""
    records = []
    
    # Sentiment
    records.append({'metric_type': 'sentiment', 'metric_name': 'Positive', 
                   'count': '4500', 'percentage': '45.0', 'hour': ''})
    records.append({'metric_type': 'sentiment', 'metric_name': 'Negative', 
                   'count': '2500', 'percentage': '25.0', 'hour': ''})
    records.append({'metric_type': 'sentiment', 'metric_name': 'Neutral', 
                   'count': '3000', 'percentage': '30.0', 'hour': ''})
    
    # Keywords
    keywords = ['ai', 'cloud', 'data', 'python', 'aws', 'spark', 'kafka', 'docker']
    for idx, keyword in enumerate(keywords):
        records.append({'metric_type': 'keywords', 'metric_name': keyword, 
                       'count': str(500 - idx * 30), 'percentage': '', 'hour': ''})
    
    # Topics
    topics = [('technology', '3000', '30.0'), ('politics', '2000', '20.0'),
              ('sports', '1800', '18.0'), ('business', '1700', '17.0'),
              ('entertainment', '1500', '15.0')]
    for topic, count, percentage in topics:
        records.append({'metric_type': 'topic', 'metric_name': topic, 
                       'count': count, 'percentage': percentage, 'hour': ''})
    
    # Entities
    entities = [('PERSON', '1200'), ('ORGANIZATION', '800'), ('LOCATION', '600')]
    for entity, count in entities:
        records.append({'metric_type': 'entities', 'metric_name': entity, 
                       'count': count, 'percentage': '', 'hour': ''})
    
    # Write CSV
    output = io.StringIO()
    fieldnames = ['metric_type', 'metric_name', 'count', 'percentage', 'hour']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_content = output.getvalue()
    
    # Upload to both possible locations
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f'batch_results/batch_{timestamp}/part-00000.csv',
        Body=csv_content.encode('utf-8')
    )
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f'results/batch/batch_{timestamp}/part-00000.csv',
        Body=csv_content.encode('utf-8')
    )
    
    print(f"Created {len(records)} batch records")
    return records

if __name__ == '__main__':
    print("Creating sample data...")
    create_speed_data()
    create_batch_data()
    print("\nData created! Now run MSCK REPAIR TABLE in Athena.")