# batch_layer.py - Saves to /results folder

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
from datetime import datetime

spark = SparkSession.builder \
    .appName("BatchLayer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

def sentiment_analysis(text):
    try:
        if not text:
            return "Neutral"
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity
        if polarity > 0:
            return "Positive"
        elif polarity < 0:
            return "Negative"
        else:
            return "Neutral"
    except:
        return "Neutral"

def extract_keywords(text, top_n=5):
    if not text:
        return []
    words = str(text).lower().split()
    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'for', 'with', 'without', 'of', 'to', 'is'}
    words = [w for w in words if w not in stopwords and len(w) > 3]
    return words[:top_n]

def topic_classification(text):
    topics = {
        'technology': ['tech', 'software', 'code', 'computer', 'ai', 'data', 'digital', 'app', 'web', 'algorithm'],
        'politics': ['government', 'election', 'policy', 'vote', 'political', 'president', 'minister', 'democracy'],
        'sports': ['game', 'team', 'score', 'win', 'sport', 'player', 'football', 'cricket', 'basketball'],
        'business': ['business', 'company', 'market', 'profit', 'stock', 'finance', 'economy', 'investment'],
        'entertainment': ['movie', 'music', 'film', 'celebrity', 'entertainment', 'show', 'hollywood']
    }
    text_lower = str(text).lower()
    for topic, keywords in topics.items():
        if any(kw in text_lower for kw in keywords):
            return topic
    return 'general'

def extract_entities(text):
    entities = []
    text_lower = str(text).lower()
    if 'president' in text_lower or 'prime minister' in text_lower or 'mr.' in text_lower or 'mrs.' in text_lower:
        entities.append('PERSON')
    if 'company' in text_lower or 'corp' in text_lower or 'inc' in text_lower or 'llc' in text_lower:
        entities.append('ORGANIZATION')
    if 'city' in text_lower or 'state' in text_lower or 'country' in text_lower or 'nation' in text_lower:
        entities.append('LOCATION')
    if 'university' in text_lower or 'college' in text_lower or 'school' in text_lower:
        entities.append('ORGANIZATION')
    return entities

sentiment_udf = udf(sentiment_analysis, StringType())
keywords_udf = udf(extract_keywords, ArrayType(StringType()))
topic_udf = udf(topic_classification, StringType())
entities_udf = udf(extract_entities, ArrayType(StringType()))

def process_batch_layer():
    print("Starting Batch Layer Processing")
    
    data_path = "s3://s3-bucket-x23424567/kinesis-data/"
    
    try:
        df = spark.read.json(data_path)
        print("Read data from S3: " + str(df.count()) + " records")
    except Exception as e:
        print("Error reading from S3: " + str(e))
        return None
    
    if df.count() == 0:
        print("No data available for batch processing")
        return None
    
    processed_df = df \
        .withColumn("sentiment", sentiment_udf(col("text"))) \
        .withColumn("keywords", keywords_udf(col("text"))) \
        .withColumn("timestamp", current_timestamp()) \
        .withColumn("topic", topic_udf(col("text"))) \
        .withColumn("entities", entities_udf(col("text")))
    
    total_count = df.count()
    
    sentiment_df = processed_df \
        .groupBy("sentiment") \
        .agg(count("*").alias("count")) \
        .withColumn("percentage", round(col("count") / total_count * 100, 2)) \
        .select(
            lit("sentiment").alias("metric_type"),
            col("sentiment").alias("metric_name"),
            col("count"),
            col("percentage"),
            lit(None).cast("int").alias("hour")
        )
    
    keywords_df = processed_df \
        .select(explode(col("keywords")).alias("keyword")) \
        .groupBy("keyword") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(20) \
        .select(
            lit("keywords").alias("metric_type"),
            col("keyword").alias("metric_name"),
            col("count"),
            lit(None).cast("double").alias("percentage"),
            lit(None).cast("int").alias("hour")
        )
    
    topic_df = processed_df \
        .groupBy("topic") \
        .agg(count("*").alias("count")) \
        .withColumn("percentage", round(col("count") / total_count * 100, 2)) \
        .select(
            lit("topic").alias("metric_type"),
            col("topic").alias("metric_name"),
            col("count"),
            col("percentage"),
            lit(None).cast("int").alias("hour")
        )
    
    entities_df = processed_df \
        .select(explode(col("entities")).alias("entity")) \
        .groupBy("entity") \
        .agg(count("*").alias("count")) \
        .select(
            lit("entities").alias("metric_type"),
            col("entity").alias("metric_name"),
            col("count"),
            lit(None).cast("double").alias("percentage"),
            lit(None).cast("int").alias("hour")
        )
    
    trends_df = processed_df \
        .withColumn("hour", hour(col("timestamp"))) \
        .select(col("hour"), explode(col("keywords")).alias("keyword")) \
        .groupBy("hour", "keyword") \
        .agg(count("*").alias("count")) \
        .orderBy("hour", col("count").desc()) \
        .select(
            lit("trends").alias("metric_type"),
            col("keyword").alias("metric_name"),
            col("count"),
            lit(None).cast("double").alias("percentage"),
            col("hour")
        )
    
    all_batch_results = sentiment_df.unionAll(keywords_df) \
        .unionAll(topic_df) \
        .unionAll(entities_df) \
        .unionAll(trends_df)
    
    # Write to /results folder with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3://s3-bucket-x23424567/results/batch/batch_results_{timestamp}"
    
    all_batch_results.coalesce(1).write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .save(output_path)
    
    print("Batch processing completed")
    print("Results saved to: " + output_path)
    print("Total records processed: " + str(total_count))
    
    print("Sentiment Distribution:")
    sentiment_df.show(truncate=False)
    
    print("Top 10 Keywords:")
    keywords_df.limit(10).show(truncate=False)
    
    print("Topic Distribution:")
    topic_df.show(truncate=False)
    
    return all_batch_results

if __name__ == "__main__":
    results = process_batch_layer()