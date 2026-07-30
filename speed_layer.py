

# speed_layer.py - Batched file writing with timestamps

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from textblob import TextBlob
import time

spark = SparkSession.builder \
    .appName("SpeedLayer") \
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

json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("text", StringType(), True),
    StructField("langs", ArrayType(StringType()), True),
    StructField("did", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

input_path = "s3://s3-bucket-x23424567/kinesis-data/"
stream_df = spark.readStream \
    .schema(json_schema) \
    .json(input_path)

processed_stream = stream_df \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("keywords", keywords_udf(col("text"))) \
    .withColumn("topic", topic_udf(col("text"))) \
    .withColumn("entities", entities_udf(col("text"))) \
    .withWatermark("timestamp", "30 seconds")

window_duration = "30 seconds"
slide_duration = "5 seconds"

# Sentiment Results
sentiment_df = processed_stream \
    .groupBy(window(col("timestamp"), window_duration, slide_duration), col("sentiment")) \
    .agg(count("*").alias("count")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("sentiment").alias("metric_type"),
        col("sentiment").alias("metric_name"),
        col("count"),
        lit(None).cast("int").alias("rank")
    )

# Trending Keywords
trending_df = processed_stream \
    .select(
        window(col("timestamp"), window_duration, slide_duration),
        explode(col("keywords")).alias("keyword")
    ) \
    .groupBy("window", "keyword") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc()) \
    .limit(10) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("trending").alias("metric_type"),
        col("keyword").alias("metric_name"),
        col("count"),
        lit(None).cast("int").alias("rank")
    )

# Topic Results
topic_df = processed_stream \
    .groupBy(window(col("timestamp"), window_duration, slide_duration), col("topic")) \
    .agg(count("*").alias("count")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("topic").alias("metric_type"),
        col("topic").alias("metric_name"),
        col("count"),
        lit(None).cast("int").alias("rank")
    )

# NER Results
ner_df = processed_stream \
    .select(
        window(col("timestamp"), window_duration, slide_duration),
        explode(col("entities")).alias("entity")
    ) \
    .groupBy("window", "entity") \
    .agg(count("*").alias("count")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("ner").alias("metric_type"),
        col("entity").alias("metric_name"),
        col("count"),
        lit(None).cast("int").alias("rank")
    )

# Top 5 Keywords
top5_df = processed_stream \
    .select(
        window(col("timestamp"), window_duration, slide_duration),
        explode(col("keywords")).alias("keyword")
    ) \
    .groupBy("window", "keyword") \
    .agg(count("*").alias("count")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("keyword"),
        col("count"),
        row_number().over(Window.partitionBy("window.start", "window.end").orderBy(col("count").desc())).alias("rank")
    ) \
    .filter(col("rank") <= 5) \
    .select(
        col("window_start"),
        col("window_end"),
        lit("top5").alias("metric_type"),
        col("keyword").alias("metric_name"),
        col("count"),
        col("rank")
    )

all_speed_results = sentiment_df.unionAll(trending_df) \
    .unionAll(topic_df) \
    .unionAll(ner_df) \
    .unionAll(top5_df)

# Write with custom file naming using foreachBatch
# def write_batch(df, epoch_id):
#     if df.count() == 0:
#         return
    
#     from datetime import datetime
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     output_path = f"s3://s3-bucket-x23424567/speed_results/speed_results_{timestamp}.json"
    
#     df.coalesce(1).write \
#         .mode("append") \
#         .format("json") \
#         .option("path", output_path) \
#         .save()
    
#     print(f"Batch written to: {output_path}")

def write_batch(df, epoch_id):
    if df.count() == 0:
        return
    
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3://s3-bucket-x23424567/results/speed/speed_results_{timestamp}.json"
    
    df.coalesce(1).write \
        .mode("append") \
        .format("json") \
        .option("path", output_path) \
        .save()
    
    print(f"Batch written to: {output_path}")

# Write stream with foreachBatch
query = all_speed_results.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

print("Speed Layer started successfully")
print("Press Ctrl+C to stop")

query.awaitTermination()