Project Overview
This project implements a Lambda Architecture for real-time social media analytics using Bluesky social media data. The system processes streaming data through both speed and batch layers to provide real-time insights and historical accuracy.

Data Source
Bluesky Social Media Platform

Live streaming of public posts via WebSocket connection

Real-time textual data for analysis

Architecture Flow
1. Data Ingestion
Python producer connects to Bluesky WebSocket

Receives live posts in real-time

Sends data to AWS Kinesis stream

Kinesis delivers data to S3 as JSON files

2. Speed Layer (Real-Time Processing)
Runs continuously using PySpark Streaming

Reads new JSON files from S3 every 10 seconds

Processes data in 30-second sliding windows with 5-second slides

Performs real-time sentiment analysis and trending keyword extraction

Saves results to S3 as JSON files

3. Batch Layer (Historical Processing)
Runs every 60 seconds using PySpark MapReduce

Processes accumulated historical data from S3

Performs comprehensive analysis including sentiment, keywords, topics, and NER

Saves results to S3 as CSV files

4. Serving Layer
AWS Athena queries speed_results and batch_results tables

Flask dashboard fetches data via REST APIs

Dashboard displays real-time metrics and historical trends

AWS Services Used
EC2 (Cloud9) for code execution

Kinesis for data ingestion

S3 for data storage

Athena for querying

Execution Instructions

Start the Pipeline

Run the main pipeline script:
./run_pipeline.sh

This starts:

Kinesis stream setup

Data producer (Bluesky ingestion)

Batch layer processing

Speed layer (streaming)

Dashboard server

Run Benchmark Independently
For performance testing without affecting real-time pipeline:


./run_benchmark.sh


System Components
Producer
Connects to Bluesky WebSocket

Sends data to Kinesis

Writes JSON files to S3

Speed Layer
PySpark streaming application

30-second windows, 5-second slides

Sentiment analysis and trending keywords (top 5)

Batch Layer
PySpark MapReduce application

Runs every 60 seconds

Sentiment analysis, keywords, topics, NER

Dashboard
Flask web application

Real-time charts and KPIs

Benchmark results visualization