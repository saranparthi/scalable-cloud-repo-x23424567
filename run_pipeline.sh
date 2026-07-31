

# #run_pipeline.sh
# #!/bin/bash

# echo "========================================="
# echo "Complete Lambda Architecture Pipeline"
# echo "========================================="

# cd ~/environment/scalable

# # Kill old processes
# echo ""
# echo "Cleaning up old processes..."
# pkill -f "kinesis_producer" 2>/dev/null
# pkill -f "spark" 2>/dev/null
# pkill -f "java" 2>/dev/null
# pkill -f "dashboard.py" 2>/dev/null
# sleep 3

# sudo rm -rf /tmp/spark-* 2>/dev/null

# # Step 1: Setup Kinesis
# echo ""
# echo "Step 1: Setting up Kinesis stream..."
# python3 kinesis_setup.py 2>/dev/null || echo "Kinesis setup already complete"

# # Step 2: Start Producer
# echo ""
# echo "Step 2: Starting S3 Producer..."
# nohup python3 kinesis_producer.py > producer_s3.log 2>&1 &
# PRODUCER_PID=$!
# echo "Producer started with PID: $PRODUCER_PID"

# # Step 3: Wait for data
# echo ""
# echo "Step 3: Waiting for data (30 seconds)..."
# sleep 30

# DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
# echo "Found $DATA_COUNT files in kinesis-data/"

# # Step 4: Run Batch Layer (Python - No Spark)
# echo ""
# echo "Step 4: Running Batch Layer..."
# echo "Batch started: $(date)" >> batch_layer.log
# python3 batch_layer.py 2>&1 | tee -a batch_layer.log
# echo "Batch finished: $(date)" >> batch_layer.log

# echo ""
# echo "Batch results:"
# aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive

# # Step 5: Start Speed Layer (PySpark with minimal memory)
# echo ""
# echo "Step 5: Starting Speed Layer..."
# nohup spark-submit \
#     --master local[1] \
#     --driver-memory 512m \
#     --executor-memory 512m \
#     --conf spark.ui.enabled=false \
#     --conf spark.sql.shuffle.partitions=1 \
#     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#     speed_layer.py > speed.log 2>&1 &
# SPEED_PID=$!
# echo "Speed Layer started with PID: $SPEED_PID"

# # Step 6: Start Dashboard
# echo ""
# echo "Step 6: Starting Dashboard..."
# nohup python3 dashboard.py > dashboard.log 2>&1 &
# DASHBOARD_PID=$!
# echo "Dashboard started with PID: $DASHBOARD_PID"
# echo "Dashboard: http://localhost:5000"

# echo ""
# echo "========================================="
# echo "Pipeline Status"
# echo "========================================="
# echo ""
# echo "Processes:"
# echo "  Producer PID: $PRODUCER_PID"
# echo "  Speed Layer PID: $SPEED_PID"
# echo "  Dashboard PID: $DASHBOARD_PID"
# echo ""
# echo "Data in S3:"
# echo "  kinesis-data: $(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l) files"
# echo "  results/speed: $(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l) files"
# echo "  results/batch: $(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l) files"
# echo ""
# echo "Logs:"
# echo "  Producer: tail -f producer_s3.log"
# echo "  Batch: tail -f batch_layer.log"
# echo "  Speed: tail -f speed.log"
# echo "  Dashboard: tail -f dashboard.log"
# echo ""
# echo "========================================="
# echo "Pipeline running! Press Ctrl+C to stop"
# echo "========================================="

# wait







# #!/bin/bash

# echo "========================================="
# echo "Complete Lambda Architecture Pipeline"
# echo "========================================="

# cd ~/environment/scalable

# # Kill old processes
# echo ""
# echo "Cleaning up old processes..."
# pkill -f "kinesis_producer" 2>/dev/null
# pkill -f "spark" 2>/dev/null
# pkill -f "java" 2>/dev/null
# pkill -f "dashboard.py" 2>/dev/null
# sleep 3

# sudo rm -rf /tmp/spark-* 2>/dev/null

# # Step 1: Setup Kinesis
# echo ""
# echo "Step 1: Setting up Kinesis stream..."
# python3 kinesis_setup.py 2>/dev/null || echo "Kinesis setup already complete"

# # Step 2: Start Producer
# echo ""
# echo "Step 2: Starting S3 Producer..."
# nohup python3 kinesis_producer.py > producer_s3.log 2>&1 &
# PRODUCER_PID=$!
# echo "Producer started with PID: $PRODUCER_PID"

# # Step 3: Wait for data
# echo ""
# echo "Step 3: Waiting for data (60 seconds)..."
# sleep 60

# DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
# echo "Found $DATA_COUNT files in kinesis-data/"

# # Step 4: Run Batch Layer
# echo ""
# echo "Step 4: Running Batch Layer..."
# echo "Batch started: $(date)" >> batch_layer.log
# python3 batch_layer.py 2>&1 | tee -a batch_layer.log
# echo "Batch finished: $(date)" >> batch_layer.log

# echo ""
# echo "Batch results:"
# aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive

# # Step 5: Run Performance Benchmark
# echo ""
# echo "Step 5: Running Performance Benchmark..."
# echo "Benchmark started: $(date)" >> benchmark.log
# spark-submit \
#     # --master local[*] \
#     --master local[4] \
#     --driver-memory 2g \
#     benchmark.py 2>&1 | tee -a benchmark.log
# echo "Benchmark finished: $(date)" >> benchmark.log

# echo ""
# echo "Benchmark results saved to S3"
# echo "Check s3://s3-bucket-x23424567/benchmark-results/"

# # Step 6: Start Speed Layer
# echo ""
# echo "Step 6: Starting Speed Layer..."
# nohup spark-submit \
#     --master local[1] \
#     --driver-memory 512m \
#     --executor-memory 512m \
#     --conf spark.ui.enabled=false \
#     --conf spark.sql.shuffle.partitions=1 \
#     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#     speed_layer.py > speed.log 2>&1 &
# SPEED_PID=$!
# echo "Speed Layer started with PID: $SPEED_PID"

# # Step 7: Start Dashboard
# echo ""
# echo "Step 7: Starting Dashboard..."
# nohup python3 dashboard.py > dashboard.log 2>&1 &
# DASHBOARD_PID=$!
# echo "Dashboard started with PID: $DASHBOARD_PID"
# echo "Dashboard: http://localhost:5000"

# echo ""
# echo "========================================="
# echo "Pipeline Status"
# echo "========================================="
# echo ""
# echo "Processes:"
# echo "  Producer PID: $PRODUCER_PID"
# echo "  Speed Layer PID: $SPEED_PID"
# echo "  Dashboard PID: $DASHBOARD_PID"
# echo ""
# echo "Data in S3:"
# echo "  kinesis-data: $(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l) files"
# echo "  results/speed: $(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l) files"
# echo "  results/batch: $(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l) files"
# echo "  benchmark-results: $(aws s3 ls s3://s3-bucket-x23424567/benchmark-results/ --recursive 2>/dev/null | wc -l) files"
# echo ""
# echo "Logs:"
# echo "  Producer: tail -f producer_s3.log"
# echo "  Batch: tail -f batch_layer.log"
# echo "  Benchmark: tail -f benchmark.log"
# echo "  Speed: tail -f speed.log"
# echo "  Dashboard: tail -f dashboard.log"
# echo ""
# echo "Benchmark graphs: s3://s3-bucket-x23424567/benchmark-results/graphs/"
# echo ""
# echo "========================================="
# echo "Pipeline running! Press Ctrl+C to stop"
# echo "========================================="

# wait
















#!/bin/bash

echo "========================================="
echo "Lambda Architecture Pipeline (Real-time)"
echo "========================================="

cd ~/environment/scalable

# Kill old processes
echo ""
echo "Cleaning up old processes..."
pkill -f "kinesis_producer" 2>/dev/null
pkill -f "spark" 2>/dev/null
pkill -f "java" 2>/dev/null
pkill -f "dashboard.py" 2>/dev/null
sleep 3

sudo rm -rf /tmp/spark-* 2>/dev/null

# Step 1: Setup Kinesis
echo ""
echo "Step 1: Setting up Kinesis stream..."
python3 kinesis_setup.py 2>/dev/null || echo "Kinesis setup already complete"

# Step 2: Start Producer
echo ""
echo "Step 2: Starting S3 Producer..."
nohup python3 kinesis_producer.py > producer_s3.log 2>&1 &
PRODUCER_PID=$!
echo "Producer started with PID: $PRODUCER_PID"

# Step 3: Wait for data
echo ""
echo "Step 3: Waiting for data (30 seconds)..."
sleep 30

DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
echo "Found $DATA_COUNT files in kinesis-data/"

# Step 4: Run Batch Layer
echo ""
echo "Step 4: Running Batch Layer..."
echo "Batch started: $(date)" >> batch_layer.log
spark-submit \
    --master local[2] \
    --driver-memory 1g \
    batch_layer.py 2>&1 | tee -a batch_layer.log
echo "Batch finished: $(date)" >> batch_layer.log

echo ""
echo "Batch results:"
aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive

# Step 5: Start Speed Layer
echo ""
echo "Step 5: Starting Speed Layer..."
nohup spark-submit \
    --master local[1] \
    --driver-memory 512m \
    --executor-memory 512m \
    --conf spark.ui.enabled=false \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    speed_layer.py > speed.log 2>&1 &
SPEED_PID=$!
echo "Speed Layer started with PID: $SPEED_PID"

# Step 6: Start Dashboard
echo ""
echo "Step 6: Starting Dashboard..."
nohup python3 dashboard.py > dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "Dashboard started with PID: $DASHBOARD_PID"
echo "Dashboard: http://localhost:5000"

echo ""
echo "========================================="
echo "Pipeline Status"
echo "========================================="
echo ""
echo "Processes:"
echo "  Producer PID: $PRODUCER_PID"
echo "  Speed Layer PID: $SPEED_PID"
echo "  Dashboard PID: $DASHBOARD_PID"
echo ""
echo "Data in S3:"
echo "  kinesis-data: $(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l) files"
echo "  results/speed: $(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l) files"
echo "  results/batch: $(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l) files"
echo ""
echo "Logs:"
echo "  Producer: tail -f producer_s3.log"
echo "  Batch: tail -f batch_layer.log"
echo "  Speed: tail -f speed.log"
echo "  Dashboard: tail -f dashboard.log"
echo ""
echo "To run benchmark: ./run_benchmark.sh"
echo "========================================="
echo "Pipeline running! Press Ctrl+C to stop"
echo "========================================="

wait
