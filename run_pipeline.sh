

# run_pipeline.sh
#!/bin/bash

echo "========================================="
echo "Complete Lambda Architecture Pipeline"
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
echo "Step 3: Waiting for data (60 seconds)..."
sleep 60

DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
echo "Found $DATA_COUNT files in kinesis-data/"


# Step 4: Start Batch Layer Scheduler
echo ""
echo "Step 4: Starting Batch Layer Scheduler..."

(
while true
do
    echo "Batch started: $(date)" >> batch_layer.log

    spark-submit \
        --master local[2] \
        --driver-memory 1g \
        batch_layer.py >> batch_layer.log 2>&1

    echo "Batch finished: $(date)" >> batch_layer.log

    sleep 60
done
) &

BATCH_PID=$!
echo "Batch Scheduler started with PID: $BATCH_PID"

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


# # Start Custom Auto-Scaler (NEW)
# echo -e "\n Starting Custom Auto-Scaler..."
# nohup python3 autoscaler.py > logs/autoscaler.log 2>&1 &
# echo " Auto-Scaler started (CPU threshold: 20%)"


# Step 7: Start Custom Auto-Scaler
echo ""
echo "Step 7: Starting Custom Auto-Scaler..."

nohup python3 -u autoscaler.py > logs/autoscaler.log 2>&1 &
AUTOSCALER_PID=$!

echo "Auto-Scaler started with PID: $AUTOSCALER_PID"
echo "Scaling logs: logs/autoscaler.log"


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
echo "========================================="
echo "Pipeline running! Press Ctrl+C to stop"
echo "========================================="

wait
