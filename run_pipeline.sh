


# #!/bin/bash

# echo "========================================="
# echo "Complete Lambda Architecture Pipeline"
# echo "========================================="

# cd ~/environment/scalable

# # Kill old processes
# echo ""
# echo "Cleaning up old processes..."
# pkill -f "kinesis_producer" 2>/dev/null
# pkill -f "spark-submit" 2>/dev/null
# pkill -f "speed_layer" 2>/dev/null
# pkill -f "dashboard.py" 2>/dev/null
# sleep 3



# # Step 2: Start Producer
# echo ""
# echo "Step 2: Starting S3 Producer..."
# nohup python3 kinesis_producer.py > producer_s3.log 2>&1 &
# PRODUCER_PID=$!
# echo "Producer started with PID: $PRODUCER_PID"
# echo "Logs: tail -f producer_s3.log"

# # Step 3: Wait for data
# echo ""
# echo "Step 3: Waiting for data to accumulate (30 seconds)..."
# sleep 30

# DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
# echo "Found $DATA_COUNT files in kinesis-data/"

# # Step 4: Run Batch Layer with logging
# echo ""
# echo "Step 4: Running Batch Layer..."
# echo "Batch Layer started at: $(date)" >> batch_layer.log
# python3 batch_layer.py 2>&1 | tee -a batch_layer.log
# echo "Batch Layer finished at: $(date)" >> batch_layer.log

# # Check batch results
# echo ""
# echo "Batch results in S3:"
# aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive

# # Step 5: Start Speed Layer with logging
# echo ""
# echo "Step 5: Starting Speed Layer..."
# nohup spark-submit \
#     --master local[*] \
#     --driver-memory 1g \
#     speed_layer.py > speed.log 2>&1 &
# SPEED_PID=$!
# echo "Speed Layer started with PID: $SPEED_PID"
# echo "Logs: tail -f speed.log"

# # Step 6: Start Dashboard
# echo ""
# echo "Step 6: Starting Dashboard..."
# nohup python3 dashboard.py > dashboard.log 2>&1 &
# DASHBOARD_PID=$!
# echo "Dashboard started with PID: $DASHBOARD_PID"
# echo "Dashboard: http://localhost:5000"

# # Show summary
# echo ""
# echo "========================================="
# echo "Pipeline Status"
# echo "========================================="
# echo ""
# echo "Running Processes:"
# echo "  Producer PID: $PRODUCER_PID"
# echo "  Speed Layer PID: $SPEED_PID"
# echo "  Dashboard PID: $DASHBOARD_PID"
# echo ""
# echo "Data in S3:"
# echo "  kinesis-data: $(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l) files"
# echo "  results/speed: $(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l) files"
# echo "  results/batch: $(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l) files"
# echo ""
# echo "Log Files:"
# echo "  Producer: tail -f producer_s3.log"
# echo "  Batch Layer: tail -f batch_layer.log"
# echo "  Speed Layer: tail -f speed.log"
# echo "  Dashboard: tail -f dashboard.log"
# echo ""
# echo "To check Athena:"
# echo "  SELECT COUNT(*) FROM speed_results;"
# echo "  SELECT COUNT(*) FROM batch_results;"
# echo ""
# echo "To view batch log: cat batch_layer.log"
# echo ""
# echo "========================================="
# echo "Pipeline is running!"
# echo "Press Ctrl+C to stop monitoring"
# echo "========================================="

# wait




#!/bin/bash

echo "========================================="
echo "Complete Lambda Architecture Pipeline"
echo "========================================="

cd ~/environment/scalable

# Kill old processes
echo ""
echo "Cleaning up old processes..."
pkill -f "kinesis_producer" 2>/dev/null
pkill -f "spark-submit" 2>/dev/null
pkill -f "speed_layer" 2>/dev/null
pkill -f "dashboard.py" 2>/dev/null
sleep 3


# Step 2: Start Producer
echo ""
echo "Step 2: Starting S3 Producer..."
nohup python3 kinesis_producer.py > producer_s3.log 2>&1 &
PRODUCER_PID=$!
echo "Producer started with PID: $PRODUCER_PID"
echo "Logs: tail -f producer_s3.log"

# Step 3: Wait for data
echo ""
echo "Step 3: Waiting for data to accumulate (30 seconds)..."
sleep 30

DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
echo "Found $DATA_COUNT files in kinesis-data/"

# Step 4: Run Batch Layer with logging (First run)
echo ""
echo "Step 4: Running Batch Layer (First run)..."
echo "Batch Layer started at: $(date)" >> batch_layer.log
python3 batch_layer.py 2>&1 | tee -a batch_layer.log
echo "Batch Layer finished at: $(date)" >> batch_layer.log

# Check batch results
echo ""
echo "Batch results in S3:"
aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive

# Step 5: Start Batch Layer with 1-minute interval in background
echo ""
echo "Step 5: Starting Batch Layer Scheduler (runs every 1 minute)..."
nohup bash -c '
while true; do
    echo "Batch Layer scheduled run at: $(date)" >> batch_layer.log
    cd /home/ec2-user/environment/scalable
    python3 batch_layer.py 2>&1 | tee -a batch_layer.log
    echo "Batch Layer scheduled run completed at: $(date)" >> batch_layer.log
    sleep 60
done
' > batch_scheduler.log 2>&1 &
BATCH_SCHEDULER_PID=$!
echo "Batch Scheduler started with PID: $BATCH_SCHEDULER_PID"
echo "Logs: tail -f batch_scheduler.log"

# Step 6: Start Speed Layer with logging
echo ""
echo "Step 6: Starting Speed Layer..."
nohup spark-submit \
    --master local[*] \
    --driver-memory 1g \
    speed_layer.py > speed.log 2>&1 &
SPEED_PID=$!
echo "Speed Layer started with PID: $SPEED_PID"
echo "Logs: tail -f speed.log"

# Step 7: Start Dashboard
echo ""
echo "Step 7: Starting Dashboard..."
nohup python3 dashboard.py > dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "Dashboard started with PID: $DASHBOARD_PID"
echo "Dashboard: http://localhost:5000"

# Show summary
echo ""
echo "========================================="
echo "Pipeline Status"
echo "========================================="
echo ""
echo "Running Processes:"
echo "  Producer PID: $PRODUCER_PID"
echo "  Batch Scheduler PID: $BATCH_SCHEDULER_PID (runs every 1 minute)"
echo "  Speed Layer PID: $SPEED_PID"
echo "  Dashboard PID: $DASHBOARD_PID"
echo ""
echo "Data in S3:"
echo "  kinesis-data: $(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l) files"
echo "  results/speed: $(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l) files"
echo "  results/batch: $(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l) files"
echo ""
echo "Log Files:"
echo "  Producer: tail -f producer_s3.log"
echo "  Batch Scheduler: tail -f batch_scheduler.log"
echo "  Batch Layer: tail -f batch_layer.log"
echo "  Speed Layer: tail -f speed.log"
echo "  Dashboard: tail -f dashboard.log"
echo ""
echo "To check Athena:"
echo "  SELECT COUNT(*) FROM speed_results;"
echo "  SELECT COUNT(*) FROM batch_results;"
echo ""
echo "========================================="
echo "Pipeline is running!"
echo "Press Ctrl+C to stop monitoring"
echo "========================================="

wait