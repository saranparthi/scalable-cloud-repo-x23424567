
#!/bin/bash

echo "========================================="
echo "BENCHMARK MODE"
echo "========================================="

cd ~/environment/scalable

# Stop normal pipeline processes
echo ""
echo "Stopping normal pipeline processes..."
pkill -f "speed_layer" 2>/dev/null
pkill -f "spark-submit.*speed_layer" 2>/dev/null
pkill -f "spark-submit.*batch_layer" 2>/dev/null
pkill -f "batch_layer" 2>/dev/null
sleep 5

# Ensure producer is running for fresh data
echo ""
echo "Checking producer..."
if ! pgrep -f "kinesis_producer" > /dev/null; then
    echo "Starting producer..."
    nohup python3 kinesis_producer.py > producer_benchmark.log 2>&1 &
    PRODUCER_PID=$!
    echo "Producer started with PID: $PRODUCER_PID"
    sleep 30
fi

# Get data count before benchmark
DATA_BEFORE=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
echo "Data files before benchmark: $DATA_BEFORE"

# Wait for fresh data
echo ""
echo "Waiting 60 seconds for fresh data..."
sleep 60

DATA_AFTER=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
echo "Data files after waiting: $DATA_AFTER"
echo "New files generated: $((DATA_AFTER - DATA_BEFORE))"

# Run Benchmark
echo ""
echo "========================================="
echo "Starting Performance Benchmark..."
echo "========================================="
echo "Benchmark started: $(date)" >> benchmark.log

spark-submit \
    --master local[4] \
    --driver-memory 2g \
    benchmark.py 2>&1 | tee -a benchmark.log

echo "Benchmark finished: $(date)" >> benchmark.log

# Show results
echo ""
echo "========================================="
echo "Benchmark Results"
echo "========================================="
echo ""
echo "Benchmark results saved to S3:"
aws s3 ls s3://s3-bucket-x23424567/benchmark-results/ --recursive

echo ""
echo "Benchmark graphs:"
aws s3 ls s3://s3-bucket-x23424567/benchmark-results/graphs/ --recursive

echo ""
echo "========================================="
echo "To view results:"
echo "  aws s3 cp s3://s3-bucket-x23424567/benchmark-results/benchmark_results.csv ."
echo "  cat benchmark_results.csv"
echo ""
echo "To restart normal pipeline: ./run_pipeline.sh"
echo "========================================="
