#!/bin/bash
# run_pipeline.sh - Complete Lambda Architecture Pipeline Execution

set -e

echo "========================================="
echo "Lambda Architecture Pipeline"
echo "========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check S3 bucket
check_s3_bucket() {
    print_status "Checking S3 bucket..."
    if aws s3 ls "s3://s3-bucket-x23424567" 2>/dev/null; then
        print_status "S3 bucket exists"
        return 0
    else
        print_status "Creating S3 bucket..."
        aws s3 mb "s3://s3-bucket-x23424567" --region us-east-1
        return $?
    fi
}

# Function to kill processes
kill_processes() {
    print_status "Cleaning up old processes..."
    pkill -f "kinesis_producer" 2>/dev/null || true
    pkill -f "speed_layer" 2>/dev/null || true
    pkill -f "spark-submit" 2>/dev/null || true
    pkill -f "dashboard.py" 2>/dev/null || true
    sleep 3
    print_status "Cleanup complete"
}

# Function to check Spark
check_spark() {
    print_status "Checking Spark installation..."
    if command_exists spark-submit; then
        print_status "Spark found: $(which spark-submit)"
        return 0
    fi
    if [ -f "/opt/spark/bin/spark-submit" ]; then
        print_status "Spark found at /opt/spark/bin/spark-submit"
        export PATH=$PATH:/opt/spark/bin
        export SPARK_HOME=/opt/spark
        return 0
    fi
    print_error "Spark not found"
    return 1
}

# Main execution
main() {
    print_status "Setting up environment..."
    cd ~/environment/scalable || { print_error "Cannot find scalable directory"; exit 1; }
    
    # Check infrastructure
    print_status "Step 1: Checking AWS infrastructure"
    echo "-----------------------------------"
    check_s3_bucket
    echo ""
    
    # Check Spark
    print_status "Step 2: Checking Spark"
    echo "-----------------------------------"
    check_spark || { print_error "Spark required"; exit 1; }
    echo ""
    
    # Kill old processes
    kill_processes
    echo ""
    
    # Start S3 producer
    print_status "Step 3: Starting S3 producer"
    echo "-----------------------------------"
    
    # Ensure kinesis_producer.py exists
    if [ ! -f "kinesis_producer.py" ]; then
        print_error "kinesis_producer.py not found"
        exit 1
    fi
    
    nohup python3 kinesis_producer.py > producer_s3.log 2>&1 &
    PRODUCER_PID=$!
    print_status "Producer started with PID: $PRODUCER_PID"
    print_status "Logs: tail -f producer_s3.log"
    echo ""
    
    # Wait for data
    print_status "Step 4: Waiting for data (60 seconds)"
    echo "-----------------------------------"
    sleep 60
    
    DATA_COUNT=$(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l)
    print_status "Found $DATA_COUNT files in kinesis-data/"
    echo ""
    
    # Start Speed Layer
    print_status "Step 5: Starting Speed Layer"
    echo "-----------------------------------"
    nohup spark-submit \
        --master local[*] \
        --driver-memory 2g \
        speed_layer.py > speed.log 2>&1 &
    SPEED_PID=$!
    print_status "Speed Layer started with PID: $SPEED_PID"
    print_status "Logs: tail -f speed.log"
    echo ""
    
    # Wait for speed layer
    print_status "Step 6: Waiting for Speed Layer (60 seconds)"
    echo "-----------------------------------"
    sleep 60
    
    SPEED_RESULTS=$(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l)
    print_status "Speed Layer produced $SPEED_RESULTS files"
    echo ""
    
    # Run Batch Layer
    print_status "Step 7: Running Batch Layer"
    echo "-----------------------------------"
    spark-submit \
        --master local[*] \
        --driver-memory 2g \
        batch_layer.py
    echo ""
    
    BATCH_RESULTS=$(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l)
    print_status "Batch Layer produced $BATCH_RESULTS files"
    echo ""
    
    # Fix dashboard path
    print_status "Step 8: Fixing dashboard path"
    echo "-----------------------------------"
    if [ -f "dashboard.py" ]; then
        sed -i "s/templates\/index.html/index.html/g" dashboard.py 2>/dev/null || true
    fi
    echo ""
    
    # Start Dashboard
    print_status "Step 9: Starting Dashboard"
    echo "-----------------------------------"
    nohup python3 dashboard.py > dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    print_status "Dashboard started with PID: $DASHBOARD_PID"
    print_status "Dashboard: http://localhost:5000"
    echo ""
    
    # Show summary
    echo "========================================="
    echo "Pipeline Status"
    echo "========================================="
    echo ""
    echo "Processes:"
    echo "  Producer: $PRODUCER_PID"
    echo "  Speed Layer: $SPEED_PID"
    echo "  Dashboard: $DASHBOARD_PID"
    echo ""
    echo "Data in S3:"
    echo "  kinesis-data: $(aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive 2>/dev/null | wc -l) files"
    echo "  results/speed: $(aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive 2>/dev/null | wc -l) files"
    echo "  results/batch: $(aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive 2>/dev/null | wc -l) files"
    echo ""
    echo "To check Athena, run in Athena console:"
    echo "  SELECT COUNT(*) FROM speed_results;"
    echo "  SELECT COUNT(*) FROM batch_results;"
    echo ""
    echo "========================================="
    echo "Press Ctrl+C to stop monitoring"
    
    wait
}

main