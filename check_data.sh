#!/bin/bash

echo "========================================="
echo "Data Flow Check"
echo "========================================="
echo ""

echo "1. Kinesis Data (Raw input):"
aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive | wc -l
aws s3 ls s3://s3-bucket-x23424567/kinesis-data/ --recursive | tail -3
echo ""

echo "2. Speed Results:"
aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive | wc -l
aws s3 ls s3://s3-bucket-x23424567/results/speed/ --recursive | tail -3
echo ""

echo "3. Batch Results:"
aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive | wc -l
aws s3 ls s3://s3-bucket-x23424567/results/batch/ --recursive | tail -3
echo ""

echo "4. Check Athena Tables (run in Athena console):"
echo "SELECT COUNT(*) FROM speed_results;"
echo "SELECT COUNT(*) FROM batch_results;"
echo ""

echo "5. Running processes:"
ps aux | grep -E "kinesis_s3_producer|speed_layer|spark-submit" | grep -v grep
