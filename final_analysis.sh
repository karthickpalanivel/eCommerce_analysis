#!/bin/bash

# Submit Spark job
spark-submit --master yarn --deploy-mode client spark_analysis.py

# Check exit status
if [ $? -eq 0 ]; then
    echo "Transformation successful"
else
    echo "Transformation failed"
fi