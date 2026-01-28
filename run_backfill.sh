#!/bin/bash

# Check for Java
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH."
    echo "Please install Java 8, 11, or 17 to run Spark."
    exit 1
fi

# Check for python dependencies
if ! python3 -c "import pyspark" &> /dev/null; then
    echo "Installing dependencies..."
    pip3 install -r requirements.txt
fi

echo "Running Backfill Job..."
# We use python3 directly. The spark_utils logic sets up the session.
# In a real cluster, you would use 'spark-submit' to utilize the cluster resources.
export PYTHONPATH=$PYTHONPATH:$(pwd)
python3 src/jobs/backfill_job.py "$@"
