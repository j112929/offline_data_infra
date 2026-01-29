#!/bin/bash

# Batch Prediction Job Runner
# Usage: ./run_batch_predict.sh [--model_name MODEL] [--mock]

# Check for Java
if ! command -v java &> /dev/null; then
    if [ -d "/opt/homebrew/opt/openjdk@11" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
        export PATH="$JAVA_HOME/bin:$PATH"
    elif [ -d "/usr/local/opt/openjdk@11" ]; then
         export JAVA_HOME="/usr/local/opt/openjdk@11"
         export PATH="$JAVA_HOME/bin:$PATH"
    fi
fi

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

echo "Running Batch Prediction Job..."
export PYTHONPATH=$PYTHONPATH:$(pwd)
python3 src/jobs/batch_predict_job.py "$@"
