#!/bin/bash
# Helper for Mac/Brew users (Java check)
if [ -z "$JAVA_HOME" ] && [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

export PYTHONPATH=$PYTHONPATH:$(pwd)
echo "Running Maintenance Job..."
python3 src/jobs/maintenance_job.py "$@"
