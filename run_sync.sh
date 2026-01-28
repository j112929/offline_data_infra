#!/bin/bash
# Convenience script to run online sync

# Helper for Mac/Brew users (Java check)
if [ -z "$JAVA_HOME" ] && [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

export PYTHONPATH=$PYTHONPATH:$(pwd)
echo "Running Online Sync..."
python3 src/jobs/online_sync_job.py "$@"
