#!/bin/bash
# Convenience script to run online sync

export PYTHONPATH=$PYTHONPATH:$(pwd)
echo "Running Online Sync..."
python3 src/jobs/online_sync_job.py "$@"
