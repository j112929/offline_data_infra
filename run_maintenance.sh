#!/bin/bash
export PYTHONPATH=$PYTHONPATH:$(pwd)
echo "Running Maintenance Job..."
python3 src/jobs/maintenance_job.py "$@"
