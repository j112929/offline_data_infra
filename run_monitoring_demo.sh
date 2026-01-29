#!/bin/bash

# Monitoring Demo - Metrics Collection and Alerting
# Usage: ./run_monitoring_demo.sh

echo "Running Monitoring Demo..."
export PYTHONPATH=$PYTHONPATH:$(pwd)

echo ""
echo "=== Metrics Collection Demo ==="
python3 src/monitoring/metrics.py

echo ""
echo "=== Alert Management Demo ==="
python3 src/monitoring/alerts.py
