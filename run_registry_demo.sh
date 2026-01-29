#!/bin/bash

# Model Registry Demo
# Usage: ./run_registry_demo.sh

echo "Running Model Registry Demo..."
export PYTHONPATH=$PYTHONPATH:$(pwd)
python3 src/model/registry.py "$@"
