#!/bin/bash

# Navigate to the correct directory
cd /home/pi/Manned_PEP

# Create necessary directories if they don't exist
mkdir -p csv_data
mkdir -p logs

# Start the data collection script and redirect output to log file
python3 auto_data_collector.py >> logs/data_collection_$(date +%Y%m%d_%H%M%S).log 2>&1 