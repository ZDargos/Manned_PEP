#!/bin/bash

# Navigate to the correct directory
cd /home/pi/Manned_PEP

# Activate virtual environment if you're using one
# source venv/bin/activate

# Start the data collection script
python3 headless_gather.py >> data_collection.log 2>&1 