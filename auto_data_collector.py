#!/usr/bin/env python3

import datetime
import sqlite3
from canlib import canlib, CanlibException
import time
import struct
import os
import signal
import sys
import csv
from database_functions import create_table_for_trial, store_data_for_trial, get_next_trial_number
import queue
import threading
import logging

# Create necessary directories with proper permissions
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
CSV_DIR = os.path.join(BASE_DIR, "csv_data")

# Create directories if they don't exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'data_collection.log')),
        logging.StreamHandler()
    ]
)

# Database configuration
FRAMES_DATABASE = os.path.join(BASE_DIR, "frames_data.db")

# CAN configuration
can_queue = queue.Queue()
running = True
POWER_THRESHOLD = 100
POWER_OFF_THRESHOLD = 50
POWER_CHECK_INTERVAL = 1.0

def export_trial_to_csv(trial_number):
    """Export a trial's data to CSV"""
    try:
        # Create CSV filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"trial_{trial_number}_{timestamp}.csv"
        csv_path = os.path.join(CSV_DIR, csv_filename)
        
        # Connect to database
        conn = sqlite3.connect(FRAMES_DATABASE)
        cursor = conn.cursor()
        
        # Get all data for this trial
        cursor.execute(f"SELECT * FROM '{trial_number}'")
        rows = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"PRAGMA table_info('{trial_number}')")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Write to CSV
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)
            
        logging.info(f"Exported trial {trial_number} to {csv_path}")
        conn.close()
    except Exception as e:
        logging.error(f"Error exporting trial {trial_number} to CSV: {e}")

def read_can_messages(trial_number, can_queue):
    """Read CAN messages and store them in the database"""
    global running
    channel = 0
    
    # Wait for motor power
    logging.info("Waiting for motor power...")
    if not detect_power(channel):
        logging.info("No motor power detected. Exiting.")
        return

    with canlib.openChannel(channel, canlib.canOPEN_ACCEPT_VIRTUAL) as ch:
        ch.setBusOutputControl(canlib.canDRIVER_NORMAL)
        ch.setBusParams(canlib.canBITRATE_100K)
        ch.busOn()
        
        logging.info(f"Starting data collection for trial {trial_number}")
        while running:
            try:
                msg = ch.read()
                pdo_label = pdo_map.get(msg.id, "Unknown_PDO")
                msg_data = format_can_message(msg)
                can_queue.put(msg_data)
                
                if detect_power_off(channel):
                    logging.info("Motor power off detected, ending trial")
                    break
                    
            except canlib.CanNoMsg:
                pass
            except KeyboardInterrupt:
                break
        ch.busOff()

def main():
    global running
    
    def signal_handler(signum, frame):
        global running
        print("\nStopping data collection...")
        running = False
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create database directory if it doesn't exist
    os.makedirs(os.path.dirname(FRAMES_DATABASE), exist_ok=True)
    
    while running:
        try:
            # Get next trial number
            trial_number = get_next_trial_number()
            
            # Create table for this trial
            create_table_for_trial(trial_number)
            
            # Start data collection
            read_can_messages(trial_number, can_queue)
            
            # Process collected data
            batch = []
            while not can_queue.empty():
                msg_data = can_queue.get()
                batch.append(msg_data)
                if len(batch) >= 50:  # Process in batches of 50
                    store_data_for_trial(batch, trial_number)
                    batch = []
            
            # Store any remaining messages
            if batch:
                store_data_for_trial(batch, trial_number)
            
            # Export to CSV
            export_trial_to_csv(trial_number)
                
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(5)  # Wait before retrying
            continue

if __name__ == "__main__":
    main() 