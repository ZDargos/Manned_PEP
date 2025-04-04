#!/usr/bin/env python3

import datetime
import sqlite3
from canlib import canlib, CanlibException
import time
import struct
import os
import signal
import sys
from database_functions import create_table_for_trial, store_data_for_trial, get_next_trial_number
import queue
import threading
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log'),
        logging.StreamHandler()
    ]
)

# Mapping from COB-ID to PDO and its information
FRAMES_DATABASE = "./frames_data.db"

can_queue = queue.Queue()
running = True
POWER_THRESHOLD = 100  # Adjust this threshold based on your motor's normal voltage
POWER_OFF_THRESHOLD = 50  # Voltage below this indicates motor is off
POWER_CHECK_INTERVAL = 1.0  # How often to check for power status (seconds)

value_range_map = {
    # COB-ID, Bytes : (Data Type, Description, Value Range, Units)
    (390, (0, 1)): ("U16", "Status word", "0-65535", ""),
    (390, (2, 3)): ("S16", "Actual speed", "-32768 to 32767", "Rpm"),
    (390, (4, 5)): ("U16", "RMS motor Current", "0-65535", "Arms"),
    (390, (6, 7)): ("S16", "DC Bus Voltage", "-32768 to 32767", "Adc"),
    (646, (0, 1)): ("S16", "Internal Speed Reference", "-32768 to 32767", "Rpm"),
    (646, (2, 3)): ("S16", "Reference Torque", "-32768 to 32767", "Nm"),
    (646, (4, 5)): ("S16", "Actual Torque", "-32768 to 32767", "Nm"),
    (646, (6, 7)): ("S16", "Field weakening control: voltage angle", "-32768 to 32767", "Deg"),
    (902, 0): ("U8", "Field weakening control: regulator status", "0-255", ""),
    (902, 1): ("U8", "Current limit: actual limit type", "0-15", ""),
    (902, (2, 3)): ("S16", "Motor voltage control: U peak normalized", "-32768 to 32767", ""),
    (902, (4, 5)): ("U16", "Digital status word", "0-65535", ""),
    (902, (6, 7)): ("S16", "Scaled throttle percent", "-32768 to 32767", ""),
    (1158, (0, 1)): ("S16", "Motor voltage control: idLimit", "-32768 to 32767", ""),
    (1158, (2, 3)): ("S16", "Motor voltage control: Idfiltered", "-32768 to 32767", "Arms"),
    (1158, (4, 5)): ("S16", "Actual currents: iq", "-32768 to 32767", "Apk"),
    (1158, (6, 7)): ("S16", "Motor measurements: DC bus current", "-32768 to 32767", "Adc"),
}

actual_values = {
    # COB-ID, Bytes : (Data Type, Description, Value Range, Units)
    (390, (0, 1)): 0,
    (390, (2, 3)): 0,
    (390, (4, 5)): 0,
    (390, (6, 7)): 0,
    (646, (0, 1)): 0,
    (646, (2, 3)): 0,
    (646, (4, 5)): 0,
    (646, (6, 7)): 0,
    (902, 0): 0,
    (902, 1): 0,
    (902, (2, 3)): 0,
    (902, (4, 5)): 0,
    (902, (6, 7)): 0,
    (1158, (0, 1)): 0,
    (1158, (2, 3)): 0,
    (1158, (4, 5)): 0,
    (1158, (6, 7)): 0,
}
pdo_map = {
    390: "PDO1",
    646: "PDO2",
    390: "PDO3",
    646: "PDO4",
}


def decode_data(msg_id, data_bytes):
    # print(f"Decoding data for msg_id: {msg_id} with data_bytes: {data_bytes}")
    data_values = {}

    for key, (data_type, description, value_range, units) in value_range_map.items():
        cob_id, byte_indices = key

        # Check if the msg_id matches the cob_id. If not, skip this iteration
        if msg_id != cob_id:
            continue

        if isinstance(byte_indices, tuple):
            start, end = key[1]
            # print(f"For msg_id {msg_id}, data_bytes length: {len(data_bytes)}")
            if end is None and start >= len(data_bytes):
                # print("Index out of range for single-byte key:", key)
                continue
            elif end is not None and (start >= len(data_bytes) or end >= len(data_bytes)):
                # print("Index out of range for multi-byte key:", key)
                continue
        else:
            start = key[1]
            if start >= len(data_bytes):
                continue  # Skip this iteration if index is out of range
            end = start  # Use start index as end index for single-byte data

        if data_type == "U16":
            value = (data_bytes[start] << 8) + data_bytes[end]
            # print("value U16: ", value)

        elif data_type == "S16":
            value = struct.unpack('>h', bytes(data_bytes[start:end+1]))[0]
            # print("value S16: ", value)

        elif data_type == "U8":
            value = data_bytes[start]
            # print("value U8: ", value)

        elif data_type == "0-15":
            value = data_bytes[start] & 0x0F
            # print("value 0-15: ", value)

        else:
            value = "Unsupported data type"
            # print("unsupported: ", value)

        data_values[description] = (value, value_range, units)
        # print("data values: ", data_values)

    return data_values


def format_can_message(msg):
    pdo_label = pdo_map.get(msg.id, "Unknown PDO")
    data_values = decode_data(msg.id, msg.data)

    return {
        'pdo_label': pdo_label,
        'id': msg.id,
        'data': data_values,
        'dlc': msg.dlc,
        'flags': msg.flags,
        'timestamp': msg.timestamp,
    }


def is_device_connected(channel):
    try:
        with canlib.openChannel(channel, canlib.canOPEN_ACCEPT_VIRTUAL) as ch:
            status = ch.getBusParams()
            return status is not None
    except CanlibException:
        return False


def detect_power(channel):
    """Monitor DC Bus Voltage to detect when motor is powered"""
    consecutive_readings = 0
    required_readings = 3  # Need 3 consecutive readings above threshold
    
    with canlib.openChannel(channel, canlib.canOPEN_ACCEPT_VIRTUAL) as ch:
        ch.setBusOutputControl(canlib.canDRIVER_NORMAL)
        ch.setBusParams(canlib.canBITRATE_100K)
        ch.busOn()
        
        while running:
            try:
                msg = ch.read()
                if msg.id == 390:  # Message ID for DC Bus Voltage
                    voltage = struct.unpack('<h', msg.data[6:8])[0]  # Extract voltage from bytes 6-7
                    logging.info(f"Current voltage: {voltage}")
                    
                    if voltage > POWER_THRESHOLD:
                        consecutive_readings += 1
                        if consecutive_readings >= required_readings:
                            logging.info(f"Motor power detected! Voltage: {voltage}")
                            return True
                    else:
                        consecutive_readings = 0
                        
                time.sleep(POWER_CHECK_INTERVAL)
            except canlib.CanNoMsg:
                time.sleep(POWER_CHECK_INTERVAL)
            except KeyboardInterrupt:
                break
        ch.busOff()
    return False


def detect_power_off(channel):
    """Monitor DC Bus Voltage to detect when motor is turned off"""
    consecutive_readings = 0
    required_readings = 3  # Need 3 consecutive readings below threshold
    
    with canlib.openChannel(channel, canlib.canOPEN_ACCEPT_VIRTUAL) as ch:
        ch.setBusOutputControl(canlib.canDRIVER_NORMAL)
        ch.setBusParams(canlib.canBITRATE_100K)
        ch.busOn()
        
        while running:
            try:
                msg = ch.read()
                if msg.id == 390:  # Message ID for DC Bus Voltage
                    voltage = struct.unpack('<h', msg.data[6:8])[0]
                    logging.info(f"Current voltage: {voltage}")
                    
                    if voltage < POWER_OFF_THRESHOLD:
                        consecutive_readings += 1
                        if consecutive_readings >= required_readings:
                            logging.info(f"Motor power off detected! Voltage: {voltage}")
                            return True
                    else:
                        consecutive_readings = 0
                        
                time.sleep(POWER_CHECK_INTERVAL)
            except canlib.CanNoMsg:
                time.sleep(POWER_CHECK_INTERVAL)
            except KeyboardInterrupt:
                break
        ch.busOff()
    return False


def read_can_messages(trial_number, can_queue):
    # Initialize and open the channel
    global running
    channel = 0
    
    # Wait for motor power
    logging.info("Waiting for motor power...")
    if not detect_power(channel):
        logging.info("No motor power detected. Exiting.")
        return

    # Now that power is detected, proceed with data collection
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
                
                # Check for power off
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
                
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)  # Wait before retrying
            continue


if __name__ == "__main__":
    main()
