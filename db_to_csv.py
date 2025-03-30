# Adjust this import based on your actual function
from maps import format_can_message_csv
import sqlite3
import csv

DATABASE_NAME = "/home/pi/Manned_PEP/frames_data.db"  # Update with the correct path

# Assuming value_range_map is defined as shown previously

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

def list_tables():
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Query to get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if tables:
            print("Tables in the database:")
            for table in tables:
                print(table[0])
        else:
            print("No tables found in the database.")
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

def export_trial_data_to_csv(trial_number):
    CSV_FILE_PATH = f"./csv_data/_data_{trial_number}.csv"
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Fetch all messages for the given trial number, sorted by timestamp
    cursor.execute(
        "SELECT * from 'trial_24'")
    messages = cursor.fetchall()

    conn.close()

    # Prepare headers for CSV based on value_range_map
    headers = ['Trial Number', 'Timestamp',
               'Message ID', 'PDO Label', 'DLC', 'Flags']
    for _, description, _, _ in value_range_map.values():
        if description not in headers:
            headers.append(description)

    # Initialize a structure to hold decoded data by timestamp
    decoded_data_by_timestamp = {}

    for trial_num, timestamp, frame_id, data in messages:
        # Decode each message
        decoded_message = format_can_message_csv({
            'id': frame_id,
            'data': data,  # Ensure this data is in the correct format for your decoding function
            'timestamp': timestamp,
            'flags': 0,
            'dlc': len(data)
        })

        # If the timestamp is not already a key in the dictionary, add it
        if timestamp not in decoded_data_by_timestamp:
            decoded_data_by_timestamp[timestamp] = []

        # Append decoded data for this timestamp
        decoded_data_by_timestamp[timestamp].append(decoded_message)

    # Open the CSV file and start writing
    with open(CSV_FILE_PATH, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()

        for timestamp, messages in decoded_data_by_timestamp.items():
            for message in messages:
                row = {
                    'Trial Number': trial_number,
                    'Timestamp': timestamp,
                    'Message ID': message['frame_id'],
                    'PDO Label': message['pdo_label'],
                    'DLC': message['dlc'],
                    'Flags': message['flags']
                }

                # Add decoded data values to the row
                for desc in value_range_map.values():
                    if desc[1] in message['data_values']:
                        row[desc[1]] = message['data_values'][desc[1]][0]
                    else:
                        row[desc[1]] = ''

                writer.writerow(row)
def export_sqlite_to_csv(table_name, output_csv):
    """
    Exports data from an SQLite database table to a CSV file.

    :param db_path: Path to the SQLite database file.
    :param table_name: Name of the table to export.
    :param output_csv: Path to the output CSV file.
    """
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        # Fetch column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Fetch data from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Write to CSV
        with open(output_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Write headers
            writer.writerows(rows)  # Write data
        
        print(f"Data exported successfully to {output_csv}")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        if conn:
            conn.close()

# Example usage
db_path = "/mnt/data/frames_data.db"
table_name = "your_table_name"  # Replace with the actual table name
output_csv = "./output.csv"
list_tables()
export_sqlite_to_csv("trial_25", output_csv)

# Example usage

#export_trial_data_to_csv(25)
