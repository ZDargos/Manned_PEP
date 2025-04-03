# Adjust this import based on your actual function
from maps import format_can_message_csv
import sqlite3
import csv
from datetime import datetime
import os

def get_database_path():
    """Creates a new database file with date-based naming"""
    date_str = datetime.now().strftime("%Y%m%d")
    db_dir = "/home/pi/Manned_PEP/data"
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, f"boat_data_{date_str}.db")

DATABASE_NAME = get_database_path()

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

def create_new_trial_table():
    """Creates a new table for the current trial with timestamp"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Create table with timestamp in name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"trial_{timestamp}"
    
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        trial_number INTEGER,
        timestamp REAL,
        frame_id INTEGER,
        data BLOB
    )
    ''')
    
    conn.commit()
    conn.close()
    return table_name

def auto_export_trial_data(table_name):
    """Automatically exports trial data to CSV with proper naming"""
    # Create CSV directory if it doesn't exist
    csv_dir = "./csv_data"
    os.makedirs(csv_dir, exist_ok=True)
    
    # Generate CSV filename based on table name
    csv_filename = f"{table_name}.csv"
    csv_path = os.path.join(csv_dir, csv_filename)
    
    # Extract trial number from table name (assuming format trial_YYYYMMDD_HHMMSS)
    trial_number = table_name.split('_')[1]
    
    # Export the data
    export_trial_data_to_csv(trial_number)
    print(f"Data exported to {csv_path}")

def cleanup_old_data(days_to_keep=30):
    """Removes database files older than specified days"""
    db_dir = "/home/pi/Manned_PEP/data"
    current_time = datetime.now()
    
    for filename in os.listdir(db_dir):
        if filename.startswith("boat_data_") and filename.endswith(".db"):
            file_path = os.path.join(db_dir, filename)
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            
            if (current_time - file_time).days > days_to_keep:
                try:
                    os.remove(file_path)
                    print(f"Removed old database: {filename}")
                except Exception as e:
                    print(f"Error removing {filename}: {e}")

# Example usage
if __name__ == "__main__":
    # Create a new trial table
    current_table = create_new_trial_table()
    print(f"Created new trial table: {current_table}")
    
    # Clean up old data (keep last 30 days)
    cleanup_old_data()
