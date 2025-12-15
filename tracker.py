import csv
import os
import pandas as pd
from datetime import datetime

# Configuration
LOG_FILE = "usage_log.csv"


def log_usage(user_id, event_type, query="-", country="-", result_count=0, extra="-"):
    """
    Logs an event to the CSV file with a User ID.
    """
    try:
        file_exists = os.path.isfile(LOG_FILE)

        with open(LOG_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header if new file
            if not file_exists:
                writer.writerow(["Timestamp", "User_ID", "Event", "Query", "Country", "Result_Count", "Extra_Info"])

            # Write Log Row
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([current_time, user_id, event_type, query, country, result_count, extra])

    except Exception as e:
        print(f"[LOG ERROR] Could not log usage: {e}")


def get_logs():
    """
    Returns the log data as a Pandas DataFrame for the Admin view.
    Handles corrupt lines gracefully.
    """
    if os.path.exists(LOG_FILE):
        try:
            # on_bad_lines='skip' prevents crashing if the file structure changed
            return pd.read_csv(LOG_FILE, on_bad_lines='skip')
        except Exception:
            return None
    return None
