import csv
import os
import pandas as pd
from datetime import datetime

# Configuration
LOG_FILE = "usage_log.csv"
DAILY_QUOTA_LIMIT = 10000


def log_usage(user_id, event_type, query="-", country="-", result_count=0, quota_units=0, extra="-"):
    """
    Logs an event with the specific Quota Units consumed.
    """
    try:
        file_exists = os.path.isfile(LOG_FILE)

        with open(LOG_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header if new file (Now includes Quota_Units)
            if not file_exists:
                writer.writerow(
                    ["Timestamp", "User_ID", "Event", "Query", "Country", "Result_Count", "Quota_Units", "Extra_Info"])

            # Write Log Row
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([current_time, user_id, event_type, query, country, result_count, quota_units, extra])

    except Exception as e:
        print(f"[LOG ERROR] Could not log usage: {e}")


def get_logs():
    if os.path.exists(LOG_FILE):
        try:
            return pd.read_csv(LOG_FILE, on_bad_lines='skip')
        except Exception:
            return None
    return None


def estimate_daily_usage():
    """
    Sums up the 'Quota_Units' column for today's logs.
    """
    if not os.path.exists(LOG_FILE):
        return 0.0

    try:
        df = pd.read_csv(LOG_FILE, on_bad_lines='skip')

        # Ensure correct data types
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df['Quota_Units'] = pd.to_numeric(df['Quota_Units'], errors='coerce').fillna(0)

        # Filter for Today
        today_str = datetime.now().strftime('%Y-%m-%d')
        today_mask = df['Timestamp'].dt.strftime('%Y-%m-%d') == today_str

        # Sum the units used today
        units_used_today = df[today_mask]['Quota_Units'].sum()

        percent_used = min(units_used_today / DAILY_QUOTA_LIMIT, 1.0)
        return percent_used, int(units_used_today)

    except Exception:
        return 0.0, 0