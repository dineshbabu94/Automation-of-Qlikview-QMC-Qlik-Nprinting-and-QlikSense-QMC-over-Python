import os
import time
import pandas as pd
import subprocess
import platform
import requests
from IPython.display import clear_output
from datetime import datetime, timedelta
import urllib.parse
from dotenv import load_dotenv
import logging
from datetime import timezone
import pytz
import re

# Set up logging to file
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(os.getenv("LOG_FILE", r"D:\OPS_Streamline\QlikSense\alert_logics.log"))
    ]
)
logger = logging.getLogger()

# Hard-code the environment file path
ENV_FILE = r"D:\OPS_Streamline\QlikSense\Variable\QlikSense_Alerts_common.env"
if not os.path.exists(ENV_FILE):
    logger.error(f"Environment file {ENV_FILE} not found")
    raise FileNotFoundError(f"Environment file {ENV_FILE} not found")
if not os.access(ENV_FILE, os.R_OK):
    logger.error(f"No read permission for {ENV_FILE}")
    raise PermissionError(f"No read permission for {ENV_FILE}")

load_dotenv(ENV_FILE, override=True)

# Validate required environment variables
REQUIRED_VARS = [
    "QLIKSENSE_DIR", "AVERAGE_TIME_FILE", "ALERT_HISTORY_FILE", "HISTORY_OUTPUT_DIR",
    "ORIGINAL_ALERT_API_URL", "NEW_ALERT_API_URL", "LOG_FILE",
    "ALERT_INTERVAL_SECONDS", "MAX_RUNNING_REMINDERS", "PRIORITY_REMINDER_THRESHOLD",
    "HOURLY_AFT40MIN_REMINDER_THRESHOLD", "HOURLY_TRIGGER_THRESHOLD_MIN",
    "RUNNING_AVG_TIME_OFFSET_SECONDS", "MIN_ISSUE_TIME_SECONDS", "MIN_ISSUE_TIME_DIFF_SECONDS",
    "DATA_SAVE_HOUR", "DATA_SAVE_MINUTE", "ALERT_HISTORY_RETENTION_DAYS",
    "MONITORING_SLEEP_SECONDS", "FILE_MOD_THRESHOLD_SECONDS", "PARQUET_READ_RETRIES", "RETRY_DELAY_SECONDS",
    "ALERT_MAX_LENGTH", "ALERT_TRUNCATION_LENGTH", "API_REQUEST_TIMEOUT_SECONDS",
    "STATUS_OUTPUT_FILE", "HISTORY_COLUMNS", "HISTORY_FILE_PREFIX"
]
for var in REQUIRED_VARS:
    if not os.getenv(var):
        logger.error(f"Missing required environment variable: {var}")
        raise ValueError(f"Missing required environment variable: {var}")

# Configuration from .env
QLIKSENSE_DIR = os.getenv("QLIKSENSE_DIR")
AVERAGE_TIME_FILE = os.getenv("AVERAGE_TIME_FILE")
ALERT_HISTORY_FILE = os.getenv("ALERT_HISTORY_FILE")
HISTORY_OUTPUT_DIR = os.getenv("HISTORY_OUTPUT_DIR")
STATUS_OUTPUT_FILE = os.getenv("STATUS_OUTPUT_FILE")
LOG_FILE = os.getenv("LOG_FILE")

ORIGINAL_ALERT_API_URL = os.getenv("ORIGINAL_ALERT_API_URL")
NEW_ALERT_API_URL = os.getenv("NEW_ALERT_API_URL")

ALERT_INTERVAL_SECONDS = int(os.getenv("ALERT_INTERVAL_SECONDS"))
MAX_RUNNING_REMINDERS = int(os.getenv("MAX_RUNNING_REMINDERS"))
PRIORITY_REMINDER_THRESHOLD = int(os.getenv("PRIORITY_REMINDER_THRESHOLD"))
HOURLY_AFT40MIN_REMINDER_THRESHOLD = int(os.getenv("HOURLY_AFT40MIN_REMINDER_THRESHOLD"))
HOURLY_TRIGGER_THRESHOLD_MIN = int(os.getenv("HOURLY_TRIGGER_THRESHOLD_MIN"))

RUNNING_AVG_TIME_OFFSET_SECONDS = int(os.getenv("RUNNING_AVG_TIME_OFFSET_SECONDS"))
MIN_ISSUE_TIME_SECONDS = int(os.getenv("MIN_ISSUE_TIME_SECONDS"))
MIN_ISSUE_TIME_DIFF_SECONDS = int(os.getenv("MIN_ISSUE_TIME_DIFF_SECONDS"))

DATA_SAVE_HOUR = int(os.getenv("DATA_SAVE_HOUR"))
DATA_SAVE_MINUTE = int(os.getenv("DATA_SAVE_MINUTE"))
ALERT_HISTORY_RETENTION_DAYS = int(os.getenv("ALERT_HISTORY_RETENTION_DAYS"))

MONITORING_SLEEP_SECONDS = float(os.getenv("MONITORING_SLEEP_SECONDS"))
FILE_MOD_THRESHOLD_SECONDS = float(os.getenv("FILE_MOD_THRESHOLD_SECONDS"))
PARQUET_READ_RETRIES = int(os.getenv("PARQUET_READ_RETRIES"))
RETRY_DELAY_SECONDS = float(os.getenv("RETRY_DELAY_SECONDS"))

ALERT_MAX_LENGTH = int(os.getenv("ALERT_MAX_LENGTH"))
ALERT_TRUNCATION_LENGTH = int(os.getenv("ALERT_TRUNCATION_LENGTH"))
API_REQUEST_TIMEOUT_SECONDS = float(os.getenv("API_REQUEST_TIMEOUT_SECONDS"))

HISTORY_COLUMNS = os.getenv("HISTORY_COLUMNS").split(",")
HISTORY_FILE_PREFIX = os.getenv("HISTORY_FILE_PREFIX")

# Dynamically generate PARQUET_FILE based on current month and year
current_date = datetime.now(pytz.timezone('Asia/Kolkata'))
PARQUET_FILE = os.path.join(QLIKSENSE_DIR, f"qliksense_executions_{current_date.strftime('%m_%Y')}.parquet")

# Log all environment variables
logger.info(f"Environment variables loaded from {ENV_FILE}:")
for var in REQUIRED_VARS:
    logger.info(f"{var}={os.getenv(var)}")
logger.info(f"PARQUET_FILE set to {PARQUET_FILE}")

# Verify file and directory access
if not os.path.exists(PARQUET_FILE):
    logger.error(f"PARQUET_FILE {PARQUET_FILE} does not exist")
    raise FileNotFoundError(f"PARQUET_FILE {PARQUET_FILE} does not exist")
if not os.access(PARQUET_FILE, os.R_OK):
    logger.error(f"No read permission for PARQUET_FILE {PARQUET_FILE}")
    raise PermissionError(f"No read permission for PARQUET_FILE {PARQUET_FILE}")
os.makedirs(HISTORY_OUTPUT_DIR, exist_ok=True)
if not os.access(HISTORY_OUTPUT_DIR, os.W_OK):
    logger.error(f"No write permission for HISTORY_OUTPUT_DIR {HISTORY_OUTPUT_DIR}")
    raise PermissionError(f"No write permission for HISTORY_OUTPUT_DIR {HISTORY_OUTPUT_DIR}")

# Verify write access to status output directory
status_output_dir = os.path.dirname(STATUS_OUTPUT_FILE)
os.makedirs(status_output_dir, exist_ok=True)
if not os.access(status_output_dir, os.W_OK):
    logger.error(f"No write permission for {status_output_dir}")
    raise PermissionError(f"No write permission for {status_output_dir}")

# Global dictionaries
sent_alerts = {}
alert_counts = {}
last_save_status = {}

def load_sent_alerts():
    """Load sent alerts from Parquet file."""
    try:
        if os.path.exists(ALERT_HISTORY_FILE):
            df = pd.read_parquet(ALERT_HISTORY_FILE)
            logger.info(f"Loaded {len(df)} alerts from {ALERT_HISTORY_FILE}")
            return {row['alert_key']: pd.Timestamp(row['last_alert_time']) for _, row in df.iterrows()}
        else:
            logger.info(f"Alert history file {ALERT_HISTORY_FILE} not found. Starting fresh.")
            return {}
    except Exception as e:
        logger.error(f"Error loading alert history: {str(e)}")
        return {}

def save_sent_alerts():
    """Save sent alerts to Parquet file."""
    try:
        df = pd.DataFrame([
            {'alert_key': key, 'last_alert_time': ts} for key, ts in sent_alerts.items()
        ])
        df.to_parquet(ALERT_HISTORY_FILE, index=False)
        logger.info(f"Saved {len(df)} alerts to {ALERT_HISTORY_FILE}")
    except Exception as e:
        logger.error(f"Error saving alert history: {str(e)}")

def purge_old_alerts():
    """Purge alerts older than retention period."""
    try:
        if os.path.exists(ALERT_HISTORY_FILE):
            df = pd.read_parquet(ALERT_HISTORY_FILE)
            if not df.empty:
                df['last_alert_time'] = pd.to_datetime(df['last_alert_time'])
                current_date = pd.Timestamp.now().date()
                retention_days_ago = current_date - timedelta(days=ALERT_HISTORY_RETENTION_DAYS)
                df = df[df['last_alert_time'].dt.date >= retention_days_ago]
                df.to_parquet(ALERT_HISTORY_FILE, index=False)
                logger.info(f"Purged alert history: Kept {len(df)} alerts from {retention_days_ago}")
                global sent_alerts
                sent_alerts = {row['alert_key']: row['last_alert_time'] for _, row in df.iterrows()}
            else:
                logger.info(f"Alert history file is empty, no purge needed.")
        else:
            logger.info(f"Alert history file {ALERT_HISTORY_FILE} not found, no purge needed.")
    except Exception as e:
        logger.error(f"Error purging alert history: {str(e)}")

# Load sent alerts at start
sent_alerts = load_sent_alerts()

def parse_time_taken(time_str):
    """Convert hh:mm:ss string to seconds."""
    try:
        if pd.isna(time_str):
            return None
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        return None

def format_time_taken(seconds):
    """Convert seconds to hh:mm:ss string."""
    if seconds is None or pd.isna(seconds):
        return None
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def clear_console():
    """Clear the console or Jupyter cell output."""
    try:
        if 'get_ipython' in globals() or 'IPython' in globals():
            clear_output(wait=True)
        else:
            if platform.system() == "Windows":
                subprocess.run("cls", shell=True)
            else:
                subprocess.run("clear", shell=True)
    except Exception as e:
        logger.error(f"Error clearing output: {str(e)}")

def get_file_mtime(file_path):
    """Get file's modification time."""
    try:
        if platform.system() == "Windows":
            os.system(f"attrib +a {file_path}")
        stat = os.stat(file_path)
        return stat.st_mtime
    except Exception as e:
        logger.error(f"Error getting mtime for {file_path}: {str(e)}")
        return None

def save_status_summary(df, output_file):
    """Save task counts by status for the current day."""
    try:
        if df is None or df.empty:
            logger.info("No data to summarize for status counts.")
            return
        
        ist = pytz.timezone('Asia/Kolkata')
        current_date = datetime.now(ist).strftime('%Y-%m-%d')
        
        df_current_day = df[df['start_date'] == current_date].copy()
        
        if df_current_day.empty:
            logger.info(f"No tasks found for current date {current_date}.")
            return
        
        status_counts = df_current_day.groupby('status').size().reset_index(name='Tasks')
        current_time = datetime.now(ist).strftime('%d/%m/%Y %H:%M:%S:%f')[:-3]
        status_counts['Time_Updated'] = current_time
        status_counts = status_counts.rename(columns={'status': 'Status'})
        status_counts = status_counts[['Status', 'Tasks', 'Time_Updated']]
        
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)
        if not os.access(output_dir, os.W_OK):
            logger.error(f"No write permission for {output_dir}")
            raise PermissionError(f"No write permission for {output_dir}")
        
        status_counts.to_parquet(output_file, index=False)
        logger.info(f"Saved status summary to {output_file} with {len(status_counts)} status records")
    except Exception as e:
        logger.error(f"Error saving status summary to {output_file}: {str(e)}")

def send_alert(task_name, start_date, status, time_taken, avg_time, start_time, end_time, run_status, task_freq=None):
    """Send an alert via API."""
    try:
        # Skip all alerts for Completed tasks
        if status == 'Completed':
            logger.info(f"Skipping alert for task {task_name}: Status is Completed")
            return True

        alert_key = f"{task_name}|{start_date}|{start_time}|{status}"
        task_key = f"{task_name}|{start_date}|{start_time}"
        
        current_time = pd.Timestamp.now()
        
        if status == 'Running':
            if alert_key in sent_alerts:
                last_alert_time = sent_alerts[alert_key]
                time_diff = (current_time - last_alert_time).total_seconds()
                if time_diff < ALERT_INTERVAL_SECONDS:
                    logger.info(f"Skipping alert for task {task_name}: Running alert sent {time_diff:.0f} seconds ago")
                    return True
            alert_counts[task_key] = alert_counts.get(task_key, 0) + 1
            if alert_counts[task_key] > MAX_RUNNING_REMINDERS:
                logger.info(f"Skipping alert for task {task_name}: Reached maximum reminders ({MAX_RUNNING_REMINDERS})")
                return True
            if alert_counts[task_key] == MAX_RUNNING_REMINDERS:
                prefix = "*Final Reminder*"
            else:
                prefix = f"*Reminder {alert_counts[task_key]}*"
        else:
            if alert_key in sent_alerts:
                logger.info(f"Skipping alert for task {task_name}: Alert already sent for {alert_key}")
                return True
            running_alert_key = f"{task_name}|{start_date}|{start_time}|Running"
            if status in ['Failed', 'Aborted', 'Skipped', 'Queued', 'Unknown Status']:
                prefix = "*Reminder Closed*" if running_alert_key in sent_alerts else "*Warning*"
            else:
                prefix = "*Warning*"
            logger.info(f"Checking Reminder Closed for task {task_name}, status {status}: "
                        f"running_alert_key={running_alert_key}, exists={running_alert_key in sent_alerts}")

        # Modified status display for API alerting
        display_status = status
        display_run_status = "No Issues Found" if run_status == "Status Changed" else run_status
        
        use_new_api = False
        if status == 'Running' and task_freq == 'Hourly':
            try:
                minute = int(start_time.split(':')[1])
                has_trigger = bool(re.search('TRIGGER', task_name, re.IGNORECASE))
                if minute >= HOURLY_TRIGGER_THRESHOLD_MIN and has_trigger and alert_counts.get(task_key, 0) >= HOURLY_AFT40MIN_REMINDER_THRESHOLD:
                    use_new_api = True
                    logger.info(f"Using NEW_ALERT_API_URL for task {task_name}: Reminder {alert_counts[task_key]}")
            except (ValueError, AttributeError) as e:
                logger.error(f"Error checking HOURLY_AFT40MIN conditions: {str(e)}")

        if not use_new_api:
            if status == 'Running' and alert_counts.get(task_key, 1) >= PRIORITY_REMINDER_THRESHOLD:
                use_new_api = True
            elif status in ['Failed', 'Aborted', 'Skipped', 'Queued', 'Unknown Status']:
                use_new_api = True

        api_url_template = NEW_ALERT_API_URL if use_new_api else ORIGINAL_ALERT_API_URL
        
        encoded_task_name = urllib.parse.quote(task_name)
        encoded_start_date = urllib.parse.quote(start_date)
        encoded_start_time = urllib.parse.quote(start_time)
        encoded_end_time = urllib.parse.quote('N/A' if pd.isna(end_time) else end_time)
        encoded_status = urllib.parse.quote(display_status)
        encoded_time_taken = urllib.parse.quote(time_taken)
        encoded_avg_time = urllib.parse.quote(avg_time if pd.notna(avg_time) else 'N/A')
        encoded_run_status = urllib.parse.quote(display_run_status)
        encoded_prefix = urllib.parse.quote(prefix)

        api_url = api_url_template.format(
            encoded_prefix, encoded_task_name, encoded_start_date, encoded_start_time,
            encoded_end_time, encoded_status, encoded_time_taken, encoded_avg_time, encoded_run_status
        )

        alert_msg = (
            f"{prefix}:\n"
            f"Task Name: *{task_name}*\n"
            f"Start Date: {start_date}\n"
            f"Start Time: {start_time}\n"
            f"End Time: {'N/A' if pd.isna(end_time) else end_time}\n"
            f"Status: *{display_status}*\n"
            f"Time Taken: *{time_taken}*\n"
            f"Average Time: {avg_time if pd.notna(avg_time) else 'N/A'}\n"
            f"Run Status: *{display_run_status}*\n"
            f"Platform: *QMC*"
        )
        if len(alert_msg) > ALERT_MAX_LENGTH:
            alert_msg = alert_msg[:ALERT_TRUNCATION_LENGTH] + "..."
        
        logger.info(f"Alert message: {alert_msg}")
        logger.info(f"Formatted API URL: {api_url}")
        
        current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
        try:
            start_date_dt = pd.to_datetime(start_date).date()
        except Exception as e:
            logger.error(f"Error parsing start_date {start_date}: {str(e)}")
            return False

        if start_date_dt == current_date:
            response = requests.get(api_url, timeout=API_REQUEST_TIMEOUT_SECONDS)
            logger.info(f"API Response: {response.status_code} - {response.text}")
            if response.status_code == 200:
                logger.info(f"Alert sent for task {task_name}")
                sent_alerts[alert_key] = current_time
                save_sent_alerts()
                return True
            else:
                logger.error(f"Failed to send alert: HTTP {response.status_code}")
                return False
        else:
            logger.info(f"Skipping alert: Start date {start_date} is not current date {current_date}")
            return True
    except Exception as e:
        logger.error(f"Error sending alert for task {task_name}: {str(e)}")
        return False

def read_parquet_file(parquet_file):
    """Read Parquet file, process data, merge with average times, send alerts."""
    try:
        df = pd.read_parquet(parquet_file)
        logger.info(f"Read {len(df)} records from {parquet_file}")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        if not df.empty:
            # Rename and extract columns
            df['start_date'] = pd.to_datetime(df['execution_start_date']).dt.strftime('%Y-%m-%d')
            df['start_time'] = pd.to_datetime(df['n_start_datetime']).dt.strftime('%H:%M:%S')
            df['end_date'] = pd.to_datetime(df['n_completed_on'], errors='coerce').dt.strftime('%Y-%m-%d')
            df['end_time'] = pd.to_datetime(df['n_completed_on'], errors='coerce').dt.strftime('%H:%M:%S')
            df['status'] = df['execution_status']
            df['time_taken'] = df['duration_hms']
            
            df = df[['task_name', 'start_date', 'start_time', 'end_date', 'end_time', 'status', 'time_taken']]
            
            # Compute time_taken for Running tasks
            df['start_datetime'] = pd.to_datetime(df['start_date'] + ' ' + df['start_time'])
            for idx, row in df.iterrows():
                if row['status'] == 'Running' or pd.isna(row.get('end_date')) or pd.isna(row.get('end_time')):
                    end_dt = pd.Timestamp.now()
                    duration = end_dt - row['start_datetime']
                    total_seconds = int(duration.total_seconds())
                    hours, remainder = divmod(total_seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    df.at[idx, 'time_taken'] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            # Merge with average times
            try:
                avg_df = pd.read_parquet(AVERAGE_TIME_FILE)
                logger.info(f"AVERAGE_TIME_FILE columns: {avg_df.columns.tolist()}")
                avg_df = avg_df[['task_name', 'new_average_time', 'task_status']].rename(
                    columns={'new_average_time': 'avg_time', 'task_status': 'task_freq'}
                )
                df = df.merge(avg_df, on='task_name', how='left')
                
                df['avg_tme_fin'] = None
                running_mask = df['status'] == 'Running'
                
                def compute_running_avg_tme_fin(row):
                    if pd.isna(row['avg_time']):
                        return None
                    start_time = row['start_time']
                    task_freq = row['task_freq']
                    task_name = row['task_name']
                    try:
                        minute = int(start_time.split(':')[1])
                        has_trigger = bool(re.search('TRIGGER', task_name, re.IGNORECASE))
                        if task_freq == 'Hourly' and minute >= HOURLY_TRIGGER_THRESHOLD_MIN and has_trigger:
                            logger.info(f"No offset for task {task_name}: task_freq={task_freq}, minute={minute}")
                            return row['avg_time']
                        else:
                            logger.info(f"Applying offset for task {task_name}: task_freq={task_freq}")
                            return format_time_taken(
                                parse_time_taken(row['avg_time']) + RUNNING_AVG_TIME_OFFSET_SECONDS
                            )
                    except (ValueError, AttributeError) as e:
                        logger.error(f"Error processing task {task_name}: {str(e)}")
                        return None
                
                df.loc[running_mask, 'avg_tme_fin'] = df.loc[running_mask].apply(compute_running_avg_tme_fin, axis=1)
                df.loc[~running_mask, 'avg_tme_fin'] = df.loc[~running_mask, 'avg_time']
                
                df['run_status'] = 'No Issue'
                df['time_taken_seconds'] = df['time_taken'].apply(parse_time_taken)
                df['avg_tme_fin_seconds'] = df['avg_tme_fin'].apply(parse_time_taken)
                
                time_issue_mask = (
                    (df['time_taken_seconds'] > df['avg_tme_fin_seconds']) &
                    (df['time_taken_seconds'] > MIN_ISSUE_TIME_SECONDS) &
                    ((df['time_taken_seconds'] - df['avg_tme_fin_seconds']) > MIN_ISSUE_TIME_DIFF_SECONDS) &
                    df['time_taken_seconds'].notna() &
                    df['avg_tme_fin_seconds'].notna()
                )
                status_issue_mask = df['status'].isin(['Failed', 'Aborted', 'Skipped', 'Queued', 'Unknown Status'])
                
                df.loc[time_issue_mask | status_issue_mask, 'run_status'] = 'Issue'
                
                status_change_mask = pd.Series(False, index=df.index)
                for idx, row in df.iterrows():
                    if row['status'] in ['Failed', 'Aborted', 'Skipped', 'Queued', 'Unknown Status']:
                        running_alert_key = f"{row['task_name']}|{row['start_date']}|{row['start_time']}|Running"
                        if running_alert_key in sent_alerts:
                            status_change_mask[idx] = True
                            logger.info(f"Status change for task {row['task_name']} to {row['status']}")
                
                df.loc[status_change_mask, 'run_status'] = df.loc[status_change_mask, 'run_status'].where(
                    df['run_status'] == 'Issue', 'Status Changed'
                )
                
                df = df.drop(columns=['time_taken_seconds', 'avg_tme_fin_seconds'])
                
                df['alert_status'] = 'Not needed'
                
                # Exclude Completed tasks from alerts
                issue_or_status_tasks = df[(df['run_status'].isin(['Issue', 'Status Changed'])) & (df['status'] != 'Completed')]
                for idx, row in issue_or_status_tasks.iterrows():
                    success = send_alert(
                        task_name=row['task_name'],
                        start_date=row['start_date'],
                        status=row['status'],
                        time_taken=row['time_taken'],
                        avg_time=row['avg_time'] if pd.notna(row['avg_time']) else 'N/A',
                        start_time=row['start_time'],
                        end_time=row.get('end_time', 'N/A'),
                        run_status=row['run_status'],
                        task_freq=row['task_freq']
                    )
                    df.at[idx, 'alert_status'] = f"Alert Sent at {pd.Timestamp.now()}" if success else 'Alert Failed'
                
            except Exception as e:
                logger.error(f"Error reading {AVERAGE_TIME_FILE}: {str(e)}")
                df['avg_time'] = None
                df['avg_tme_fin'] = None
                df['task_freq'] = None
                df['run_status'] = 'No Issue'
                df['alert_status'] = 'Not needed'
                
            columns_to_keep = ['task_name', 'task_freq', 'start_date', 'start_time', 'status', 'time_taken', 'avg_time', 'avg_tme_fin', 'run_status', 'alert_status']
            if 'end_date' in df.columns:
                columns_to_keep.insert(5, 'end_date')
            if 'end_time' in df.columns:
                columns_to_keep.insert(5, 'end_time')
            df = df[columns_to_keep]
            
            df['start_datetime'] = pd.to_datetime(df['start_date'] + ' ' + df['start_time'])
            df = df.sort_values(by=['start_datetime'], ascending=False)
            df = df.drop(columns=['start_datetime'])
        
        return df
    except Exception as e:
        logger.error(f"Error reading Parquet file {parquet_file}: {str(e)}")
        return None

def main():
    last_modified = 0
    last_update_time = None
    last_save_date = None
    last_purge_date = None
    output_dir = HISTORY_OUTPUT_DIR
    
    ist = pytz.timezone('Asia/Kolkata')
    
    while True:
        try:
            current_time = datetime.now(ist)
            current_date = current_time.date()
            current_date_str = current_date.strftime("%Y-%m-%d")
            
            # Update PARQUET_FILE dynamically
            global PARQUET_FILE
            PARQUET_FILE = os.path.join(QLIKSENSE_DIR, f"qliksense_executions_{current_time.strftime('%m_%Y')}.parquet")
            
            output_file = os.path.join(output_dir, f"{HISTORY_FILE_PREFIX}{current_date_str.replace('-', '')}.parquet")
            
            save_status_msg = ""
            if current_time.hour >= DATA_SAVE_HOUR and current_time.minute >= DATA_SAVE_MINUTE:
                if os.path.exists(output_file):
                    mtime = datetime.fromtimestamp(os.path.getmtime(output_file), tz=ist)
                    save_status_msg = f"History for {current_date_str} already saved at {mtime.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                elif last_save_date is None or last_save_date != current_date:
                    if os.path.exists(PARQUET_FILE):
                        df = read_parquet_file(PARQUET_FILE)
                        if df is not None:
                            df_current_date = df[df['start_date'] == current_date_str].copy()
                            if not df_current_date.empty:
                                try:
                                    columns_to_save = [col for col in HISTORY_COLUMNS if col in df_current_date.columns]
                                    os.makedirs(output_dir, exist_ok=True)
                                    df_to_save = df_current_date[columns_to_save].copy()
                                    df_to_save.to_parquet(output_file, index=False)
                                    save_status_msg = f"History for {current_date_str} saved at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                                    last_save_date = current_date
                                    last_save_status[current_date_str] = save_status_msg
                                except Exception as e:
                                    save_status_msg = f"Failed to save history for {current_date_str}: {str(e)}"
                            else:
                                save_status_msg = f"No tasks with start_date {current_date_str} to save."
                            last_save_date = current_date
                        else:
                            save_status_msg = f"Failed to read {PARQUET_FILE}: DataFrame is None"
                    else:
                        save_status_msg = f"Parquet file {PARQUET_FILE} does not exist"
                else:
                    save_status_msg = f"History for {current_date_str} already processed"
            
            if current_time.hour >= DATA_SAVE_HOUR and current_time.minute >= DATA_SAVE_MINUTE:
                if last_purge_date is None or last_purge_date != current_date:
                    purge_old_alerts()
                    last_purge_date = current_date
            
            if os.path.exists(PARQUET_FILE):
                current_modified = get_file_mtime(PARQUET_FILE)
                if current_modified is None:
                    logger.error(f"Could not get modification time for {PARQUET_FILE}")
                elif last_modified == 0 or abs(current_modified - last_modified) > FILE_MOD_THRESHOLD_SECONDS:
                    for attempt in range(PARQUET_READ_RETRIES):
                        df = read_parquet_file(PARQUET_FILE)
                        if df is not None:
                            save_status_summary(df, STATUS_OUTPUT_FILE)
                            last_update_time = pd.Timestamp.now()
                            clear_console()
                            print(f"[{last_update_time}] Data Refreshed")
                            print(f"History Save Status: {save_status_msg}")
                            filtered_df = df[(df['start_date'] == current_date_str) & ((df['status'] == 'Running') | (df['run_status'] == 'Issue'))]
                            print(f"Tasks with status='Running' or run_status='Issue' started today ({current_date_str}): {len(filtered_df)}")
                            if not filtered_df.empty:
                                display_columns = ['task_name', 'task_freq', 'start_date', 'start_time', 'status', 'time_taken', 'avg_time', 'avg_tme_fin', 'run_status', 'alert_status']
                                if 'end_date' in df.columns:
                                    display_columns.insert(5, 'end_date')
                                if 'end_time' in df.columns:
                                    display_columns.insert(5, 'end_time')
                                filtered_df = filtered_df[display_columns]
                                print(filtered_df.to_string(index=False))
                            else:
                                print(f"No tasks with status='Running' or run_status='Issue' started today ({current_date_str}).")
                            last_modified = current_modified
                            break
                        else:
                            logger.error(f"Attempt {attempt + 1} failed to read {PARQUET_FILE}")
                            time.sleep(RETRY_DELAY_SECONDS)
            else:
                clear_console()
                logger.info(f"Waiting for Parquet file {PARQUET_FILE}...")
                print(f"Waiting for Parquet file {PARQUET_FILE}...")
                print(f"History Save Status: {save_status_msg}")
                last_modified = 0
                last_update_time = None
            
            time.sleep(MONITORING_SLEEP_SECONDS)
        except KeyboardInterrupt:
            clear_console()
            print("Monitoring Stopped by user")
            logger.info("Monitoring stopped by user.")
            break
        except Exception as e:
            clear_console()
            logger.error(f"Error in monitoring loop: {str(e)}")
            print(f"Error in monitoring loop: {str(e)}")
            time.sleep(RETRY_DELAY_SECONDS)

if __name__ == "__main__":
    main()