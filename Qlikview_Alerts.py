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
import pytz
import re

# Set up logging to file
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('alert_logics.log')
    ]
)
logger = logging.getLogger()

# Load environment variables from .env file
ENV_FILE = r"D:\OPS_Streamline\Variable\Alert_logics_common.env"
if not os.path.exists(ENV_FILE):
    logger.error(f"Environment file {ENV_FILE} not found")
    raise FileNotFoundError(f"Environment file {ENV_FILE} not found")
if not os.access(ENV_FILE, os.R_OK):
    logger.error(f"No read permission for {ENV_FILE}")
    raise PermissionError(f"No read permission for {ENV_FILE}")

# Explicitly load .env file
load_dotenv(ENV_FILE, override=True)

# Validate required environment variables
REQUIRED_VARS = [
    "PARQUET_FILE", "AVERAGE_TIME_FILE", "ALERT_HISTORY_FILE", "HISTORY_OUTPUT_DIR",
    "ORIGINAL_ALERT_API_URL", "NEW_ALERT_API_URL",
    "ALERT_INTERVAL_SECONDS", "MAX_RUNNING_REMINDERS", "PRIORITY_REMINDER_THRESHOLD",
    "HOURLY_AFT40MIN_REMINDER_THRESHOLD", "HOURLY_TRIGGER_THRESHOLD_MIN",
    "RUNNING_AVG_TIME_OFFSET_SECONDS", "MIN_ISSUE_TIME_SECONDS", "MIN_ISSUE_TIME_DIFF_SECONDS",
    "HIVE_ERROR_MIN_SECONDS", "HIVE_ERROR_MAX_SECONDS",
    "DATA_SAVE_HOUR", "DATA_SAVE_MINUTE", "ALERT_HISTORY_RETENTION_DAYS",
    "MONITORING_SLEEP_SECONDS", "FILE_MOD_THRESHOLD_SECONDS", "PARQUET_READ_RETRIES", "RETRY_DELAY_SECONDS",
    "ALERT_MAX_LENGTH", "ALERT_TRUNCATION_LENGTH", "API_REQUEST_TIMEOUT_SECONDS",
    "STATUS_OUTPUT_FILE",
    "NA_PLACEHOLDER", "TRIGGER_PATTERN", "STATUS_DISPLAY_MAP",
    "DAILY_FILE_PREFIX", "DAILY_SAVE_COLUMNS", "STATUS_TIME_FORMAT", "TIMEZONE"
]
for var in REQUIRED_VARS:
    if not os.getenv(var):
        logger.error(f"Missing required environment variable: {var}")
        raise ValueError(f"Missing required environment variable: {var}")

# Configuration from .env with fallbacks
PARQUET_FILE = os.getenv("PARQUET_FILE")
AVERAGE_TIME_FILE = os.getenv("AVERAGE_TIME_FILE")
ALERT_HISTORY_FILE = os.getenv("ALERT_HISTORY_FILE")
HISTORY_OUTPUT_DIR = os.getenv("HISTORY_OUTPUT_DIR")
STATUS_OUTPUT_FILE = os.getenv("STATUS_OUTPUT_FILE")

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
HIVE_ERROR_MIN_SECONDS = int(os.getenv("HIVE_ERROR_MIN_SECONDS"))
HIVE_ERROR_MAX_SECONDS = int(os.getenv("HIVE_ERROR_MAX_SECONDS"))

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

# New environment variables
NA_PLACEHOLDER = os.getenv("NA_PLACEHOLDER")
TRIGGER_PATTERN = os.getenv("TRIGGER_PATTERN")
DAILY_FILE_PREFIX = os.getenv("DAILY_FILE_PREFIX")
TIMEZONE = os.getenv("TIMEZONE")

# Parse STATUS_DISPLAY_MAP (e.g., "Finished:Completed" into {"Finished": "Completed"})
try:
    STATUS_DISPLAY_MAP = dict(item.split(":") for item in os.getenv("STATUS_DISPLAY_MAP").split(","))
except Exception as e:
    logger.error(f"Error parsing STATUS_DISPLAY_MAP: {str(e)}")
    raise ValueError("Invalid STATUS_DISPLAY_MAP format")

# Parse DAILY_SAVE_COLUMNS (e.g., "task_name,start_date,..." into a list)
DAILY_SAVE_COLUMNS = os.getenv("DAILY_SAVE_COLUMNS").split(",")

# Parse STATUS_TIME_FORMAT
STATUS_TIME_FORMAT = os.getenv("STATUS_TIME_FORMAT")

# Log all environment variables
logger.info(f"Environment variables loaded from {ENV_FILE}:")
for var in REQUIRED_VARS:
    logger.info(f"{var}={os.getenv(var)}")

# Verify file and directory access at startup
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

# Global dictionary to track sent alerts with timestamps and alert counts
sent_alerts = {}
alert_counts = {}  # Tracks number of Running alerts per task
last_save_status = {}  # Tracks save status and timestamp for console output

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
    """Purge alerts older than retention period from alert_history.parquet."""
    try:
        if os.path.exists(ALERT_HISTORY_FILE):
            df = pd.read_parquet(ALERT_HISTORY_FILE)
            if not df.empty:
                df['last_alert_time'] = pd.to_datetime(df['last_alert_time'])
                current_date = pd.Timestamp.now().date()
                retention_days_ago = current_date - timedelta(days=ALERT_HISTORY_RETENTION_DAYS)
                df = df[df['last_alert_time'].dt.date >= retention_days_ago]
                df.to_parquet(ALERT_HISTORY_FILE, index=False)
                logger.info(f"Purged alert history: Kept {len(df)} alerts from {retention_days_ago} to {current_date}")
                global sent_alerts
                sent_alerts = {row['alert_key']: row['last_alert_time'] for _, row in df.iterrows()}
            else:
                logger.info(f"Alert history file is empty, no purge needed.")
        else:
            logger.info(f"Alert history file {ALERT_HISTORY_FILE} not found, no purge needed.")
    except Exception as e:
        logger.error(f"Error purging alert history: {str(e)}")

# Load sent alerts at script start
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
    """Get the file's modification time, forcing a fresh stat call."""
    try:
        if platform.system() == "Windows":
            os.system(f"attrib +a {file_path}")
        stat = os.stat(file_path)
        return stat.st_mtime
    except Exception as e:
        logger.error(f"Error getting mtime for {file_path}: {str(e)}")
        return None

def save_status_summary(df, output_file):
    """Save a summary of task counts by status for the current day to a Parquet file with the current timestamp."""
    try:
        if df is None or df.empty:
            logger.info("No data to summarize for status counts.")
            return
        
        # Get current date in yyyy-mm-dd format
        ist = pytz.timezone(TIMEZONE)
        current_date = datetime.now(ist).strftime('%Y-%m-%d')
        
        # Filter tasks for the current day only
        df_current_day = df[df['start_date'] == current_date].copy()
        
        if df_current_day.empty:
            logger.info(f"No tasks found for current date {current_date}.")
            return
        
        # Group by status and count tasks for the current day
        status_counts = df_current_day.groupby('status').size().reset_index(name='Tasks')
        
        # Add current timestamp in specified format with millisecond precision
        current_time = datetime.now(ist).strftime(STATUS_TIME_FORMAT)[:-3]  # Truncate to milliseconds
        status_counts['Time_Updated'] = current_time
        
        # Rename columns to match required format
        status_counts = status_counts.rename(columns={'status': 'Status'})
        
        # Ensure columns are in the desired order
        status_counts = status_counts[['Status', 'Tasks', 'Time_Updated']]
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)
        
        # Verify write permissions
        if not os.access(output_dir, os.W_OK):
            logger.error(f"No write permission for {output_dir}")
            raise PermissionError(f"No write permission for {output_dir}")
        
        # Save to Parquet, overwriting any existing file
        status_counts.to_parquet(output_file, index=False)
        logger.info(f"Saved status summary to {output_file} with {len(status_counts)} status records for {current_date}")
        
    except Exception as e:
        logger.error(f"Error saving status summary to {output_file}: {str(e)}")

def send_alert(task_name, start_date, status, time_taken, avg_time, start_time, end_time, run_status, task_freq=None):
    """Send an alert via the API with task details, using NEW_ALERT_API_URL for specific conditions."""
    try:
        # Skip sending alerts for status mapped to 'Completed' (e.g., 'Finished')
        if status in STATUS_DISPLAY_MAP and STATUS_DISPLAY_MAP[status] == STATUS_DISPLAY_MAP.get("Finished"):
            logger.info(f"Skipping alert for task {task_name}: Status is {status}")
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
            if status == 'Failed':
                time_taken_seconds = parse_time_taken(time_taken)
                if time_taken_seconds is not None and HIVE_ERROR_MIN_SECONDS <= time_taken_seconds <= HIVE_ERROR_MAX_SECONDS:
                    prefix = "*Possibly HIVE Error*"
                else:
                    prefix = "*Reminder Closed*" if running_alert_key in sent_alerts else "*Warning*"
            elif status == 'Aborted':
                prefix = "*Reminder Closed*" if running_alert_key in sent_alerts else "*Warning*"
            else:
                prefix = "*Warning*"
            logger.info(f"Checking Reminder Closed for task {task_name}, status {status}: "
                        f"running_alert_key={running_alert_key}, exists={running_alert_key in sent_alerts}, "
                        f"prefix={prefix}")

        display_status = STATUS_DISPLAY_MAP.get(status, status)
        
        # Determine which API to use
        use_new_api = False
        if status == 'Running' and task_freq == 'Hourly':
            try:
                minute = int(start_time.split(':')[1])
                has_trigger = bool(re.search(TRIGGER_PATTERN, task_name, re.IGNORECASE))
                if minute >= HOURLY_TRIGGER_THRESHOLD_MIN and has_trigger and alert_counts.get(task_key, 0) >= HOURLY_AFT40MIN_REMINDER_THRESHOLD:
                    use_new_api = True
                    logger.info(f"Using NEW_ALERT_API_URL for task {task_name}: Reminder {alert_counts.get(task_key, 0)}, "
                                f"task_freq={task_freq}, minute={minute}, has_trigger={has_trigger}, "
                                f"threshold_min={HOURLY_TRIGGER_THRESHOLD_MIN}")
            except (ValueError, AttributeError) as e:
                logger.error(f"Error checking HOURLY_AFT40MIN conditions for task {task_name}: {str(e)}")

        # Apply existing PRIORITY_REMINDER_THRESHOLD logic
        if not use_new_api:
            if status == 'Running' and alert_counts.get(task_key, 1) >= PRIORITY_REMINDER_THRESHOLD:
                use_new_api = True
            elif status in ['Failed', 'Aborted']:
                use_new_api = True

        api_url_template = NEW_ALERT_API_URL if use_new_api else ORIGINAL_ALERT_API_URL
        
        # Encode individual field values to handle special characters
        encoded_task_name = urllib.parse.quote(task_name)
        encoded_start_date = urllib.parse.quote(start_date)
        encoded_start_time = urllib.parse.quote(start_time)
        encoded_end_time = urllib.parse.quote(NA_PLACEHOLDER if pd.isna(end_time) else end_time)
        encoded_status = urllib.parse.quote(display_status)
        encoded_time_taken = urllib.parse.quote(time_taken)
        encoded_avg_time = urllib.parse.quote(avg_time if pd.notna(avg_time) else NA_PLACEHOLDER)
        encoded_prefix = urllib.parse.quote(prefix)

        # Format the API URL with encoded fields
        api_url = api_url_template.format(
            encoded_prefix,
            encoded_task_name,
            encoded_start_date,
            encoded_start_time,
            encoded_end_time,
            encoded_status,
            encoded_time_taken,
            encoded_avg_time
        )

        # Construct alert message for logging and reference
        alert_msg = (
            f"{prefix}:\n"
            f"Task Name: *{task_name}*\n"
            f"Start Date: {start_date}\n"
            f"Start Time: {start_time}\n"
            f"End Time: {NA_PLACEHOLDER if pd.isna(end_time) else end_time}\n"
            f"Status: *{display_status}*\n"
            f"Time Taken: *{time_taken}*\n"
            f"Average Time: {avg_time if pd.notna(avg_time) else NA_PLACEHOLDER}\n"
            f"Platform: *QMC*"
        )
        if len(alert_msg) > ALERT_MAX_LENGTH:
            alert_msg = alert_msg[:ALERT_TRUNCATION_LENGTH] + "..."
        
        logger.info(f"Alert message for reference: {alert_msg}")
        logger.info(f"Formatted API URL: {api_url}")
        
        current_date = datetime.now(pytz.timezone(TIMEZONE)).date()
        try:
            start_date_dt = pd.to_datetime(start_date).date()
        except Exception as e:
            logger.error(f"Error parsing start_date {start_date} for task {task_name}: {str(e)}")
            return False

        if start_date_dt == current_date:
            response = requests.get(api_url, timeout=API_REQUEST_TIMEOUT_SECONDS)
            logger.info(f"API Response: {response.status_code} - {response.text}")
            
            if response.status_code == 200:
                logger.info(f"Alert sent for task {task_name}: {alert_msg}")
                sent_alerts[alert_key] = current_time
                save_sent_alerts()
                return True
            else:
                logger.error(f"Failed to send alert for task {task_name}: HTTP {response.status_code}")
                return False
        else:
            logger.info(f"Skipping alert for task {task_name}: Start date {start_date} is not current date {current_date}")
            return True

    except Exception as e:
        logger.error(f"Error sending alert for task {task_name}: {str(e)}")
        return False

def read_parquet_file(parquet_file):
    """Read the Parquet file, compute time_taken, merge with average time data, send alerts, and return a sorted DataFrame."""
    try:
        df = pd.read_parquet(parquet_file)
        logger.info(f"Read {len(df)} records from {parquet_file}")
        logger.info(f"Columns in {parquet_file}: {df.columns.tolist()}")
        if not df.empty:
            df['start_datetime'] = pd.to_datetime(df['start_date'] + ' ' + df['start_time'])
            df['time_taken'] = None
            
            for idx, row in df.iterrows():
                start_dt = row['start_datetime']
                if row['status'] == 'Running' or ('end_date' not in df.columns or pd.isna(row.get('end_date'))) or ('end_time' not in df.columns or pd.isna(row.get('end_time'))):
                    end_dt = pd.Timestamp.now()
                else:
                    end_dt = pd.to_datetime(row['end_date'] + ' ' + row['end_time'])
                
                duration = end_dt - start_dt
                total_seconds = int(duration.total_seconds())
                hours, remainder = divmod(total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_taken = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                df.at[idx, 'time_taken'] = time_taken
            
            try:
                avg_df = pd.read_parquet(AVERAGE_TIME_FILE)
                logger.info(f"AVERAGE_TIME_FILE columns: {avg_df.columns.tolist()}")
                avg_df = avg_df[['task_name', 'new_average_time_taken', 'task_freq']].rename(
                    columns={'new_average_time_taken': 'avg_time'}
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
                        has_trigger = bool(re.search(TRIGGER_PATTERN, task_name, re.IGNORECASE))
                        if task_freq == 'Hourly' and minute >= HOURLY_TRIGGER_THRESHOLD_MIN and has_trigger:
                            logger.info(f"No offset for task {task_name}: task_freq={task_freq}, minute={minute}, "
                                        f"has_trigger={has_trigger}, threshold_min={HOURLY_TRIGGER_THRESHOLD_MIN}")
                            return row['avg_time']
                        else:
                            logger.info(f"Applying offset for task {task_name}: task_freq={task_freq}, minute={minute}, "
                                        f"has_trigger={has_trigger}, threshold_min={HOURLY_TRIGGER_THRESHOLD_MIN}")
                            return format_time_taken(
                                parse_time_taken(row['avg_time']) + RUNNING_AVG_TIME_OFFSET_SECONDS
                            )
                    except (ValueError, AttributeError) as e:
                        logger.error(f"Error processing task {task_name}: start_time={start_time}, task_freq={task_freq}, error={str(e)}")
                        return None
                
                df.loc[running_mask, 'avg_tme_fin'] = df.loc[running_mask].apply(compute_running_avg_tme_fin, axis=1)
                
                non_running_mask = ~running_mask
                df.loc[non_running_mask, 'avg_tme_fin'] = df.loc[non_running_mask, 'avg_time']
                
                logger.info(f"Tasks with computed avg_tme_fin:\n{df[['task_name', 'start_time', 'status', 'avg_time', 'task_freq', 'avg_tme_fin']].to_string(index=False)}")
                
                df['run_status'] = 'No Issue'
                df['time_taken_seconds'] = df['time_taken'].apply(parse_time_taken)
                df['avg_tme_fin_seconds'] = df['avg_tme_fin'].apply(parse_time_taken)
                
                time_issue_mask = (df['time_taken_seconds'] > df['avg_tme_fin_seconds']) & \
                                  (df['time_taken_seconds'] > MIN_ISSUE_TIME_SECONDS) & \
                                  ((df['time_taken_seconds'] - df['avg_tme_fin_seconds']) > MIN_ISSUE_TIME_DIFF_SECONDS) & \
                                  df['time_taken_seconds'].notna() & \
                                  df['avg_tme_fin_seconds'].notna()
                status_issue_mask = df['status'].isin(['Failed', 'Aborted'])
                
                potential_issues = df[status_issue_mask | (df['time_taken_seconds'] > df['avg_tme_fin_seconds'])]
                if not potential_issues.empty:
                    logger.info(f"Potential Issue tasks (before filters):\n{potential_issues[['task_name', 'status', 'time_taken', 'avg_tme_fin', 'time_taken_seconds', 'avg_tme_fin_seconds']].to_string(index=False)}")
                
                non_issues = df[~time_issue_mask & ~status_issue_mask]
                if not non_issues.empty:
                    logger.info(f"Non-Issue tasks:\n{non_issues[['task_name', 'status', 'time_taken_seconds', 'avg_tme_fin_seconds']].to_string(index=False)}")
                
                status_change_mask = pd.Series(False, index=df.index)
                for idx, row in df.iterrows():
                    if row['status'] in ['Failed', 'Aborted']:
                        running_alert_key = f"{row['task_name']}|{row['start_date']}|{row['start_time']}|Running"
                        if running_alert_key in sent_alerts:
                            status_change_mask[idx] = True
                            logger.info(f"Detected status change for task {row['task_name']}: "
                                        f"Running to {row['status']}, running_alert_key={running_alert_key}")
                        else:
                            logger.info(f"No status change for task {row['task_name']}: "
                                        f"status={row['status']}, running_alert_key={running_alert_key} not in sent_alerts")
                
                df.loc[time_issue_mask | status_issue_mask, 'run_status'] = 'Issue'
                df.loc[status_change_mask, 'run_status'] = df.loc[status_change_mask, 'run_status'].where(
                    df['run_status'] == 'Issue', 'Status Changed'
                )
                
                df = df.drop(columns=['time_taken_seconds', 'avg_tme_fin_seconds'])
                
                df['alert_status'] = 'Not needed'
                
                # Exclude tasks with status mapped to 'Completed' from alerts
                issue_or_status_tasks = df[(df['run_status'].isin(['Issue', 'Status Changed'])) & 
                                         (~df['status'].isin([k for k, v in STATUS_DISPLAY_MAP.items() if v == STATUS_DISPLAY_MAP.get("Finished")]))]
                logger.info(f"Number of tasks with issues or status changes (excluding Finished): {len(issue_or_status_tasks)}")
                if not issue_or_status_tasks.empty:
                    logger.info(f"Tasks with issues or status changes (excluding Finished):\n{issue_or_status_tasks[['task_name', 'start_date', 'start_time', 'status', 'time_taken', 'avg_time', 'avg_tme_fin', 'run_status']].to_string(index=False)}")
                
                for idx, row in issue_or_status_tasks.iterrows():
                    success = send_alert(
                        task_name=row['task_name'],
                        start_date=row['start_date'],
                        status=row['status'],
                        time_taken=row['time_taken'],
                        avg_time=row['avg_time'] if pd.notna(row['avg_time']) else NA_PLACEHOLDER,
                        start_time=row['start_time'],
                        end_time=row.get('end_time', NA_PLACEHOLDER),
                        run_status=row['run_status'],
                        task_freq=row['task_freq']
                    )
                    if success:
                        df.at[idx, 'alert_status'] = f"Alert Sent at {pd.Timestamp.now()}"
                    else:
                        df.at[idx, 'alert_status'] = 'Alert Failed'
                
            except Exception as e:
                logger.error(f"Error reading {AVERAGE_TIME_FILE}: {str(e)}")
                df['avg_time'] = None
                df['avg_tme_fin'] = None
                df['task_freq'] = None
                df['run_status'] = 'No Issue'
                df['alert_status'] = 'Not needed'
                
            columns_to_keep = ['task_name', 'task_freq', 'start_date', 'start_time', 'status', 'time_taken', 'avg_time', 'avg_tme_fin', 'run_status', 'alert_status']
            if 'end_time' in df.columns:
                columns_to_keep.insert(5, 'end_time')
            if 'end_date' in df.columns:
                columns_to_keep.insert(5, 'end_date')
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
    
    ist = pytz.timezone(TIMEZONE)
    
    while True:
        try:
            current_time = datetime.now(ist)
            current_date = current_time.date()
            current_date_str = current_date.strftime("%Y-%m-%d")
            
            logger.info(f"Current time: {current_time}, checking save condition (hour >= {DATA_SAVE_HOUR}, minute >= {DATA_SAVE_MINUTE})")
            
            save_status_msg = ""
            output_file = os.path.join(output_dir, f"{DAILY_FILE_PREFIX}{current_date.strftime('%Y%m%d')}.parquet")
            
            if current_time.hour >= DATA_SAVE_HOUR and current_time.minute >= DATA_SAVE_MINUTE:
                if os.path.exists(output_file):
                    mtime = datetime.fromtimestamp(os.path.getmtime(output_file), tz=ist)
                    save_status_msg = f"History for {current_date_str} already saved at {mtime.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    logger.info(save_status_msg)
                    last_save_date = current_date
                elif last_save_date is None or last_save_date != current_date:
                    logger.info(f"Attempting to save daily Parquet file at {current_time} for date {current_date}")
                    if os.path.exists(PARQUET_FILE):
                        df = read_parquet_file(PARQUET_FILE)
                        if df is not None:
                            logger.info(f"Filtering tasks with start_date {current_date_str}")
                            df_current_date = df[df['start_date'] == current_date_str].copy()
                            logger.info(f"Found {len(df_current_date)} tasks for {current_date_str}")
                            if not df_current_date.empty:
                                try:
                                    available_columns = df_current_date.columns.tolist()
                                    logger.info(f"Available columns in df_current_date: {available_columns}")
                                    
                                    desired_columns = DAILY_SAVE_COLUMNS
                                    columns_to_save = [col for col in desired_columns if col in available_columns]
                                    if not columns_to_save:
                                        raise ValueError("No desired columns available for saving")
                                    
                                    logger.info(f"Saving columns: {columns_to_save}")
                                    os.makedirs(output_dir, exist_ok=True)
                                    df_to_save = df_current_date[columns_to_save].copy()
                                    df_to_save.to_parquet(output_file, index=False)
                                    save_status_msg = f"History for {current_date_str} saved at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')} with columns {columns_to_save}"
                                    logger.info(f"Saved daily Parquet file: {output_file} with {len(df_to_save)} records")
                                    last_save_date = current_date
                                    last_save_status[current_date_str] = save_status_msg
                                except Exception as e:
                                    save_status_msg = f"Failed to save history for {current_date_str}: {str(e)}"
                                    logger.error(f"Error saving daily Parquet file {output_file}: {str(e)}")
                            else:
                                save_status_msg = f"No tasks with start_date {current_date_str} to save."
                                logger.info(save_status_msg)
                                last_save_date = current_date
                                last_save_status[current_date_str] = save_status_msg
                        else:
                            save_status_msg = f"Failed to read {PARQUET_FILE}: DataFrame is None"
                            logger.error(save_status_msg)
                    else:
                        save_status_msg = f"Parquet file {PARQUET_FILE} does not exist"
                        logger.error(save_status_msg)
                else:
                    save_status_msg = f"History for {current_date_str} already processed (last_save_date={last_save_date})"
                    logger.info(save_status_msg)
            else:
                next_save_time = datetime(current_time.year, current_time.month, current_time.day, DATA_SAVE_HOUR, DATA_SAVE_MINUTE, tzinfo=ist)
                if current_time >= next_save_time:
                    next_save_time += timedelta(days=1)
                save_status_msg = f"History for {current_date_str} will be checked at {next_save_time.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                logger.info(save_status_msg)
            
            if current_time.hour >= DATA_SAVE_HOUR and current_time.minute >= DATA_SAVE_MINUTE:
                if last_purge_date is None or last_purge_date != current_date:
                    purge_old_alerts()
                    last_purge_date = current_date
                else:
                    logger.info(f"Skipping purge: Already purged for {current_date}")
            
            if os.path.exists(PARQUET_FILE):
                current_modified = get_file_mtime(PARQUET_FILE)
                
                if current_modified is None:
                    logger.error(f"Could not get modification time for {PARQUET_FILE}")
                elif last_modified == 0 or abs(current_modified - last_modified) > FILE_MOD_THRESHOLD_SECONDS:
                    for attempt in range(PARQUET_READ_RETRIES):
                        df = read_parquet_file(PARQUET_FILE)
                        if df is not None:
                            # Save status summary to qmc_current_executions.parquet
                            save_status_summary(df, STATUS_OUTPUT_FILE)
                            
                            last_update_time = pd.Timestamp.now()
                            clear_console()
                            print(f"[{last_update_time}] Data Refreshed")
                            print(f"History Save Status: {save_status_msg}")
                            filtered_df = df[(df['status'] == 'Running') | (df['run_status'] == 'Issue')]
                            print(f"Tasks with status='Running' or run_status='Issue': {len(filtered_df)}")
                            if not filtered_df.empty:
                                display_columns = ['task_name', 'task_freq', 'start_date', 'start_time', 'status', 'time_taken', 'avg_time', 'avg_tme_fin', 'run_status', 'alert_status']
                                if 'end_date' in filtered_df.columns:
                                    display_columns.insert(4, 'end_date')
                                if 'end_time' in filtered_df.columns:
                                    display_columns.insert(5, 'end_time')
                                filtered_df = filtered_df[display_columns]
                                print(filtered_df.to_string(index=False))
                            else:
                                print("No tasks with status='Running' or run_status='Issue' found.")
                            last_modified = current_modified
                            break
                        else:
                            logger.error(f"Attempt {attempt + 1} failed to read {PARQUET_FILE}")
                            time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        logger.error(f"All {PARQUET_READ_RETRIES} attempts to read {PARQUET_FILE} failed")
                else:
                    clear_console()
                    print(f"[{last_update_time}] Data Refreshed")
                    print(f"History Save Status: {save_status_msg}")
                    if 'df' in locals() and not df.empty:
                        # Save status summary even if file hasn't been modified to ensure real-time updates
                        save_status_summary(df, STATUS_OUTPUT_FILE)
                        
                        filtered_df = df[(df['status'] == 'Running') | (df['run_status'] == 'Issue')]
                        print(f"Tasks with status='Running' or run_status='Issue': {len(filtered_df)}")
                        if not filtered_df.empty:
                            display_columns = ['task_name', 'task_freq', 'start_date', 'start_time', 'status', 'time_taken', 'avg_time', 'avg_tme_fin', 'run_status', 'alert_status']
                            if 'end_date' in filtered_df.columns:
                                display_columns.insert(4, 'end_date')
                            if 'end_time' in filtered_df.columns:
                                display_columns.insert(5, 'end_time')
                            filtered_df = filtered_df[display_columns]
                            print(filtered_df.to_string(index=False))
                        else:
                            print("No tasks with status='Running' or run_status='Issue' found.")
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
            print("Monitoring stopped by user")
            logger.info("Monitoring stopped by user.")
            break
        except Exception as e:
            clear_console()
            logger.error(f"Error in monitoring loop: {str(e)}")
            print(f"Error in monitoring loop: {str(e)}")
            print(f"History Save Status: {save_status_msg}")
            time.sleep(RETRY_DELAY_SECONDS)

if __name__ == "__main__":
    main()