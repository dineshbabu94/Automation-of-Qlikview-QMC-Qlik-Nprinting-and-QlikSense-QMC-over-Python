import os
import time
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import subprocess
import platform
from IPython.display import clear_output  # For Jupyter Lab
from dotenv import load_dotenv

# Load environment variables from .env file
ENV_FILE = r"D:\OPS_Streamline\Variable\log_logics.env"
if not os.path.exists(ENV_FILE):
    raise FileNotFoundError(f"Environment file {ENV_FILE} not found")

load_dotenv(ENV_FILE)

# Validate required environment variables
REQUIRED_VARS = [
    "FOLDER_PATH_LOGINDEX", "FOLDER_PATH_RESULTS", "PARQUET_FILE",
    "INDEX_TRACKER_FILE", "RESULTS_TRACKER_FILE",
    "FILE_AGE_THRESHOLD", "RETRY_ATTEMPTS", "RETRY_DELAY", "TRACKER_RESET_INTERVAL"
]
for var in REQUIRED_VARS:
    if not os.getenv(var):
        raise ValueError(f"Missing required environment variable: {var}")

# Configuration from .env
FOLDER_PATH_LOGINDEX = os.getenv("FOLDER_PATH_LOGINDEX")
FOLDER_PATH_RESULTS = os.getenv("FOLDER_PATH_RESULTS")
PARQUET_FILE = os.getenv("PARQUET_FILE")
INDEX_TRACKER_FILE = os.getenv("INDEX_TRACKER_FILE")
RESULTS_TRACKER_FILE = os.getenv("RESULTS_TRACKER_FILE")

FILE_AGE_THRESHOLD = int(os.getenv("FILE_AGE_THRESHOLD"))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY"))
TRACKER_RESET_INTERVAL = int(os.getenv("TRACKER_RESET_INTERVAL"))

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
        print(f"[{datetime.now()}] Error clearing output: {str(e)}")
        print("\n" * 50)  # Fallback

def parse_xml_logindex(file_path):
    """Parse a TaskLogIndex XML file and return the last LogIndex record, LogID, start datetime, and logs."""
    log_messages = []
    for attempt in range(RETRY_ATTEMPTS):
        try:
            if not file_path.startswith(FOLDER_PATH_LOGINDEX):
                log_messages.append(f"[{datetime.now()}] Error: Unexpected file path {file_path} (expected {FOLDER_PATH_LOGINDEX})")
                return None, None, None, log_messages
            
            tree = ET.parse(file_path)
            root = tree.getroot()
            log_entries = root.findall(".//LogIndex")
            if log_entries:
                last_entry = log_entries[-1]
                task_name = last_entry.get("TaskName")
                start_date = last_entry.get("Date")  # YYYY-MM-DD
                start_time = last_entry.get("Time")  # HH:MM:SS
                log_id = last_entry.get("LogID")
                if task_name and start_date and start_time and log_id:
                    if task_name.startswith("BBTT/"):
                        task_name = task_name[len("BBTT/"):]
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    if start_date == current_date:
                        try:
                            start_datetime = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S")
                            return {
                                "task_name": task_name,
                                "start_date": start_date,
                                "start_time": start_time
                            }, log_id, start_datetime, log_messages
                        except ValueError as e:
                            log_messages.append(f"[{datetime.now()}] Error parsing start datetime in {file_path}: {str(e)}")
                            return None, None, None, log_messages
                    return None, None, None, log_messages
                return None, None, None, log_messages
            return None, None, None, log_messages
        except (ET.ParseError, IOError) as e:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
                continue
            log_messages.append(f"[{datetime.now()}] Error parsing XML file {file_path} after {RETRY_ATTEMPTS} attempts: {str(e)}")
            return None, None, None, log_messages
        except Exception as e:
            log_messages.append(f"[{datetime.now()}] Error processing file {file_path}: {str(e)}")
            return None, None, None, log_messages

def parse_xml_taskresult(file_path):
    """Parse a TaskResult XML file and return status, end_date, end_time, RunID, start datetime, and logs."""
    log_messages = []
    for attempt in range(RETRY_ATTEMPTS):
        try:
            if not file_path.startswith(FOLDER_PATH_RESULTS):
                log_messages.append(f"[{datetime.now()}] Error: Unexpected file path {file_path} (expected {FOLDER_PATH_RESULTS})")
                return None, None, None, log_messages
            
            tree = ET.parse(file_path)
            root = tree.getroot()
            run_id = root.get("RunID")
            started_at = root.get("StartedAt")  # e.g., "4/23/2025 3:57:43 PM"
            finished_at = root.get("FinishedAt")  # e.g., "4/23/2025 4:04:44 PM"
            task_manually_aborted = root.get("TaskManuallyAborted") == "True"
            internal_error = root.get("InternalError") == "True"
            
            # Initialize status
            status = None
            for log_entry in root.findall(".//LogFinalEntry"):
                if log_entry.get("Text").startswith("TaskResult.status="):
                    status = log_entry.get("Text").split("=")[1]
                    break
            
            # Check for failure indicators in LogFinalEntries
            has_failure = False
            for log_entry in root.findall(".//LogFinalEntry"):
                if log_entry.get("Type") == "Error" and (
                    "failed" in log_entry.get("Text").lower() or
                    "TaskFailedException" in log_entry.get("Text")
                ):
                    has_failure = True
                    break
            
            # Determine final status
            final_status = None
            if task_manually_aborted:
                final_status = "Aborted"
            elif internal_error or has_failure:
                final_status = "Failed"
            elif status:
                final_status = status
            else:
                log_messages.append(f"[{datetime.now()}] No valid status found in {file_path}")
                return None, None, None, log_messages
            
            if started_at and finished_at and run_id:
                try:
                    # Parse StartedAt (M/DD/YYYY h:mm:ss AM/PM)
                    start_datetime = datetime.strptime(started_at, "%m/%d/%Y %I:%M:%S %p")
                    # Parse FinishedAt
                    dt = datetime.strptime(finished_at, "%m/%d/%Y %I:%M:%S %p")
                    end_date = dt.strftime("%Y-%m-%d")
                    end_time = dt.strftime("%H:%M:%S")
                    
                    return {
                        "status": final_status,
                        "end_date": end_date,
                        "end_time": end_time
                    }, run_id, start_datetime, log_messages
                except ValueError as e:
                    log_messages.append(f"[{datetime.now()}] Error parsing datetimes in {file_path}: {str(e)}")
                    return None, None, None, log_messages
            return None, None, None, log_messages
        except (ET.ParseError, IOError) as e:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
                continue
            log_messages.append(f"[{datetime.now()}] Error parsing XML file {file_path} after {RETRY_ATTEMPTS} attempts: {str(e)}")
            return None, None, None, log_messages
        except Exception as e:
            log_messages.append(f"[{datetime.now()}] Error processing file {file_path}: {str(e)}")
            return None, None, None, log_messages

def update_file_tracker(folder_path, tracker_file, processed_files, prefix="TaskLogIndex_"):
    """Update the file tracker Parquet with filenames and modified times for a given folder."""
    log_messages = []
    try:
        current_files = []
        for file_name in os.listdir(folder_path):
            if file_name.startswith(prefix) and file_name.endswith(".xml"):
                file_path = os.path.join(folder_path, file_name)
                try:
                    mtime = os.path.getmtime(file_path)
                    current_files.append({"filename": file_name, "modified_time": mtime})
                except Exception as e:
                    log_messages.append(f"[{datetime.now()}] Error accessing {file_name} for tracker: {str(e)}")
        
        if current_files:
            df = pd.DataFrame(current_files)
            schema = pa.schema([
                ("filename", pa.string()),
                ("modified_time", pa.float64())
            ])
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            pq.write_table(table, tracker_file)
            processed_files.update(df["filename"].tolist())
    except Exception as e:
        log_messages.append(f"[{datetime.now()}] Error updating file tracker {tracker_file}: {str(e)}")
    return log_messages

def process_folder(logindex_path, results_path, index_tracker_file, results_tracker_file, processed_files):
    """Process new or modified XML files from both folders and return records and logs."""
    records = []
    log_messages = []
    current_time = time.time()
    
    try:
        # Get TaskLogIndex files
        index_files = {}
        for file_name in os.listdir(logindex_path):
            if file_name.startswith("TaskLogIndex_") and file_name.endswith(".xml"):
                file_path = os.path.join(logindex_path, file_name)
                try:
                    mtime = os.path.getmtime(file_path)
                    if current_time - mtime > FILE_AGE_THRESHOLD:
                        index_files[file_name] = mtime
                    else:
                        log_messages.append(f"[{datetime.now()}] Skipping {file_name}: File too new (mtime: {mtime}, current: {current_time})")
                except Exception as e:
                    log_messages.append(f"[{datetime.now()}] Error accessing {file_name}: {str(e)}")
        
        # Get TaskResult files
        result_files = {}
        for file_name in os.listdir(results_path):
            if file_name.startswith("TaskResult_") and file_name.endswith(".xml"):
                file_path = os.path.join(results_path, file_name)
                try:
                    mtime = os.path.getmtime(file_path)
                    if current_time - mtime > FILE_AGE_THRESHOLD:
                        result_files[file_name] = mtime
                    else:
                        log_messages.append(f"[{datetime.now()}] Skipping {file_name}: File too new (mtime: {mtime}, current: {current_time})")
                except Exception as e:
                    log_messages.append(f"[{datetime.now()}] Error accessing {file_name}: {str(e)}")
        
        # Read tracker files
        index_tracker = {}
        if os.path.exists(index_tracker_file):
            try:
                df_tracker = pd.read_parquet(index_tracker_file)
                index_tracker = dict(zip(df_tracker["filename"], df_tracker["modified_time"]))
            except Exception as e:
                log_messages.append(f"[{datetime.now()}] Error reading index tracker {index_tracker_file}: {str(e)}")
        
        results_tracker = {}
        if os.path.exists(results_tracker_file):
            try:
                df_tracker = pd.read_parquet(results_tracker_file)
                results_tracker = dict(zip(df_tracker["filename"], df_tracker["modified_time"]))
            except Exception as e:
                log_messages.append(f"[{datetime.now()}] Error reading results tracker {results_tracker_file}: {str(e)}")
        
        # Determine files to process
        files_to_process = set()
        for index_file_name in index_files:
            # Skip if already processed in this reset cycle and no TaskResult update
            if index_file_name in processed_files:
                identifier = index_file_name.replace("TaskLogIndex_", "").replace(".xml", "")
                result_file_name = f"TaskResult_{identifier}.xml"
                if result_file_name in result_files and (result_file_name not in results_tracker or abs(result_files[result_file_name] - results_tracker.get(result_file_name, 0)) > 1):
                    files_to_process.add(index_file_name)
                    log_messages.append(f"[{datetime.now()}] Reprocessing {index_file_name} due to updated {result_file_name}")
                continue
            # Add new or modified TaskLogIndex files
            if index_file_name not in index_tracker or abs(index_files[index_file_name] - index_tracker.get(index_file_name, 0)) > 1:
                files_to_process.add(index_file_name)
            # Add TaskLogIndex files with corresponding TaskResult files
            identifier = index_file_name.replace("TaskLogIndex_", "").replace(".xml", "")
            result_file_name = f"TaskResult_{identifier}.xml"
            if result_file_name in result_files:
                files_to_process.add(index_file_name)
        
        # Process TaskLogIndex files
        for file_name in files_to_process:
            file_path = os.path.join(logindex_path, file_name)
            record, log_id, logindex_start_datetime, file_logs = parse_xml_logindex(file_path)
            log_messages.extend(file_logs)
            if record and (log_id or logindex_start_datetime):
                # Extract the common identifier
                identifier = file_name.replace("TaskLogIndex_", "").replace(".xml", "")
                result_file_name = f"TaskResult_{identifier}.xml"
                result_file_path = os.path.join(results_path, result_file_name)
                
                if os.path.exists(result_file_path):
                    result_record, run_id, result_start_datetime, result_logs = parse_xml_taskresult(result_file_path)
                    log_messages.extend(result_logs)
                    if result_record and ((log_id and run_id and log_id == run_id) or 
                                         (logindex_start_datetime and result_start_datetime and logindex_start_datetime == result_start_datetime)):
                        # Update record if either LogID/RunID or start datetimes match
                        record.update(result_record)
                    else:
                        # Log why the match was skipped
                        reason = []
                        if not result_record:
                            reason.append("Invalid result record")
                        if log_id and run_id and log_id != run_id:
                            reason.append(f"LogID/RunID mismatch (LogID: {log_id}, RunID: {run_id})")
                        if logindex_start_datetime and result_start_datetime and logindex_start_datetime != result_start_datetime:
                            reason.append(f"Start datetime mismatch (LogIndex: {logindex_start_datetime}, TaskResult: {result_start_datetime})")
                        log_messages.append(f"[{datetime.now()}] Skipped matching {file_name} with {result_file_name}: {', '.join(reason)}")
                        record.update({
                            "status": "Running",
                            "end_date": None,
                            "end_time": None
                        })
                else:
                    log_messages.append(f"[{datetime.now()}] No TaskResult file found for {file_name}: {result_file_name}")
                    record.update({
                        "status": "Running",
                        "end_date": None,
                        "end_time": None
                    })
                records.append(record)
        
        # Update both trackers
        log_messages.extend(update_file_tracker(logindex_path, index_tracker_file, processed_files, prefix="TaskLogIndex_"))
        log_messages.extend(update_file_tracker(results_path, results_tracker_file, processed_files, prefix="TaskResult_"))
    except Exception as e:
        log_messages.append(f"[{datetime.now()}] Error processing folder: {str(e)}")
    
    return records, log_messages

def save_to_parquet(records, parquet_file, append=False):
    """Save records to Parquet, updating with the latest record for duplicates, and handle same-task running conflicts."""
    log_messages = []
    if not records:
        return log_messages
    
    try:
        df_new = pd.DataFrame(records)
        if df_new.empty:
            return log_messages
        
        # Define unique key for deduplication
        key_columns = ["task_name", "start_date", "start_time"]
        # Add a status priority for determining "latest" record
        status_priority = {"Aborted": 3, "Failed": 2, "Finished": 2, "Running": 1}
        df_new["status_priority"] = df_new["status"].map(status_priority).fillna(0)
        # Convert end_time to timestamp for comparison (handle None)
        df_new["end_timestamp"] = pd.to_datetime(df_new["end_date"] + " " + df_new["end_time"], errors="coerce")
        
        if append and os.path.exists(parquet_file):
            # Read existing Parquet file
            df_existing = pd.read_parquet(parquet_file)
            df_existing["status_priority"] = df_existing["status"].map(status_priority).fillna(0)
            df_existing["end_timestamp"] = pd.to_datetime(df_existing["end_date"] + " " + df_existing["end_time"], errors="coerce")
            df_existing["key"] = df_existing[key_columns].apply(lambda x: "|".join(x.astype(str)), axis=1)
            
            # Create key for new records
            df_new["key"] = df_new[key_columns].apply(lambda x: "|".join(x.astype(str)), axis=1)
            
            # Check for new runs that should abort existing "Running" tasks with the same task_name
            for _, new_row in df_new.iterrows():
                if new_row["status"] == "Running":
                    # Find existing "Running" records for the same task_name
                    running_mask = (
                        (df_existing["task_name"] == new_row["task_name"]) &
                        (df_existing["status"] == "Running") &
                        (df_existing["key"] != new_row["key"])  # Exclude the current record if already in df_existing
                    )
                    if running_mask.any():
                        # Update existing "Running" records to "Aborted"
                        df_existing.loc[running_mask, "status"] = "Aborted"
                        df_existing.loc[running_mask, "end_date"] = new_row["start_date"]
                        df_existing.loc[running_mask, "end_time"] = new_row["start_time"]
                        df_existing.loc[running_mask, "status_priority"] = status_priority["Aborted"]
                        df_existing.loc[running_mask, "end_timestamp"] = pd.to_datetime(
                            new_row["start_date"] + " " + new_row["start_time"], errors="coerce"
                        )
                        log_messages.append(
                            f"[{datetime.now()}] Aborted older runs for {new_row['task_name']} started at "
                            f"{new_row['start_date']} {new_row['start_time']}"
                        )
            
            # Combine existing and new records
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Sort by key, status_priority (descending), and end_timestamp (descending) to keep the latest
            df_combined = df_combined.sort_values(
                by=["key", "status_priority", "end_timestamp"],
                ascending=[True, False, False]
            )
            # Keep only the first record for each key (the latest)
            df_combined = df_combined.drop_duplicates(subset=["key"], keep="first")
            
            # Remove temporary columns
            df_combined = df_combined.drop(columns=["key", "status_priority", "end_timestamp"])
            
            # Write back to Parquet
            table = pa.Table.from_pandas(df_combined, schema=pa.schema([
                ("task_name", pa.string()),
                ("start_date", pa.string()),
                ("start_time", pa.string()),
                ("status", pa.string()),
                ("end_date", pa.string()),
                ("end_time", pa.string())
            ]), preserve_index=False)
            pq.write_table(table, parquet_file)
            
            log_messages.append(f"[{datetime.now()}] Processed {len(records)} records, saved {len(df_combined)} unique records to {parquet_file}")
        else:
            # Initial run: write new records directly
            df_new = df_new.drop(columns=["status_priority", "end_timestamp"])
            table = pa.Table.from_pandas(df_new, schema=pa.schema([
                ("task_name", pa.string()),
                ("start_date", pa.string()),
                ("start_time", pa.string()),
                ("status", pa.string()),
                ("end_date", pa.string()),
                ("end_time", pa.string())
            ]), preserve_index=False)
            pq.write_table(table, parquet_file)
            log_messages.append(f"[{datetime.now()}] Initial save: Saved {len(df_new)} records to {parquet_file}")
    except Exception as e:
        log_messages.append(f"[{datetime.now()}] Error writing to Parquet file: {str(e)}")
    return log_messages

def main():
    processed_files = set()
    last_reset_time = time.time()
    
    # Initial processing
    try:
        clear_console()
        if os.path.exists(INDEX_TRACKER_FILE):
            os.remove(INDEX_TRACKER_FILE)
        if os.path.exists(RESULTS_TRACKER_FILE):
            os.remove(RESULTS_TRACKER_FILE)
        records, log_messages = process_folder(FOLDER_PATH_LOGINDEX, FOLDER_PATH_RESULTS, INDEX_TRACKER_FILE, RESULTS_TRACKER_FILE, processed_files)
        log_messages.extend(save_to_parquet(records, PARQUET_FILE, append=False))
        
        initial_logs = []
        if records:
            for i, record in enumerate(records[:10]):
                initial_logs.append(f"  Record {i+1}: {record}")
            if len(records) > 10:
                initial_logs.append(f"  ... ({len(records) - 10} more records)")
            initial_logs.append(f"[{datetime.now()}] Initial processing: Processed {len(records)} records from {len(processed_files)} files. Saved to {PARQUET_FILE}")
        else:
            initial_logs.append(f"[{datetime.now()}] Initial processing: No valid records found.")
        initial_logs.extend(log_messages)
        
        clear_console()
        for log in reversed(initial_logs):
            print(log)
    except Exception as e:
        clear_console()
        print(f"[{datetime.now()}] Error in initial processing: {str(e)}")
        return
    
    # Continuous monitoring
    while True:
        try:
            clear_console()
            # Periodic tracker reset
            current_time = time.time()
            if current_time - last_reset_time > TRACKER_RESET_INTERVAL:
                if os.path.exists(INDEX_TRACKER_FILE):
                    os.remove(INDEX_TRACKER_FILE)
                if os.path.exists(RESULTS_TRACKER_FILE):
                    os.remove(RESULTS_TRACKER_FILE)
                processed_files.clear()
                log_messages.append(f"[{datetime.now()}] Reset trackers and processed_files set")
                last_reset_time = current_time
            
            new_records, log_messages = process_folder(FOLDER_PATH_LOGINDEX, FOLDER_PATH_RESULTS, INDEX_TRACKER_FILE, RESULTS_TRACKER_FILE, processed_files)
            log_messages.extend(save_to_parquet(new_records, PARQUET_FILE, append=True))
            
            refresh_logs = []
            if new_records:
                for i, record in enumerate(new_records[:10]):
                    refresh_logs.append(f"  Record {i+1}: {record}")
                if len(new_records) > 10:
                    refresh_logs.append(f"  ... ({len(new_records) - 10} more records)")
                refresh_logs.append(f"[{datetime.now()}] Refreshed: Processed {len(new_records)} new records from {len(processed_files)} total files.")
            else:
                refresh_logs.append(f"[{datetime.now()}] Refreshed: No new records found.")
            refresh_logs.extend(log_messages)
            
            for log in reversed(refresh_logs):
                print(log)
            
            time.sleep(10)
        except KeyboardInterrupt:
            clear_console()
            print(f"[{datetime.now()}] Monitoring stopped by user.")
            break
        except Exception as e:
            clear_console()
            print(f"[{datetime.now()}] Error in monitoring loop: {str(e)}")
            time.sleep(10)

if __name__ == "__main__":
    main()