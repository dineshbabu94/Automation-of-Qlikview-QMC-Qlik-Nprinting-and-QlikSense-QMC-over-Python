import os
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime
import pytz
import logging
from IPython.display import clear_output, display

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Clear existing handlers to prevent duplicates
if logger.hasHandlers():
    logger.handlers.clear()

# File handler
log_file = os.getenv("LOG_FILE", r"D:\OPS_Streamline\QlikSense\update_qliksense_executions.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Load environment variables
dotenv_path = r"D:\OPS_Streamline\QlikSense\Variable\qlik_sense_db_creds.env"
if not os.path.exists(dotenv_path):
    logger.error(f"Environment file {dotenv_path} not found")
    raise FileNotFoundError(f"Environment file {dotenv_path} not found")
if not os.access(dotenv_path, os.R_OK):
    logger.error(f"No read permission for {dotenv_path}")
    raise PermissionError(f"No read permission for {dotenv_path}")

load_dotenv(dotenv_path=dotenv_path)

# Validate required environment variables
REQUIRED_VARS = [
    "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
    "OUTPUT_DIR", "EXECUTION_START_HOUR", "EXECUTION_END_HOUR", "LOG_FILE"
]
for var in REQUIRED_VARS:
    if not os.getenv(var):
        logger.error(f"Missing required environment variable: {var}")
        raise ValueError(f"Missing required environment variable: {var}")

# Configuration from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
EXECUTION_START_HOUR = int(os.getenv("EXECUTION_START_HOUR"))
EXECUTION_END_HOUR = int(os.getenv("EXECUTION_END_HOUR"))
LOG_FILE = os.getenv("LOG_FILE")

# Log environment variables (mask password)
logger.info(f"Environment variables loaded from {dotenv_path}:")
for var in REQUIRED_VARS:
    if var == "DB_PASSWORD":
        logger.info(f"{var}=[MASKED]")
    else:
        logger.info(f"{var}={os.getenv(var)}")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)
if not os.access(OUTPUT_DIR, os.W_OK):
    logger.error(f"No write permission for {OUTPUT_DIR}")
    raise PermissionError(f"No write permission for {OUTPUT_DIR}")

def execute_postgresql_query(query):
    """
    Connects to PostgreSQL using SQLAlchemy and returns query results as a DataFrame.
    """
    db_connection_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = None
    try:
        engine = create_engine(db_connection_str)
        with engine.connect() as connection:
            # Execute SET TIME ZONE using text construct
            connection.execute(text("SET TIME ZONE 'Asia/Kolkata'"))
            connection.commit()  # Ensure the session setting is applied
        logger.info("Successfully connected to PostgreSQL database and set time zone")
        df = pd.read_sql(query, engine)
        df.columns = [col.lower() for col in df.columns]
        df = df.rename(columns={
            'appname': 'task_name',
            'starttimeist': 'n_start_datetime',
            'stoptimeist': 'n_completed_on',
            'status': 'execution_status',
            'time_taken': 'duration_hms'
        })
        df['execution_start_date'] = pd.to_datetime(df['n_start_datetime']).dt.date
        df = df[['task_name', 'execution_start_date', 'n_start_datetime', 'n_completed_on', 'execution_status', 'duration_hms']]
        logger.info(f"Query executed successfully, retrieved {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error during database operation: {str(e)}")
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose()
            logger.info("Database connection closed")

def process_parquet_file():
    """
    Reads the Parquet file, filters out Running records and records with null or invalid n_completed_on,
    appends new data from the query, deduplicates, and overwrites the file.
    Displays fresh query results and the number of records in the console.
    """
    try:
        ist = pytz.timezone('Asia/Kolkata')
        current_date = datetime.now(ist)
        month_year_str = current_date.strftime("%m_%Y")
        file_name = f"qliksense_executions_{month_year_str}.parquet"
        output_file_path = os.path.join(OUTPUT_DIR, file_name)

        current_hour = current_date.hour
        if not (EXECUTION_START_HOUR <= current_hour <= EXECUTION_END_HOUR):
            logger.info(f"Outside execution hours ({EXECUTION_START_HOUR}-{EXECUTION_END_HOUR}), skipping processing")
            return

        # Read existing Parquet file
        if os.path.exists(output_file_path):
            try:
                df_existing = pd.read_parquet(output_file_path)
                logger.info(f"Read {len(df_existing)} records from {output_file_path}")
            except Exception as e:
                logger.error(f"Error reading {output_file_path}: {str(e)}")
                df_existing = pd.DataFrame()
        else:
            logger.info(f"Parquet file {output_file_path} does not exist, starting fresh")
            df_existing = pd.DataFrame()

        # Filter out Running records and records with null or invalid n_completed_on
        if not df_existing.empty:
            df_existing['n_completed_on'] = pd.to_datetime(df_existing['n_completed_on'], errors='coerce')
            df_existing = df_existing[
                (df_existing['execution_status'] != 'Running') &
                (df_existing['n_completed_on'].notnull()) &
                (~df_existing['n_completed_on'].isna())
            ].copy()
            logger.info(f"Filtered out Running records and records with null/invalid n_completed_on, {len(df_existing)} records remain")

        # Execute the query for the current month
        query = """
        with data as (SELECT
            a."Name" AS appname,
            (er."StartTime" + INTERVAL '5 hours 30 minutes') AS starttimeist,
            CASE er."Status"
                WHEN 2 THEN NULL
                ELSE (er."StopTime" + INTERVAL '5 hours 30 minutes')
            END AS stoptimeist,
            CASE er."Status"
                WHEN 0 THEN 'Idle'
                WHEN 2 THEN 'Running'
                WHEN 7 THEN 'Completed'
                WHEN 8 THEN 'Failed'
                WHEN 6 THEN 'Aborted'
                WHEN 9 THEN 'Skipped'
                WHEN 12 THEN 'Queued'
                ELSE 'Unknown Status'
            END AS status,
            CASE er."Status"
                WHEN 2 THEN NULL
                ELSE TO_CHAR((er."StopTime" - er."StartTime"), 'HH24:MI:SS')
            END AS time_taken
        FROM
            "public"."ExecutionResults" er
        JOIN
            "public"."Apps" a ON er."AppID" = a."ID"
        WHERE
            er."StartTime" >= DATE_TRUNC('month', CURRENT_DATE) 
    AND er."StartTime" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' 
            AND er."Status" <> 9
        ORDER BY
            er."StartTime" desc
 ) select * from data where status <> 'Unknown Status';
        """
        df_new = execute_postgresql_query(query)
        if df_new.empty:
            logger.info("No new data from query")
            print("No new query results to display")
            print(f"Number of records retrieved: 0")
        else:
            logger.info(f"Retrieved {len(df_new)} new records from query")
            # Display fresh query results and record count
            print(f"\nFresh Query Results at {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')}:")
            print(f"Number of records retrieved: {len(df_new)}\n")
            display(df_new)  # Use IPython display for clean DataFrame output

        # Combine existing and new data
        if not df_existing.empty or not df_new.empty:
            expected_columns = ['task_name', 'execution_start_date', 'n_start_datetime', 'n_completed_on', 'execution_status', 'duration_hms']
            if not df_new.empty:
                if not all(col in df_new.columns for col in expected_columns):
                    logger.error(f"New data missing required columns: {expected_columns}")
                    return
                df_new = df_new[expected_columns].copy()
            if not df_existing.empty:
                df_existing = df_existing[expected_columns].copy()

            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates()
            logger.info(f"After deduplication, {len(df_combined)} unique records remain")

            try:
                df_combined.to_parquet(output_file_path, index=False)
                logger.info(f"Overwrote {output_file_path} with {len(df_combined)} records")
            except Exception as e:
                logger.error(f"Error saving to {output_file_path}: {str(e)}")
        else:
            logger.info("No data to process, Parquet file not modified")

    except Exception as e:
        logger.error(f"Error in process_parquet_file: {str(e)}")

def main():
    """
    Main loop to process the Parquet file every 30 seconds.
    """
    logger.info("Starting update_qliksense_executions script")
    while True:
        try:
            clear_output(wait=True)
            logger.info(f"Starting new iteration at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
            process_parquet_file()
            logger.info("Sleeping for 30 seconds")
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("Script stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            time.sleep(30)

if __name__ == "__main__":
    main()