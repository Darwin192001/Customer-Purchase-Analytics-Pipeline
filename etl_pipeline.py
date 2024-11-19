import os
import pandas as pd
import logging
from datetime import datetime
import snowflake.connector

# Logging setup
LOG_FILE = "etl_pipeline.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    'user': 'darwin',
    'password': '*1Darwin*1',
    'account': 'raphmmj-jl14137',
    'warehouse': 'timesheet',
    'database': 'Mpg',
    'schema': 'time'
}

# Snowflake connection
def connect_to_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

# Extract function
def extract(file_path):
    try:
        logging.info(f"Extracting data from {file_path}")
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

# Transform function
def transform(dataframe):
    try:
        logging.info("Transforming data")
        dataframe['transaction_date'] = pd.to_datetime(dataframe['transaction_date'])
        dataframe['amount'] = dataframe['amount'].astype(float)
        return dataframe
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

# Load to Snowflake stage
def load_to_stage(dataframe, stage_name):
    try:
        logging.info(f"Loading data to Snowflake stage: {stage_name}")
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        # Save dataframe as a CSV file for staging
        stage_file = "stage_file.csv"
        dataframe.to_csv(stage_file, index=False, header=True)

        # Upload CSV file to Snowflake stage
        cursor.execute(f"PUT file://{os.path.abspath(stage_file)} @{stage_name}")
        conn.close()
        logging.info("Data successfully loaded to stage")
        os.remove(stage_file)  # Clean up local file
    except Exception as e:
        logging.error(f"Error loading to stage: {e}")
        raise

# Load from stage to Snowflake table
def load_from_stage_to_table(stage_name, table_name):
    try:
        logging.info(f"Loading data from stage {stage_name} to table {table_name}")
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        # Copy data from stage to table
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE';
        """)
        conn.close()
        logging.info(f"Data successfully loaded into table {table_name}")
    except Exception as e:
        logging.error(f"Error loading data from stage to table: {e}")
        raise

# Trigger mechanism: Automatically detect new rows
def monitor_and_trigger(file_path, stage_name, table_name):
    try:
        # Check if a trigger file exists
        trigger_file = "trigger_checkpoint.txt"
        last_processed = 0

        if os.path.exists(trigger_file):
            with open(trigger_file, "r") as f:
                last_processed = int(f.read().strip())

        # Load the file and find new rows
        data = pd.read_csv(file_path)
        new_data = data.iloc[last_processed:]

        if not new_data.empty:
            logging.info(f"New rows detected: {len(new_data)}")
            transformed_data = transform(new_data)
            load_to_stage(transformed_data, stage_name)
            load_from_stage_to_table(stage_name, table_name)

            # Update the checkpoint
            with open(trigger_file, "w") as f:
                f.write(str(len(data)))

        else:
            logging.info("No new rows detected. Skipping...")
    except Exception as e:
        logging.error(f"Error in monitoring and triggering: {e}")
        raise

# Main ETL Pipeline
def etl_pipeline(file_path, stage_name, table_name):
    try:
        logging.info("Starting ETL pipeline")
        monitor_and_trigger(file_path, stage_name, table_name)
        logging.info("ETL pipeline completed successfully")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

# Run the ETL pipeline
if __name__ == "__main__":
    # Define file paths and Snowflake objects
    FILE_PATH = "data/customers.csv"  # Your source data
    STAGE_NAME = "Mpg.time.CUSTOMER_STAGE"
    TABLE_NAME = "Mpg.time.CUSTOMER_PURCHASES"

    etl_pipeline(FILE_PATH, STAGE_NAME, TABLE_NAME)
