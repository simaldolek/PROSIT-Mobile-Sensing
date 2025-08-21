# Script to create a timezone lookup table for timezone based on the timezone_id.
# Idea is to create a table with participantid, timezone_id, timezone_name, start_time, end_time.
import os
import pandas as pd
import logging  # Import the logging module
from helper.logging_setup import setup_logging
from helper.timing_decorator import timing_decorator

from helper.multiprocessing_handler_new import run_multiprocessing
from helper.postgresql_hook import PostgreSQLHook

# config parameters
config_file="config.ini"    # name of the config file in the config folder
max_processes = 10
batch_size = 1000
upload_batch_size = 100
lookup_table = "lookup_timezone"    # The lookup table we are going to create
# dict of columns names with their data types
lookup_table_columns = {"participantid": "TEXT", "timezone_id": "TEXT", "start_time": "TIMESTAMP", "end_time": "TIMESTAMP"}
primary_key_columns = {
    "participantid": "TEXT",
    "timezone_id": "TEXT",
    "start_time": "TIMESTAMP",
    "end_time": "TIMESTAMP"
}


def create_lookup_table(postgres_hook, schema, table_name, table_columns):
    """Create the lookup table if it doesn't already exist."""
    if not postgres_hook.table_exists(schema, table_name):
        postgres_hook.create_table(schema, table_name, table_columns, primary_key_columns)
        logging.info(f"Table {table_name} created in schema {schema}.")
    else:
        logging.info(f"Table {table_name} already exists in schema {schema}.")


def process_timezone(participantid, *args, **kwargs):
    print("Processing timezone for participant:", participantid)
    schema = kwargs.get('schema')

    db = PostgreSQLHook(config_file, section='DATABASE_PSQL')

    # fetch all the location data for the participant
    query = f"SELECT participantid, measuredat, timezone_id FROM {kwargs['schema']}.location WHERE participantid = %s and timezone_id != 'Unknown_Timezone'"

    location_data = db.fetch_all_data(query, (participantid,))
    
    if not location_data:
        logging.info(f"No location data found for participant {participantid}.")
        return

    # Create a DataFrame from the fetched data
    df = pd.DataFrame(location_data, columns=['participantid', 'measuredat', 'timezone_id'])

    # Convert 'measuredat' column from UNIX timestamp (bigint) to datetime
    df['measuredat'] = pd.to_datetime(df['measuredat'], unit='s')

    # Sort the data by timestamp
    df.sort_values(by='measuredat', inplace=True)

    # Create a column to track when a timezone change happens (contiguous runs)
    df['timezone_change'] = (df['timezone_id'] != df['timezone_id'].shift()).cumsum()

    # Collapse contiguous runs to segments with start_time only
    segments = df.groupby(['participantid', 'timezone_change', 'timezone_id']).agg(
        start_time=('measuredat', 'min')
    ).reset_index()

    # For each participant, order by start_time and set end_time = next start_time; last gets sentinel
    segments.sort_values(by=['participantid', 'start_time'], inplace=True)
    segments['end_time'] = segments.groupby('participantid')['start_time'].shift(-1)

    # Fill last segment end_time with far-future sentinel to ensure coverage
    sentinel = pd.Timestamp('2100-01-01 00:00:00')
    segments['end_time'] = segments['end_time'].fillna(sentinel)

    # Convert timestamps to strings for DB insertion
    segments['start_time'] = segments['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    segments['end_time'] = segments['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Select relevant columns for the lookup table
    lookup_data = segments[['participantid', 'timezone_id', 'start_time', 'end_time']]

    table_name = f"{schema}.{lookup_table}"
    columns = lookup_table_columns.keys() 
    # Convert the DataFrame to a list of tuples
    lookup_data = lookup_data.to_records(index=False).tolist()

    db.insert_data(table_name, columns, primary_key_columns, lookup_data, batch_size=upload_batch_size)
    logging.info(f"Participant {participantid} timezone data processed.")

    return
    

@timing_decorator
def main():
    # get an object of the Database class
    db = PostgreSQLHook(config_file, section='DATABASE_PSQL')

    # fetch schemas
    query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name like 'study%'"
    # schemas = db.fetch_data(query)
    # schemas = [schema[0] for schema in schemas]
    schemas = ["study_test"]
    # dict of columns names with their data types
    lookup_table_columns = {"participantid": "TEXT", "timezone_id": "TEXT", "start_time": "TIMESTAMP", "end_time": "TIMESTAMP"}


    for schema in schemas:
        print(f"Processing schema: {schema}")

        if not db.table_exists(schema, lookup_table):
            logging.info(f"Table {lookup_table} does not exist in schema {schema}. Creating table.")
            create_lookup_table(db, schema, lookup_table, lookup_table_columns)
        else:
            logging.info(f"Table {lookup_table} already exists in schema {schema}.")
        
        # fetch distinct participant ids
        query = f"SELECT DISTINCT participantid FROM {schema}.location"
        participantids = db.fetch_all_data(query)
        participantids = [pid[0] for pid in participantids]

        num_threads = min(max_processes, os.cpu_count(), len(participantids))
        if len(participantids) == 0:
            print(f"No participants to process in schema {schema}.")
            logging.info(f"No participants to process in schema {schema}.")
            continue
        print(f"Processing {len(participantids)} participants with {num_threads} threads.")
        # Call the run_multiprocessing function with the correct parameters
        
         # Define the additional parameters for the processing function
        process_kwargs = {
            'schema': schema,
            'batch_size': 100,
            'upload_batch_size': 50

        }
        pool_size = num_threads

        # Run the feneralized multiprocessing function to compute time zone
        run_multiprocessing(participantids, process_timezone, pool_size, **process_kwargs)

        logging.info(f"Processing schema {schema} completed.")
        
    
if __name__ == "__main__":
    setup_logging(log_filename="timezone_lookup_creation.log")
    main()


# to run the script
# python3 W_timezone_lookup_creation.py