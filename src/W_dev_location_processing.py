# important working code
# The script uses the decrypt_data function from the helper/crypt.py file to decrypt the location data.
# The script uses the get_timezone function from the helper/crypt.py file to calculate the timezone based on the latitude and longitude.

import os
import re
from helper.config_loader import get_decryption_key
from helper.location_processing import decrypt_data, get_timezone
from helper.multiprocessing_handler_new import run_multiprocessing
from helper.postgresql_hook import PostgreSQLHook
import logging  # Import the logging module
from helper.logging_setup import setup_logging
from helper.timing_decorator import timing_decorator

# config parameters
config_file="config.ini"    # name of the config file in the config folder
decryption_key = get_decryption_key(config_file, 'key2')
max_processes = 10
batch_size = 1000
upload_batch_size = 5
# column name to be processed:
# location_decrypted ( decrypting location data )
# timezone_id ( computing timezone based on location data )
column_name = "timezone_id"


def _extract_lat_lon(loc_str: str):
    """Extract latitude and longitude from a pipe-delimited string like 'lat|lon|...'.
    Returns (lat, lon) as floats if valid, else (None, None)."""
    if not isinstance(loc_str, str):
        return None, None
    s = loc_str.strip()
    # fast path: split by '|'
    parts = [p.strip() for p in s.split('|') if p is not None]
    if len(parts) >= 2:
        try:
            lat = float(parts[0])
            lon = float(parts[1])
            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                return lat, lon
        except Exception:
            pass
    # fallback regex
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*\|\s*(-?\d+(?:\.\d+)?)", s)
    if m:
        try:
            lat = float(m.group(1)); lon = float(m.group(2))
            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                return lat, lon
        except Exception:
            return None, None
    return None, None


def process_column_data(participant_id, *args, **kwargs):
    """
    Function to process data based on column name using Redis for caching.
    Can handle either decryption or timezone calculation based on the provided column_name.
    """
    # Extract values from kwargs for better readability
    schema = kwargs.get('schema')
    decryption_key = kwargs.get('decryption_key')
    batch_size = kwargs.get('batch_size', 100)
    upload_batch_size = kwargs.get('upload_batch_size', 50)

    if not schema:
        logging.error("Schema is missing.")
        return

    # Assuming PostgreSQLHook is defined elsewhere and properly configured
    db = PostgreSQLHook(config_file, section='DATABASE_PSQL')

    # print the count of below query
    count_query = f"SELECT COUNT(*) FROM {schema}.location WHERE participantid = %s and {column_name} is null"
    count = db.fetch_all_data(count_query, (participant_id,))
    print(count)
    
    if count[0][0] == 0:
        logging.info(f"No data to process for participant {participant_id} in column {column_name}.")
        return
    else:
        print(f"Processing {count[0][0]} rows for participant {participant_id} in column {column_name}.")

    # SQL Queries
    query = f"SELECT * FROM {schema}.location WHERE participantid = %s and {column_name} is null"
    update_query = f"UPDATE {schema}.location SET {column_name} = %s WHERE _id = %s"

    # Fetch data in batches
    data_generator = db.fetch_data_in_batches(query, (participant_id,), batch_size)
    update_data = []

    # Iterate through each batch of data
    for batch in data_generator:
        for row in batch:
            # Extract necessary information from the row
            record_id = row[0]
            processed_value = None

            # check if value0 is null, if yes skip the row
            if row[3] is None or row[3] == 'None' or row[3] == 'none':
                print(f"Skipping row with null value0 for participant {participant_id}")
                continue

            # Process based on the column to be updated
            if column_name == "location_decrypted":
                loc_data = row[4]  # Assuming location data is in column index 4
                # # check if location data is of type b'' and convert to string
                if isinstance(loc_data, bytes):
                    loc_data = loc_data.decode('utf-8')
                # for rows with missing location data, enter 'None' in the location_decrypted column
                if loc_data.lower() == "none":# or len(loc_data) != 280:
                    processed_value = "None"
                    update_data.append((processed_value, record_id))
                    continue
                # cache_key_location = f"location_cache:{loc_data}"

                processed_value = decrypt_data(loc_data, decryption_key)

            elif column_name == "timezone_id":
                loc_val = row[5]
                if loc_val is None or str(loc_val).lower() in {"none", "null", ""}:
                    continue
                if isinstance(loc_val, bytes):
                    try:
                        loc_val = loc_val.decode('utf-8')
                    except Exception:
                        continue
                lat, lon = _extract_lat_lon(loc_val)
                if lat is None or lon is None:
                    # cannot parse coordinates reliably; mark as Unknown_Timezone
                    processed_value = "Unknown_Timezone"
                    update_data.append((processed_value, record_id))
                    continue
                processed_value = get_timezone(lat, lon) or "Unknown_Timezone"

            else:
                logging.error(f"Column name {column_name} not recognized.")
                return
            
            # Collect data for batch update if we have a valid processed value
            # if processed_value is not None:
            update_data.append((processed_value, record_id))
            # print(update_data)

            # Execute batch update if we have reached the upload batch size
            if len(update_data) >= upload_batch_size:
                db.execute_query(update_query, update_data)
                update_data = []

    # Execute any remaining updates
    if update_data:
        print(f"Executing remaining updates for participant {participant_id}")
        db.execute_query(update_query, update_data)
    
    return f"Participant {participant_id} data processed for column {column_name}."


@timing_decorator
def main():
    # get an object of the Database class
    db = PostgreSQLHook(config_file, section='DATABASE_PSQL')

    # fetch schemas
    query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name like 'study%'"
    # schemas = db.fetch_data(query)
    # schemas = [schema[0] for schema in schemas]
    schemas = ["study_test"]
    
    for schema in schemas:
        print(f"Processing schema: {schema}")
        location_table_name = 'location'
        if not db.table_exists(schema, location_table_name):
            logging.info(f"Table {location_table_name} does not exist in schema {schema}. Skipping schema.")
            continue
        
        if not db.column_exists(schema, column_name, 'location'):
            logging.info(f"Adding column {column_name} to table 'location' in schema '{schema}'")
            db.add_column(schema, 'location', column_name, 'text')

        # fetch distinct participant ids
        query = f"SELECT DISTINCT participantid FROM {schema}.location where {column_name} is null and value0 is not null"
        participant_ids = db.fetch_all_data(query)
        participant_ids = [pid[0] for pid in participant_ids]
        # participant_ids = ['prositaiy428']  # for testing

        num_threads = min(max_processes, os.cpu_count(), len(participant_ids))
        if len(participant_ids) == 0:
            print(f"No participants to process in schema {schema}.")
            logging.info(f"No participants to process in schema {schema}.")
            continue
        print(f"Processing {len(participant_ids)} participants with {num_threads} threads.")
        # Call the run_multiprocessing function with the correct parameters
        
         # Define the additional parameters for the processing function
        process_kwargs = {
            'schema': schema,
            'decryption_key': decryption_key,
            'batch_size': 100,
            'upload_batch_size': 100
        }
        pool_size = num_threads

        # Run the feneralized multiprocessing function to compute time zone
        run_multiprocessing(participant_ids, process_column_data, pool_size, **process_kwargs)


        logging.info(f"Processing schema {schema} completed.")
        

if __name__ == "__main__":
    # set up logging
    log_path = setup_logging(log_filename="process_timezones.log")
    print(f"starting the script with log file: {log_path}")
    main()
    print("script finished")


# to run this script use the following command:
'''
python3 W_dev_location_processing.py
'''