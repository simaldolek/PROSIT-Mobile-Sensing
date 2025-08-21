"""
PySpark script to process iOS screen events only.
This script uses the pyspark_utilities module for reusable components.
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import os
from datetime import datetime

# Add the src directory to the path to import utilities
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from helper.pyspark_utilities import *

# -----------------------
# Configuration
# -----------------------
INPUT_SCHEMA = "study_test"
OUTPUT_SCHEMA_NAME = "user1_workspace"
INPUT_TABLES = {
    "ios_lock_state": "lock_state",
    "ios_analytics": "analytics",
    "timezone_lookup": "lookup_timezone"
}
# add today's date to the output table name
OUTPUT_TABLE = "derived_events_screen_ios_pyspark_" + datetime.now().strftime("%Y%m%d")
OUTPUT_SCHEMA = StructType([
    StructField("participantid", StringType()),
    StructField("date_utc", DateType()),
    StructField("event_start_time_utc", StringType()),
    StructField("event_end_time_utc", StringType()),
    StructField("date_local", DateType()),
    StructField("event_start_time_local", StringType()),
    StructField("event_end_time_local", StringType()),
    StructField("screen_on_time_in_secs", DoubleType())
])
COMPOSITE_KEYS = ["participantid", "event_start_time_utc", "event_end_time_utc"]

PROCESSING_CONFIG = {
    "max_screen_time": 36000,  # 10 hours in seconds
    "batch_size": 1000,
    "fetch_size": 1000
}

# -----------------------
# iOS-specific Processing
# -----------------------
def process_ios_data(spark, config, logger):
    """Extract and transform iOS screen events."""
    
    # Check if required tables exist
    has_lock_state = table_exists(INPUT_SCHEMA, INPUT_TABLES["ios_lock_state"], config)
    has_analytics = table_exists(INPUT_SCHEMA, INPUT_TABLES["ios_analytics"], config)
    
    if not has_lock_state:
        logger.warning("Skipping iOS processing: lock_state table missing")
        return None
    
    logger.info("Reading iOS lock state data...")
    lock_state_df = read_event_data(spark, INPUT_TABLES["ios_lock_state"], config, INPUT_SCHEMA)
    
    # Combine with analytics data if available
    if has_analytics:
        logger.info("Reading iOS analytics data...")
        analytics_df = read_event_data(spark, INPUT_TABLES["ios_analytics"], config, INPUT_SCHEMA) \
            .withColumn("value0", lower(col("value0"))) \
            .filter(col("value0").contains("terminating"))
        
        combined_df = lock_state_df.withColumn("value0", lower(col("value0"))).unionByName(analytics_df)
        logger.info("Combined lock_state and analytics data")
    else:
        logger.warning("iOS analytics table missing — processing only lock_state")
        combined_df = lock_state_df.withColumn("value0", lower(col("value0")))
    
    # Remove duplicates wrt participantid, measuredat and value0
    combined_df = combined_df.dropDuplicates(["participantid", "measuredat", "value0"])

    logger.info(f"Total events after deduplication: {combined_df.count()}")
    
    # Detect unlock → lock events (screen sessions)
    logger.info("Detecting unlock → lock event pairs...")
    window_spec = Window.partitionBy("participantid").orderBy("measuredat")
    
    paired = combined_df \
        .withColumn("next_value", lead("value0").over(window_spec)) \
        .withColumn("next_measuredat", lead("measuredat").over(window_spec)) \
        .filter((col("value0") == "unlocked") & (col("next_value") == "locked")) \
        .withColumn("timescreen", col("next_measuredat").cast("long") - col("measuredat").cast("long")) \
        .filter(col("timescreen") <= PROCESSING_CONFIG["max_screen_time"]) \
        .filter(col("timescreen") > 0)  # Ensure positive duration
    
    screen_sessions_count = paired.count()
    logger.info(f"Found {screen_sessions_count} valid screen sessions")
    
    if screen_sessions_count == 0:
        logger.warning("No valid screen sessions found")
        return None
    
    # Add timezone information
    logger.info("Adding timezone information...")
    timezone_df = read_timezone_data(spark, config, INPUT_SCHEMA, INPUT_TABLES["timezone_lookup"])
    with_timezones = get_best_timezone_match(paired, timezone_df)

    # Deduplicate: ensure 1 row per screen session (start/end) per participant
    w_dedup = Window.partitionBy("participantid", "measuredat", "next_measuredat").orderBy(col("measuredat").desc())
    with_timezones = with_timezones.withColumn("rn", row_number().over(w_dedup)) \
                                     .filter(col("rn") == 1) \
                                     .drop("rn")
    
    # Calculate all time fields
    logger.info("Calculating event times...")
    result_df = calculate_event_times(with_timezones) \
        .selectExpr(
            "participantid",
            "date_utc", "event_start_time_utc", "event_end_time_utc",
            "date_local", "event_start_time_local", "event_end_time_local",
            "timescreen as screen_on_time_in_secs"
        )
    
    return result_df

# -----------------------
# Main Pipeline
# -----------------------
def main():
    logger = setup_logger("ScreenTimeProcessor-iOS")
    logger.info("Starting iOS screen time processing")
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize Spark
        spark = create_spark_session("ScreenTimeFeaturesProcessor-iOS")
        
        # Ensure output table exists with composite key
        create_table_if_not_exists(
            OUTPUT_SCHEMA_NAME, 
            OUTPUT_TABLE, 
            OUTPUT_SCHEMA,
            config, 
            COMPOSITE_KEYS
        )
        
        # Process iOS data
        ios_df = process_ios_data(spark, config, logger)
        
        if ios_df is None:
            logger.warning("No iOS data to process.")
            return
        
        # Log sample data for verification
        sample = ios_df.limit(1).collect()
        if sample:
            logger.info(f"Sample UTC time: {sample[0]['event_start_time_utc']}")
            logger.info(f"Sample local time: {sample[0]['event_start_time_local']}")
            logger.info(f"Sample screen time: {sample[0]['screen_on_time_in_secs']} seconds")
        
        # Write to database with upsert
        logger.info(f"Writing {ios_df.count()} records to {OUTPUT_SCHEMA_NAME}.{OUTPUT_TABLE}")
        upsert_to_postgres(ios_df, OUTPUT_TABLE, config, COMPOSITE_KEYS, OUTPUT_SCHEMA_NAME)
        
        logger.info("iOS processing completed successfully")
        
    except Exception as e:
        logger.error(f"iOS processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


# To run this script, use the command:
'''
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  --driver-memory 10g \
  --executor-memory 15g \
  --executor-cores 8 \
  --num-executors 10 \
  --conf spark.executor.memoryOverhead=3g \
  --conf spark.default.parallelism=80 \
  --conf spark.sql.shuffle.partitions=80 \
  --conf spark.network.timeout=600s \
  --conf spark.memory.fraction=0.8 \
  --conf spark.sql.session.timeZone=UTC \
  /home/prositadmin/dev_workspace/src/W_create_derived_events_screen_ios_pyspark.py \
    > /home/prositadmin/dev_workspace/logs/W_create_derived_events_screen_ios_pyspark.log 2>&1 &

tail -f /home/prositadmin/dev_workspace/logs/W_create_derived_events_screen_ios_pyspark.log
cat /home/prositadmin/dev_workspace/logs/W_create_derived_events_screen_ios_pyspark.log
rm /home/prositadmin/dev_workspace/logs/W_create_derived_events_screen_ios_pyspark.log
'''