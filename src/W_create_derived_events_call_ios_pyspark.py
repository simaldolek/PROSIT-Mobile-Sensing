"""
PySpark script to process iOS call events using modular utilities.
This script uses the pyspark_utilities module for reusable components.
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
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
    "ios_call": "call",
    "ios_analytics": "analytics",
    "timezone_lookup": "lookup_timezone"
}
# add today's date to the output table name
OUTPUT_TABLE = "derived_events_call_ios_pyspark_" + datetime.now().strftime("%Y%m%d")
OUTPUT_SCHEMA = StructType([
    StructField("participantid", StringType()),
    StructField("date_utc", DateType()),
    StructField("event_start_time_utc", StringType()),
    StructField("event_end_time_utc", StringType()),
    StructField("date_local", DateType()),
    StructField("event_start_time_local", StringType()),
    StructField("event_end_time_local", StringType()),
    StructField("call_duration_in_secs", DoubleType()),
    StructField("call_type", StringType())
])
COMPOSITE_KEYS = ["participantid", "event_start_time_utc", "event_end_time_utc"]

PROCESSING_CONFIG = {
    "max_call_time": 36000,  # 10 hours in seconds
    "batch_size": 1000,
    "fetch_size": 1000
}

# -----------------------
# iOS Call Processing
# -----------------------
def process_call_sequences(df):
    """Process call sequences to identify complete calls and detect anomalies."""
    if df is None:
        return None, None  # Returning both processed and anomaly DataFrames

    window_spec = Window.partitionBy("participantid").orderBy("measuredat")

    sequenced_df = df.withColumn("next_event", lead("value0").over(window_spec)) \
                     .withColumn("next_time", lead("measuredat").over(window_spec)) \
                     .withColumn("next_next_event", lead("value0", 2).over(window_spec)) \
                     .withColumn("next_next_time", lead("measuredat", 2).over(window_spec)) \
                     .withColumn("call_span_secs", unix_timestamp(col("next_next_time")) - unix_timestamp(col("measuredat")))

    max_duration = 43200  # 12 hours in seconds

    # Incoming answered calls (valid)
    incoming_answered = sequenced_df.filter(
        (upper(col("value0")) == "CALL_INCOMING") &
        (upper(col("next_event")) == "CALL_CONNECTED") &
        (upper(col("next_next_event")) == "CALL_DISCONNECTED") &
        (col("call_span_secs") < max_duration)
    ).select(
        col("participantid"),
        col("measuredat").alias("call_start"),
        col("next_time").alias("call_connected"),
        col("next_next_time").alias("call_end"),
        col("timezone_id"),
        lit("incoming_call_answered").alias("call_type"),
        col("call_span_secs").alias("call_duration_in_secs")
    )

    # Incoming unanswered calls
    incoming_unanswered = sequenced_df.filter(
        (upper(col("value0")) == "CALL_INCOMING") &
        (upper(col("next_event")) == "CALL_DISCONNECTED") &
        (col("next_next_event").isNull() | (upper(col("next_next_event")) != "CALL_CONNECTED")) &
        ((unix_timestamp(col("next_time")) - unix_timestamp(col("measuredat"))) < max_duration)
    ).select(
        col("participantid"),
        col("measuredat").alias("call_start"),
        lit(None).cast(TimestampType()).alias("call_connected"),
        col("next_time").alias("call_end"),
        col("timezone_id"),
        lit("incoming_call_unanswered").alias("call_type"),
        lit(0).alias("call_duration_in_secs")
    )

    # Outgoing answered calls
    outgoing_answered = sequenced_df.filter(
        (upper(col("value0")) == "CALL_DIALING") &
        (upper(col("next_event")) == "CALL_CONNECTED") &
        (upper(col("next_next_event")) == "CALL_DISCONNECTED") &
        (col("call_span_secs") < max_duration)
    ).select(
        col("participantid"),
        col("measuredat").alias("call_start"),
        col("next_time").alias("call_connected"),
        col("next_next_time").alias("call_end"),
        col("timezone_id"),
        lit("outgoing_call_answered").alias("call_type"),
        col("call_span_secs").alias("call_duration_in_secs")
    )

    # Outgoing unanswered calls
    outgoing_unanswered = sequenced_df.filter(
        (upper(col("value0")) == "CALL_DIALING") &
        (upper(col("next_event")) == "CALL_DISCONNECTED") &
        (col("next_next_event").isNull() | (upper(col("next_next_event")) != "CALL_CONNECTED")) &
        ((unix_timestamp(col("next_time")) - unix_timestamp(col("measuredat"))) < max_duration)
    ).select(
        col("participantid"),
        col("measuredat").alias("call_start"),
        lit(None).cast(TimestampType()).alias("call_connected"),
        col("next_time").alias("call_end"),
        col("timezone_id"),
        lit("outgoing_call_unanswered").alias("call_type"),
        lit(0).alias("call_duration_in_secs")
    )

    # Combine valid calls
    valid_calls = incoming_answered.unionByName(incoming_unanswered) \
                                   .unionByName(outgoing_answered) \
                                   .unionByName(outgoing_unanswered)

    # Anomalies: calls exceeding max duration
    anomalies = sequenced_df.filter(
        (
            ((upper(col("value0")) == "CALL_INCOMING") &
             (upper(col("next_event")) == "CALL_CONNECTED") &
             (upper(col("next_next_event")) == "CALL_DISCONNECTED")) |

            ((upper(col("value0")) == "CALL_DIALING") &
             (upper(col("next_event")) == "CALL_CONNECTED") &
             (upper(col("next_next_event")) == "CALL_DISCONNECTED"))
        ) &
        (col("call_span_secs") >= max_duration)
    ).select(
        col("participantid"),
        col("measuredat").alias("call_start"),
        col("next_time").alias("call_connected"),
        col("next_next_time").alias("call_end"),
        col("timezone_id"),
        col("call_span_secs").alias("anomaly_duration_secs"),
        col("value0"),
        col("next_event"),
        col("next_next_event")
    )

    return valid_calls, anomalies

def process_ios_call_data(spark, config, logger):
    """Extract and transform iOS call events."""
    has_call = table_exists(INPUT_SCHEMA, INPUT_TABLES["ios_call"], config)
    has_analytics = table_exists(INPUT_SCHEMA, INPUT_TABLES["ios_analytics"], config)
    if not has_call:
        logger.warning("Skipping iOS call processing: call table missing")
        return None

    logger.info("Reading iOS call data...")
    call_df = read_event_data(spark, INPUT_TABLES["ios_call"], config, INPUT_SCHEMA)

    # Combine with analytics data if available
    if has_analytics:
        logger.info("Reading iOS analytics data...")
        analytics_df = read_event_data(spark, INPUT_TABLES["ios_analytics"], config, INPUT_SCHEMA) \
            .withColumn("value0", lower(col("value0"))) \
            .filter(col("value0").contains("terminating")) \
            .withColumn("value0", lit("CALL_DISCONNECTED"))
        call_df = call_df.unionByName(analytics_df)
        logger.info("Combined call and analytics data")
    else:
        logger.warning("iOS analytics table missing — processing only call events")

    # Remove duplicates wrt participantid, measuredat and value0
    call_df = call_df.dropDuplicates(["participantid", "measuredat", "value0"])
    logger.info(f"Total call events after deduplication: {call_df.count()}")

    # Add timezone information
    logger.info("Adding timezone information...")
    timezone_df = read_timezone_data(spark, config, INPUT_SCHEMA, INPUT_TABLES["timezone_lookup"])
    with_timezones = get_best_timezone_match(call_df, timezone_df)

    # Deduplicate: ensure 1 row per raw call event per participant
    w_dedup = Window.partitionBy("participantid", "measuredat", "value0").orderBy(col("measuredat").desc())
    with_timezones = with_timezones.withColumn("rn", row_number().over(w_dedup)) \
                                     .filter(col("rn") == 1) \
                                     .drop("rn")

    # Detect call sequences (valid and anomalies)
    logger.info("Processing call sequences...")
    call_sequences, anomalies = process_call_sequences(with_timezones)
    if call_sequences is None:
        logger.warning("No valid call sequences found")
        return None
    if anomalies is not None and anomalies.count() > 0:
        logger.warning(f"Detected {anomalies.count()} anomalous call sequences exceeding 12 hours")
        # Optionally: anomalies.coalesce(1).write.csv("/home/prositadmin/dev_workspace/logs/anomalous_calls", header=True, mode="overwrite")

    # Rename columns for compatibility with calculate_event_times
    call_sequences = call_sequences.withColumnRenamed("call_start", "measuredat") \
                                   .withColumnRenamed("call_end", "next_measuredat")

    # Calculate all time fields
    logger.info("Calculating event times...")
    final_df = calculate_event_times(call_sequences)

    result_df = final_df.select(
        col("participantid"),
        col("date_utc"),
        col("event_start_time_utc"),
        col("event_end_time_utc"),
        col("date_local"),
        col("event_start_time_local"),
        col("event_end_time_local"),
        col("call_duration_in_secs"),
        col("call_type")
    )
    return result_df

# -----------------------
# Main Pipeline
# -----------------------
def main():
    logger = setup_logger("CallProcessor-iOS")
    logger.info("Starting iOS call event processing")
    try:
        config = load_config()
        spark = create_spark_session("CallFeaturesProcessor-iOS")
        create_table_if_not_exists(
            OUTPUT_SCHEMA_NAME,
            OUTPUT_TABLE,
            OUTPUT_SCHEMA,
            config,
            COMPOSITE_KEYS
        )
        call_df = process_ios_call_data(spark, config, logger)
        if call_df is None:
            logger.warning("No iOS call data to process.")
            return
        sample = call_df.limit(1).collect()
        if sample:
            logger.info(f"Sample UTC time: {sample[0]['event_start_time_utc']}")
            logger.info(f"Sample local time: {sample[0]['event_start_time_local']}")
            logger.info(f"Sample call duration: {sample[0]['call_duration_in_secs']} seconds")
        logger.info(f"Writing {call_df.count()} records to {OUTPUT_SCHEMA_NAME}.{OUTPUT_TABLE}")
        upsert_to_postgres(call_df, OUTPUT_TABLE, config, COMPOSITE_KEYS, OUTPUT_SCHEMA_NAME)
        logger.info("iOS call event processing completed successfully")
    except Exception as e:
        logger.error(f"iOS call event processing failed: {str(e)}")
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
  /home/prositadmin/dev_workspace/src/W_create_derived_events_call_ios_pyspark.py \
    > /home/prositadmin/dev_workspace/logs/W_create_derived_events_call_ios_pyspark.log 2>&1 &

tail -f /home/prositadmin/dev_workspace/logs/W_create_derived_events_call_ios_pyspark.log
cat /home/prositadmin/dev_workspace/logs/W_create_derived_events_call_ios_pyspark.log
rm /home/prositadmin/dev_workspace/logs/W_create_derived_events_call_ios_pyspark.log
'''
