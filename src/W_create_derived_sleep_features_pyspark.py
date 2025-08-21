"""
Description: This PySpark script processes sleep data to derive nightly sleep features using ONLY screen time data.
Accelerometer-based computation has been removed per user request.
This is a modularized version that uses reusable utilities.
Author: Mohamed Muzamil (mohamed.muzamilh@dal.ca)
Date: 2025-08-18

Features computed:
- total_screen_minutes: Screen-on minutes during sleep window
- total_active_minutes: Minutes with any screen activity during sleep window
- total_inactive_minutes: Minutes without screen activity during sleep window

Assumptions:
- Sleep window: 22:00 (10pm) to 08:00 (8am next day), can be configured
- Inactivity = no screen events
"""
import sys
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

# Add utilities directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from helper.pyspark_utilities import *

# --- Configuration ---
INPUT_SCHEMA = "study_test"
OUTPUT_SCHEMA_NAME = "user1_workspace"
INPUT_TABLES = {
    "ios_screen": "lock_state",
    "android_screen": "powerstate",
    "timezone_lookup": "lookup_timezone"
}
# add today's date to the output table name
OUTPUT_TABLE = "feature_daily_sleep_pyspark_" + datetime.now().strftime("%Y%m%d")
COMPOSITE_KEYS = ["participantid", "date_local"]

# Sleep detection parameters
SLEEP_WINDOW_START = 22  # 10pm
SLEEP_WINDOW_END = 8     # 8am next day
MIN_SLEEP_DURATION = 30  # minutes
MAX_SLEEP_INTERRUPTION = 15  # minutes

# Output schema with explicit types
OUTPUT_SCHEMA = StructType([
    StructField("participantid", StringType(), False),
    StructField("date_local", DateType(), False),
    StructField("total_night_screen_minutes", IntegerType(), True),
    StructField("total_night_screen_interruptions", IntegerType(), True),
    StructField("total_inactive_minutes", IntegerType(), True)
])

def preprocess_screen_data(spark, config, logger):
    """Load and preprocess screen data with proper type conversion"""
    screen_dfs = []
    # iOS screen data
    if table_exists(INPUT_SCHEMA, INPUT_TABLES["ios_screen"], config):
        ios_screen = read_event_data(spark, INPUT_TABLES["ios_screen"], config, INPUT_SCHEMA)
        ios_screen = ios_screen.withColumn(
            "event_type",
            when(col("value0") == "UNLOCKED", "on")
            .when(col("value0") == "LOCKED", "off")
            .otherwise("unknown")
        ).withColumn(
            "participantid", col("participantid").cast("string")
        ).withColumnRenamed("measuredat", "timestamp")
        screen_dfs.append(ios_screen)
    # Android screen data
    if table_exists(INPUT_SCHEMA, INPUT_TABLES["android_screen"], config):
        android_screen = read_event_data(spark, INPUT_TABLES["android_screen"], config, INPUT_SCHEMA)
        android_screen = android_screen.withColumn(
            "event_type",
            when(col("value0") == "screen_on", "on")
            .when(col("value0") == "screen_off", "off")
            .otherwise("unknown")
        ).withColumn(
            "participantid", col("participantid").cast("string")
        ).withColumnRenamed("measuredat", "timestamp")
        screen_dfs.append(android_screen)
    if not screen_dfs:
        return None
    # Union and standardize screen data
    screen_df = screen_dfs[0]
    for df in screen_dfs[1:]:
        screen_df = screen_df.unionByName(df)
    return screen_df.select(
        "participantid",
        col("timestamp").cast("timestamp").alias("measuredat"),
        "event_type"
    ).distinct()

def detect_sleep_periods_screen_only(events_df, timezone_df, logger):
    """Sleep detection logic using only screen events"""
    events_with_tz = get_best_timezone_match(events_df, timezone_df)

    # Deduplicate: ensure 1 row per raw screen event per participant
    w_dedup = Window.partitionBy("participantid", "measuredat", "event_type").orderBy(col("measuredat").desc())
    events_with_tz = events_with_tz.withColumn("rn", row_number().over(w_dedup)) \
                                     .filter(col("rn") == 1) \
                                     .drop("rn")
    events_local = events_with_tz.withColumn(
        "local_time",
        from_utc_timestamp(col("measuredat"), coalesce(col("timezone_id"), lit("UTC")))
    ).withColumn(
        # Assign day using 6AM–6AM boundary: shift local time by -6 hours before extracting date
        "date_local",
        to_date(col("local_time") - expr("INTERVAL 6 HOURS"))
    ).withColumn(
        "local_hour", hour(col("local_time"))
    ).withColumn(
        "date_local",
        when(col("local_hour") >= SLEEP_WINDOW_START, to_date(col("local_time")))
        .otherwise(to_date(col("local_time") - expr("INTERVAL 1 DAY")))
    ).filter(
        (col("local_hour") >= SLEEP_WINDOW_START) |
        (col("local_hour") < SLEEP_WINDOW_END)
    )
    def detect_sleep(pdf):
        import pandas as pd
        if pdf.empty:
            return pd.DataFrame([{k: None if k in ["date_local"] else 0 for k in OUTPUT_SCHEMA.names}])
        pdf["participantid"] = pdf["participantid"].astype(str)
        pdf = pdf.sort_values("local_time")
        sleep_window_duration_minutes = (24 - SLEEP_WINDOW_START + SLEEP_WINDOW_END) * 60
        # Find screen sessions (interruptions) and total screen minutes
        screen_minutes = set()
        session_starts = 0
        prev_state = None
        for _, row in pdf.iterrows():
            minute = row["local_time"].floor('T')
            if row.get("event_type") == "on":
                screen_minutes.add(minute)
                if prev_state != "on":
                    session_starts += 1
                prev_state = "on"
            elif row.get("event_type") == "off":
                prev_state = "off"
        total_night_screen_minutes = len(screen_minutes)
        total_night_screen_interruptions = session_starts
        total_inactive_minutes = sleep_window_duration_minutes - total_night_screen_minutes
        return pd.DataFrame([{
            "participantid": str(pdf["participantid"].iloc[0]),
            "date_local": pdf["date_local"].iloc[0],
            "total_night_screen_minutes": total_night_screen_minutes,
            "total_night_screen_interruptions": total_night_screen_interruptions,
            "total_inactive_minutes": total_inactive_minutes
        }])
    sleep_features = events_local.groupBy("participantid", "date_local").applyInPandas(
        detect_sleep,
        schema=OUTPUT_SCHEMA
    )
    return sleep_features

def process_sleep_data_screen_only(spark, config, logger):
    """Main processing pipeline using only screen data"""
    logger.info("Starting sleep data processing (screen-only)")
    timezone_df = read_timezone_data(spark, config, INPUT_SCHEMA, INPUT_TABLES["timezone_lookup"])
    screen_df = preprocess_screen_data(spark, config, logger)
    if screen_df:
        logger.info(f"Processed {screen_df.count()} screen events")
    else:
        logger.warning("No screen data available")
        screen_df = spark.createDataFrame([], StructType([
            StructField("participantid", StringType()),
            StructField("measuredat", TimestampType()),
            StructField("event_type", StringType())
        ]))
    events_df = screen_df.select(
        "participantid", "measuredat", "event_type"
    )
    if events_df.isEmpty():
        logger.error("No events available for sleep detection")
        return None
    sleep_features = detect_sleep_periods_screen_only(events_df, timezone_df, logger)
    return sleep_features

def main():
    """Main execution function (screen-only)"""
    logger = setup_logger("SleepDetectionV2")
    logger.info("Starting sleep detection pipeline (screen-only)")
    try:
        config = load_config()
        spark = create_spark_session("SleepDetectionV2")
        create_table_if_not_exists(OUTPUT_SCHEMA_NAME, OUTPUT_TABLE, OUTPUT_SCHEMA, config, COMPOSITE_KEYS)
        result_df = process_sleep_data_screen_only(spark, config, logger)
        if result_df:
            logger.info(f"Writing {result_df.count()} records to {OUTPUT_SCHEMA_NAME}.{OUTPUT_TABLE}")
            upsert_to_postgres(result_df, OUTPUT_TABLE, config, COMPOSITE_KEYS, OUTPUT_SCHEMA_NAME)
            logger.info("Sleep detection (screen-only) completed successfully")
        else:
            logger.warning("No sleep features were generated")
    except Exception as e:
        logger.error(f"Error in sleep detection: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()

# to run this script use the following command:
'''
spark-submit \
  --master spark://localhost:7077 \
  --jars /home/prositadmin/dev_workspace/src/postgresql-42.6.0.jar \
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
  /home/prositadmin/dev_workspace/src/W_create_derived_sleep_features_pyspark.py \
    > /home/prositadmin/dev_workspace/logs/W_create_derived_sleep_features_pyspark.log 2>&1 &

tail -f /home/prositadmin/dev_workspace/logs/W_create_derived_sleep_features_pyspark.log
cat /home/prositadmin/dev_workspace/logs/W_create_derived_sleep_features_pyspark.log
rm /home/prositadmin/dev_workspace/logs/W_create_derived_sleep_features_pyspark.log
'''
