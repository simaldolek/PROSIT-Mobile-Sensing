"""
Description: Optimized PySpark script for processing Android accelerometer data sequentially by participant
"""

import sys
import os
from pyspark.sql.functions import col, sqrt, lag, when, round as spark_round, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType, DateType
from datetime import datetime

# Add the src directory to the path to import utilities
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from helper.pyspark_utilities import *

# --- Configuration ---
INPUT_SCHEMA_NAME = "study_test"
OUTPUT_SCHEMA_NAME = "user1_workspace"
INPUT_TABLES = {
    "accelerometer": "accelerometer_m_s2__x_y_z",
    "timezone": "lookup_timezone"
}
# add today's date to the output table name
OUTPUT_TABLE = "derived_accelerometer_android_pyspark_" + datetime.now().strftime("%Y%m%d")
COMPOSITE_KEYS = ["_id"]
PROCESSING_CONFIG = {
    "pa_threshold": 0.4,
    "batch_size": 50  # Process 50 participants at a time
}

OUTPUT_SCHEMA = StructType([
    StructField("_id", StringType(), False),
    StructField("participantid", StringType(), True),
    StructField("measuredat_utc", StringType(), True),
    StructField("measuredat_local", StringType(), True),
    StructField("date_utc", DateType(), True),
    StructField("date_local", DateType(), True),
    StructField("acc_x", FloatType(), True),
    StructField("acc_y", FloatType(), True),
    StructField("acc_z", FloatType(), True),
    StructField("euclidean_norm_g", FloatType(), True),
    StructField("euclidean_distance_g", FloatType(), True),
    StructField("non_vigorous_pa", IntegerType(), True),
    StructField("vigorous_pa", IntegerType(), True)
])

def get_distinct_participants(spark, config, logger):
    """Get list of distinct participant IDs from accelerometer data."""
    if not table_exists(INPUT_SCHEMA_NAME, INPUT_TABLES["accelerometer"], config):
        logger.warning(f"Skipping: {INPUT_TABLES['accelerometer']} not found.")
        return None

    logger.info("Fetching distinct participant IDs...")
    query = f"""
        SELECT DISTINCT participantid 
        FROM {INPUT_SCHEMA_NAME}.{INPUT_TABLES['accelerometer']}
        ORDER BY participantid
    """
    return spark.read.format("jdbc") \
        .option("url", config["staging_db"]["url"]) \
        .option("dbtable", f"({query}) as tmp") \
        .option("user", config["staging_db"]["user"]) \
        .option("password", config["staging_db"]["password"]) \
        .load() \
        .rdd.map(lambda x: x[0]).collect()

def process_participant_batch(spark, config, logger, participant_ids, timezone_df):
    """Process a batch of participant IDs."""
    logger.info(f"Processing batch of {len(participant_ids)} participants")
    
    # Read only data for these participants
    participant_list = ",".join([f"'{pid}'" for pid in participant_ids])
    query = f"""
        SELECT 
            _id,
            participantid,
            measuredat::text AS measuredat_str,
            value0 as acc_x,
            value1 as acc_y,
            value2 as acc_z
        FROM {INPUT_SCHEMA_NAME}.{INPUT_TABLES['accelerometer']}
        WHERE participantid IN ({participant_list})
    """
    
    df = spark.read.format("jdbc") \
        .option("url", config["staging_db"]["url"]) \
        .option("dbtable", f"({query}) as tmp") \
        .option("user", config["staging_db"]["user"]) \
        .option("password", config["staging_db"]["password"]) \
        .load() \
        .withColumn("measuredat", col("measuredat_str").cast("timestamp")) \
        .drop("measuredat_str")

    # Convert m/s^2 to g for all axes (Android only)
    df = df.withColumn("acc_x", col("acc_x") / 9.81) \
           .withColumn("acc_y", col("acc_y") / 9.81) \
           .withColumn("acc_z", col("acc_z") / 9.81)
    
    # Filter for plausible g values
    df = df.filter((col("acc_x").between(-20, 20)) &
                   (col("acc_y").between(-20, 20)) &
                   (col("acc_z").between(-20, 20)))

    with_timezones = get_best_timezone_match(df, timezone_df)

    # Deduplicate in case multiple timezone rows overlapped; keep 1 per _id
    w_dedup = Window.partitionBy("_id").orderBy(col("measuredat").desc())
    with_timezones = with_timezones.withColumn("rn", row_number().over(w_dedup)) \
                                     .filter(col("rn") == 1) \
                                     .drop("rn")

    logger.info("Calculating physical activity metrics.")
    w = Window.partitionBy("participantid").orderBy("measuredat")
    processed_data = with_timezones.withColumn(
        "euclidean_norm",
        spark_round(sqrt(col("acc_x")**2 + col("acc_y")**2 + col("acc_z")**2), 4)
    ).withColumn(
        "euclidean_distance",
        spark_round(when(lag("acc_x", 1).over(w).isNull(), lit(None)).otherwise(
            sqrt((col("acc_x") - lag("acc_x", 1).over(w))**2 + 
                 (col("acc_y") - lag("acc_y", 1).over(w))**2 + 
                 (col("acc_z") - lag("acc_z", 1).over(w))**2)
        ), 4)
    ).withColumn("non_vigorous_pa", when(col("euclidean_norm") < PROCESSING_CONFIG["pa_threshold"], 1).otherwise(0)) \
     .withColumn("vigorous_pa", when(col("euclidean_norm") >= PROCESSING_CONFIG["pa_threshold"], 1).otherwise(0))

    # Rename metrics to include units (g)
    processed_data = processed_data.withColumnRenamed("euclidean_norm", "euclidean_norm_g") \
                                   .withColumnRenamed("euclidean_distance", "euclidean_distance_g")

    logger.info("Calculating final time fields.")
    final_df = calculate_point_times(processed_data, timestamp_col="measuredat")
    
    return final_df.select([field.name for field in OUTPUT_SCHEMA.fields])

# --- Main Execution ---
def main():
    """Orchestrates the Spark job with participant-by-participant processing."""
    logger = setup_logger("AccelerometerProcessor-Android")
    logger.info("Starting optimized Android accelerometer processing.")
    try:
        config = load_config()
        spark = create_spark_session("AccelerometerProcessor-Android-Optimized")
        
        create_table_if_not_exists(OUTPUT_SCHEMA_NAME, OUTPUT_TABLE, OUTPUT_SCHEMA, config, COMPOSITE_KEYS)
        
        # Get all participant IDs once
        participant_ids = get_distinct_participants(spark, config, logger)
        if not participant_ids:
            logger.warning("No participant IDs found - exiting.")
            return
            
        # Load timezone data once (broadcast for efficiency)
        timezone_df = read_timezone_data(spark, config, INPUT_SCHEMA_NAME)
        timezone_df = broadcast(timezone_df)
        
        # Process in batches
        total_participants = len(participant_ids)
        batch_size = PROCESSING_CONFIG["batch_size"]
        
        logger.info(f"Beginning processing of {total_participants} participants in batches of {batch_size}")
        
        for i in range(0, total_participants, batch_size):
            batch = participant_ids[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_participants//batch_size)+1} - Participant: {batch[0]}")
            
            try:
                result_df = process_participant_batch(spark, config, logger, batch, timezone_df)
                
                if result_df is not None:
                    result_df = result_df.persist()
                    record_count = result_df.count()
                else:
                    record_count = 0

                if record_count > 0:
                    logger.info(f"Writing {record_count} records to {OUTPUT_SCHEMA_NAME}.{OUTPUT_TABLE}")
                    upsert_to_postgres(result_df, OUTPUT_TABLE, config, COMPOSITE_KEYS, OUTPUT_SCHEMA_NAME)
                else:
                    logger.warning(f"No data processed for batch {i//batch_size + 1}")
                    
            except Exception as batch_error:
                logger.error(f"Error processing batch {i//batch_size + 1}: {str(batch_error)}")
                # Continue with next batch
                continue
            finally:
                try:
                    if result_df is not None:
                        result_df.unpersist()
                except Exception:
                    pass
            
            # for testing purposes break after first batch
            # if i == 1:
                # break
                
        logger.info("Processing completed for all participants.")
            
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()


# To run this script, use the command:
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
  /home/prositadmin/dev_workspace/src/W_create_derived_accelerometer_android_pyspark.py \
  > /home/prositadmin/dev_workspace/logs/W_create_derived_accelerometer_android_pyspark.log 2>&1 &

tail -f /home/prositadmin/dev_workspace/logs/W_create_derived_accelerometer_android_pyspark.log
cat /home/prositadmin/dev_workspace/logs/W_create_derived_accelerometer_android_pyspark.log
rm /home/prositadmin/dev_workspace/logs/W_create_derived_accelerometer_android_pyspark.log
'''