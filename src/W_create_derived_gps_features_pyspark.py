"""
Description: This PySpark script processes GPS/location data to derive daily mobility features.
Computes haversine distance, stay points, location entropy, and location variance.
This is a modularized version that uses reusable utilities.
"""

import sys
import os
from pyspark.sql.functions import col, when, regexp_extract, radians, sin, cos, sqrt, asin, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
from datetime import datetime

# Add the src directory to the path to import utilities
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from helper.pyspark_utilities import *

# --- Configuration ---
INPUT_SCHEMA = "study_test"
OUTPUT_SCHEMA_NAME = "user1_workspace"
INPUT_TABLES = {
    "location": "location"
}
# add today's date to the output table name
OUTPUT_TABLE = "feature_daily_gps_pyspark_" + datetime.now().strftime("%Y%m%d")
COMPOSITE_KEYS = ["participantid", "date_utc"]

OUTPUT_SCHEMA = StructType([
    StructField("participantid", StringType(), False),
    StructField("date_utc", DateType(), True),
    StructField("date_local", DateType(), True),
    StructField("total_haversine_meters", FloatType(), True),
    StructField("number_of_stay_points", IntegerType(), True),
    StructField("total_time_at_clusters_seconds", IntegerType(), True),
    StructField("location_entropy", FloatType(), True),
    StructField("location_variance", FloatType(), True),
    StructField("radius_of_gyration_meters", FloatType(), True) # RENAMED for units
])

# --- Data Processing ---
def process_gps_data(spark, config, logger):
    """Main processing pipeline for GPS/location data."""
    if not table_exists(INPUT_SCHEMA, INPUT_TABLES["location"], config):
        logger.warning(f"Skipping: {INPUT_TABLES['location']} not found.")
        return None

    logger.info(f"Reading data from {INPUT_TABLES['location']}")
    df = read_location_data(spark, INPUT_TABLES["location"], config, INPUT_SCHEMA)
    
    logger.info("Extracting latitude and longitude from location_decrypted")
    df = extract_lat_lon_from_location(df)
    
    df = df.filter(
        (col("lat").isNotNull()) & 
        (col("lon").isNotNull()) & 
        (~isnan(col("lat"))) & 
        (~isnan(col("lon")))
    )

    logger.info("Processing timezone_id from location table only")
    df = df.withColumn(
        "timezone_id",
        when(
            (col("timezone_id") == "Unknown_Timezone") | 
            col("timezone_id").isNull(), 
            None
        ).otherwise(col("timezone_id"))
    )
    
    df = df.withColumn(
        "effective_timezone",
        when(col("timezone_id").isNotNull(), col("timezone_id")).otherwise(lit("UTC"))
    ).withColumn(
        "measuredat_local",
        expr("from_utc_timestamp(measuredat, effective_timezone)")
    )

    # Assign day using 6AM–6AM boundary: shift local time by -6 hours before extracting date
    df = df.withColumn("date_local", to_date(col("measuredat_local") - expr("INTERVAL 6 HOURS")))
    df = df.withColumn("date_utc", to_date(col("measuredat")))

    logger.info("Computing GPS features")
    df_with_features = compute_gps_features(df)
    
    logger.info("Aggregating daily GPS features")
    daily_features = aggregate_daily_gps_features(df_with_features).groupBy("participantid", "date_utc", "date_local").agg(
        sum("total_haversine_meters").alias("total_haversine_meters"),
        sum("number_of_stay_points").alias("number_of_stay_points"),
        sum("total_time_at_clusters_seconds").alias("total_time_at_clusters_seconds"),
        sum("location_entropy").alias("location_entropy"),
        sum("location_variance").alias("location_variance"),
        sum("radius_of_gyration").alias("radius_of_gyration_meters")
    )
    
    daily_features = daily_features.select(
        col("participantid"),
        col("date_utc"),
        col("date_local"),
        col("total_haversine_meters"),
        col("number_of_stay_points"),
        col("total_time_at_clusters_seconds"),
        col("location_entropy"),
        col("location_variance"),
        col("radius_of_gyration_meters")
    )
    daily_features = daily_features.withColumnRenamed("total_haversine", "total_haversine_meters")
    daily_features = daily_features.withColumnRenamed("total_time_at_clusters", "total_time_at_clusters_seconds")

    from pyspark.sql.functions import round as spark_round
    final_df = daily_features
    final_df = final_df.withColumn("total_haversine_meters", spark_round(col("total_haversine_meters"), 0))
    final_df = final_df.withColumn("location_entropy", spark_round(col("location_entropy"), 2))
    final_df = final_df.withColumn("location_variance", spark_round(col("location_variance"), 2))
    final_df = final_df.withColumn("radius_of_gyration_meters", spark_round(col("radius_of_gyration_meters"), 2))

    final_df = final_df.select([field.name for field in OUTPUT_SCHEMA.fields])
    
    return final_df


# --- Main Execution ---
def main():
    """Orchestrates the Spark job."""
    logger = setup_logger("GPSFeaturesProcessor")
    logger.info("Starting GPS features processing.")
    try:
        config = load_config()
        spark = create_spark_session("GPSFeaturesProcessor")
        
        create_table_if_not_exists(OUTPUT_SCHEMA_NAME, OUTPUT_TABLE, OUTPUT_SCHEMA, config, COMPOSITE_KEYS)
        
        result_df = process_gps_data(spark, config, logger)
        
        if result_df:
            logger.info(f"Writing {result_df.count()} records to {OUTPUT_SCHEMA_NAME}.{OUTPUT_TABLE}")
            # Filter out rows with null date_utc or participantid to avoid NOT NULL constraint errors
            result_df = result_df.filter(col("date_utc").isNotNull() & col("participantid").isNotNull())

            # Upsert to Postgres
            upsert_to_postgres(result_df, OUTPUT_TABLE, config, COMPOSITE_KEYS, OUTPUT_SCHEMA_NAME)
            logger.info("Processing completed successfully.")
        else:
            logger.warning("No data was processed.")
            
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()


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
  /home/prositadmin/dev_workspace/src/W_create_derived_gps_features_pyspark.py \
    > /home/prositadmin/dev_workspace/logs/W_create_derived_gps_features_pyspark.log 2>&1 &

tail -f /home/prositadmin/dev_workspace/logs/W_create_derived_gps_features_pyspark.log
cat /home/prositadmin/dev_workspace/logs/W_create_derived_gps_features_pyspark.log
rm /home/prositadmin/dev_workspace/logs/W_create_derived_gps_features_pyspark.log
'''