"""
PySpark utilities module for mobile sensor data processing.
Contains reusable functions for database operations, data transformations,
and common processing patterns.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType, DateType, BooleanType
from pyspark.sql.types import *
import logging
import configparser
import psycopg2
import os

# -----------------------
# Setup Logging
# -----------------------
def setup_logger(name: str):
    """Setup and return a logger with the given name."""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)

# -----------------------
# Configuration Loading
# -----------------------
def load_config(config_file: str = "path/to/config.ini"):
    """Load configuration from INI file and return app config dictionary."""
    config = configparser.ConfigParser()
    config.read(config_file)
    host = config.get("DATABASE_PSQL", "host")
    port = config.get("DATABASE_PSQL", "port")
    user = config.get("DATABASE_PSQL", "user")
    password = config.get("DATABASE_PSQL", "password")
    
    return {
        "staging_db": {
            "url": f"jdbc:postgresql://{host}:{port}/staging_db",
            "user": user,
            "password": password
        },
        "db_connection": {
            "host": host,
            "port": port,
            "user": user,
            "password": password
        }
    }

# -----------------------
# Spark Session Initialization
# -----------------------
def create_spark_session(app_name: str, master: str = "spark://localhost:7077"):
    """Create and configure Spark session with optimized settings."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars", "path/to/postgresql-42.6.0.jar")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", "/tmp/spark_local"))
        .config("spark.executor.instances", "10")
        .config("spark.executor.cores", "8")
        .config("spark.executor.memory", "15g")
        .config("spark.executor.memoryOverhead", "3g")
        .config("spark.default.parallelism", "80")
        .config("spark.sql.shuffle.partitions", "80")
        .config("spark.network.timeout", "600s")
        .config("spark.memory.fraction", "0.8")
        .config("spark.jdbc.fetchsize", "1000")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

# -----------------------
# Database Utilities
# -----------------------
def table_exists(schema: str, table: str, config: dict) -> bool:
    """Return True if schema.table exists in Postgres."""
    try:
        conn = psycopg2.connect(
            dbname="staging_db",
            user=config["db_connection"]["user"],
            password=config["db_connection"]["password"],
            host=config["db_connection"]["host"],
            port=config["db_connection"]["port"],
        )
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table),
        )
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error checking table existence for {schema}.{table}: {str(e)}")
        return False

def create_table_if_not_exists(schema: str, table: str, table_schema: StructType, config: dict, composite_keys: list = None):
    """Create table with proper schema and composite key if it doesn't exist."""
    try:
        conn = psycopg2.connect(
            dbname="staging_db",
            user=config["db_connection"]["user"],
            password=config["db_connection"]["password"],
            host=config["db_connection"]["host"],
            port=config["db_connection"]["port"],
        )
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table),
        )
        exists = cur.fetchone()[0]
        
        if not exists:
            # Create table with proper column definitions
            columns = []
            for field in table_schema.fields:
                if isinstance(field.dataType, StringType):
                    pg_type = "TEXT"
                elif isinstance(field.dataType, DateType):
                    pg_type = "DATE"
                elif isinstance(field.dataType, DoubleType):
                    pg_type = "DOUBLE PRECISION"
                elif isinstance(field.dataType, IntegerType):
                    pg_type = "INTEGER"
                elif isinstance(field.dataType, LongType):
                    pg_type = "BIGINT"
                else:
                    pg_type = "TEXT"
                columns.append(f"{field.name} {pg_type}")
            
            # Build CREATE TABLE statement with primary key
            if composite_keys:
                create_sql = f"CREATE TABLE {schema}.{table} ({', '.join(columns)}, PRIMARY KEY ({', '.join(composite_keys)}))"
            else:
                create_sql = f"CREATE TABLE {schema}.{table} ({', '.join(columns)})"
            
            cur.execute(create_sql)
            conn.commit()
            
            logger = logging.getLogger(__name__)
            logger.info(f"Created table {schema}.{table} with composite key: {composite_keys}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error creating table {schema}.{table}: {str(e)}")
        raise

# -----------------------
# Data Readers
# -----------------------
def read_event_data(spark: SparkSession, table: str, config: dict, schema: str = "study_test"):
    """Read event data from Postgres and cast measuredat to timestamp."""
    query = f"""
        SELECT participantid, measuredat, value0
        FROM {schema}.{table}
    """
    return (
        spark.read.format("jdbc")
        .option("url", config["staging_db"]["url"])
        .option("dbtable", f"({query}) as tmp")
        .option("user", config["staging_db"]["user"])
        .option("password", config["staging_db"]["password"])
        .load()
        .withColumn("measuredat", to_timestamp(col("measuredat")))
    )

def read_timezone_data(spark: SparkSession, config: dict, schema: str = "study_test", table: str = "lookup_timezone"):
    """Load timezone lookup table from Postgres."""
    return (
        spark.read.format("jdbc")
        .option("url", config["staging_db"]["url"])
        .option("dbtable", f"{schema}.{table}")
        .option("user", config["staging_db"]["user"])
        .option("password", config["staging_db"]["password"])
        .load()
    )

def read_accelerometer_data(spark: SparkSession, table: str, config: dict, schema: str = "study_test"):
    """Read accelerometer data with all three axes and timestamp processing."""
    query = f"""
        SELECT 
            _id,
            participantid,
            measuredat::text AS measuredat_str,
            value0,
            value1,
            value2
        FROM {schema}.{table}
    """
    return (
        spark.read.format("jdbc")
        .option("url", config["staging_db"]["url"])
        .option("dbtable", f"({query}) as tmp")
        .option("user", config["staging_db"]["user"])
        .option("password", config["staging_db"]["password"])
        .load()
        .withColumn("measuredat", to_timestamp(col("measuredat_str"), "yyyy-MM-dd HH:mm:ss"))
        .drop("measuredat_str")
        .withColumnRenamed("value0", "acc_x")
        .withColumnRenamed("value1", "acc_y")
        .withColumnRenamed("value2", "acc_z")
    )

def read_location_data(spark: SparkSession, table: str, config: dict, schema: str = "study_test"):
    """Read location data with robust timestamp parsing (with/without milliseconds)."""
    query = f"""
        SELECT 
            _id,
            participantid,
            measuredat::text AS measuredat_str,
            uploadedat::text AS uploadedat_str,
            value0,
            location_decrypted,
            timezone_id
        FROM {schema}.{table}
        ORDER BY participantid, measuredat
    """
    return (
        spark.read.format("jdbc")
        .option("url", config["staging_db"]["url"])
        .option("dbtable", f"({query}) as tmp")
        .option("user", config["staging_db"]["user"])
        .option("password", config["staging_db"]["password"])
        .load()
        # Some sources have milliseconds, some do not; coalesce multiple parsers
        .withColumn(
            "measuredat",
            coalesce(
                to_timestamp(col("measuredat_str"), "yyyy-MM-dd HH:mm:ss.SSS"),
                to_timestamp(col("measuredat_str"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("measuredat_str"))
            )
        )
        .withColumn(
            "uploadedat",
            coalesce(
                to_timestamp(col("uploadedat_str"), "yyyy-MM-dd HH:mm:ss.SSS"),
                to_timestamp(col("uploadedat_str"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("uploadedat_str"))
            )
        )
        .drop("measuredat_str", "uploadedat_str")
    )

# -----------------------
# Data Transformations
# -----------------------
def get_best_timezone_match(df, timezone_df):
    """
    Match each row in df to the best timezone in timezone_df:
    - Prefer ranges where measuredat is inside [start_time, end_time]
    - Otherwise, pick latest timezone before measuredat
    - If none found, timezone_id stays null
    Expects df to have a 'measuredat' column (timestamp).
    """
    import logging
    logger = logging.getLogger("SleepFeaturesProcessor")
    logger.info(f"get_best_timezone_match: columns at entry: {df.columns}")
    logger.info(f"get_best_timezone_match: row count at entry: {df.count()}")
    if 'measuredat' not in df.columns:
        logger.error(f"get_best_timezone_match: 'measuredat' column missing! Columns: {df.columns}")
        raise ValueError("Input DataFrame must contain a 'measuredat' column for timezone matching.")

    events_df = df.alias("events")
    timezones_df = timezone_df.alias("timezones")

    joined = events_df.join(
        broadcast(timezones_df),
        (col("events.participantid") == col("timezones.participantid")) &
        (col("events.measuredat") >= col("timezones.start_time")) &
        (col("events.measuredat") <= col("timezones.end_time")),
        "left"
    )

    measuredat_ts = col("events.measuredat").cast("long")
    start_ts = col("timezones.start_time").cast("long")
    end_ts = col("timezones.end_time").cast("long")

    # Determine stable partition keys for one-to-one matching
    # Prefer '_id' if present; otherwise fall back to (participantid, measuredat)
    partition_cols = ["events._id"] if "_id" in df.columns else ["events.participantid", "events.measuredat"]

    # Deterministic ordering to break ties when multiple timezone rows overlap
    # Prefer later start_time, then earlier end_time, then timezone_id
    window_spec = Window.partitionBy(*[col(c) for c in partition_cols]) \
                         .orderBy(col("timezones.start_time").desc(),
                                  col("timezones.end_time").asc(),
                                  col("timezones.timezone_id").asc())

    # Use row_number (not rank) to guarantee a single row per event even on perfect ties
    result = joined.withColumn("rn", row_number().over(window_spec)) \
        .filter((col("rn") == 1) | col("timezones.participantid").isNull()) \
        .drop("rn") \
        .select(
            col("events.*"),
            col("timezones.timezone_id")
        )
    

    return result

def calculate_event_times(df):
    """Calculate all time fields with strict UTC preservation."""
    # Add effective timezone with fallback logic
    df = df.withColumn(
        "effective_timezone",
        when(col("timezone_id").isNotNull(), col("timezone_id")).otherwise(lit("UTC"))
    ).withColumn(
        "used_utc_fallback",
        col("timezone_id").isNull()
    )

    # Calculate next_measuredat for event duration
    window_spec = Window.partitionBy("participantid").orderBy("measuredat")
    df = df.withColumn("next_measuredat", lead("measuredat").over(window_spec))
    
    return df \
        .withColumn("event_start_time_utc", 
            date_format(col("measuredat"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("event_end_time_utc", 
            date_format(col("next_measuredat"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn(
            "event_end_time_utc",
            when(col("event_end_time_utc").isNull(), col("event_start_time_utc"))
            .otherwise(col("event_end_time_utc"))
        ) \
        .withColumn("date_utc", to_date(col("measuredat"))) \
        .withColumn("weekday_utc", date_format(col("measuredat"), "EEEE")) \
        .withColumn("measuredat_local", expr("from_utc_timestamp(measuredat, effective_timezone)")) \
        .withColumn("shifted_local_for_date", expr("from_utc_timestamp(measuredat, effective_timezone) - interval 6 hours")) \
        .withColumn("date_local", to_date(col("shifted_local_for_date"))) \
        .withColumn("weekday_local", date_format(col("shifted_local_for_date"), "EEEE")) \
        .withColumn("event_start_time_local",
            date_format(
                expr("from_utc_timestamp(measuredat, effective_timezone)"),
                "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("event_end_time_local",
            date_format(
                expr("from_utc_timestamp(next_measuredat, effective_timezone)"),
                "yyyy-MM-dd'T'HH:mm:ss"))

def calculate_point_times(df, timestamp_col="measuredat"):
    """For point events, add measuredat_utc/local and date_utc/date_local (6 AM boundary), no weekdays."""
    df = df.withColumn(
        "effective_timezone",
        when(col("timezone_id").isNotNull(), col("timezone_id")).otherwise(lit("UTC"))
    )
    return df \
        .withColumn("measuredat_utc", date_format(col(timestamp_col), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("measuredat_local", date_format(from_utc_timestamp(col(timestamp_col), col("effective_timezone")), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("date_utc", to_date(col(timestamp_col))) \
        .withColumn("shifted_local_for_date", expr("from_utc_timestamp(measuredat, effective_timezone) - interval 6 hours")) \
        .withColumn("date_local", to_date(col("shifted_local_for_date"))) \
        .drop("shifted_local_for_date")

def calculate_time_fields(df, timestamp_col="measuredat"):
    """Add formatted UTC & local timestamps plus weekday labels for accelerometer data.
    Falls back to UTC if no timezone is available, mirroring original iOS logic.
    """
    # Add used_utc_fallback column
    df = df.withColumn("used_utc_fallback", col("timezone_id").isNull())
    
    return df.withColumn(
        "measuredat_utc",
        date_format(col(timestamp_col), "dd-MM-yyyy HH:mm:ss")
    ).withColumn(
        "weekday_utc",
        date_format(col(timestamp_col), "EEEE")
    ).withColumn(
        "measuredat_local",
        when(
            col("timezone_id").isNotNull(),
            date_format(from_utc_timestamp(col(timestamp_col), col("timezone_id")), "dd-MM-yyyy HH:mm:ss")
        ).otherwise(
            date_format(col(timestamp_col), "dd-MM-yyyy HH:mm:ss")
        )
    ).withColumn(
        "weekday_local",
        when(
            col("timezone_id").isNotNull(),
            date_format(from_utc_timestamp(col(timestamp_col), col("timezone_id")), "EEEE")
        ).otherwise(
            date_format(col(timestamp_col), "EEEE")
        )
    )

def calculate_accelerometer_time_fields(df, timestamp_col="measuredat"):
    """Add formatted UTC & local timestamps plus weekday labels for accelerometer data.
    Uses effective_timezone pattern consistent with screen/call scripts.
    """
    # Add effective timezone with fallback logic (same as screen/call scripts)
    df_with_times = df.withColumn(
        "effective_timezone",
        when(col("timezone_id").isNotNull(), col("timezone_id")).otherwise("UTC")
    )

    return df_with_times.withColumn(
        "measuredat_utc",
        date_format(col(timestamp_col), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "weekday_utc",
        date_format(col(timestamp_col), "EEEE")
    ).withColumn(
        "measuredat_local",
        date_format(from_utc_timestamp(col(timestamp_col), col("effective_timezone")), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "weekday_local",
        date_format(from_utc_timestamp(col(timestamp_col), col("effective_timezone")), "EEEE")
    )

# -----------------------
# GPS/Location Processing Functions
# -----------------------
def extract_lat_lon_from_location(df):
    """Alternative approach using split function"""
    # Split by pipe and take first two elements
    split_parts = split(col("location_decrypted"), "\\|")
    
    # Trim whitespace from first two parts
    lat_part = trim(split_parts.getItem(0))
    lon_part = trim(split_parts.getItem(1))
    
    # Try to cast to double - will return null if not numeric
    lat = when(lat_part.rlike("^[+-]?\\d+\\.\\d+$"), lat_part.cast("double"))
    lon = when(lon_part.rlike("^[+-]?\\d+\\.\\d+$"), lon_part.cast("double"))
    
    return df \
        .withColumn("lat", lat) \
        .withColumn("lon", lon)


def calculate_haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance between two points in meters."""
    # Convert to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    
    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = sin(dlat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Earth radius in meters
    r = 6371000
    return c * r

def compute_gps_features(df):
    """Compute all GPS features: haversine distance, stay points, entropy, variance."""
    from pyspark.sql.functions import lag, lead, sum as spark_sum, count, var_pop, log, when, isnan, isnull
    
    # Ensure measuredat is a timestamp for timezone matching using a robust method
    df = df.withColumn("measuredat", to_timestamp(col("measuredat"), "yyyy-MM-dd HH:mm:ss"))
    
    # Calculate time differences and distances
    window_spec = Window.partitionBy("participantid", "date_local").orderBy("measuredat")
    
    df_with_features = df.withColumn(
        "prev_lat", lag("lat").over(window_spec)
    ).withColumn(
        "prev_lon", lag("lon").over(window_spec)
    ).withColumn(
        "next_measuredat", lead("measuredat").over(window_spec)
    ).withColumn(
        "time_spent_seconds",
        when(col("next_measuredat").isNotNull(),
             col("next_measuredat").cast("long") - col("measuredat").cast("long")
        ).otherwise(0)
    )
    
    # Calculate haversine distances
    df_with_features = df_with_features.withColumn(
        "haversine_distance",
        when(
            (col("prev_lat").isNotNull()) & (col("prev_lon").isNotNull()) &
            (~isnan(col("lat"))) & (~isnan(col("lon"))) &
            (~isnan(col("prev_lat"))) & (~isnan(col("prev_lon"))),
            calculate_haversine_distance(col("prev_lat"), col("prev_lon"), col("lat"), col("lon"))
        ).otherwise(0.0)
    )
    
    return df_with_features

def aggregate_daily_gps_features(df):
    """Aggregate GPS features by participant and date."""
    from pyspark.sql.functions import sum as spark_sum, count, var_pop, log, when, collect_list, size, countDistinct, concat, round, lit, sqrt
    
    # Filter for stay points (>= 10 minutes = 600 seconds)
    stay_points_df = df.filter(col("time_spent_seconds") >= 600)
    
    # Daily aggregations
    daily_features = df.groupBy("participantid", "date_utc", "date_local").agg(
        # Total haversine distance
        spark_sum("haversine_distance").alias("total_haversine_meters"),
        
        # Location variance (sum of lat/lon variances)
        when(
            count("lat") > 1,
            var_pop("lat") + var_pop("lon")
        ).otherwise(None).alias("location_variance"),
        
        # Radius of Gyration (square root of the sum of variances) - ADDED
        when(
            count("lat") > 1,
            sqrt(var_pop("lat") + var_pop("lon"))
        ).otherwise(None).alias("radius_of_gyration")
    )
    
    # Stay points features
    stay_features = stay_points_df.groupBy("participantid", "date_utc", "date_local").agg(
        # Number of unique stay points (rounded to 4 decimal places)
        countDistinct(
            concat(
                round(col("lat"), 4).cast("string"),
                lit("_"),
                round(col("lon"), 4).cast("string")
            )
        ).alias("number_of_stay_points"),
        
        # Total time at clusters
        spark_sum("time_spent_seconds").alias("total_time_at_clusters_seconds"),
        
        # Collect time spent for entropy calculation
        collect_list("time_spent_seconds").alias("time_list")
    )
    
    # Calculate location entropy using a UDF for Shannon entropy
    from pyspark.sql.types import DoubleType
    from pyspark.sql.functions import udf
    import math
    
    def calculate_entropy(time_list, total_time):
        if not time_list or total_time <= 0:
            return 0.0
        entropy = 0.0
        for time_val in time_list:
            if time_val > 0:
                prob = time_val / total_time
                entropy -= prob * math.log(prob)
        return entropy
    
    entropy_udf = udf(calculate_entropy, DoubleType())
    
    stay_features = stay_features.withColumn(
        "location_entropy",
        entropy_udf(col("time_list"), col("total_time_at_clusters_seconds"))
    ).drop("time_list")
    
    # Join daily features with stay features
    result = daily_features.join(
        stay_features, 
        ["participantid", "date_utc", "date_local"], 
        "left"
    ).select(
        col("participantid"),
        col("date_utc"),
        col("date_local"),
        col("total_haversine_meters").cast("float"),
        col("number_of_stay_points").cast("integer"),
        col("total_time_at_clusters_seconds").cast("integer"),
        col("location_entropy"),
        col("location_variance"),
        col("radius_of_gyration") # ADDED
    ).fillna(0, subset=["number_of_stay_points", "total_time_at_clusters_seconds", "location_entropy", "radius_of_gyration"])
    
    return result

    
# -----------------------
# Data Writers
# -----------------------
def write_to_postgres(df, table: str, config: dict, schema: str = "study_test", mode: str = "append"):
    """Write DataFrame to Postgres table with specified mode."""
    df.write.format("jdbc") \
        .option("url", config["staging_db"]["url"]) \
        .option("dbtable", f"{schema}.{table}") \
        .option("user", config["staging_db"]["user"]) \
        .option("password", config["staging_db"]["password"]) \
        .option("stringtype", "unspecified") \
        .mode(mode).save()

def get_sql_type(dtype):
    if isinstance(dtype, StringType):
        return "TEXT"
    elif isinstance(dtype, IntegerType):
        return "INTEGER"
    elif isinstance(dtype, LongType):
        return "BIGINT"
    elif isinstance(dtype, FloatType):
        return "REAL"
    elif isinstance(dtype, DoubleType):
        return "DOUBLE PRECISION"
    elif isinstance(dtype, TimestampType):
        return "TIMESTAMP"
    elif isinstance(dtype, DateType):
        return "DATE"
    elif isinstance(dtype, BooleanType):
        return "BOOLEAN"
    else:
        return "TEXT"  # Default type

def upsert_to_postgres(df, table: str, config: dict, composite_keys: list, schema: str = "study_test"):
    """Upsert DataFrame to Postgres table using composite keys."""
    # Create temporary table name
    temp_table = f"{table}_temp"
    
    # Deduplicate DataFrame based on composite keys before writing
    # Keep the last occurrence of each composite key combination
    logger = logging.getLogger(__name__)
    original_count = df.count()
    
    # Create a window to rank duplicates by composite keys
    window_spec = Window.partitionBy(*composite_keys).orderBy(col("participantid").desc())
    df_deduped = df.withColumn("row_number", row_number().over(window_spec)) \
                   .filter(col("row_number") == 1) \
                   .drop("row_number")
    
    deduped_count = df_deduped.count()
    if original_count != deduped_count:
        logger.warning(f"Removed {original_count - deduped_count} duplicate rows based on composite keys")
    
    # Write to temporary table
    write_to_postgres(df_deduped, temp_table, config, schema, mode="overwrite")
    
    # Perform upsert using SQL
    try:
        conn = psycopg2.connect(
            dbname="staging_db",
            user=config["db_connection"]["user"],
            password=config["db_connection"]["password"],
            host=config["db_connection"]["host"],
            port=config["db_connection"]["port"],
        )
        cur = conn.cursor()
        
        # Get column names from DataFrame
        columns = df_deduped.columns
        
        # Dynamically create the CREATE TABLE statement
        columns_with_types = [f"{col} {get_sql_type(df_deduped.schema[col].dataType)}" for col in columns]
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            {', '.join(columns_with_types)},
            PRIMARY KEY ({', '.join(composite_keys)})
        )
        """
        cur.execute(create_table_sql)

        # Build the upsert query using INSERT ... ON CONFLICT
        insert_columns = ", ".join(columns)
        update_assignments = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in composite_keys])
        
        upsert_sql = f"""
        INSERT INTO {schema}.{table} ({insert_columns})
        SELECT {insert_columns} FROM {schema}.{temp_table}
        ON CONFLICT ({', '.join(composite_keys)}) 
        DO UPDATE SET {update_assignments}
        """
        
        cur.execute(upsert_sql)
        
        # Drop temporary table
        cur.execute(f"DROP TABLE {schema}.{temp_table}")
        
        conn.commit()
        logger.info(f"Upserted {deduped_count} records to {schema}.{table}")
        
    except Exception as e:
        logger.error(f"Error upserting to {schema}.{table}: {str(e)}")
        raise
