# PROSIT Mobile Sensing – PySpark Processing Pipeline

This document explains the end‑to‑end data processing pipeline for PROSIT Mobile Sensing using PySpark and PostgreSQL. It covers both iOS and Android data and describes the components from raw data preparation through feature computation.


## Overview
- The pipeline transforms raw sensor/event data into derived events and daily features.
- Processing stages:
  1) Decrypt and normalize location data.
  2) Build a timezone lookup table per participant with time ranges.
  3) Compute intermediate/derived events and features using PySpark scripts.
  4) Aggregate final features in PostgreSQL using SQL scripts.


## Prerequisites
- Spark cluster or local standalone with master at `spark://localhost:7077` (configurable).
- PostgreSQL with a database named `staging_db` and schemas:
  - Input/raw schemas: typically `study_test` (configurable).
  - Output schemas: e.g., `user1_workspace` (configurable).
- PostgreSQL JDBC driver `postgresql-42.6.0.jar` available to Spark.
- Configuration file at `config/config.ini` with a section `DATABASE_PSQL` containing `host`, `port`, `user`, `password`.
- Python packages: pyspark, psycopg2, pandas, rncryptor, timezonefinder (see helper modules).


## Helper Modules (under `src/helper/`)
- `config_loader.py`: Load decryption keys and DB credentials.
- `location_processing.py`: Decrypt location payloads and map lat/lon to IANA timezones.
- `postgresql_hook.py`: Connection pooling and convenient DB operations.
- `logging_setup.py`: Timestamped log configuration.
- `timing_decorator.py`: Function timing logs.
- `pyspark_utilities.py`: Reusable Spark session creation, Postgres readers/writers, timezone matching, and common time field utilities.


## Stage 1: Location Decryption and Normalization
- Script: `src/W_dev_location_processing.py`
- Purpose:
  - Decrypt `location.value1` (or equivalent) into `location.location_decrypted` when `column_name = 'location_decrypted'`.
  - Compute `location.timezone_id` using lat/lon extracted from `location_decrypted` when `column_name = 'timezone_id'`.
- Key logic:
  - Batches null targets per participant; runs parallelized by participant IDs.
  - Robust lat/lon parsing via `_extract_lat_lon()`.
  - Timezone computed using `helper/location_processing.get_timezone()`.
- Output: Populates columns `location.location_decrypted` and/or `location.timezone_id` in `study_test.location`.

How to run (example):
```
python3 src/W_dev_location_processing.py
```


## Stage 2: Timezone Lookup Table
- Script: `src/W_timezone_lookup_creation.py`
- Purpose: Build `lookup_timezone` with contiguous ranges of a participant’s timezone over time.
- Key logic:
  - Reads `study_test.location (participantid, measuredat, timezone_id)` excluding `Unknown_Timezone`.
  - Collapses contiguous runs of the same `timezone_id` into segments with `start_time` and `end_time`.
- Output table schema: `lookup_timezone(participantid TEXT, timezone_id TEXT, start_time TIMESTAMP, end_time TIMESTAMP)` in `study_test`.

How to run (example):
```
python3 src/W_timezone_lookup_creation.py
```


## Stage 3: PySpark Derived Events and Features
Common utilities used across scripts:
- Spark session: `helper/pyspark_utilities.create_spark_session()`.
- Read from Postgres: `read_event_data()`, `read_location_data()`, etc.
- Timezone attachment: `get_best_timezone_match(events_df, timezone_df)` using `lookup_timezone`.
- Time field computations: `calculate_event_times()`, `calculate_point_times()`.
- Upsert results: `upsert_to_postgres()` with composite keys.

All scripts assume:
- Input schema: `study_test` (configurable per file as `INPUT_SCHEMA` or similar).
- Output schema: `user1_workspace` (or configured constant per file).
- Spark master: `spark://localhost:7077` (configurable).


### 3A. Screen Events – Android
- Script: `src/W_create_derived_events_screen_android_pyspark.py`
- Inputs: `study_test.powerstate` and `study_test.lookup_timezone`.
- Logic:
  - Normalize `value0` to locked/unlocked.
  - Pair consecutive unlocked → locked events per participant to form screen sessions.
  - Compute UTC/local start/end, date fields, and duration (`screen_on_time_in_secs`).
- Output: `user1_workspace.derived_events_screen_android_pyspark_YYYYMMDD` (composite keys: `participantid, event_start_time_utc, event_end_time_utc`).

Run (example):
```
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  src/W_create_derived_events_screen_android_pyspark.py
```


### 3B. Screen Events – iOS
- Script: `src/W_create_derived_events_screen_ios_pyspark.py`
- Inputs: `study_test.lock_state`, `study_test.analytics` (optional), and `study_test.lookup_timezone`.
- Logic:
  - Combine `lock_state` with `analytics` (if present) where app termination indicates a locked event.
  - Pair unlocked → locked events per participant and derive durations.
- Output: `user1_workspace.derived_events_screen_ios_pyspark_YYYYMMDD` (composite keys: `participantid, event_start_time_utc, event_end_time_utc`).

Run (example):
```
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  src/W_create_derived_events_screen_ios_pyspark.py
```


### 3C. Call Events – iOS
- Script: `src/W_create_derived_events_call_ios_pyspark.py`
- Inputs: `study_test.call`, `study_test.analytics` (optional), and `study_test.lookup_timezone`.
- Logic:
  - Identify complete calls (incoming/outgoing) by sequencing events (incoming/dialing → connected → disconnected).
  - Flag anomalies (calls spanning ≥ 12 hours) for inspection.
  - Compute UTC/local start/end, date fields, and `call_duration_in_secs`.
- Output: `user1_workspace.derived_events_call_ios_pyspark_YYYYMMDD` (composite keys: `participantid, event_start_time_utc, event_end_time_utc`).

Run (example):
```
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  src/W_create_derived_events_call_ios_pyspark.py
```


### 3D. Accelerometer – Android
- Script: `src/W_create_derived_accelerometer_android_pyspark.py`
- Inputs: `study_test.accelerometer_m_s2__x_y_z`, `study_test.lookup_timezone`.
- Logic:
  - Read participant batches; convert m/s^2 to g.
  - Compute `euclidean_norm_g`, `euclidean_distance_g`, and binary flags for non‑vigorous/vigorous PA.
  - Add formatted UTC/local timestamps and dates per point.
- Output: `user1_workspace.derived_accelerometer_android_pyspark_YYYYMMDD` (composite key: `_id`).

Run (example):
```
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  src/W_create_derived_accelerometer_android_pyspark.py
```


### 3E. Accelerometer – iOS
- Script: `src/W_create_derived_accelerometer_ios_pyspark.py`
- Inputs: `study_test.accelerometer`, `study_test.lookup_timezone`.
- Logic:
  - Read participant batches (iOS data already in g units).
  - Compute `euclidean_norm_g`, `euclidean_distance_g`, and PA flags; add UTC/local timestamps and dates.
- Output: `user1_workspace.derived_accelerometer_ios_pyspark_YYYYMMDD` (composite key: `_id`).

Run (example):
```
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  src/W_create_derived_accelerometer_ios_pyspark.py
```


### 3F. GPS/Location Features (Daily)
- Script: `src/W_create_derived_gps_features_pyspark.py`
- Inputs: `study_test.location` with `location_decrypted`, `timezone_id`; and `study_test.lookup_timezone`.
- Logic:
  - Extract lat/lon from `location_decrypted`.
  - Use timezone fallback to UTC when missing; compute date boundaries using a 6 AM local cut.
  - Compute: total haversine distance, number of stay points (≥10 min), total time at clusters, location entropy, location variance, radius of gyration.
- Output: `user1_workspace.feature_daily_gps_pyspark_YYYYMMDD` (composite keys: `participantid, date_utc`).

Run (example):
```
spark-submit \
  --master spark://localhost:7077 \
  --jars src/postgresql-42.6.0.jar \
  src/W_create_derived_gps_features_pyspark.py
```


## Stage 4: Final Feature Computation in PostgreSQL
- SQL scripts are under `src/psql_queries/` and compute daily/weekly features from the derived tables created by the PySpark jobs.
- Examples (non‑exhaustive):
  - `create_daily_accelerometer_android_timeofday_4parts_psql.sql`
  - `create_daily_accelerometer_ios_timeofday_4parts_psql.sql`
  - `create_daily_calls_android_psql.sql`
  - Additional SQL files for screen and sleep features.

Run (example from psql):
```
\i src/psql_queries/create_daily_accelerometer_android_timeofday_4parts_psql.sql
```


### 4.1 Details of PSQL functions (6AM–6AM day boundary)
All functions below aggregate per participant per day using a 6AM local boundary:
- Reassigned date rule: events with time >= 06:00:00 belong to the same calendar date; times 00:00–05:59 are assigned to the previous calendar day.
- `weekday_local` is consistently computed as `TRIM(TO_CHAR(<reassigned date>, 'Day'))`.

Functions and outputs:

- `src/psql_queries/create_daily_accelerometer_android_timeofday_4parts_psql.sql`
  - Function: `create_feature_daily_accelerometer_android_timeofday_4parts_psql(study_schema_name TEXT)`
  - Input table: `%I.derived_accelerometer_android_pyspark_20250808` (from Stage 3D)
  - Output table: `%I.feature_daily_activity_android_timeofday_4parts_psql_20250818`
  - Output columns: `participantid, date_local, weekday_local, non_vigorous_pa_seconds, vigorous_pa_seconds, total_active_seconds, percentage_vigorous_active_daily, morning_pa_seconds, afternoon_pa_seconds, evening_pa_seconds, nighttime_pa_seconds, avg_euclidean_norm, max_euclidean_norm, activity_variability`
  - Run: `SELECT create_feature_daily_accelerometer_android_timeofday_4parts_psql('user1_workspace');`

- `src/psql_queries/create_daily_accelerometer_ios_timeofday_4parts_psql.sql`
  - Function: `create_feature_daily_accelerometer_ios_timeofday_4parts_psql(study_schema_name TEXT)`
  - Input table: `%I.derived_accelerometer_ios_pyspark_20250818` (from Stage 3E)
  - Batching: internal batching by participant in groups of 10
  - Output table: `%I.feature_daily_activity_ios_timeofday_4parts_psql_20250818`
  - Output columns: same schema as Android accelerometer (see above)
  - Run: `SELECT create_feature_daily_accelerometer_ios_timeofday_4parts_psql('user1_workspace');`

- `src/psql_queries/create_daily_screen_android_timeofday_4parts_psql.sql`
  - Function: `create_feature_daily_screen_android_timeofday_4parts_psql_20250818(study_schema_name TEXT)`
  - Input table: `%I.derived_events_screen_android_pyspark_20250818` (from Stage 3A)
  - Logic: splits screen sessions across 4 buckets respecting 6AM boundary; filters out sessions > 3 hours
  - Output table: `%I.feature_daily_screen_android_timeofday_4parts_psql_20250818`
  - Output columns: `participantid, date_local, weekday_local, total_screen_time_in_seconds, num_of_events_total, morning_screen_time_in_seconds, num_of_events_morning, afternoon_screen_time_in_seconds, num_of_events_afternoon, evening_screen_time_in_seconds, num_of_events_evening, nighttime_screen_time_in_seconds, num_of_events_nighttime`
  - Run: `SELECT create_feature_daily_screen_android_timeofday_4parts_psql_20250818('user1_workspace');`

- `src/psql_queries/create_daily_screen_ios_timeofday_4parts_psql.sql`
  - Function: `create_feature_daily_screen_ios_timeofday_4parts_psql_20250818(study_schema_name TEXT)`
  - Input table: `%I.derived_events_screen_ios_pyspark_20250818` (from Stage 3B)
  - Logic: same as Android screen; splits across buckets and uses 6AM boundary; outliers > 3 hours removed
  - Output table: `%I.feature_daily_screen_ios_timeofday_4parts_psql_20250818`
  - Output columns: same schema as Android screen (see above)
  - Run: `SELECT create_feature_daily_screen_ios_timeofday_4parts_psql_20250818('user1_workspace');`

- `src/psql_queries/create_daily_calls_android_psql.sql`
  - Function: `create_daily_calls_android_psql(study_schema_name TEXT)`
  - Input table: `study_prositsm.calls__calldate_calldurations_calltype_phonenumberhash` (raw calls)
  - Logic: 6AM boundary applied to `value0` event time; enforces 0 duration for missed/rejected; aggregates counts/durations
  - Output table: `%I.feature_daily_calls_android_psql_20250818`
  - Output columns: `participantid, date_local, weekday_local, num_all_calls, duration_all_calls_seconds, num_calls_made, duration_calls_made_seconds, num_calls_received, duration_calls_received_seconds, num_missed_calls, num_rejected_calls`
  - Run: `SELECT create_daily_calls_android_psql('user1_workspace');`

- `src/psql_queries/create_daily_calls_ios_psql.sql`
  - Function: `create_daily_calls_ios_psql(study_schema_name TEXT, run_date TEXT)`
  - Input table: `%I.derived_events_call_ios_pyspark_<run_date>` (from Stage 3C)
  - Logic: uses 6AM boundary on `event_start_time_local`; aggregates counts/durations by call type
  - Output table: `%I.feature_daily_calls_ios_psql_<run_date>`
  - Output columns: same schema as Android calls (see above)
  - Run: `SELECT create_daily_calls_ios_psql('user1_workspace', 'YYYYMMDD');`


## Configuration Notes
- Default input schema in scripts is `study_test`. Change via constants at the top of each script.
- Output schema is set to `user1_workspace` in examples; adjust as needed.
- Spark master is set to `spark://localhost:7077`; override in `create_spark_session()` if required.
- JDBC jar path must be reachable by Spark (`--jars` CLI option or `spark.jars` config).


## Operational Tips
- Ensure `W_dev_location_processing.py` and `W_timezone_lookup_creation.py` complete before screen/call/accelerometer/GPS stages.
- Inspect logs under your configured logs directory to verify sample outputs and row counts.
- Upserts are used to avoid duplicates; composite keys are declared per script.
- Large jobs process participants in batches; adjust batch sizes and Spark resources based on cluster capacity.
