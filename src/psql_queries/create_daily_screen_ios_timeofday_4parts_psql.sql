/*
====================================================================================
Function : create_feature_daily_screen_ios_timeofday_4parts_psql_20250818
 
Purpose  :
    Computes daily iOS screen time split into 4 time-of-day buckets over a 6AM–6AM day.
====================================================================================
*/

-- DROP FUNCTION IF EXISTS create_feature_daily_screen_ios_timeofday_4parts_psql_20250818;

CREATE OR REPLACE FUNCTION create_feature_daily_screen_ios_timeofday_4parts_psql_20250818(
    study_schema_name TEXT
) RETURNS VOID AS $$
BEGIN
    -- Validate schema name
    IF study_schema_name IS NULL OR study_schema_name = '' THEN
        RAISE EXCEPTION 'Study schema name cannot be empty';
    END IF;

    -- Create output table if not exists
    EXECUTE format($f$
        CREATE TABLE IF NOT EXISTS %I.feature_daily_screen_ios_timeofday_4parts_psql_20250818 (
            participantid VARCHAR NOT NULL,
            date_local DATE NOT NULL,
            weekday_local VARCHAR(10),
            total_screen_time_in_seconds INTEGER,
            num_of_events_total INTEGER,
            morning_screen_time_in_seconds INTEGER,
            num_of_events_morning INTEGER,
            afternoon_screen_time_in_seconds INTEGER,
            num_of_events_afternoon INTEGER,
            evening_screen_time_in_seconds INTEGER,
            num_of_events_evening INTEGER,
            nighttime_screen_time_in_seconds INTEGER,
            num_of_events_nighttime INTEGER,
            PRIMARY KEY (participantid, date_local)
        );
    $f$, study_schema_name);

    -- Main logic
    EXECUTE format($f$
        WITH base_events AS (
            SELECT 
                participantid,
                CAST(event_start_time_local AS timestamp) AS event_start,
                (CAST(event_start_time_local AS timestamp) + (screen_on_time_in_secs || ' seconds')::interval) AS event_end,
                screen_on_time_in_secs::integer AS total_duration_secs,

                -- Assign date_local using 6AM boundary
                CASE 
                    WHEN CAST(event_start_time_local AS timestamp)::time >= TIME '06:00:00'
                    THEN CAST(event_start_time_local AS date)
                    ELSE CAST(event_start_time_local AS date) - INTERVAL '1 day'
                END::date AS date_local,

                TRIM(TO_CHAR(
                    CASE 
                        WHEN CAST(event_start_time_local AS timestamp)::time >= TIME '06:00:00'
                        THEN CAST(event_start_time_local AS date)
                        ELSE CAST(event_start_time_local AS date) - INTERVAL '1 day'
                    END::date, 'Day'
                )) AS weekday_local
            FROM %I.derived_events_screen_ios_pyspark_20250818
            WHERE screen_on_time_in_secs::integer <= 10800  -- Remove outliers > 3 hours
        ),

        split_buckets AS (
            SELECT 
                participantid,
                date_local,
                weekday_local,
                total_duration_secs,
                event_start,
                event_end,

                -- Time bucket splits
                GREATEST(0, EXTRACT(EPOCH FROM (
                    LEAST(event_end, date_local + TIME '12:00:00') -
                    GREATEST(event_start, date_local + TIME '06:00:00')
                ))::int) AS morning_seconds,

                GREATEST(0, EXTRACT(EPOCH FROM (
                    LEAST(event_end, date_local + TIME '18:00:00') -
                    GREATEST(event_start, date_local + TIME '12:00:00')
                ))::int) AS afternoon_seconds,

                GREATEST(0, EXTRACT(EPOCH FROM (
                    LEAST(event_end, date_local + INTERVAL '1 day') -
                    GREATEST(event_start, date_local + TIME '18:00:00')
                ))::int) AS evening_seconds,

                GREATEST(0, EXTRACT(EPOCH FROM (
                    LEAST(event_end, date_local + INTERVAL '1 day' + TIME '06:00:00') -
                    GREATEST(event_start, date_local + INTERVAL '1 day')
                ))::int) AS nighttime_seconds
            FROM base_events
        ),

        daily_aggregates AS (
            SELECT
                participantid,
                date_local,
                weekday_local,

                -- Totals
                SUM(morning_seconds + afternoon_seconds + evening_seconds + nighttime_seconds) AS total_screen_time_in_seconds,
                COUNT(*) AS num_of_events_total,

                -- Bucket totals
                SUM(morning_seconds) AS morning_screen_time_in_seconds,
                SUM(CASE WHEN morning_seconds > 0 THEN 1 ELSE 0 END) AS num_of_events_morning,

                SUM(afternoon_seconds) AS afternoon_screen_time_in_seconds,
                SUM(CASE WHEN afternoon_seconds > 0 THEN 1 ELSE 0 END) AS num_of_events_afternoon,

                SUM(evening_seconds) AS evening_screen_time_in_seconds,
                SUM(CASE WHEN evening_seconds > 0 THEN 1 ELSE 0 END) AS num_of_events_evening,

                SUM(nighttime_seconds) AS nighttime_screen_time_in_seconds,
                SUM(CASE WHEN nighttime_seconds > 0 THEN 1 ELSE 0 END) AS num_of_events_nighttime
            FROM split_buckets
            GROUP BY participantid, date_local, weekday_local
        )

        INSERT INTO %I.feature_daily_screen_ios_timeofday_4parts_psql_20250818 (
            participantid,
            date_local,
            weekday_local,
            total_screen_time_in_seconds,
            num_of_events_total,
            morning_screen_time_in_seconds,
            num_of_events_morning,
            afternoon_screen_time_in_seconds,
            num_of_events_afternoon,
            evening_screen_time_in_seconds,
            num_of_events_evening,
            nighttime_screen_time_in_seconds,
            num_of_events_nighttime
        )
        SELECT 
            participantid,
            date_local,
            weekday_local,
            COALESCE(total_screen_time_in_seconds, 0),
            COALESCE(num_of_events_total, 0),
            COALESCE(morning_screen_time_in_seconds, 0),
            COALESCE(num_of_events_morning, 0),
            COALESCE(afternoon_screen_time_in_seconds, 0),
            COALESCE(num_of_events_afternoon, 0),
            COALESCE(evening_screen_time_in_seconds, 0),
            COALESCE(num_of_events_evening, 0),
            COALESCE(nighttime_screen_time_in_seconds, 0),
            COALESCE(num_of_events_nighttime, 0)
        FROM daily_aggregates
        ON CONFLICT (participantid, date_local) DO UPDATE SET
            total_screen_time_in_seconds = EXCLUDED.total_screen_time_in_seconds,
            num_of_events_total = EXCLUDED.num_of_events_total,
            morning_screen_time_in_seconds = EXCLUDED.morning_screen_time_in_seconds,
            num_of_events_morning = EXCLUDED.num_of_events_morning,
            afternoon_screen_time_in_seconds = EXCLUDED.afternoon_screen_time_in_seconds,
            num_of_events_afternoon = EXCLUDED.num_of_events_afternoon,
            evening_screen_time_in_seconds = EXCLUDED.evening_screen_time_in_seconds,
            num_of_events_evening = EXCLUDED.num_of_events_evening,
            nighttime_screen_time_in_seconds = EXCLUDED.nighttime_screen_time_in_seconds,
            num_of_events_nighttime = EXCLUDED.num_of_events_nighttime;

    $f$, study_schema_name, study_schema_name, study_schema_name);
END;
$$ LANGUAGE plpgsql;


-- Run the function to create the feature table in user1_workspace
SELECT create_feature_daily_screen_ios_timeofday_4parts_psql_20250818('user1_workspace');

-- ============================================================================
-- Simple Test Query for 4-Bucket Screen Time Function (iOS)
-- Purpose: Validate the simplified function output
-- ============================================================================

-- 1. Quick validation of results
SELECT 
  'Function Output' AS test_phase,
  COUNT(*) AS total_days,
  SUM(CASE WHEN nighttime_screen_time_in_seconds > 0 THEN 1 ELSE 0 END) AS days_with_nighttime,
  SUM(CASE WHEN morning_screen_time_in_seconds > 0 THEN 1 ELSE 0 END) AS days_with_morning,
  SUM(CASE WHEN afternoon_screen_time_in_seconds > 0 THEN 1 ELSE 0 END) AS days_with_afternoon,
  SUM(CASE WHEN evening_screen_time_in_seconds > 0 THEN 1 ELSE 0 END) AS days_with_evening,
  AVG(total_screen_time_in_seconds) AS avg_total_screen_time_in_seconds
FROM user1_workspace.feature_daily_screen_ios_timeofday_4parts_psql_20250818;

-- 2. Sample output showing the expected format
SELECT 
  participantid,
  date_local,
  total_screen_time_in_seconds,
  num_of_events_total,
  morning_screen_time_in_seconds,
  num_of_events_morning,
  afternoon_screen_time_in_seconds,
  num_of_events_afternoon,
  evening_screen_time_in_seconds,
  num_of_events_evening,
  nighttime_screen_time_in_seconds,
  num_of_events_nighttime
FROM user1_workspace.feature_daily_screen_ios_timeofday_4parts_psql_20250818
ORDER BY date_local DESC, participantid
LIMIT 20;

-- 3. Validate that totals match (bucket sums should equal total_screen_time_in_seconds)
SELECT 
  participantid,
  date_local,
  total_screen_time_in_seconds,
  (morning_screen_time_in_seconds + afternoon_screen_time_in_seconds + evening_screen_time_in_seconds + nighttime_screen_time_in_seconds) AS sum_of_buckets,
  CASE 
    WHEN total_screen_time_in_seconds = (morning_screen_time_in_seconds + afternoon_screen_time_in_seconds + evening_screen_time_in_seconds + nighttime_screen_time_in_seconds) 
    THEN 'MATCH' 
    ELSE 'MISMATCH' 
  END AS validation_status
FROM user1_workspace.feature_daily_screen_ios_timeofday_4parts_psql_20250818
WHERE total_screen_time_in_seconds > 0
ORDER BY date_local 
LIMIT 10;


-- 4. Exact query structure with added grouping by validation_status
WITH base_data AS (
  SELECT 
    participantid,
    date_local,
    total_screen_time_in_seconds,
    (morning_screen_time_in_seconds + afternoon_screen_time_in_seconds + evening_screen_time_in_seconds + nighttime_screen_time_in_seconds) AS sum_of_buckets,
    CASE 
      WHEN total_screen_time_in_seconds = (morning_screen_time_in_seconds + afternoon_screen_time_in_seconds + evening_screen_time_in_seconds + nighttime_screen_time_in_seconds) 
      THEN 'MATCH' 
      ELSE 'MISMATCH' 
    END AS validation_status
  FROM user1_workspace.feature_daily_screen_ios_timeofday_4parts_psql_20250818
  WHERE total_screen_time_in_seconds > 0
)

-- Show counts by validation status
SELECT 
  validation_status,
  COUNT(*) AS record_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM base_data), 2) AS percentage
FROM base_data
GROUP BY validation_status
ORDER BY validation_status;
