/*
============================================================================
Function : create_feature_daily_accelerometer_ios_timeofday_4parts_psql
 
Purpose  : For a given study schema, computes daily iOS accelerometer
           aggregates based on a 4-part 6AM–6AM day cycle, using event-based
           accelerometer data (seconds). Assumes 86,400 seconds per day.
============================================================================
*/

-- Drop existing function if exists
DROP FUNCTION IF EXISTS create_feature_daily_accelerometer_ios_timeofday_4parts_psql(TEXT);

CREATE OR REPLACE FUNCTION create_feature_daily_accelerometer_ios_timeofday_4parts_psql(
    study_schema_name TEXT
) RETURNS VOID AS $$
DECLARE
    max_batch INTEGER := 0;
    i INTEGER;
BEGIN
    IF study_schema_name IS NULL OR study_schema_name = '' THEN
        RAISE EXCEPTION 'Study schema name cannot be empty';
    END IF;

    -- Create output table if not exists
    EXECUTE format($sql$
        CREATE TABLE IF NOT EXISTS %I.feature_daily_activity_ios_timeofday_4parts_psql_20250818 (
            participantid TEXT NOT NULL,
            date_local DATE NOT NULL,
            weekday_local TEXT,
            non_vigorous_pa_seconds INTEGER,
            vigorous_pa_seconds INTEGER,
            total_active_seconds INTEGER,
            percentage_vigorous_active_daily NUMERIC(5,2),
            morning_pa_seconds INTEGER,
            afternoon_pa_seconds INTEGER,
            evening_pa_seconds INTEGER,
            nighttime_pa_seconds INTEGER,
            avg_euclidean_norm NUMERIC(10,4),
            max_euclidean_norm NUMERIC(10,4),
            activity_variability NUMERIC(10,4),
            PRIMARY KEY (participantid, date_local)
        );
    $sql$, study_schema_name);

    -- Build a temporary participant list and batch them in groups of 10
    CREATE TEMP TABLE IF NOT EXISTS pid_batches (
        participantid TEXT PRIMARY KEY,
        batch_no INTEGER
    ) ON COMMIT DROP;

    -- Refresh temp table for this run
    TRUNCATE pid_batches;

    -- Populate participant ids with batch numbers (10 per batch)
    EXECUTE format($sql$
        INSERT INTO pid_batches (participantid, batch_no)
        SELECT participantid,
               ((ROW_NUMBER() OVER (ORDER BY participantid) - 1) / 10) + 1 AS batch_no
        FROM (
            SELECT DISTINCT participantid
            FROM %I.derived_accelerometer_ios_pyspark_20250818
        ) d
    $sql$, study_schema_name);

    -- Determine total batches
    SELECT COALESCE(MAX(batch_no), 0) INTO max_batch FROM pid_batches;

    -- Loop over batches and insert per batch
    FOR i IN 1..max_batch LOOP
        EXECUTE format($sql$
            WITH base_data AS (
                SELECT 
                    s.participantid,
                    COALESCE(
                        CASE
                            WHEN s.measuredat_local LIKE '%%T%%' THEN TO_TIMESTAMP(REPLACE(s.measuredat_local, 'T', ' '), 'YYYY-MM-DD HH24:MI:SS.MS')
                            WHEN s.measuredat_local LIKE '____-__-__ __:__:__' THEN TO_TIMESTAMP(s.measuredat_local, 'YYYY-MM-DD HH24:MI:SS')
                            ELSE NULL
                        END,
                        CASE
                            WHEN s.measuredat_utc LIKE '%%T%%' THEN TO_TIMESTAMP(REPLACE(s.measuredat_utc, 'T', ' '), 'YYYY-MM-DD HH24:MI:SS.MS')
                            WHEN s.measuredat_utc LIKE '____-__-__ __:__:__' THEN TO_TIMESTAMP(s.measuredat_utc, 'YYYY-MM-DD HH24:MI:SS')
                            ELSE NULL
                        END
                    ) AS ts,
                    s.non_vigorous_pa::DOUBLE PRECISION AS non_vigorous_pa,
                    s.vigorous_pa::DOUBLE PRECISION AS vigorous_pa,
                    s.euclidean_norm_g::DOUBLE PRECISION AS euclidean_norm
                FROM %I.derived_accelerometer_ios_pyspark_20250818 s
                INNER JOIN pid_batches pb ON pb.participantid = s.participantid
                WHERE pb.batch_no = %s
                  AND (s.euclidean_norm_g::DOUBLE PRECISION) BETWEEN 0 AND 20
            ),

            classified AS (
                SELECT 
                    participantid,
                    CASE 
                        WHEN ts::time >= TIME '06:00:00' THEN DATE(ts)
                        ELSE DATE(ts - INTERVAL '1 day')
                    END AS date_local,
                    TRIM(TO_CHAR(ts, 'Day')) AS weekday_local,
                    non_vigorous_pa,
                    vigorous_pa,
                    euclidean_norm,
                    CASE 
                        WHEN ts::time BETWEEN TIME '06:00:00' AND TIME '11:59:59' THEN 'morning'
                        WHEN ts::time BETWEEN TIME '12:00:00' AND TIME '17:59:59' THEN 'afternoon'
                        WHEN ts::time BETWEEN TIME '18:00:00' AND TIME '23:59:59' THEN 'evening'
                        ELSE 'nighttime'
                    END AS time_bucket
                FROM base_data
                WHERE ts IS NOT NULL
            ),

            aggregated AS (
                SELECT 
                    participantid,
                    date_local,
                    MAX(weekday_local) AS weekday_local,
                    SUM(non_vigorous_pa)::INTEGER AS non_vigorous_pa_seconds,
                    SUM(vigorous_pa)::INTEGER AS vigorous_pa_seconds,
                    SUM(non_vigorous_pa + vigorous_pa)::INTEGER AS total_active_seconds,
                    ROUND(((SUM(vigorous_pa)::NUMERIC * 100.0::NUMERIC) / 86400::NUMERIC), 2) AS percentage_vigorous_active_daily,
                    SUM(CASE WHEN time_bucket = 'morning' THEN non_vigorous_pa + vigorous_pa ELSE 0 END)::INTEGER AS morning_pa_seconds,
                    SUM(CASE WHEN time_bucket = 'afternoon' THEN non_vigorous_pa + vigorous_pa ELSE 0 END)::INTEGER AS afternoon_pa_seconds,
                    SUM(CASE WHEN time_bucket = 'evening' THEN non_vigorous_pa + vigorous_pa ELSE 0 END)::INTEGER AS evening_pa_seconds,
                    SUM(CASE WHEN time_bucket = 'nighttime' THEN non_vigorous_pa + vigorous_pa ELSE 0 END)::INTEGER AS nighttime_pa_seconds,
                    AVG(euclidean_norm) AS avg_euclidean_norm,
                    MAX(euclidean_norm) AS max_euclidean_norm,
                    COALESCE(STDDEV(euclidean_norm), 0.0) AS activity_variability
                FROM classified
                GROUP BY participantid, date_local
            )

            INSERT INTO %I.feature_daily_activity_ios_timeofday_4parts_psql_20250818 (
                participantid,
                date_local,
                weekday_local,
                non_vigorous_pa_seconds,
                vigorous_pa_seconds,
                total_active_seconds,
                percentage_vigorous_active_daily,
                morning_pa_seconds,
                afternoon_pa_seconds,
                evening_pa_seconds,
                nighttime_pa_seconds,
                avg_euclidean_norm,
                max_euclidean_norm,
                activity_variability
            )
            SELECT * FROM aggregated
            ON CONFLICT (participantid, date_local)
            DO UPDATE SET
                weekday_local = EXCLUDED.weekday_local,
                non_vigorous_pa_seconds = EXCLUDED.non_vigorous_pa_seconds,
                vigorous_pa_seconds = EXCLUDED.vigorous_pa_seconds,
                total_active_seconds = EXCLUDED.total_active_seconds,
                percentage_vigorous_active_daily = EXCLUDED.percentage_vigorous_active_daily,
                morning_pa_seconds = EXCLUDED.morning_pa_seconds,
                afternoon_pa_seconds = EXCLUDED.afternoon_pa_seconds,
                evening_pa_seconds = EXCLUDED.evening_pa_seconds,
                nighttime_pa_seconds = EXCLUDED.nighttime_pa_seconds,
                avg_euclidean_norm = EXCLUDED.avg_euclidean_norm,
                max_euclidean_norm = EXCLUDED.max_euclidean_norm,
                activity_variability = EXCLUDED.activity_variability;
        $sql$, study_schema_name, i, study_schema_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT create_feature_daily_accelerometer_ios_timeofday_4parts_psql('user1_workspace');