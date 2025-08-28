/*
====================================================================================
Function : create_daily_calls_android_psql

Purpose  : Creates and populates a daily summary table of Android call events.
====================================================================================
*/

-- Drop existing function with its argument type to allow recreation
DROP FUNCTION IF EXISTS create_daily_calls_android_psql(TEXT);

CREATE OR REPLACE FUNCTION create_daily_calls_android_psql(
    study_schema_name TEXT
) RETURNS VOID AS $$
BEGIN
    -- Safety check for the schema name
    IF study_schema_name IS NULL OR study_schema_name = '' THEN
        RAISE EXCEPTION 'Study schema name cannot be empty';
    END IF;

    -- Create the output table if it doesn't exist
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I.feature_daily_calls_android_psql_20250818 (
            participantid VARCHAR(50) NOT NULL,
            date_local DATE NOT NULL,
            weekday_local TEXT,
            num_all_calls INTEGER,
            duration_all_calls_seconds INTEGER,
            num_calls_made INTEGER,
            duration_calls_made_seconds INTEGER,
            num_calls_received INTEGER,
            duration_calls_received_seconds INTEGER,
            num_missed_calls INTEGER,
            num_rejected_calls INTEGER,
            CONSTRAINT pk_daily_calls_android_psql PRIMARY KEY (participantid, date_local)
        );',
        study_schema_name
    );

    -- Main logic: Aggregate data and insert/update the output table
    EXECUTE format($sql$
    WITH base_data AS (
        SELECT
            participantid,
            (
                CASE 
                    WHEN (CAST(value0 AS TIMESTAMP))::time >= TIME '06:00:00' 
                        THEN (CAST(value0 AS TIMESTAMP))::date
                    ELSE ((CAST(value0 AS TIMESTAMP))::date - INTERVAL '1 day')::date
                END
            ) AS date_local,
            TRIM(TO_CHAR(
                CASE 
                    WHEN (CAST(value0 AS TIMESTAMP))::time >= TIME '06:00:00' 
                        THEN (CAST(value0 AS TIMESTAMP))::date
                    ELSE ((CAST(value0 AS TIMESTAMP))::date - INTERVAL '1 day')::date
                END, 'Day'
            )) AS weekday_local,
            CASE
                WHEN value2 = 'outgoing' THEN 'outgoing_call_answered'
                WHEN value2 = 'incoming' THEN 'incoming_call_answered'
                WHEN value2 = 'missed'   THEN 'incoming_call_unanswered'
                WHEN value2 = 'rejected' THEN 'outgoing_call_unanswered'
                ELSE value2
            END AS call_type,
            CASE
                WHEN value2 IN ('missed', 'rejected') THEN 0
                WHEN value1 ~ '^[0-9]+$' THEN CAST(value1 AS INTEGER)
                ELSE 0
            END AS fixed_call_duration_in_secs
        FROM study_prositsm.calls__calldate_calldurations_calltype_phonenumberhash
        WHERE
            value0 ~ '^\d{4}-\d{2}-\d{2}'
            AND value1 ~ '^[0-9]+$'
            AND CAST(value1 AS INTEGER) BETWEEN 0 AND 43200
    )
    INSERT INTO %I.feature_daily_calls_android_psql_20250818 (
        participantid,
        date_local,
        weekday_local,
        num_all_calls,
        duration_all_calls_seconds,
        num_calls_made,
        duration_calls_made_seconds,
        num_calls_received,
        duration_calls_received_seconds,
        num_missed_calls,
        num_rejected_calls
    )
    SELECT
        participantid,
        date_local,
        MAX(weekday_local) AS weekday_local,
        COUNT(*) AS num_all_calls,
        COALESCE(SUM(fixed_call_duration_in_secs), 0)::INTEGER AS duration_all_calls_seconds,
        COUNT(CASE WHEN call_type = 'outgoing_call_answered' THEN 1 END) AS num_calls_made,
        COALESCE(SUM(CASE WHEN call_type = 'outgoing_call_answered' THEN fixed_call_duration_in_secs ELSE 0 END), 0)::INTEGER AS duration_calls_made_seconds,
        COUNT(CASE WHEN call_type = 'incoming_call_answered' THEN 1 END) AS num_calls_received,
        COALESCE(SUM(CASE WHEN call_type = 'incoming_call_answered' THEN fixed_call_duration_in_secs ELSE 0 END), 0)::INTEGER AS duration_calls_received_seconds,
        COUNT(CASE WHEN call_type = 'incoming_call_unanswered' THEN 1 END) AS num_missed_calls,
        COUNT(CASE WHEN call_type = 'outgoing_call_unanswered' THEN 1 END) AS num_rejected_calls
    FROM base_data
    GROUP BY participantid, date_local
    ON CONFLICT (participantid, date_local)
    DO UPDATE SET
        weekday_local = EXCLUDED.weekday_local,
        num_all_calls = EXCLUDED.num_all_calls,
        duration_all_calls_seconds = EXCLUDED.duration_all_calls_seconds,
        num_calls_made = EXCLUDED.num_calls_made,
        duration_calls_made_seconds = EXCLUDED.duration_calls_made_seconds,
        num_calls_received = EXCLUDED.num_calls_received,
        duration_calls_received_seconds = EXCLUDED.duration_calls_received_seconds,
        num_missed_calls = EXCLUDED.num_missed_calls,
        num_rejected_calls = EXCLUDED.num_rejected_calls;
    $sql$, study_schema_name);

    -- Create indexes for performance
    EXECUTE format('
        CREATE INDEX IF NOT EXISTS idx_daily_calls_android_psql_participant ON %I.feature_daily_calls_android_psql_20250818 (participantid);
        CREATE INDEX IF NOT EXISTS idx_daily_calls_android_psql_date ON %I.feature_daily_calls_android_psql_20250818 (date_local);',
        study_schema_name, study_schema_name
    );

    -- Add metadata comments
    EXECUTE format('
        COMMENT ON TABLE %I.feature_daily_calls_android_psql_20250818 IS
        ''Daily aggregates of Android call metrics, including counts and durations for all call types.'';
        COMMENT ON COLUMN %I.feature_daily_calls_android_psql_20250818.duration_all_calls_seconds IS
        ''Total duration of all calls (answered, missed, and rejected) in seconds.'';',
        study_schema_name, study_schema_name
    );
END;
$$ LANGUAGE plpgsql;

-- Example usage
SELECT create_daily_calls_android_psql('user1_workspace');


-- This query provides a high-level summary of the table's contents.
SELECT
    'Basic Checks' AS test_phase,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT participantid) AS distinct_participants,
    COUNT(*) FILTER (WHERE weekday_local IS NULL) AS rows_with_null_weekday,
    COUNT(*) FILTER (WHERE duration_all_calls_seconds < 0) AS negative_duration_rows
FROM user1_workspace.feature_daily_calls_android_psql_20250818;

-- 2. Verify totals match by summing made + received durations = all call durations
-- This check expects 'MATCH' since missed/rejected durations are forced to 0
SELECT
    participantid,
    date_local,
    duration_all_calls_seconds,
    duration_calls_made_seconds + duration_calls_received_seconds AS combined_answered_duration,
    duration_all_calls_seconds - (duration_calls_made_seconds + duration_calls_received_seconds) AS residual_difference,
    CASE
        WHEN duration_all_calls_seconds = (duration_calls_made_seconds + duration_calls_received_seconds)
        THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS validation_status
FROM user1_workspace.feature_daily_calls_android_psql_20250818
WHERE duration_all_calls_seconds > 0
ORDER BY date_local DESC
LIMIT 20;

-- 3. Percentage of records where the durations match
-- This query provides a quantitative summary of the previous check, showing the
-- percentage of records that pass the validation. You should now expect this to
-- show a percentage of 100.00 for 'MATCH'.
WITH check_durations AS (
    SELECT
        duration_all_calls_seconds,
        (duration_calls_made_seconds + duration_calls_received_seconds) AS sum_answered,
        CASE
            WHEN duration_all_calls_seconds = (duration_calls_made_seconds + duration_calls_received_seconds)
            THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS status
    FROM user1_workspace.feature_daily_calls_android_psql_20250818
    WHERE duration_all_calls_seconds > 0
)
SELECT
    status AS validation_status,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM check_durations), 2) AS percentage
FROM check_durations
GROUP BY status
ORDER BY status;

-- 4. Daily Distribution Summary
-- This query gives you an overview of the data's temporal distribution.
SELECT
    date_local,
    COUNT(DISTINCT participantid) AS num_participants,
    SUM(num_all_calls) AS total_calls,
    SUM(duration_all_calls_seconds) AS total_duration_seconds,
    ROUND(AVG(duration_all_calls_seconds), 2) AS avg_duration_per_day
FROM user1_workspace.feature_daily_calls_android_psql_20250818
GROUP BY date_local
ORDER BY date_local DESC
LIMIT 10;

-- 5. Top 10 participants with most call time
-- This helps identify the most active users in the dataset based on total call duration.
SELECT
    participantid,
    SUM(duration_all_calls_seconds) AS total_duration_all_calls,
    SUM(num_all_calls) AS total_num_calls,
    SUM(duration_calls_made_seconds) AS total_made,
    SUM(duration_calls_received_seconds) AS total_received,
    SUM(num_missed_calls) AS missed,
    SUM(num_rejected_calls) AS rejected
FROM user1_workspace.feature_daily_calls_android_psql_20250818
GROUP BY participantid
ORDER BY total_duration_all_calls DESC
LIMIT 10;