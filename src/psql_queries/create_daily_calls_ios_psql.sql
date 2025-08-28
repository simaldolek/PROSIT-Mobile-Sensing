/*
====================================================================================
Function : create_daily_calls_ios_psql

Purpose  : Summarizes daily iOS call events into a versioned daily summary table.
====================================================================================
*/

DROP FUNCTION IF EXISTS create_daily_calls_ios_psql(TEXT, TEXT);

CREATE OR REPLACE FUNCTION create_daily_calls_ios_psql(
    study_schema_name TEXT,
    run_date TEXT  -- Format: 'YYYYMMDD'
) RETURNS VOID AS $$
DECLARE
    safe_study_name TEXT;
    src_table TEXT;
    dest_table TEXT;
BEGIN
    -- Validate inputs
    IF study_schema_name IS NULL OR study_schema_name = '' THEN
        RAISE EXCEPTION 'Study schema name cannot be empty';
    END IF;
    IF run_date IS NULL OR run_date = '' THEN
        RAISE EXCEPTION 'Run date cannot be empty';
    END IF;

    -- Sanitize schema name
    safe_study_name := regexp_replace(study_schema_name, '[^a-zA-Z0-9]', '_', 'g');

    -- Build table names dynamically
    src_table  := format('%I.derived_events_call_ios_pyspark_%s', study_schema_name, run_date);
    dest_table := format('%I.feature_daily_calls_ios_psql_%s', study_schema_name, run_date);

    -- Drop the destination table if exists
    EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', dest_table);

    -- Create the destination table
    EXECUTE format($sql$
        CREATE TABLE %s (
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

            CONSTRAINT pk_daily_calls_ios_%s_%s PRIMARY KEY (participantid, date_local)
        );
    $sql$, dest_table, safe_study_name, run_date);

    -- Populate the table with aggregated data (using 6AM local boundary)
    EXECUTE format($sql$
        INSERT INTO %s (
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
            (
                CASE 
                    WHEN (event_start_time_local::timestamp)::time >= TIME '06:00:00' 
                        THEN (event_start_time_local::timestamp)::date
                    ELSE ((event_start_time_local::timestamp)::date - INTERVAL '1 day')::date
                END
            ) AS date_local,
            MAX(TRIM(TO_CHAR(
                CASE 
                    WHEN (event_start_time_local::timestamp)::time >= TIME '06:00:00' 
                        THEN (event_start_time_local::timestamp)::date
                    ELSE ((event_start_time_local::timestamp)::date - INTERVAL '1 day')::date
                END, 'Day'
            ))) AS weekday_local,
            COUNT(*) AS num_all_calls,
            COALESCE(SUM(call_duration_in_secs), 0)::INTEGER AS duration_all_calls_seconds,
            COUNT(*) FILTER (WHERE call_type = 'outgoing_call_answered') AS num_calls_made,
            COALESCE(SUM(call_duration_in_secs) FILTER (WHERE call_type = 'outgoing_call_answered'), 0)::INTEGER AS duration_calls_made_seconds,
            COUNT(*) FILTER (WHERE call_type = 'incoming_call_answered') AS num_calls_received,
            COALESCE(SUM(call_duration_in_secs) FILTER (WHERE call_type = 'incoming_call_answered'), 0)::INTEGER AS duration_calls_received_seconds,
            COUNT(*) FILTER (WHERE call_type = 'incoming_call_unanswered') AS num_missed_calls,
            COUNT(*) FILTER (WHERE call_type = 'outgoing_call_unanswered') AS num_rejected_calls
        FROM %s
        GROUP BY participantid,
            (
                CASE 
                    WHEN (event_start_time_local::timestamp)::time >= TIME '06:00:00' 
                        THEN (event_start_time_local::timestamp)::date
                    ELSE ((event_start_time_local::timestamp)::date - INTERVAL '1 day')::date
                END
            )
    $sql$, dest_table, src_table);

    -- Create unique indexes for this run
    EXECUTE format('CREATE INDEX idx_pid_%s ON %s (participantid)', run_date, dest_table);
    EXECUTE format('CREATE INDEX idx_date_%s ON %s (date_local)', run_date, dest_table);

    RAISE NOTICE 'Created table % from %', dest_table, src_table;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
SELECT create_daily_calls_ios_psql('user1_workspace', '20250818');

