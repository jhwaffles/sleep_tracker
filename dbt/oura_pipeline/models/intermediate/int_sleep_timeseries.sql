-- File: dbt/models/intermediate/int_sleep_timeseries.sql

WITH sleep_records AS (
    SELECT * FROM {{ ref('stg_sleep') }}
),

hrv_base AS (
    SELECT
        sleep_records.id AS sleep_id,
        sleep_records.day,
        (
            (hrv::jsonb ->> 'timestamp')::TIMESTAMP WITH TIME ZONE + 
            (hrv_data.ordinality - 1) * (hrv::jsonb ->> 'interval')::FLOAT * INTERVAL '1 second'
        ) AS timestamp,
        'hrv' AS metric_name,
        hrv_data.value::FLOAT AS metric_value

    FROM
        sleep_records,
        LATERAL jsonb_array_elements_text(hrv::jsonb -> 'items') WITH ORDINALITY AS hrv_data(value, ordinality)
    WHERE
        sleep_records.hrv IS NOT NULL AND hrv_data.value IS NOT NULL
),

heart_rate_base AS (
    SELECT
        sleep_records.id AS sleep_id,
        sleep_records.day,
        (
            (heart_rate::jsonb ->> 'timestamp')::TIMESTAMP WITH TIME ZONE + 
            (heart_rate_data.ordinality - 1) * (heart_rate::jsonb ->> 'interval')::FLOAT * INTERVAL '1 second'
        ) AS timestamp,
        'heart_rate' AS metric_name,
        heart_rate_data.value::FLOAT AS metric_value
    FROM
        sleep_records,
        LATERAL jsonb_array_elements_text(heart_rate::jsonb -> 'items') WITH ORDINALITY AS heart_rate_data(value,ordinality)
    WHERE
        sleep_records.hrv IS NOT NULL AND heart_rate_data.value IS NOT NULL
),


movement_base AS (
    SELECT
        sleep_records.id AS sleep_id,
        sleep_records.day,
        (
            sleep_records.bedtime_start::TIMESTAMPTZ + (movement_data.ordinality - 1)* INTERVAL '30 second'
        ) AS timestamp,
        'movement' AS metric_name,
        movement_data.value::INTEGER AS metric_value
    FROM
        sleep_records,
        LATERAL regexp_split_to_table(sleep_records.movement_30_sec,'') WITH ORDINALITY AS movement_data(value, ordinality)
    WHERE
        sleep_records.movement_30_sec IS NOT NULL

),

sleep_phase_base AS (
    SELECT
        sleep_records.id AS sleep_id,
        sleep_records.day,
        (   
            sleep_records.bedtime_start::TIMESTAMPTZ + (sleep_phase_data.ordinality - 1)*INTERVAL '300 second'
        ) AS timestamp,
        'sleep_phase' AS metric_name,
        sleep_phase_data.value::INTEGER AS metric_value
    FROM
        sleep_records,
        LATERAL regexp_split_to_table(sleep_records.sleep_phase_5_min,'') WITH ORDINALITY AS sleep_phase_data(value,ordinality)
    WHERE
       sleep_records.sleep_phase_5_min IS NOT NULL
)

-- TODO: Combine all four base models into one final table using UNION ALL
SELECT * FROM hrv_base
UNION ALL
SELECT * FROM heart_rate_base
UNION ALL
SELECT * FROM movement_base
UNION ALL
SELECT * FROM sleep_phase_base
-- ... etc.
