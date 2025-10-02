-- File: dbt/oura_pipeline/models/marts/fct_sleep_epochs.sql

-- step 1 create movement aggregate 5 minutes
-- step 2 pivot the other metrics. align time stamps across all metrics. basically get it to minutes, 45,50,55,etc
-- step 3 join

WITH timeseries AS (
    SELECT * FROM {{ref('int_sleep_timeseries')}}
),

movement AS (
    SELECT
        sleep_id,
        DATE_TRUNC('hour', timestamp) +
            (EXTRACT(minute FROM timestamp)::int/5*5)*INTERVAL '1 minute' AS epoch_timestamp --floor division trick
        MAX(metric_value) AS max_movement,
        AVG(metric_value) AS avg_movement
    FROM timeseries
    WHERE metric_name = 'movement'
    GROUP BY sleep_id, epoch_timestamp
),

pivoted AS (
    SELECT 
        sleep_id,
        DATE_TRUNC('hour', timestamp) +
            (EXTRACT(minute FROM timestamp)::int/5*5)*INTERVAL '1 minute' AS epoch_timestamp --floor division trick
        MAX(CASE WHEN metric_name = 'hrv' THEN metric_value END) AS hrv,
        MAX(CASE WHEN metric_name = 'heart_rate' THEN metric_value END) AS heart_rate,
        MAX(CASE WHEN metric_name = 'sleep_phase' THEN metric_value END) AS sleep_phase
    FROM timeseries
    WHERE metric_name IN ('hrv','heart_rate','sleep_phase')
    GROUP BY sleep_id, epoch_timestamp
)

SELECT
    pivoted.sleep_id,
    pivoted.epoch_timestamp,
    pivoted.hrv,
    pivoted.heart_rate,
    pivoted.sleep_phase,
    movement.max_movement,
    movement.avg_movement
FROM pivoted
LEFT JOIN movement 
ON pivoted.sleep_id = movement.sleep_id
AND pivoted.epoch_timestamp = movement.epoch_timestamp
ORDER BY pivoted.epoch_timestamp