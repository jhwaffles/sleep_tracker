-- File: dbt/models/staging/stg_sleep.sql

WITH source AS (
    -- We select everything from the raw source table first
    SELECT * FROM {{ source('public', 'raw_sleep') }}
),

ranked AS (
    -- Next, we rank the sleep periods for each day
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY total_sleep_duration DESC) as sleep_period_rank
    FROM
        source
)

SELECT
    id,
    day,
    awake_time,
    bedtime_end,
    bedtime_start,
    time_in_bed AS time_in_bed_duration,
    deep_sleep_duration,
    rem_sleep_duration,
    total_sleep_duration,
    latency AS latency_duration, 
    light_sleep_duration,
    efficiency,
    sleep_algorithm_version,
    sleep_analysis_reason,
    restless_periods,
    heart_rate,
    hrv,
    movement_30_sec,
    sleep_phase_5_min,
    (readiness::jsonb ->> 'score')::INTEGER AS readiness_score,
    (readiness::jsonb ->> 'temperature_deviation')::FLOAT AS readiness_temperature_deviation,
    (readiness::jsonb ->> 'temperature_trend_deviation')::FLOAT AS readiness_temperature_trend_deviation,
    (readiness::jsonb -> 'contributors' ->> 'activity_balance')::INTEGER AS readiness_activity_balance,
    (readiness::jsonb -> 'contributors' ->> 'body_temperature')::INTEGER AS readiness_body_temperature,
    (readiness::jsonb -> 'contributors' ->> 'hrv_balance')::INTEGER AS hrv_balance,
    (readiness::jsonb -> 'contributors' ->> 'previous_day_activity')::INTEGER AS previous_day_activity,
    (readiness::jsonb -> 'contributors' ->> 'previous_night')::INTEGER AS previous_night,
    (readiness::jsonb -> 'contributors' ->> 'recovery_index')::INTEGER AS recovery_index,
    (readiness::jsonb -> 'contributors' ->> 'resting_heart_rate')::INTEGER AS resting_heart_rate,
    (readiness::jsonb -> 'contributors' ->> 'sleep_balance')::INTEGER AS sleep_balance,
    type AS sleep_type

FROM
    ranked
WHERE
    sleep_period_rank = 1