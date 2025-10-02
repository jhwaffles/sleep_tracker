-- File: dbt/oura_pipeline/models/marts/fct_daily_sleep.sql

WITH daily_sleep AS (
    SELECT * FROM {{ ref('stg_daily_sleep') }}
),

sleep_records AS (
    SELECT * FROM {{ ref('stg_sleep') }}
)

SELECT
    daily_sleep.sleep_score,
    daily_sleep.sleep_score_contributor_efficiency,
    daily_sleep.sleep_score_contributor_latency,
    daily_sleep.sleep_score_contributor_rem_sleep,
    daily_sleep.sleep_score_contributor_restfulness,
    daily_sleep.sleep_score_contributor_timing,
    daily_sleep.sleep_score_contributor_total_sleep,
    sleep_records.day,
    (sleep_records.day - INTERVAL '1 day') AS day_minus_one,
    sleep_records.awake_time,
    sleep_records.bedtime_end,
    sleep_records.bedtime_start,
    sleep_records.time_in_bed_duration,
    sleep_records.deep_sleep_duration,
    sleep_records.rem_sleep_duration,
    sleep_records.total_sleep_duration,
    sleep_records.latency_duration,
    sleep_records.light_sleep_duration,
    sleep_records.efficiency,
    sleep_records.restless_periods,
    sleep_records.readiness_score,
    sleep_records.readiness_temperature_deviation,
    sleep_records.readiness_temperature_trend_deviation,
    sleep_records.readiness_activity_balance,
    sleep_records.readiness_body_temperature,
    sleep_records.hrv_balance,
    sleep_records.previous_day_activity,
    sleep_records.previous_night,
    sleep_records.recovery_index,
    sleep_records.resting_heart_rate,
    sleep_records.sleep_balance

FROM
daily_sleep
LEFT JOIN sleep_records
ON
daily_sleep.day = sleep_records.day