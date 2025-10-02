SELECT
id,
cast(day AS date),
score::INTEGER AS sleep_score,
cast(timestamp AS timestamptz),
(contributors::jsonb ->> 'efficiency')::INTEGER AS sleep_score_contributor_efficiency,
(contributors::jsonb ->> 'latency')::INTEGER AS sleep_score_contributor_latency,
(contributors::jsonb ->> 'rem_sleep')::INTEGER AS sleep_score_contributor_rem_sleep,
(contributors::jsonb ->> 'restfulness')::INTEGER AS sleep_score_contributor_restfulness,
(contributors::jsonb ->> 'timing')::INTEGER AS sleep_score_contributor_timing,
(contributors::jsonb ->> 'total_sleep')::INTEGER AS sleep_score_contributor_total_sleep

FROM
{{source('public','raw_daily_sleep') }}