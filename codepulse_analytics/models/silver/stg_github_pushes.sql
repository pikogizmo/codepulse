SELECT
  created_at,
  SAFE_CAST(JSON_VALUE(payload, '$.push_id') AS INT64) as push_id,
  repo.name
FROM {{ ref('stg_github_events') }}
WHERE type = 'PushEvent'