SELECT
name,
COUNT(push_id) AS push_count
FROM {{ ref('stg_github_pushes') }}
GROUP BY name
HAVING COUNT(push_id) <= 1000
ORDER BY push_count DESC
LIMIT 10