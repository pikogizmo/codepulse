select
    created_at,
    safe_cast(json_value(payload, '$.push_id') as int64) as push_id,
    repo.name as repo_name
from {{ ref('stg_github_events') }}
where type = 'PushEvent'