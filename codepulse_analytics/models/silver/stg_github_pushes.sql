select
    created_at,
    safe_cast(json_value(payload, '$.push_id') as int64) as push_id,
    repo.name as repo_name,
    actor.login as actor_name
from {{ ref('stg_github_events') }}
where type = 'PushEvent'
  and lower(actor.login) not like '%[bot]%'
  and lower(actor.login) not like '%bot'
  and lower(actor.login) not like 'dependabot%'
  and lower(actor.login) not like 'renovate%'
  and lower(actor.login) not like 'github-actions%'