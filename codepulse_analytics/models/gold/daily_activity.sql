select
    date(created_at) as event_date,
    type as event_type,
    count(*) as event_count
from {{ ref('stg_github_events') }}
group by 1, 2
