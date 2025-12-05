select
    type as event_type,
    count(*) as total_events
from {{ ref('stg_github_events') }}
group by 1
order by 2 desc
