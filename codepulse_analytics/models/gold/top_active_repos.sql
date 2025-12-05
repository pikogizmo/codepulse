select
    repo_name,
    count(*) as push_count,
    count(distinct actor_name) as contributor_count
from {{ ref('stg_github_pushes') }}
group by repo_name
order by push_count desc
limit 10