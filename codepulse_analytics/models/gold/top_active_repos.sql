select
    repo_name,
    count(*) as push_count
from {{ ref('stg_github_pushes') }}
group by repo_name
having count(*) <= 1000
order by push_count desc
limit 10