with contributor_pushes as (
    select
        repo_name,
        actor_name,
        count(*) as actor_push_count
    from {{ ref('stg_github_pushes') }}
    group by repo_name, actor_name
),

repo_stats as (
    select
        repo_name,
        sum(actor_push_count) as total_pushes,
        count(distinct actor_name) as contributor_count,
        min(actor_push_count) as min_contributor_pushes
    from contributor_pushes
    group by repo_name
    having count(distinct actor_name) >= 2
)

select
    repo_name,
    total_pushes,
    contributor_count,
    min_contributor_pushes
from repo_stats
order by min_contributor_pushes desc
limit 10