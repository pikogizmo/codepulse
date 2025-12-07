with contributor_pushes as (
    select
        repo_name,
        actor_name,
        count(*) as actor_push_count
    from {{ ref('stg_github_pushes') }}
    group by repo_name, actor_name
),

repo_median as (
    select
        repo_name,
        actor_name,
        actor_push_count,
        percentile_cont(actor_push_count, 0.5) over (partition by repo_name) as median_contributor_pushes
    from contributor_pushes
),

repo_stats as (
    select
        repo_name,
        sum(actor_push_count) as total_pushes,
        count(distinct actor_name) as contributor_count,
        any_value(median_contributor_pushes) as median_contributor_pushes
    from repo_median rm
    group by repo_name
    having count(*) >= 2
)

select
    repo_name,
    total_pushes,
    contributor_count,
    median_contributor_pushes
from repo_stats
order by median_contributor_pushes desc
limit 10