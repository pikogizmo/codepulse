select
    approx_quantiles(push_count, 100)[offset(50)] as p50,
    approx_quantiles(push_count, 100)[offset(90)] as p90,
    approx_quantiles(push_count, 100)[offset(95)] as p95,
    approx_quantiles(push_count, 100)[offset(99)] as p99,
    max(push_count) as max_pushes
from (
    select
        repo_name,
        count(*) as push_count
    from {{ ref('stg_github_pushes') }}
    group by repo_name
)
