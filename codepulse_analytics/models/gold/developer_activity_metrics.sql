with daily_activity as (
    select
        actor_name,
        date(created_at) as activity_date,
        count(*) as daily_pushes
    from {{ ref('stg_github_pushes') }}
    group by actor_name, date(created_at)
),

developer_base as (
    select
        actor_name,
        count(*) as total_pushes,
        count(distinct date(created_at)) as active_days,
        count(distinct repo_name) as repos_contributed_to,
        min(date(created_at)) as first_activity_date,
        max(date(created_at)) as last_activity_date
    from {{ ref('stg_github_pushes') }}
    group by actor_name
),

streak_calc as (
    select
        actor_name,
        activity_date,
        activity_date - interval cast(row_number() over (
            partition by actor_name 
            order by activity_date
        ) as int64) day as streak_group
    from daily_activity
),

max_streaks as (
    select
        actor_name,
        max(streak_length) as streak_days
    from (
        select
            actor_name,
            streak_group,
            count(*) as streak_length
        from streak_calc
        group by actor_name, streak_group
    )
    group by actor_name
),

ranked_developers as (
    select
        db.*,
        ms.streak_days,
        round(db.total_pushes / db.active_days, 2) as avg_daily_pushes,
        ntile(100) over (order by db.total_pushes) as activity_percentile
    from developer_base db
    left join max_streaks ms on db.actor_name = ms.actor_name
)

select
    actor_name,
    total_pushes,
    active_days,
    avg_daily_pushes,
    repos_contributed_to,
    activity_percentile,
    case
        when activity_percentile >= 90 then 'power_user'
        when activity_percentile >= 50 then 'regular'
        else 'casual'
    end as developer_tier,
    first_activity_date,
    last_activity_date,
    coalesce(streak_days, 1) as streak_days
from ranked_developers
order by total_pushes desc
