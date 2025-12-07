with contributor_activity as (
    select
        repo_name,
        actor_name,
        count(*) as push_count,
        min(date(created_at)) as first_push,
        max(date(created_at)) as last_push
    from {{ ref('stg_github_pushes') }}
    group by repo_name, actor_name
),

daily_repo_activity as (
    select
        repo_name,
        date(created_at) as activity_date,
        count(*) as daily_pushes
    from {{ ref('stg_github_pushes') }}
    group by repo_name, date(created_at)
),

repo_base_stats as (
    select
        repo_name,
        sum(push_count) as total_pushes,
        count(distinct actor_name) as unique_contributors
    from contributor_activity
    group by repo_name
),

-- Bus factor: minimum contributors responsible for 50% of pushes
bus_factor_calc as (
    select
        repo_name,
        count(*) as bus_factor
    from (
        select
            ca.repo_name,
            ca.actor_name,
            ca.push_count,
            sum(ca.push_count) over (
                partition by ca.repo_name 
                order by ca.push_count desc
            ) as cumulative_pushes,
            rbs.total_pushes
        from contributor_activity ca
        join repo_base_stats rbs on ca.repo_name = rbs.repo_name
    )
    where cumulative_pushes <= total_pushes * 0.5 + 1
    group by repo_name
),

-- Contributor retention: active in both halves of the period
period_bounds as (
    select
        min(date(created_at)) as period_start,
        max(date(created_at)) as period_end,
        date_add(min(date(created_at)), interval cast(date_diff(max(date(created_at)), min(date(created_at)), day) / 2 as int64) day) as midpoint
    from {{ ref('stg_github_pushes') }}
),

retention_calc as (
    select
        ca.repo_name,
        count(distinct ca.actor_name) as total_contributors,
        count(distinct case 
            when ca.first_push <= pb.midpoint
            and ca.last_push >= pb.midpoint
            then ca.actor_name 
        end) as retained_contributors
    from contributor_activity ca
    cross join period_bounds pb
    group by ca.repo_name
),

-- Activity consistency (lower stddev = more consistent)
consistency_calc as (
    select
        repo_name,
        stddev(daily_pushes) as activity_stddev,
        avg(daily_pushes) as avg_daily_activity
    from daily_repo_activity
    group by repo_name
),

-- Week over week velocity trend
weekly_activity as (
    select
        repo_name,
        extract(week from activity_date) as week_num,
        sum(daily_pushes) as weekly_pushes
    from daily_repo_activity
    group by repo_name, extract(week from activity_date)
),

velocity_trend as (
    select
        repo_name,
        round(avg(
            case when prev_week_pushes > 0 
            then (weekly_pushes - prev_week_pushes) / prev_week_pushes * 100 
            else 0 end
        ), 2) as velocity_trend_pct
    from (
        select
            repo_name,
            weekly_pushes,
            lag(weekly_pushes) over (partition by repo_name order by week_num) as prev_week_pushes
        from weekly_activity
    )
    group by repo_name
),

-- Combine all metrics
combined_metrics as (
    select
        rbs.repo_name,
        rbs.total_pushes,
        rbs.unique_contributors,
        coalesce(bf.bus_factor, 1) as bus_factor,
        round(coalesce(rc.retained_contributors / nullif(rc.total_contributors, 0), 0), 2) as contributor_retention_ratio,
        round(coalesce(cc.activity_stddev / nullif(cc.avg_daily_activity, 0), 0), 2) as activity_consistency_score,
        coalesce(vt.velocity_trend_pct, 0) as velocity_trend
    from repo_base_stats rbs
    left join bus_factor_calc bf on rbs.repo_name = bf.repo_name
    left join retention_calc rc on rbs.repo_name = rc.repo_name
    left join consistency_calc cc on rbs.repo_name = cc.repo_name
    left join velocity_trend vt on rbs.repo_name = vt.repo_name
),

-- Calculate collaboration score and health score
scored_repos as (
    select
        *,
        -- Collaboration score: weighted combination of diversity and retention
        round(
            (least(unique_contributors, 20) / 20.0 * 40) + 
            (contributor_retention_ratio * 30) + 
            (least(bus_factor, 5) / 5.0 * 30)
        , 1) as collaboration_score
    from combined_metrics
)

select
    repo_name,
    total_pushes,
    unique_contributors,
    contributor_retention_ratio,
    activity_consistency_score,
    bus_factor,
    collaboration_score,
    velocity_trend,
    -- Health score: composite 0-100 score
    round(
        collaboration_score * 0.4 +
        least(total_pushes / 100.0, 30) +
        case when velocity_trend > 0 then 15 when velocity_trend = 0 then 10 else 5 end +
        case when activity_consistency_score < 1 then 15 else 15 - least(activity_consistency_score, 3) * 5 end
    , 1) as health_score,
    case
        when collaboration_score >= 70 and velocity_trend > 0 then 'thriving'
        when collaboration_score >= 50 then 'healthy'
        when collaboration_score >= 30 then 'at_risk'
        else 'declining'
    end as health_tier
from scored_repos
where total_pushes >= 5  -- Filter out very low activity repos
order by health_score desc
