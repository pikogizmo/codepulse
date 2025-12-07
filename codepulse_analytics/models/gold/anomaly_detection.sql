with daily_push_counts as (
    select
        date(created_at) as event_date,
        count(*) as daily_pushes
    from {{ ref('stg_github_pushes') }}
    group by date(created_at)
),

-- Calculate rolling 7-day statistics (excluding current day)
rolling_stats as (
    select
        event_date,
        daily_pushes,
        avg(daily_pushes) over (
            order by event_date
            rows between 7 preceding and 1 preceding
        ) as rolling_7d_avg,
        stddev(daily_pushes) over (
            order by event_date
            rows between 7 preceding and 1 preceding
        ) as rolling_7d_stddev
    from daily_push_counts
),

-- Calculate z-scores and deviations
anomaly_calc as (
    select
        event_date,
        daily_pushes,
        round(rolling_7d_avg, 2) as rolling_7d_avg,
        round(rolling_7d_stddev, 2) as rolling_7d_stddev,
        case 
            when rolling_7d_stddev > 0 
            then round((daily_pushes - rolling_7d_avg) / rolling_7d_stddev, 2)
            else 0 
        end as z_score,
        case 
            when rolling_7d_avg > 0 
            then round((daily_pushes - rolling_7d_avg) / rolling_7d_avg * 100, 2)
            else 0 
        end as pct_deviation
    from rolling_stats
    where rolling_7d_avg is not null  -- Need at least 1 prior day
)

select
    event_date,
    daily_pushes,
    rolling_7d_avg,
    rolling_7d_stddev,
    z_score,
    abs(z_score) > 2 as is_anomaly,
    case
        when z_score > 2 then 'spike'
        when z_score < -2 then 'drop'
        else 'normal'
    end as anomaly_type,
    pct_deviation
from anomaly_calc
order by event_date
