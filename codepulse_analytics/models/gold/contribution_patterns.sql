with hourly_daily_activity as (
    select
        extract(hour from created_at) as hour_of_day,
        extract(dayofweek from created_at) as day_of_week,
        count(*) as push_count,
        count(distinct actor_name) as unique_contributors
    from {{ ref('stg_github_pushes') }}
    group by 
        extract(hour from created_at),
        extract(dayofweek from created_at)
),

totals as (
    select sum(push_count) as total_pushes
    from hourly_daily_activity
),

hourly_totals as (
    select
        hour_of_day,
        sum(push_count) as hour_total
    from hourly_daily_activity
    group by hour_of_day
),

daily_totals as (
    select
        day_of_week,
        sum(push_count) as day_total
    from hourly_daily_activity
    group by day_of_week
),

peak_hours as (
    select hour_of_day
    from hourly_totals
    order by hour_total desc
    limit 5
),

peak_days as (
    select day_of_week
    from daily_totals
    order by day_total desc
    limit 2
)

select
    h.hour_of_day,
    h.day_of_week,
    case h.day_of_week
        when 1 then 'Sunday'
        when 2 then 'Monday'
        when 3 then 'Tuesday'
        when 4 then 'Wednesday'
        when 5 then 'Thursday'
        when 6 then 'Friday'
        when 7 then 'Saturday'
    end as day_name,
    h.push_count as total_pushes,
    h.unique_contributors,
    round(h.push_count / t.total_pushes * 100, 2) as pct_of_total,
    case when ph.hour_of_day is not null then true else false end as is_peak_hour,
    case when pd.day_of_week is not null then true else false end as is_peak_day
from hourly_daily_activity h
cross join totals t
left join peak_hours ph on h.hour_of_day = ph.hour_of_day
left join peak_days pd on h.day_of_week = pd.day_of_week
order by h.day_of_week, h.hour_of_day
