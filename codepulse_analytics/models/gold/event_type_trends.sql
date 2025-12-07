with daily_events as (
    select
        date(created_at) as event_date,
        type as event_type,
        count(*) as event_count
    from {{ ref('stg_github_events') }}
    group by date(created_at), type
),

daily_totals as (
    select
        event_date,
        sum(event_count) as total_daily_events
    from daily_events
    group by event_date
),

event_ranks as (
    select
        event_type,
        sum(event_count) as total_count,
        row_number() over (order by sum(event_count) desc) as rank
    from daily_events
    group by event_type
)

select
    de.event_date,
    de.event_type,
    de.event_count,
    dt.total_daily_events,
    round(de.event_count / dt.total_daily_events * 100, 2) as pct_of_daily,
    er.rank as event_popularity_rank
from daily_events de
join daily_totals dt on de.event_date = dt.event_date
join event_ranks er on de.event_type = er.event_type
where er.rank <= 8  -- Top 8 event types for cleaner visualization
order by de.event_date, er.rank
