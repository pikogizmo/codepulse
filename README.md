# CodePulse

A data pipeline that pulls GitHub events from the public GitHub Archive and transforms them using dbt + BigQuery.

## Dashboard

[View Live Dashboard](https://lookerstudio.google.com/reporting/05a3cdc4-4aff-4c2d-8443-d8f24e93947d)

## How it works

```
GitHub Archive → BigQuery → dbt → Analytics tables
```

The pipeline runs daily via GitHub Actions and processes the last 7 days of events:

- `stg_github_events` - raw events (last 7 days)
- `stg_github_pushes` - push events, bots filtered out
- `top_active_repos` - top 10 repos by human activity
- `daily_activity` - events by day for trends
- `event_breakdown` - event type distribution

## Stack

- dbt (BigQuery adapter)
- GitHub Actions for scheduling
- Looker Studio for visualization

## Usage

```bash
cd codepulse_analytics
dbt run
```
