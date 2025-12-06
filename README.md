# CodePulse

Automated analytics pipeline for GitHub activity using dbt + BigQuery.

[View Dashboard](https://lookerstudio.google.com/reporting/05a3cdc4-4aff-4c2d-8443-d8f24e93947d) · [View dbt Docs](https://pikogizmo.github.io/codepulse/)

## How it works

```
GitHub Archive → BigQuery → dbt → Analytics
```

Runs daily via GitHub Actions, processes 7 days of events (~25M rows).

**Models:**
- `stg_github_events` - raw events
- `stg_github_pushes` - filtered (bots removed)
- `top_active_repos` - ranked by balanced team activity
- `daily_activity` - trends over time
- `event_breakdown` - activity by type
- `push_percentiles` - data profiling

## Stack

- dbt + BigQuery
- GitHub Actions
- Looker Studio

## Run locally

```bash
cd codepulse_analytics && dbt run
```
