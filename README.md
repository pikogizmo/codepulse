# CodePulse

Analytics pipeline that processes GitHub Archive data to surface insights about developer activity and repository health.

[Dashboard](https://lookerstudio.google.com/reporting/05a3cdc4-4aff-4c2d-8443-d8f24e93947d) · [dbt Docs](https://pikogizmo.github.io/codepulse/)

## Architecture

```
GitHub Archive (BigQuery public dataset)
        ↓
   dbt models
        ↓
  Looker Studio
```

Pulls 7 days of GitHub events (~25M rows), runs daily via GitHub Actions.

## Models

**Staging**
- `stg_github_events` - Raw events from GitHub Archive
- `stg_github_pushes` - Push events with bots filtered out

**Analytics**
- `developer_activity_metrics` - Developer stats: push counts, streaks, activity tiers
- `repository_health_scores` - Repo scoring: bus factor, retention, health tiers
- `contribution_patterns` - Activity heatmap by hour/day
- `event_type_trends` - Event composition over time
- `daily_activity` / `event_breakdown` - Summary tables

## Local Setup

```bash
cd codepulse_analytics
export GCP_PROJECT_ID=your-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
dbt run --profiles-dir .
```

## Stack

- dbt + BigQuery
- GitHub Actions (scheduled runs)
- Looker Studio (dashboards)
