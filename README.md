# CodePulse

A data pipeline that pulls GitHub events from the public GitHub Archive and transforms them using dbt + BigQuery.

## How it works

```
GitHub Archive → BigQuery → dbt → Analytics tables
```

The pipeline runs daily via GitHub Actions and builds three models:

- `stg_github_events` - raw events from yesterday
- `stg_github_pushes` - just the push events  
- `top_active_repos` - top 10 repos by push count

## Stack

- dbt (BigQuery adapter)
- GitHub Actions for scheduling

## Usage

```bash
cd codepulse_analytics
dbt run
```

