# CodePulse

Real-time analytics pipeline that processes GitHub events data using dbt and BigQuery.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  GitHub Archive │────▶│    BigQuery     │────▶│   dbt Models    │
│   (source)      │     │   (warehouse)   │     │   (transform)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                              ┌─────────────────────────┘
                              ▼
                ┌──────────────────────────────┐
                │      GitHub Actions          │
                │   (scheduled daily @ 6AM)    │
                └──────────────────────────────┘
```

## Data Flow

| Layer  | Model               | Description                    |
|--------|---------------------|--------------------------------|
| Bronze | `stg_github_events` | Raw events from GitHub Archive |
| Silver | `stg_github_pushes` | Filtered push events           |
| Gold   | `top_active_repos`  | Top 10 most active repos       |

## Tech Stack

- **dbt** - data transformation
- **BigQuery** - data warehouse
- **GitHub Actions** - orchestration

## Running Locally

```bash
cd codepulse_analytics
dbt run
```

## Pipeline Schedule

Runs daily at 6:00 AM UTC via GitHub Actions.
