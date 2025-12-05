-- Query yesterday's GitHub events dynamically
-- GitHub Archive tables are named by date: githubarchive.day.YYYYMMDD

{% set yesterday = (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).strftime('%Y%m%d') %}

SELECT
    id,
    type,
    actor,
    repo,
    payload,
    public,
    created_at,
    org
FROM `githubarchive.day.{{ yesterday }}`