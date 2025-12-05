{% set yesterday = (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).strftime('%Y%m%d') %}

select
    id,
    type,
    actor,
    repo,
    payload,
    public,
    created_at,
    org
from `githubarchive.day.{{ yesterday }}`