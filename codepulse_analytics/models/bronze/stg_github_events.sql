{% set end_date = modules.datetime.date.today() - modules.datetime.timedelta(days=1) %}
{% set start_date = end_date - modules.datetime.timedelta(days=6) %}

select
    id,
    type,
    actor,
    repo,
    payload,
    public,
    created_at,
    org
from `githubarchive.day.*`
where _table_suffix between '{{ start_date.strftime('%Y%m%d') }}' and '{{ end_date.strftime('%Y%m%d') }}'