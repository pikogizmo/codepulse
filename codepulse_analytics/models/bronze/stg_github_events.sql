{% set days = 7 %}

{% for i in range(days) %}
{% set day = (modules.datetime.date.today() - modules.datetime.timedelta(days=i+1)).strftime('%Y%m%d') %}

select id, type, actor, repo, payload, public, created_at, org
from `githubarchive.day.{{ day }}`

{% if not loop.last %}union all{% endif %}
{% endfor %}