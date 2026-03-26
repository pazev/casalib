WITH __base AS (
    {{ input_query | indent(4) }}
)
{% for other in others %},
__other_{{ loop.index0 }} AS (
    {{ other | indent(4) }}
)
{% endfor %}
SELECT
    base.*
{% for i in range(others | length) %}
    , __other_{{ i }}.*
{% endfor %}
FROM __base AS base
{% for i in range(others | length) %}
LEFT JOIN __other_{{ i }}
    ON {{ join_ons[i] }}
{% endfor %}
