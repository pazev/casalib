WITH __base AS (
    {{ input_query | indent(4) }}
)
{% for other in other_queries %},
__other_{{ loop.index0 }} AS (
    {{ other | indent(4) }}
)
{% endfor %}
SELECT
    base.*
{% for i in range(other_queries | length) %}
    , __other_{{ i }}.*
{% endfor %}
FROM __base AS base
{% for i in range(other_queries | length) %}
LEFT JOIN __other_{{ i }}
    ON {{ join_ons[i] }}
{% endfor %}
