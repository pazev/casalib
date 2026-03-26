WITH __base AS (
    {{ input_query | indent(4) }}
)
SELECT
    {{ select_list }}
FROM __base
{% if groupby %}
GROUP BY {{ groupby | join(', ') }}
{% endif %}
