WITH __base AS (
    {{ input_query | indent(4) }}
)
SELECT
    {{ keys | join(',\n    ') }},
    JSON_OBJECT(
        {% for col in columns %}'{{ col }}' VALUE CAST({{ col }} AS VARCHAR){% if not loop.last %},
        {% endif %}{% endfor %}
    ) AS __json
FROM __base
