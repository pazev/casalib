WITH __base AS (
    {{ input_query | indent(4) }}
)
SELECT
    {{ keys | join(',\n    ') }},
    map_from_arrays(
        ARRAY[{% for col in columns %}'{{ col }}'{% if not loop.last %}, {% endif %}{% endfor %}],
        ARRAY[{% for col in columns %}CAST({{ col }} AS VARCHAR){% if not loop.last %}, {% endif %}{% endfor %}]
    ) AS __json
FROM __base
