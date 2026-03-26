WITH __base AS (
    {{ input_query | indent(4) }}
)
SELECT
    {{ keys | join(',\n    ') }},
    map_from_arrays(
        ARRAY[{{ col_names }}],
        ARRAY[{{ col_values }}]
    ) AS __json
FROM __base
