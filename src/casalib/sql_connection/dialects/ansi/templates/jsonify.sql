WITH __base AS (
    {{ input_query | indent(4) }}
)
SELECT
    {{ keys | join(',\n    ') }},
    JSON_OBJECT(
        {{ json_args }}
    ) AS __json
FROM __base
