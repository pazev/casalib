WITH __base AS (
    {{ input_query | indent(4) }}
),
__counts AS (
    SELECT
        {{ keys | join(',\n        ') }},
        COUNT(*) AS __n
    FROM __base
    GROUP BY {{ keys | join(', ') }}
)
SELECT base.*
FROM __base AS base
INNER JOIN __counts AS counts
    ON {{ join_on }}
WHERE counts.__n > 1
