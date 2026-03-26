WITH __base AS (
    {{ input_query | indent(4) }}
),
__latest AS (
    SELECT
        {{ columns | join(',\n        ') }},
        MAX({{ date_ingestion }}) AS {{ date_ingestion }}
    FROM __base
    GROUP BY {{ columns | join(', ') }}
)
SELECT base.*
FROM __base AS base
INNER JOIN __latest
    ON {{ join_on }}
