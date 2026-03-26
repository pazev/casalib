WITH __base AS (
    {{ input_query | indent(4) }}
)
SELECT *
FROM __base
LIMIT {{ num_samples }}
