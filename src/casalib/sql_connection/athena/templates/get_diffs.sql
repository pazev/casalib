WITH __left AS (
    {{ input_query | indent(4) }}
),
__right AS (
    {{ other | indent(4) }}
)
SELECT
    {{ key_cols }},
    {{ diff_cols }}
FROM __left AS l
FULL OUTER JOIN __right AS r
    ON {{ join_on }}
WHERE
    {{ diff_where }}
