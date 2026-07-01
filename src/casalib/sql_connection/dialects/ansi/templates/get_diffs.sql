WITH __left AS (
    {{ input_query | indent(4) }}
),
__right AS (
    {{ other_query | indent(4) }}
)
SELECT
    {% for lk, rk in join_on %}COALESCE(l.{{ lk }}, r.{{ rk }}) AS {{ lk }}{% if not loop.last %},
    {% endif %}{% endfor %},
    {% for lc, rc in compare_cols %}l.{{ lc }} AS {{ lc }}__left,
    r.{{ rc }} AS {{ rc }}__right{% if not loop.last %},
    {% endif %}{% endfor %}
FROM __left AS l
FULL OUTER JOIN __right AS r
    ON {% for lk, rk in join_on %}l.{{ lk }} = r.{{ rk }}{% if not loop.last %} AND {% endif %}{% endfor %}
WHERE
    {% for lc, rc in compare_cols %}(l.{{ lc }} IS DISTINCT FROM r.{{ rc }}){% if not loop.last %}
    OR {% endif %}{% endfor %}
