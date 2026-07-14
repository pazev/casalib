WITH
input_query_ AS (
    {{ input_query | indent(4) }}
)
,
cols_before_ AS (
    SELECT
        *{% if agg_def.cols_before %},
        {% for col in agg_def.cols_before %}
        {{col.sql_code}} AS {{col.alias}}{%if not loop.last%},{%endif%}
        {% endfor %}
        {% endif %}

    FROM
        input_query_
)
,
agg_ AS (
    SELECT
        {% if agg_def.groupby %}
        {% for _, alias in agg_def.groupby %}
        {{alias}},
        {% endfor %}
        {% endif %}
        {% if agg_def.ops %}
        {% for col in agg_def.ops %}
        {{col.sql_code}} as {{col.alias}},
        {% endfor %}
        {% endif %}
        COUNT(*) as count_rows_
    FROM
        cols_before_
    {% if agg_def.groupby %}
    GROUP BY
        {% for _, alias in agg_def.groupby %}
        {{loop.index}}{%if not loop.last%},{%endif%}

        {% endfor %}
    {%- endif %}
)
,
cols_after_ AS (
    SELECT
        *{% if agg_def.cols_after %},
        {% for col in agg_def.cols_after %}
        {{col.sql_code}} as {{col.alias}}{%if not loop.last%},{%endif%}
        {% endfor %}
        {% endif %}

    FROM
        agg_
)
SELECT * FROM cols_after_
