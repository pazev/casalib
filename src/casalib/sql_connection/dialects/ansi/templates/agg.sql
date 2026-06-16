WITH
input_query_ AS (
    {{ input_query | indent(4) }}
)
,
cols_before_ AS (
    SELECT
        *
        {%- if agg_def.cols_before %}
        ,
        {%- for sql_code, alias in agg_def.cols_before %}
        {{sql_code}} AS {{alias}}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
)
,
agg_ AS (
    SELECT
        {%- if agg_def.groupby %}
        {%- for _, alias in agg_def.groupby %}
        {{alias}}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
        {%- if agg_def.groupby and agg_def.ops %}
        ,
        {%- endif %}
        {%- if agg_def.ops %}
        {%- for sql_code, alias in agg_def.ops %}
        {{sql_code}} as {{alias}}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
    FROM
        cols_before_
    {%- if agg_def.groupby %}
    GROUP BY
        {%- for _, alias in agg_def.groupby %}
        {{alias}}{%if not loop.last%},{%endif%}
        {%- endfor %}
    {%- endif %}
)
,
cols_after_ AS (
    SELECT
        *
        {%- if agg_def.cols_after %}
        ,
        {%- for sql_code, alias in agg_def.cols_after %}
        {{sql_code}} as {{alias}}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
    FROM
        agg_
)
SELECT * FROM cols_after_
