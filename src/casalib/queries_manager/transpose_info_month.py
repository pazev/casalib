from typing import List, Literal, Tuple, Union
import jinja2


template_str = '''
with
input_ as (
    {{ input_query | indent(4) }}
)
,
input_partition_col_ as (
    select
        *,
        {%- for sql_code, alias in additional_columns_tuples %}
        {{ sql_code }} as {{ alias }},
        {%- endfor %}
        {{ partition_col }} as partition_col_
    from input_
)
,
-- Seleciona as partições
list_partitions_ as (
    with
    mesref__ (mesref) as (values ('{{mesref}}'))
    ,
    treat_mesref__ as (
        select date_parse(mesref, '%Y%m') as mesref
        from mesref__
    )
    ,
    list_mesref__ as (
        {%- for idx in range(num_months) %}
        select
            date_format(
                date_add('month', -{{loop.index}}, mesref),
                '%Y%m'
            ) as mesref,
            {{ loop.index }} as rn
        from treat_mesref__
        {% if not loop.last %}union all{% endif %}
        {%- endfor %}
    )
    ,
    list_partitions__ as (
        select
            substring(partition_col_, 1, 6) as partition_col_anomes_,
            max(partition_col_) as partition_col_
        from input_partition_col_ group by 1
    )
    ,
    cross__ as (
        select
            lm.mesref,
            lm.rn,
            max(
                case
                    when lp.partition_col_anomes_ = lm.mesref
                    then partition_col_
                end
            ) as partition_col_,
            max(
                case
                    when lp.partition_col_anomes_ <= lm.mesref
                    then partition_col_
                end
            ) as last_partition_col_
        from
                list_mesref__ as lm
            left join
                list_partitions__ as lp
                    on  lm.mesref >= lp.partition_col_anomes_
        group by
            1, 2
    )
    select * from cross__
)
,
input_filt_ as (
    select * from input_partition_col_
    where partition_col_ in (
        {%- if table_type == 'monthly' %}
        select partition_col_ from list_partitions_
        where partition_col_ is not null
        {%- elif table_type == 'last_partition_before' %}
        select last_partition_col_ from list_partitions_
        where last_partition_col_ is not null
        {%- else %}
        -- table_type must be 'monthly' or 'last_partition_before'
        {%- endif %}
    )
)
,
cross_ as (
    select
        ifilt_.*,
        listp_.rn
    from
            input_filt_ as ifilt_
        left join
            list_partitions_ as listp_
                on
                {%- if table_type == 'monthly' %}
                ifilt_.partition_col_ = listp_.partition_col_
                {%- elif table_type == 'last_partition_before' %}
                ifilt_.partition_col_ = listp_.last_partition_col_
                {%- else %}
                -- table_type must be 'monthly' or 'last_partition_before'
                {%- endif %}
)
,
transpose_ as (
    select
        {%- for sql_code, alias in keys %}
        {{ sql_code }} as {{ alias }},
        {%- endfor %}
        {%- for idx in range(num_months) %}
        {%- for col, alias in columns_to_transpose_tuples %}
        max(
            case
                when rn = {{ idx + 1 }}
                then {{ col }}
            end
        ) as {{ alias }}_m{{ idx + 1 }},
        {%- endfor %}
        {%- endfor %}
        '{{ mesref }}' as mesref
    from
        cross_
    group by
        {%- for sql_code, alias in keys %}
        {{ loop.index }}{%if not loop.last%},{% endif %}
        {%- endfor %}
)

select * from transpose_
'''


def transpose_function(
    input_query: str,
    mesref: str,
    keys: List[Union[str, Tuple[str, str]]],
    partition_col: str,
    num_months: int,
    table_type: Literal['monthly', 'last_partition_before'],
    columns_to_transpose_tuples: List[Union[str, Tuple[str, str]]],
    additional_columns_tuples: List[Union[str, Tuple[str, str]]],
) -> str:
    '''
    Generate a query that transposes an input query into
    months.

    It expects that the input query points to a table (or
    CTE) with a set of keys and several informations to be
    transposed. It also expects that the table is organised
    using a date column with format YYYYMMDD (it can be
    adjusted passing a sql_code to partition_col).

    The query will select the months and transpose them.

    We support two types of queries:
        - monthly: the process seeks for the last partition
            of each month offset;
        - last_partition: the process seeks for the last
            partition before each month offset

    Parameters:
        input_query: query that generates the CTE to be
            transposed
        mesref: reference month to be computed. We always
            make reference to the first day of the month.
        keys: list of strings and tuples, with the keys to
            be used in the query. If a tuple is passed,
            the first element must be the sql_code and the
            second one the alias.
        partition_col: sql_code or column that partition the
            data.
        num_months: number of months to transpose the data.
        table_type: string, that must be monthly or
            last_partition_before.
        columns_to_transpose_tuples: list of strings and
            tuples, with columns to be transposed. If tuple,
            the first element must be the sql_code and the
            second one the alias.
        additional_columns_tuples: list of strings and
            tuples, with columns to be created in the query.
            May help in some more complex queries.
            If tuple, the first element must be the sql_code
            and the second one the alias.
    Returns:
        Query to be executed.
    '''
    # Checks
    acceptable_table_type = ['monthly', 'last_partition_before']
    if table_type not in acceptable_table_type:
        raise ValueError(
            'table_type must be in {}. Please check (= `{}`).'
            .format(acceptable_table_type, table_type)
        )

    if num_months <= 0:
        raise ValueError(
            'num_months must be >= 1 (= `{}`)'
            .format(num_months)
        )

    # Adjust List[Union[str, Tuple[str, str]] to List[Tuple[str, str]]
    keys_adj = [
        adj_tuple
        for elem in keys
        for adj_tuple in [elem if isinstance(elem, tuple) else (elem, elem)]
    ]

    columns_to_transpose_tuples_adj = [
        adj_tuple
        for elem in columns_to_transpose_tuples
        for adj_tuple in [elem if isinstance(elem, tuple) else (elem, elem)]
    ]

    additional_columns_tuples_adj = [
        adj_tuple
        for elem in additional_columns_tuples
        for adj_tuple in [elem if isinstance(elem, tuple) else (elem, elem)]
    ]

    env = jinja2.Environment()
    template = env.from_string(template_str)

    query = template.render(
        input_query=input_query,
        additional_columns_tuples=additional_columns_tuples_adj,
        keys=keys_adj,
        mesref=mesref,
        num_months=num_months,
        table_type=table_type,
        partition_col=partition_col,
        columns_to_transpose_tuples=columns_to_transpose_tuples_adj,
    )

    return query
