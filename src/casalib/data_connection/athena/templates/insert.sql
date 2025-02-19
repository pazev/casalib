insert into {{ schema_name }}.{{ table_name }}
with
input_ as (
    {{ query|indent(4) }}
),
reordering as (
    select
        {%- for col in columns_types %}
        {{ col }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from
        input_
)
select * from reordering