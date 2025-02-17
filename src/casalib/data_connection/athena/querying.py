from datetime import datetime
import logging
import textwrap
from typing import Union

import awswrangler as wr
from awswrangler.typing import AthenaCTASSettings
import boto3
import jinja2
import pandas as pd


def run_query_get_pandas(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    s3_output: str,
    table_prefix: str,
    query: str
) -> pd.DataFrame:
    """ Faz uma query no Athena """
    dttm = datetime.now().strftime('%Y%m%d%H%M%S')
    temp_table_name = f'{table_prefix}__temp__{dttm}'

    ctas_settings = AthenaCTASSettings(
        database=default_schema_name,
        temp_table_name=temp_table_name
    )

    params = {
        'ctas_parameters': ctas_settings,
        'boto3_session': boto3_session,
        'database': default_schema_name,
        'workgroup': workgroup,
        's3_output': f'{s3_output}',
        'ctas_approach': True,
        'use_threads': True,
        'sql': query,
    }

    try:
        dff = wr.athena.read_sql_query(**params)
    except Exception as exc:
        sql_numbered = "\n".join(
            [
                f'{idx+1:05} {line}'
                for idx, line in enumerate(query.split('\n'))
            ]
        )

        logging.error(
            'Error executing the query below:\n'
            f'{sql_numbered}'
        )
        logging.exception(exc)
        raise exc

    return dff


def run_table_get_pandas(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    s3_output: str,
    table_prefix: str,
    table_name: str,
    samples: Union[int, None],
) -> pd.DataFrame:
    """ Faz uma query no Athena """
    env = jinja2.Environment()

    schema, table_name_final = [
        default_schema_name,
        *table_name.split('.')
    ][-2:]

    template_str = textwrap.dedent('''
    select * from {{schema}}.{{table}}
    {%- if samples %}
    limit {{samples}}
    {%- endif %}
    ''')

    template = env.from_string(template_str)

    query = template.render(
        schema=schema, table=table_name_final, samples=samples
    )

    dff = run_query_get_pandas(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=default_schema_name,
        workgroup=workgroup,
        s3_output=s3_output,
        table_prefix=table_prefix,
        query=query,
    )

    return dff
