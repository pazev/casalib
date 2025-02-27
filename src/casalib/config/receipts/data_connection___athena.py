from typing import Optional

from casalib.data_connection.athena.athena import (
    AthenaConnection,
    Boto3SessionMaker
)


def make(
    schema_name: str,
    workgroup: str,
    s3_staging_dir: str,
    data_catalog: str,
    table_prefix: str,
    boto3_aws_access_key_id: Optional[str] = None,
    boto3_aws_secret_access_key: Optional[str] = None,
    boto3_aws_session_token: Optional[str] = None,
    boto3_region_name: str = 'us-east-1'
) -> AthenaConnection:
    """ Cria uma conexão com o Athena """
    athena = AthenaConnection(
        schema_name=schema_name,
        workgroup=workgroup,
        s3_staging_dir=s3_staging_dir,
        data_catalog=data_catalog,
        table_prefix=table_prefix,
        boto3_session_maker=Boto3SessionMaker(
            aws_access_key_id=boto3_aws_access_key_id,
            aws_secret_access_key=boto3_aws_secret_access_key,
            aws_session_token=boto3_aws_session_token,
            region_name=boto3_region_name,
        ),
    )

    return athena
