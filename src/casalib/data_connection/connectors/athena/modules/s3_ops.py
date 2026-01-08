"""
Utilities module to perform various S3 operations
related to Athena.
"""
# pylint: disable=too-many-arguments
from itertools import chain
import re
from typing import Dict, List, Tuple

import boto3


def get_bucket_prefix(uri: str) -> Tuple[str, str]:
    """
    Given a URI, extracts the bucket and the object prefix
    """
    re_obj = re.match(
        r's3:\/\/(.+?)\/(.*?)\/*$',
        uri
    )

    if re_obj:
        bucket, prefix = re_obj.groups()
        prefix += '/'
        return bucket, prefix

    raise ValueError(
        "uri passed doesn't respect the s3 convention"
    )


def list_files_prefix(
    boto3_session: boto3.Session,
    bucket: str,
    prefix: str,
) -> List[Dict[str, str]]:
    """ List the objects in a bucket/prefix """
    s3 = boto3_session.client('s3')
    params = {
        'Bucket': bucket,
        'Prefix': prefix,
    }
    results = []
    tkn = 'ContinationToken'
    next_tkn = 'NextContinuationToken'

    while True:
        res = s3.list_objects_v2(**params)

        if 'Contents' in res:
            results.append(res['Contents'])

        if tkn not in res:
            break

        params = params | {tkn: res[next_tkn]}

    files = [
        {'Bucket': bucket, 'Key': file['Key']}
        for file in chain.from_iterable(results)
    ]

    return files


def delete_objects(
    boto3_session: boto3.Session,
    files_list: List[Dict[str, str]],
) -> None:
    """ Delete the objects informed in the list """
    s3 = boto3_session.client('s3')

    for file in files_list:
        s3.delete_object(**file)
