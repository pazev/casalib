from itertools import chain
import re
from typing import List, Tuple

import boto3


def get_bucket_prefix(uri: str) -> Tuple[str, str]:
    """ Dado um URI, extrai o bucket e o prefixo do objeto """
    bucket, prefix = re.match(
        r's3:\/\/(.+?)\/(.*)\/?$',
        uri
    ).groups()
    return bucket, prefix


def list_files_prefix(
    boto3_session: boto3.Session,
    bucket: str,
    prefix: str,
) -> List[Tuple[str, str]]:
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
    files_list: List[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    """ Delete the objects informed in the list """
    s3 = boto3_session.client('s3')

    for file in files_list:
        s3.delete_object(**file)
