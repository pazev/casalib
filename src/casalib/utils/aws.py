from typing import Dict, List

import boto3


def s3_split_bucket_key(uri: str):
    """ Split URI into bucket/prefix """
    _, _, bucket, *key_parts = uri.split('/')
    return (bucket, '/'.join(key_parts))


def s3_list_files(
    bucket: str, prefix: str, boto3_session: boto3.Session
) -> List[Dict[str, str]]:
    """ List files in a Bucket/Preffix """
    client = boto3_session.client('s3')
    pages = (
        client
        .get_paginator('list_objects_v2')
        .paginate(Bucket=bucket, Prefix=prefix)
    )
    contents = [
        content
        for page in pages
        for content in page['Contents']
        for _ in [content.__setitem__('Bucket', bucket)]
    ]
    return contents


# def list_files_prefix(
#     boto3_session: boto3.Session,
#     bucket: str,
#     prefix: str,
# ) -> List[Tuple[str, str]]:
#     """ List the objects in a bucket/prefix """
#     s3 = boto3_session.client('s3')
#     params = {
#         'Bucket': bucket,
#         'Prefix': prefix,
#     }
#     results = []
#     tkn = 'ContinationToken'
#     next_tkn = 'NextContinuationToken'

#     while True:
#         res = s3.list_objects_v2(**params)

#         if 'Contents' in res:
#             results.append(res['Contents'])

#         if tkn not in res:
#             break

#         params = params | {tkn: res[next_tkn]}

#     files = [
#         {'Bucket': bucket, 'Key': file['Key']}
#         for file in chain.from_iterable(results)
#     ]

#     return files