"""
Module to download, via requests, a URL into a file object.
"""
import shutil
from typing import BinaryIO
import warnings

import requests


def download_file(
    fileobj: BinaryIO, url: str, verify_ssl: bool = True
) -> requests.Response:
    """ Downloads files into a file object """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action='ignore',
            message='Unverified HTTPS request'
        )
        response = requests.get(
            url=url,
            verify=verify_ssl,
            stream=True,
            timeout=60,
        )

        shutil.copyfileobj(response.raw, fileobj)

    return response
