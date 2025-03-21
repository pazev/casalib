"""
Módulo para baixar, via requests, uma URL para um file
object.
"""
import shutil
from typing import BinaryIO
import warnings

import requests


def download_file(
    fileobj: BinaryIO, url: str, verify_ssl: bool = True
):
    """ Baixa arquivos para um file object """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action='ignore',
            message='Unverified HTTPS request'
        )
        response = requests.get(
            url=url, verify=verify_ssl, stream=True
        )

        shutil.copyfileobj(response.raw, fileobj)

    return response
