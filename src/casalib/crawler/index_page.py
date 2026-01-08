"""
In this module, we have functions to extract information
from "Index of" file listing pages, which occur frequently.

We have two functions:
1. get_index_page: fetches the index page as HTML;
   checks that everything went fine and returns a dictionary
   with the information;

2. process_index_page: processes the information obtained by
   the get_index_page function, generating the list of
   available links.

"""
# pylint: disable=unused-argument
import re
from typing import Any, Dict
import warnings

from bs4 import BeautifulSoup
import requests


def get_index_page(
    url: str,
    verify_ssl: bool = True,
    **kwargs: Any
) -> Dict[str, Any]:
    """ Gets the url code """
    # Loads list
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action='ignore',
            message='Unverified HTTPS request',
        )
        response = requests.get(
            url=url,
            verify=verify_ssl,
            timeout=30,
        )

    if response.status_code != 200:
        raise RuntimeError(
            'Site returned status code '
            f'{response.status_code}. Please check.'
        )

    return {'url': url, 'content': response.text}


def process_index_page(
    url: str,
    content: str,
    filtering_regex: str,
    **kwargs: Any
) -> Dict[str, Any]:
    """
    Processes the HTML list, extracting the desired
    parameters into a dictionary.
    """
    soup = BeautifulSoup(content, 'html.parser')
    elems = soup.select('li a')

    regex = re.compile(filtering_regex)
    links = [
        elem.attrs['href']
        for elem in elems
        if regex.match(str(elem.attrs['href']))
    ]

    return {
        'url': url,
        'content': content,
        'filtering_regex': filtering_regex,
        'links': links
    }
