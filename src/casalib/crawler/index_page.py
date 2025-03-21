"""
Neste módulo, temos funções para extrair informações de
páginas de listagem de arquivos "Index of", que acontecem
com frequência.

Temos duas funções:
1. get_index_page: captura a página de index, como HTML;
   verifica se ocorreu tudo certo e retorna um dicionário
   com as informações;

2. process_index_page: processa a informação obtida pela
   função get_index_page, gerando a lista de links
   disponíveis.
"""
from typing import Any, Dict
import warnings


def get_index_page(
    url: str,
    verify_ssl: bool = True,
    **kwargs
) -> Dict[str, Any]:
    """ Captura a lista de meses """
    import requests

    # Carrega lista
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action='ignore', message='Unverified HTTPS request'
        )
        response = requests.get(url, verify=verify_ssl)

    if response.status_code != 200:
        raise RuntimeError(
            'Site retornou status code '
            f'{response.status_code}. Por favor, verifique.'
        )

    return {'url': url, 'content': response.text}


def process_index_page(
    url: str,
    content: str,
    filtering_regex: str,
    **kwargs
) -> Dict[str, Any]:
    """ Processa a lista HTML, extraindo os parámetros
        desejados em um dicionário
    """
    import re

    from bs4 import BeautifulSoup

    soup = BeautifulSoup(content, 'html.parser')
    elems = soup.select('li a')

    regex = re.compile(filtering_regex)
    links = [
        elem.attrs['href']
        for elem in elems
        if regex.match(elem.attrs['href'])
    ]

    return {
        'url': url,
        'content': content,
        'filtering_regex': filtering_regex,
        'links': links
    }
