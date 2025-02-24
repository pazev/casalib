import os
from pathlib import Path
from typing import List

from cookiecutter.main import cookiecutter


BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / 'templates'

def list_templates() -> List[str]:
    """ Lista os templates presentes na biblioteca, dentro
        do diretório templates
    """
    return [
        f
        for f in os.listdir(TEMPLATES_DIR)
        if not os.path.isfile(TEMPLATES_DIR / f)
    ]


def run_template(name: str) -> None:
    """ Executa um template, passando o nome do template
        para ser executado
    """
    cookiecutter(str(TEMPLATES_DIR / name))
