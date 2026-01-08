"""
Module with commands for managing casalib templates.
"""
import os
from pathlib import Path
from typing import List

from cookiecutter.main import cookiecutter  # type: ignore


BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / 'templates'

def list_templates() -> List[str]:
    """ Lists the templates present in the library, inside
        the templates directory
    """
    return [
        f
        for f in os.listdir(TEMPLATES_DIR)
        if not os.path.isfile(TEMPLATES_DIR / f)
    ]


def run_template(name: str) -> None:
    """ Executes a template, passing the template name
        to be executed
    """
    cookiecutter(str(TEMPLATES_DIR / name))
