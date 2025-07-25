""" Module to clear all caches and jupyter checkpoints """
import os
from pathlib import Path
import shutil
from typing import Optional

import context


def clear_python_cache(root: Optional[Path] = None):
    """ Clear all __pycache__ and .ipynb_checkpoints folders
    """
    root = root or context.PROJ_ROOT

    dirs_to_remove = [
        Path(root) / dir_
        for root, dirs, _ in os.walk(root)
        for dir_ in dirs
        if dir_ in ('.ipynb_checkpoints', '__pycache__')
    ]

    for dir_ in dirs_to_remove:
        shutil.rmtree(dir_)
