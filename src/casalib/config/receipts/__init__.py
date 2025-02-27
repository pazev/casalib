import importlib
import os
from pathlib import Path


PLUGIN_FLD = Path(__file__).parent


def list_plugins_():
    """ Carrega os plugins """
    list_plugins = [
        file_prefix

        for file in os.listdir(PLUGIN_FLD)

        for file_path in [Path(PLUGIN_FLD) / file]
        for file_prefix, file_ext in [
            os.path.splitext(file_path.name)
        ]

        if file_ext == '.py'
        if file_path.is_file()
        if file_prefix not in ['__init__']
        if not file_prefix.startswith('.')
    ]
    return list_plugins


def load_plugins():
    """ Carrega os plugins """
    plugins = {
        name: importlib.import_module(
            f'.{name}',
            'casalib.config.receipts'
        )
        for name in list_plugins_()
    }

    return plugins


PLUGINS = load_plugins()
