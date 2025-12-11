"""
Template para rodar a receita de um objeto salvo em uma
configuração.
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from . import receipts


def default_cfg_() -> Path:
    """ Carrega o local padrão das configurações """
    # Default file
    home_folder = (
        os.environ.get('USERPROFILE') or
        os.environ.get('HOME')
    )

    if home_folder is None:
        raise ValueError('Can not find home folder.')

    dft_fld = Path(home_folder)
    dft_file = (
        dft_fld /
        '.config' /
        'casalib' /
        'config.yaml'
    )

    return dft_file


def load_cfg_(config_file: Optional[str] = None) -> Any:
    """ Carrega as configurações """
    config_file_ = config_file or default_cfg_()

    with open(config_file_, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def make_obj_params_(
    obj_type: str, params: Dict[str, Any]
) -> Any:
    """ Cria o objeto """
    obj = receipts.load_plugins()[obj_type].make(**params)
    return obj


def load_obj(
    name: str, config_file: Optional[str] = None
) -> Any:
    """ Cria um objeto a partir das configurações """
    config = load_cfg_(config_file)

    try:
        config_params = config[name]
    except KeyError as exc:
        key_error = exc.args[0]
        known_configs = list(config.keys())

        raise KeyError(
            f'A configuração {key_error} não existe; temos '
            f'as seguintes configurações: {known_configs}'
        ) from exc

    return make_obj_params_(**config_params)
