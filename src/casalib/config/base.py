import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from . import receipts


def default_cfg_() -> Path:
    """ Carrega o local padrão das configurações """
    # Default file
    dft_fld = Path(
        os.environ.get('USERPROFILE') or
        os.environ.get('HOME')
    )
    dft_file = dft_fld / '.config' / 'casalib' / 'config.yaml'

    return dft_file


def load_cfg_() -> Dict:
    """ Carrega as configurações """
    with open(default_cfg_(), 'r') as f:
        config = yaml.safe_load(f)

    return config


def make_obj_params_(
    obj_type: str, params: Dict[str, Any]
) -> Any:
    """ Cria o objeto """
    obj = receipts.load_plugins()[obj_type].make(**params)
    return obj


def load_obj(name: str) -> Any:
    """ Cria um objeto a partir das configurações """
    config = load_cfg_()
    try:
        config_params = config[name]
    except KeyError as exc:
        key_error = exc.args[0]
        known_configs = list(config.keys())

        raise KeyError(
            f'A configuração {key_error} não existe; temos as '
            f'seguintes configurações: {known_configs}'
        )

    return make_obj_params_(**config_params)
