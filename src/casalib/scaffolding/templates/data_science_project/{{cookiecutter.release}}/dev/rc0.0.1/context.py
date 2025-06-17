"""
File to add project context to any file in the project.
"""
import os
from pathlib import Path
import sys
from typing import Union


CUR_FLD = Path(__file__).parent
MAX_DEPTH = 6
FILE_TO_SEEK = 'project_config.yaml'


def discover_proj_root_fld(cur_fld: Union[str, Path]) -> Path:
    """ Discover the root folder """
    proj_root_fld = None

    # Try to find the project file
    for idx in range(MAX_DEPTH):
        files = os.listdir(cur_fld)

        if FILE_TO_SEEK in files:
            proj_root_fld = cur_fld
            break

        cur_fld = cur_fld.parent

    if proj_root_fld is None:
        raise FileNotFoundError(
            f'{FILE_TO_SEEK} not found in the project tree.'
        )

    return proj_root_fld


# Project folder definition
PROJ_ROOT = discover_proj_root_fld(CUR_FLD)

LIBS_FLD = PROJ_ROOT / 'libs'
sys.path.insert(0, str(LIBS_FLD))

DOCS_FLD = PROJ_ROOT / 'docs'

DEV_FLD = PROJ_ROOT / 'dev'
DEV_QUERIES_FLD = DEV_FLD / 'queries'

PRD_FLD = PROJ_ROOT / 'prod'
PRD_QUERIES_FLD = PRD_FLD / 'queries'


# Add queries directory to Python path based on the current
# folder
if DEV_FLD in CUR_FLD.parents:
    sys.path.insert(1, str(DEV_QUERIES_FLD))

if PRD_FLD in CUR_FLD.parents:
    sys.path.insert(1, str(PRD_QUERIES_FLD))
