"""
Module with functions to configure the logging package
"""
import logging
from typing import List, Optional, Tuple

def create_filter_packages(
    pkg_ignlevel_message: List[
        Tuple[str, int, Optional[str]]
    ]
):
    class FilterPackages(logging.Filter):
        """ Class to filter packages """
        def filter(self, record):
            """ Filtering method """
            for pkg, ignlevel, msg in pkg_ignlevel_message:
                if pkg not in record.name:
                    continue

                if record.levelno > ignlevel:
                    continue

                if not msg:
                    return False
                else:
                    return not(msg in record.msg)

            return True

    return FilterPackages


def set_logging(
    file: str = 'out.log',
    force: bool = False,
    pkg_ignlevel_message: Optional[List[
        Tuple[str, int, Optional[str]]
    ]] = None,
):
    """ Set logging """
    pkg_ignlevel_message = pkg_ignlevel_message or []

    filter_ = create_filter_packages(
        pkg_ignlevel_message=pkg_ignlevel_message
    )

    logging.basicConfig(
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(file),
        ],
        level=logging.INFO,
        format=(
            '%(asctime)s %(name)-20s %(levelname)-8s '
            '%(message)s'
        ),
        force=force,
    )

    for handler in logging.root.handlers:
        handler.addFilter(filter_)
