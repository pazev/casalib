"""
Module with functions to configure the logging package
"""
#pylint: disable=too-few-public-methods
import logging
from typing import List, Optional, Tuple


class FilterPackages(logging.Filter):
    """ Class to filter packages """

    def __init__(
        self,
        pkg_ignlevel_message: Optional[
            List[Tuple[str, int, Optional[str]]]
        ] = None,
    ):
        """ Init """
        self.pkg_ignlevel_message = pkg_ignlevel_message
        super().__init__()

    def filter(self, record):
        """ Filtering method """
        if self.pkg_ignlevel_message is None:
            return True

        for pkg, ignlevel, msg in self.pkg_ignlevel_message:
            if pkg not in record.name:
                continue

            if record.levelno > ignlevel:
                continue

            if not msg:
                return False
            return not msg in record.msg

        return True


def set_logging_function(
    file: str = 'out.log',
    force: bool = False,
    pkg_ignlevel_message: Optional[List[
        Tuple[str, int, Optional[str]]
    ]] = None,
):
    """ Set logging """
    pkg_ignlevel_message = pkg_ignlevel_message or []

    filter_ = FilterPackages(
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
