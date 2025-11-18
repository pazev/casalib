""" Module with base definitions, to run a remote program """
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Union


class AbstractRemoteJob(ABC):
    """ Abstraction for remote job """
    @abstractmethod
    def get_basename(self) -> str:
        """ Return basename """

    @abstractmethod
    def run(
        self,
        main_program: Union[str, Path],
        libs_to_send: Optional[List[Union[str, Path]]] = None,
        arguments_dict: Optional[Dict[str, str]] = None,
        max_runtime_in_seconds: int = 7200,
    ):
        """ Run a Job """
