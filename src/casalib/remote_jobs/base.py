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
    ) -> None:
        """
        Run a Job

        Arguments
            main_program: program to be executed
            libs_to_send: parameter to send files and
                folders to the remote machine.

                If you pass a file, it will be added to the
                root folder in the remote machine.

                If you pass a folder, all contents of that
                folder will be added to a same name folder
                in the remote machine.
            arguments_dict: arguments to be sent
            max_runtime_in_seconds: max time to execute the
                program; must stop after ending.
        """


def list_files(path: Union[str, Path]) -> Dict[Path, Path]:
    ''' List all files in a given path '''
    import os

    base_path_ = Path(path)
    base_path_parent = base_path_.parent

    if base_path_.is_file():
        return {
            Path(base_path_): base_path_.relative_to(base_path_parent)
        }

    return {
        path_file: rel_path_file
        for root, _, file_list in os.walk(base_path_)
        for file in file_list
        for path_file in [Path(root) / file]
        for rel_path_file in [path_file.relative_to(base_path_parent)]
    }


def make_tar_gz_file(
    output_file: Union[str, Path],
    dict_files: Dict[Path, Path]
) -> Path:
    """ Create a tar.gz file with all the passed contents.
    """
    import tarfile
    output_file_ = Path(output_file)

    with tarfile.open(output_file_, 'w:gz') as f:
        for orig, dest in dict_files.items():
            f.add(orig, dest)

    return output_file_
