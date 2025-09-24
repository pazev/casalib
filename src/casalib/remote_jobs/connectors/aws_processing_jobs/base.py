from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Type, Union

import boto3
import sagemaker
from sagemaker.processing import Processor, ProcessingInput

from ...base import AbstractRemoteJob


@dataclass
class ProcessingJob(AbstractRemoteJob):
    basename: str
    processor_class: Optional[Type[Processor]] = None

    def get_basename(self) -> str:
        return self.basename

    def run(
        self,
        main_program: Union[str, Path],
        libs_to_send: Optional[List[Union[str, Path]]] = None,
        arguments_dict: Optional[Dict[str, str]] = None,
        max_seconds: int = 7200
    ):
        """ Run a Job """


def gen_processing_input(
    source: Union[Path, str],
    destination: str = '/opt/ml/processing/libs_cp/',
) -> List[ProcessingInput]:
    """
    Create a list of ProcessingInputs to be uploaded to
    ProcessingJob.
    """
    destination = re.sub(r'(\/*)$', '', destination)
    source_ = Path(source)

    if not source_.exists():
        raise FileNotFoundError(f'File not found: {source}')

    # If is a falder, send all files in subfolders
    if source_.is_dir():
        return [
            ProcessingInput(
                source=str(path_),
                destination=f'{destination}/{str(relative_path)}'
            )
            for path_top in [source_]
            for root, dirs, files in os.walk(path_top)
            for root_path in [Path(root)]
            for file in files
            for path_ in [root_path / file]
            for relative_path in [path_.relative_to(path_top.parent)]
        ]

    # If single file, send it
    return [
        ProcessingInput(
            source=str(source_),
            destination=f'{destination}/{source_.name}'
        )
    ]


def run_processor(
    script_processor: Processor,
    main_program: str,
    libs_to_send: Optional[List[str]] = None,
    arguments_dict: Optional[Dict[str, str]] = None
):
    """
    The main program will be send as main_program.py to the
    ProcessingJob. The bootloader will load it and run the
    function main(**kwargs).
    """
    CUR_DIR = Path(__file__).parent
    arguments_dict = arguments_dict or {}

    libs_to_send = libs_to_send or []

    files_inputs = [
        processing_input
        for path_ in libs_to_send
        for processing_input in gen_processing_input(source=path_)
    ]

    files_inputs.extend(
        gen_processing_input(
            main_program,
            '/opt/ml/processing/libs_cp/main_program.py'
        )
    )

    # Run params
    run_params = {
        'code': str(CUR_DIR / 'bootloader.py'),
        'inputs': files_inputs,
    }

    if arguments_dict:
        run_params['arguments'] = [
            elem
            for key, val in arguments_dict.items()
            for elem in [f'--{key}', val]
        ]

    script_processor.run(**run_params)
