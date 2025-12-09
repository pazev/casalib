"""
Modules with classes and functions to trigger a remote
job
"""
from collections import ChainMap
from dataclasses import dataclass
import os
from pathlib import Path
import re
import tempfile
from typing import Dict, List, Optional, Protocol, Union

import boto3
import sagemaker
from sagemaker.processing import Processor, ProcessingInput
from sagemaker.pytorch import PyTorchProcessor

from ....base import (
    AbstractRemoteJob, list_files, make_tar_gz_file
)


class AwsProcessingJobsMaker(Protocol):
    ''' Protocol to create a Processor class '''
    # pylint: disable=too-many-arguments
    # pylint: disable=too-few-public-methods
    def __call__(
        self,
        base_job_name: str,
        sagemaker_role: str,
        sagemaker_session: sagemaker.Session,
        instance_type: str = 'ml.g5.4xlarge',
        max_runtime_in_seconds: int = 7200,
    ) -> Processor:
        ...


@dataclass
class ProcessingJob(AbstractRemoteJob):
    """
    Class to run a ProcessingJob

    The function processor_maker must have the prototype
        func(
            base_job_name: str,
            sagemaker_role: str,
            sagemaker_session: sagemaker.Session,
            instance_type: str,
            max_runtime_in_seconds: int
        )
    """
    # pylint: disable=too-many-instance-attributes
    basename: str
    instance_type: str = 'ml.g5.4xlarge'
    processor_maker: Optional[AwsProcessingJobsMaker] = None
    boto3_session: Optional[str] = None
    sagemaker_role: Optional[str] = None
    sagemaker_session: Optional[sagemaker.Session] = None
    default_bucket: Optional[str] = None
    default_bucket_prefix: Optional[str] = None

    def __post_init__(self):
        """ Post-init validations """
        if (
            (self.sagemaker_session is None) and
            (
                (self.default_bucket is None) or
                (self.default_bucket_prefix is None)
            )
        ):
            raise ValueError(
                'You must set sagemaker_session or pass '
                'the params default_bucket and '
                'default_bucket_prefix.'
            )

    def get_basename(self) -> str:
        """ Return basename """
        return self.basename

    def get_sagemaker_session_(self) -> sagemaker.Session:
        """ Get the sagemaker.Session """
        if self.sagemaker_session is not None:
            return self.sagemaker_session

        if self.default_bucket is not None and self.default_bucket_prefix is not None:
            boto3_session = self.boto3_session or boto3.Session()

            return sagemaker.Session(
                boto_session=boto3_session,
                default_bucket=self.default_bucket,
                default_bucket_prefix=self.default_bucket_prefix,
            )

        raise ValueError(
            'sagemaker_session or '
            'default_bucket/default_bucket_prefix must be '
            'set.'
        )

    def get_sagemaker_role_(self) -> str:
        """ Get the SageMaker role """
        return self.sagemaker_role or sagemaker.get_execution_role()

    def make_processor_(
        self,
        max_runtime_in_seconds: int
    ) -> Processor:
        """ Create o Processor """
        if self.processor_maker is not None:
            return self.processor_maker(
                base_job_name=self.get_basename(),
                sagemaker_role=self.get_sagemaker_role_(),
                sagemaker_session=self.get_sagemaker_session_(),
                instance_type=self.instance_type,
                max_runtime_in_seconds=max_runtime_in_seconds,
            )  # type: ignore

        return standard_processor_(
            base_job_name=self.get_basename(),
            sagemaker_role=self.get_sagemaker_role_(),
            sagemaker_session=self.get_sagemaker_session_(),
            instance_type=self.instance_type,
            max_runtime_in_seconds=max_runtime_in_seconds,
        )

    def run(
        self,
        main_program: Union[str, Path],
        libs_to_send: Optional[List[Union[str, Path]]] = None,
        arguments_dict: Optional[Dict[str, str]] = None,
        max_runtime_in_seconds: int = 7200,
    ):
        """ Run a Job """
        script_processor = self.make_processor_(
            max_runtime_in_seconds=max_runtime_in_seconds
        )

        run_processor(
            script_processor=script_processor,
            main_program=main_program,
            libs_to_send=libs_to_send,
            arguments_dict=arguments_dict,
        )


def standard_processor_(
    base_job_name: str,
    sagemaker_role: str,
    sagemaker_session: sagemaker.Session,
    instance_type: str = 'ml.g5.4xlarge',
    max_runtime_in_seconds: int = 7200,
) -> Processor:
    """
    Create a standard Processor if one is not provided at
    processor_maker
    """
    params = {
        'framework_version': '2.1',
        'py_version': 'py310',
        'base_job_name': base_job_name,
        'role': sagemaker_role,
        'command': ['python3'],
        'instance_count': 1,
        'instance_type': instance_type,
        'sagemaker_session': sagemaker_session,
    }

    if max_runtime_in_seconds:
        params['max_runtime_in_seconds'] = max_runtime_in_seconds

    return PyTorchProcessor(**params)  # type: ignore


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
    main_program: Union[str, Path],
    libs_to_send: Optional[List[Union[str, Path]]] = None,
    arguments_dict: Optional[Dict[str, str]] = None,
):
    """
    The main program will be send as main_program.py to the
    ProcessingJob. The bootloader will load it and run the
    function main(**kwargs).
    """
    cur_dir = Path(__file__).parent
    arguments_dict = arguments_dict or {}

    libs_to_send = libs_to_send or []

    files_to_send = ChainMap(
        *[list_files(path) for path in libs_to_send]
    )

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        output_file_ = make_tar_gz_file(
            tmp.name,
            files_to_send
        )

        files_inputs = []

        files_inputs.extend(
            gen_processing_input(
                str(main_program),
                '/opt/ml/processing/libs_cp/main_program.py'
            )
        )

        files_inputs.extend(
            gen_processing_input(
                str(output_file_),
                '/opt/ml/processing/libs_cp/contents_libs_to_send.tar.gz'
            )
        )

        # Run params
        run_params = {
            'code': str(cur_dir / 'bootloader.py'),
            'inputs': files_inputs,
        }

        if arguments_dict:
            run_params['arguments'] = [
                elem
                for key, val in arguments_dict.items()
                for elem in [f'--{key}', val]
            ]

        script_processor.run(**run_params)
