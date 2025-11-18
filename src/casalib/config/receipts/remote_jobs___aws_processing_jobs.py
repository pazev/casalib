"""
Receipt to quick create a remote job to execute at
SageMaker Training Jobs.
"""
# pylint: disable=import-outside-toplevel
from pathlib import Path
from typing import Dict, List, Optional, Union


def make(
    default_instance_type: str,
    default_bucket: str,
    default_bucket_prefix: str,
):
    """ Receipt to quick load a remote job; not ready """
    from casalib.remote_jobs.connectors.aws_processing_jobs import (
        ProcessingJob
    )

    def create_function(
        basename: str,
        main_program: Union[str, Path],
        libs_to_send: Optional[List[Union[str, Path]]] = None,
        arguments_dict: Optional[Dict[str, str]] = None,
        max_runtime_in_seconds: int = 7200,
        instance_type: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
    ):
        """ Function to run the processing job """
        # pylint: disable=too-many-arguments
        proc = ProcessingJob(
            basename,
            instance_type=instance_type or default_instance_type,
            default_bucket=s3_bucket or default_bucket,
            default_bucket_prefix=s3_prefix or default_bucket_prefix,
        )

        proc.run(
            main_program=main_program,
            libs_to_send=libs_to_send,
            arguments_dict=arguments_dict,
            max_runtime_in_seconds=max_runtime_in_seconds,
        )

    return create_function
