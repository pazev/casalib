"""AWS Athena dialect and worker."""
from .dialect import AwsAthenaDialect
from .worker import AwsAthenaWorker

__all__ = ["AwsAthenaDialect", "AwsAthenaWorker"]
