"""Factory function for Athena connections."""
from typing import Optional

import boto3

from ..connection import AsyncConnection, Connection
from ..dialects.presto import PrestoDialect
from ..workers.athena import (
    AsyncAwsAthenaWorker,
    AwsAthenaWorker,
)


def make_athena(  # pylint: disable=too-many-arguments
    database: str,
    s3_output: str,
    region: str,
    workgroup: str = "primary",
    poll_interval: float = 0.5,
    catalog: str = "AwsDataCatalog",
    session: Optional[boto3.Session] = None,
) -> Connection:
    """Create a fully wired Athena Connection.

    Builds an ``AwsAthenaWorker``, pairs it with
    ``PrestoDialect`` (Athena speaks Presto SQL),
    and returns a ready-to-use ``Connection``.

    Args:
        database: Default Glue/Athena database
            name.
        s3_output: S3 URI where Athena writes
            query results and table data
            (e.g. ``s3://my-bucket/athena/``).
        region: AWS region name
            (e.g. ``us-east-1``).
        workgroup: Athena workgroup name.
            Defaults to ``"primary"``.
        poll_interval: Seconds between query
            status polls. Defaults to ``0.5``.
        catalog: Athena data catalog name.
            Defaults to ``"AwsDataCatalog"``.
        session: Optional pre-built boto3
            Session for dependency injection.
            When ``None`` (default) a session
            is created lazily from ``region``.

    Returns:
        A ``Connection`` configured with
        ``PrestoDialect`` and an
        ``AwsAthenaWorker``.

    Example::

        conn = make_athena(
            database="my_db",
            s3_output="s3://bucket/results/",
            region="us-east-1",
        )
        df = conn.table("my_db.orders").collect()
    """
    worker = AwsAthenaWorker(
        database=database,
        s3_output=s3_output,
        region=region,
        workgroup=workgroup,
        poll_interval=poll_interval,
        catalog=catalog,
        session_=session,
    )
    return Connection(
        dialect=PrestoDialect,
    ).set_worker(worker)


def make_athena_async(  # pylint: disable=too-many-arguments
    database: str,
    s3_output: str,
    region: str,
    workgroup: str = "primary",
    poll_interval: float = 0.5,
    catalog: str = "AwsDataCatalog",
    session: Optional[boto3.Session] = None,
) -> AsyncConnection:
    """Create a fully wired async Athena
    Connection.

    Builds an ``AwsAthenaWorker``, wraps it
    in ``AsyncAwsAthenaWorker``, and returns
    a ready-to-use ``AsyncConnection`` using
    ``PrestoDialect``.

    Args:
        database: Default Glue/Athena database.
        s3_output: S3 URI for query results.
        region: AWS region name.
        workgroup: Athena workgroup name.
        poll_interval: Seconds between polls.
        catalog: Athena data catalog name.
        session: Optional pre-built boto3
            Session for dependency injection.

    Returns:
        An ``AsyncConnection`` configured with
        ``PrestoDialect`` and an
        ``AsyncAwsAthenaWorker``.

    Example::

        conn = make_athena_async(
            database="my_db",
            s3_output="s3://bucket/results/",
            region="us-east-1",
        )
        df = await conn.table(
            "my_db.orders"
        ).collect()
    """
    sync_worker = AwsAthenaWorker(
        database=database,
        s3_output=s3_output,
        region=region,
        workgroup=workgroup,
        poll_interval=poll_interval,
        catalog=catalog,
        session_=session,
    )
    return AsyncConnection(
        dialect=PrestoDialect,
    ).set_worker(
        AsyncAwsAthenaWorker(
            sync_worker=sync_worker
        )
    )
