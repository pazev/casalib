"""Factory function for Athena connections."""
from ..connection import Connection
from ..dialects.presto import PrestoDialect
from ..workers.athena import AwsAthenaWorker


def make_athena(
    database: str,
    s3_output: str,
    region: str,
    workgroup: str = "primary",
    poll_interval: float = 0.5,
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
    )
    return Connection(
        dialect=PrestoDialect,
    ).set_worker(worker)
