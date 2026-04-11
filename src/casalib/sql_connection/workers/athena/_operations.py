"""Athena operations — standalone functions.

Each function receives all arguments it needs
explicitly, with no dependency on the worker
instance. AwsAthenaWorker delegates to these.
"""
import fnmatch
import time
from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
)

import awswrangler as wr
import boto3
import pandas as pd

from ...metadata import Metadata, TableInfo


# ----------------------------------
# S3 helpers
# ----------------------------------

def _delete_s3_prefix(path: str) -> None:
    """Delete all S3 objects under path."""
    paths = wr.s3.list_objects(path=path)
    if paths:
        wr.s3.delete_objects(path=paths)


def _delete_query_results(
    s3_output: str,
    execution_id: str,
) -> None:
    """Delete Athena result files for an
    execution from S3.

    Athena writes a result file (.csv or .txt)
    and a sidecar .metadata file. Both are
    deleted if present.
    """
    base = s3_output.rstrip("/")
    candidates = [
        f"{base}/{execution_id}.csv",
        f"{base}/{execution_id}.txt",
        f"{base}/{execution_id}.csv.metadata",
        f"{base}/{execution_id}.txt.metadata",
    ]
    try:
        wr.s3.delete_objects(path=candidates)
    except Exception:  # pylint: disable=broad-except
        pass


# ----------------------------------
# run_query helpers (boto3 only)
# ----------------------------------

def _wait(
    client: Any,
    execution_id: str,
    poll_interval: float,
) -> None:
    terminal = {
        "SUCCEEDED", "FAILED", "CANCELLED"
    }
    while True:
        resp = client.get_query_execution(
            QueryExecutionId=execution_id
        )
        status = (
            resp["QueryExecution"]["Status"]
        )
        state = status["State"]
        if state in terminal:
            if state != "SUCCEEDED":
                reason = status.get(
                    "StateChangeReason", ""
                )
                raise RuntimeError(
                    f"Query {state}: {reason}"
                )
            return
        time.sleep(poll_interval)


def _fetch(
    client: Any,
    execution_id: str,
) -> pd.DataFrame:
    paginator = client.get_paginator(
        "get_query_results"
    )
    pages = paginator.paginate(
        QueryExecutionId=execution_id
    )
    headers: Optional[List[str]] = None
    rows: List[List[Optional[str]]] = []
    for page in pages:
        result = page["ResultSet"]
        if headers is None:
            col_info = result[
                "ResultSetMetadata"
            ]["ColumnInfo"]
            headers = [
                c["Label"] for c in col_info
            ]
            data_rows = result["Rows"][1:]
        else:
            data_rows = result["Rows"]
        for row in data_rows:
            rows.append([
                d.get("VarCharValue")
                for d in row["Data"]
            ])
    if headers is None:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=headers)


@contextmanager
def _managed_execution(
    client: Any,
    query: str,
    database: str,
    s3_output: str,
    workgroup: str,
) -> Generator[str, None, None]:
    """Context manager that starts a query and
    cancels + cleans up on any interruption.

    Yields the execution ID. If any exception
    (including KeyboardInterrupt) escapes the
    body, the remote query is stopped and its
    S3 result files are deleted before the
    exception is re-raised.
    """
    resp = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": database
        },
        ResultConfiguration={
            "OutputLocation": s3_output
        },
        WorkGroup=workgroup,
    )
    execution_id = resp["QueryExecutionId"]
    try:
        yield execution_id
    except BaseException:
        try:
            client.stop_query_execution(
                QueryExecutionId=execution_id
            )
        except Exception:  # pylint: disable=broad-except
            pass
        _delete_query_results(
            s3_output, execution_id
        )
        raise


def run_query(
    client: Any,
    query: str,
    database: str,
    s3_output: str,
    workgroup: str,
    poll_interval: float,
) -> pd.DataFrame:
    """Execute a query via boto3 Athena API.

    Wraps execution in a context manager: if
    interrupted for any reason, the remote
    query is cancelled and its S3 result files
    are deleted.

    Args:
        client: boto3 Athena client.
        query: SQL query string to execute.
        database: Default Glue/Athena database.
        s3_output: S3 URI for query results.
        workgroup: Athena workgroup name.
        poll_interval: Seconds between polls.

    Returns:
        DataFrame with query results, or an
        empty DataFrame for non-SELECT
        statements.

    Raises:
        RuntimeError: If the query fails or
            is cancelled on the Athena side.
    """
    with _managed_execution(
        client, query, database,
        s3_output, workgroup,
    ) as execution_id:
        _wait(client, execution_id, poll_interval)
        return _fetch(client, execution_id)


# ----------------------------------
# awswrangler operations
# ----------------------------------

def create_insert(
    query: str,
    table_name: str,
    s3_output: str,
    workgroup: str,
    partition_cols: Optional[List[str]] = None,
) -> str:
    """Create a table or insert into an existing
    one using awswrangler.

    Args:
        query: SELECT query to materialise.
        table_name: Destination table name
            (schema.table format).
        s3_output: S3 URI for output data.
        workgroup: Athena workgroup name.
        partition_cols: Partition keys used
            only when creating a new table.

    Returns:
        The resolved table name.
    """
    schema, table = table_name.split(".", 1)
    exists = wr.catalog.does_table_exist(
        database=schema, table=table
    )
    if not exists:
        wr.athena.create_ctas_table(
            sql=query,
            database=schema,
            ctas_table=table,
            ctas_database=schema,
            s3_output=s3_output,
            partitioning_info=(
                partition_cols or []
            ),
            wait=True,
        )
    else:
        wr.athena.start_query_execution(
            sql=(
                f"INSERT INTO {table_name}"
                f"\n{query}"
            ),
            database=schema,
            s3_output=s3_output,
            workgroup=workgroup,
            wait=True,
        )
    return table_name


def create_ctas(
    query: str,
    table_name: str,
    s3_output: str,
    partition_cols: Optional[List[str]] = None,
) -> str:
    """Create a table from query using CTAS
    via awswrangler.

    Args:
        query: SELECT query to materialise.
        table_name: Name for the new table
            (schema.table format).
        s3_output: S3 URI for output data.
        partition_cols: Partition keys for
            the new table.

    Returns:
        The resolved table name.

    Raises:
        Exception: If the table already exists.
    """
    schema, table = table_name.split(".", 1)
    wr.athena.create_ctas_table(
        sql=query,
        database=schema,
        ctas_table=table,
        ctas_database=schema,
        s3_output=s3_output,
        partitioning_info=(
            partition_cols or []
        ),
        wait=True,
    )
    return table_name


def get_query_metadata(
    query: str,
    database: str,
) -> Metadata:
    """Return metadata for a query result set
    using awswrangler.

    Executes a zero-row version of the query
    to infer the column schema.

    Args:
        query: SQL query to inspect.
        database: Default Glue/Athena database.

    Returns:
        Metadata with column types.
    """
    df = wr.athena.read_sql_query(
        sql=(
            f"SELECT * FROM ({query})"
            f" LIMIT 0"
        ),
        database=database,
        ctas_approach=False,
    )
    cols: Dict[str, str] = {
        str(c): str(t)
        for c, t in df.dtypes.items()
    }
    return Metadata(cols=cols)


def _get_partition_cols(
    schema: str,
    table: str,
    region: str,
) -> Dict[str, str]:
    glue = boto3.client(
        "glue", region_name=region
    )
    resp = glue.get_table(
        DatabaseName=schema, Name=table
    )
    return {
        col["Name"]: col["Type"]
        for col in resp["Table"].get(
            "PartitionKeys", []
        )
    }


def get_table_metadata(
    table_name: str,
    region: str,
) -> Metadata:
    """Return metadata for a physical table
    using awswrangler and boto3 Glue.

    Args:
        table_name: Fully qualified name
            (schema.table).
        region: AWS region name.

    Returns:
        Metadata with column and partition
        information.
    """
    schema, table = table_name.split(".", 1)
    col_types = wr.catalog.get_table_types(
        database=schema, table=table
    )
    params = wr.catalog.get_table_parameters(
        database=schema, table=table
    )
    partition_cols = _get_partition_cols(
        schema, table, region
    )
    return Metadata(
        cols=col_types,
        table=TableInfo(
            table_name=table_name,
            schema=schema,
            table=table,
            partition_cols=partition_cols,
            custom_metadata=params,
        ),
    )


def drop(table_name: str) -> None:
    """Drop a table and delete its S3 data.

    Deletes all S3 objects at the table's
    storage location before removing the
    Glue catalog entry.

    Args:
        table_name: Fully qualified name
            (schema.table).
    """
    schema, table = table_name.split(".", 1)
    location = wr.catalog.get_table_location(
        database=schema, table=table
    )
    if location:
        _delete_s3_prefix(location)
    wr.catalog.delete_table_if_exists(
        database=schema, table=table
    )


def _get_raw_partitions(
    schema: str,
    table: str,
    filters: Tuple[str, ...],
) -> Dict[str, List[str]]:
    """Return matching partitions with S3 paths.

    Args:
        schema: Glue database name.
        table: Table name.
        filters: fnmatch patterns, one per
            partition column. Empty means all.

    Returns:
        Mapping of S3 path to list of partition
        column values.
    """
    raw = wr.catalog.get_partitions(
        database=schema, table=table
    )
    if not filters:
        return {
            path: list(vals.values())
            for path, vals in raw.items()
        }
    result: Dict[str, List[str]] = {}
    for path, vals_dict in raw.items():
        row = tuple(vals_dict.values())
        if all(
            fnmatch.fnmatch(v, f)
            for v, f in zip(row, filters)
        ):
            result[path] = list(row)
    return result


def list_partitions(
    table_name: str,
    *filters: str,
) -> List[Tuple[str, ...]]:
    """List partitions using awswrangler,
    optionally filtered via fnmatch.

    Args:
        table_name: Fully qualified name
            (schema.table).
        *filters: Optional fnmatch patterns,
            one per partition column.

    Returns:
        List of tuples of partition values.
    """
    schema, table = table_name.split(".", 1)
    raw = _get_raw_partitions(
        schema, table, filters
    )
    return [tuple(v) for v in raw.values()]


def drop_partitions(
    table_name: str,
    *filters: str,
) -> None:
    """Drop partitions and delete their S3 data.

    Deletes all S3 objects for each matching
    partition before removing the catalog
    entries.

    Args:
        table_name: Fully qualified name
            (schema.table).
        *filters: fnmatch patterns, one per
            partition column.
    """
    schema, table = table_name.split(".", 1)
    raw = _get_raw_partitions(
        schema, table, filters
    )
    if not raw:
        return
    for path in raw:
        _delete_s3_prefix(path)
    wr.catalog.delete_partitions_if_exist(
        table=table,
        database=schema,
        partitions_values=list(raw.values()),
    )
