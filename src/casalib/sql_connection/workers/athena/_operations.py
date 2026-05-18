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
# Type normalisation
# ----------------------------------

_CATALOG = "AwsDataCatalog"

# Common Athena type aliases unified to a
# canonical form for compatibility checks.
_TYPE_ALIASES: Dict[str, str] = {
    "string": "varchar",
    "int": "integer",
    "long": "bigint",
    "float": "real",
}


def _normalize_type(t: str) -> str:
    """Lowercase, strip precision spec, apply
    alias map.

    ``decimal(10, 2)`` → ``decimal``,
    ``string`` → ``varchar``, etc.

    Args:
        t: Raw Athena type string.

    Returns:
        Normalised type string.
    """
    t = t.strip().lower().split("(")[0]
    return _TYPE_ALIASES.get(t, t)


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


def _col_info(
    client: Any,
    execution_id: str,
) -> Dict[str, str]:
    """Return {label: native_type} from a
    completed query execution.

    Reads the first page of results only;
    ColumnInfo is present even for 0-row
    result sets.

    Args:
        client: boto3 Athena client.
        execution_id: Completed query ID.

    Returns:
        Ordered mapping of column label to
        Athena native type string.
    """
    resp = client.get_query_results(
        QueryExecutionId=execution_id,
        MaxResults=1,
    )
    info = (
        resp["ResultSet"]
        ["ResultSetMetadata"]
        ["ColumnInfo"]
    )
    return {c["Label"]: c["Type"] for c in info}


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


def run_query(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    session: boto3.Session,
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
        session: boto3 Session.
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
    client = session.client("athena")
    with _managed_execution(
        client, query, database,
        s3_output, workgroup,
    ) as execution_id:
        _wait(client, execution_id, poll_interval)
        return _fetch(client, execution_id)


# ----------------------------------
# Metadata helpers
# ----------------------------------

def _parse_table_meta(
    resp: Dict[str, Any],
) -> Tuple[
    Dict[str, str],
    Dict[str, str],
    Dict[str, str],
]:
    """Parse a GetTableMetadata API response.

    Args:
        resp: Full boto3 response dict.

    Returns:
        Tuple of (non-partition cols,
        partition cols, table parameters),
        each as an ordered {name: type} dict.
        Column order matches the API response.
    """
    meta = resp["TableMetadata"]
    cols = {
        c["Name"]: c["Type"]
        for c in meta.get("Columns", [])
    }
    part_cols = {
        c["Name"]: c["Type"]
        for c in meta.get("PartitionKeys", [])
    }
    params = {
        k: str(v)
        for k, v in meta.get(
            "Parameters", {}
        ).items()
    }
    return cols, part_cols, params


def _check_schema_compatibility(
    table_meta: Metadata,
    query_meta: Metadata,
) -> None:
    """Verify the query output is compatible
    with the target table schema.

    Every column defined in the table must be
    present in the query output with a
    compatible type. Extra columns in the query
    are ignored (they will be excluded by
    ``_reorder_query``).

    Args:
        table_meta: Metadata of the target
            table.
        query_meta: Metadata inferred from the
            source query.

    Raises:
        ValueError: A required column is absent
            from the query output.
        TypeError: A column's type in the query
            output is incompatible with the
            table definition.
    """
    for col, col_type in table_meta.cols.items():
        if col not in query_meta.cols:
            raise ValueError(
                f"Column '{col}' is required by"
                f" table but missing from query."
            )
        q_type = _normalize_type(
            query_meta.cols[col]
        )
        t_type = _normalize_type(col_type)
        if q_type != t_type:
            raise TypeError(
                f"Column '{col}': table expects"
                f" '{col_type}', query returns"
                f" '{query_meta.cols[col]}'."
            )


def _reorder_query(
    query: str,
    col_order: List[str],
) -> str:
    """Wrap query in a CTE and SELECT columns
    in ``col_order``.

    The result is suitable for use as the
    body of an ``INSERT INTO`` statement whose
    target table defines columns in that order.

    Args:
        query: Original SQL query.
        col_order: Column names in the order
            required by the target table
            (non-partition first, partition
            last).

    Returns:
        Rewritten SQL query.
    """
    cols = ",\n    ".join(col_order)
    return (
        f"WITH __src AS (\n"
        f"{query}\n"
        f")\n"
        f"SELECT\n"
        f"    {cols}\n"
        f"FROM __src"
    )


# ----------------------------------
# awswrangler / boto3 operations
# ----------------------------------

def _insert_into_existing(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    session: boto3.Session,
    query: str,
    table_name: str,
    schema: str,
    s3_output: str,
    workgroup: str,
    database: str,
    catalog: str,
    poll_interval: float,
) -> None:
    """Validate schema and INSERT query results
    into an existing Athena table.

    Args:
        session: boto3 Session.
        query: SELECT query to materialise.
        table_name: Destination table name
            (schema.table format).
        schema: Glue database name.
        s3_output: S3 URI for output data.
        workgroup: Athena workgroup name.
        database: Default Athena database for
            metadata queries.
        catalog: Athena data catalog name.
        poll_interval: Seconds between status
            polls for the metadata query.

    Raises:
        ValueError: A required column is absent
            from the query output.
        TypeError: A column's type in the query
            is incompatible with the table.
    """
    table_meta = get_table_metadata(
        session=session,
        table_name=table_name,
        catalog=catalog,
    )
    query_meta = get_query_metadata(
        session=session,
        query=query,
        database=database,
        s3_output=s3_output,
        workgroup=workgroup,
        poll_interval=poll_interval,
        catalog=catalog,
    )
    _check_schema_compatibility(
        table_meta, query_meta
    )
    col_order = list(table_meta.cols.keys())
    ordered_query = _reorder_query(
        query, col_order
    )
    wr.athena.start_query_execution(
        sql=(
            f"INSERT INTO {table_name}\n"
            f"{ordered_query}"
        ),
        database=schema,
        s3_output=s3_output,
        workgroup=workgroup,
        wait=True,
        boto3_session=session,
    )


def create_insert(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    session: boto3.Session,
    query: str,
    table_name: str,
    s3_output: str,
    workgroup: str,
    database: str,
    catalog: str = _CATALOG,
    poll_interval: float = 0.5,
    partition_cols: Optional[List[str]] = None,
) -> str:
    """Create a table or insert into an existing
    one using awswrangler.

    When the table already exists, the query
    schema is validated against the table
    schema before inserting:

    1. Both missing columns and type mismatches
       raise an error.
    2. The query columns are reordered to match
       the table's column definition order
       (non-partition columns first, partition
       columns last).

    Args:
        session: boto3 Session.
        query: SELECT query to materialise.
        table_name: Destination table name
            (schema.table format).
        s3_output: S3 URI for output data.
        workgroup: Athena workgroup name.
        database: Default Athena database for
            metadata queries.
        catalog: Athena data catalog name.
        poll_interval: Seconds between status
            polls for the metadata query.
        partition_cols: Partition keys used
            only when creating a new table.

    Returns:
        The resolved table name.

    Raises:
        ValueError: A required column is absent
            from the query output.
        TypeError: A column's type in the query
            is incompatible with the table.
    """
    schema, table = table_name.split(".", 1)
    exists = wr.catalog.does_table_exist(
        database=schema, table=table,
        boto3_session=session,
    )
    if not exists:
        return create_ctas(
            session=session,
            query=query,
            table_name=table_name,
            s3_output=s3_output,
            partition_cols=partition_cols,
        )
    _insert_into_existing(
        session=session,
        query=query,
        table_name=table_name,
        schema=schema,
        s3_output=s3_output,
        workgroup=workgroup,
        database=database,
        catalog=catalog,
        poll_interval=poll_interval,
    )
    return table_name


def create_ctas(
    session: boto3.Session,
    query: str,
    table_name: str,
    s3_output: str,
    partition_cols: Optional[List[str]] = None,
) -> str:
    """Create a table from query using CTAS
    via awswrangler.

    Args:
        session: boto3 Session.
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
        boto3_session=session,
    )
    return table_name


def get_query_metadata(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    session: boto3.Session,
    query: str,
    database: str,
    s3_output: str,
    workgroup: str,
    poll_interval: float,
    catalog: str = _CATALOG,  # pylint: disable=unused-argument
) -> Metadata:
    """Return metadata for a query result set.

    Runs ``SELECT * FROM (<query>) LIMIT 0``
    via the boto3 Athena client and reads the
    native Athena column types from the
    ``ColumnInfo`` section of the response.
    No temporary tables are created.

    Args:
        session: boto3 Session.
        query: SQL query to inspect.
        database: Default Glue/Athena database.
        s3_output: S3 URI for query results.
        workgroup: Athena workgroup name.
        poll_interval: Seconds between polls.
        catalog: Athena data catalog name
            (reserved for future use).

    Returns:
        Metadata with native Athena column
        types.
    """
    client = session.client("athena")
    meta_query = (
        f"SELECT * FROM ({query})"
        f" AS __meta_src LIMIT 0"
    )
    with _managed_execution(
        client, meta_query, database,
        s3_output, workgroup,
    ) as execution_id:
        _wait(client, execution_id, poll_interval)
        cols = _col_info(client, execution_id)
    return Metadata(cols=cols)


def get_table_metadata(
    session: boto3.Session,
    table_name: str,
    catalog: str = _CATALOG,
) -> Metadata:
    """Return metadata for a physical table
    using the boto3 Athena GetTableMetadata API.

    Returns native Athena types (e.g.
    ``varchar``, ``bigint``) rather than
    pandas dtype strings.

    Args:
        session: boto3 Session.
        table_name: Fully qualified name
            (schema.table).
        catalog: Athena data catalog name.
            Defaults to ``AwsDataCatalog``.

    Returns:
        Metadata with native Athena column and
        partition information.
    """
    client = session.client("athena")
    schema, table = table_name.split(".", 1)
    resp = client.get_table_metadata(
        CatalogName=catalog,
        DatabaseName=schema,
        TableName=table,
    )
    cols, part_cols, params = _parse_table_meta(
        resp
    )
    all_cols = {**cols, **part_cols}
    return Metadata(
        cols=all_cols,
        table=TableInfo(
            table_name=table_name,
            schema=schema,
            table=table,
            partition_cols=part_cols,
            custom_metadata=params,
        ),
    )


def drop(
    session: boto3.Session,
    table_name: str,
) -> None:
    """Drop a table and delete its S3 data.

    Deletes all S3 objects at the table's
    storage location before removing the
    Glue catalog entry.

    Args:
        session: boto3 Session.
        table_name: Fully qualified name
            (schema.table).
    """
    schema, table = table_name.split(".", 1)
    location = wr.catalog.get_table_location(
        database=schema, table=table,
        boto3_session=session,
    )
    if location:
        _delete_s3_prefix(location)
    wr.catalog.delete_table_if_exists(
        database=schema, table=table,
        boto3_session=session,
    )


def _get_raw_partitions(
    session: boto3.Session,
    schema: str,
    table: str,
    filters: Tuple[str, ...],
) -> Dict[str, List[str]]:
    """Return matching partitions with S3 paths.

    Args:
        session: boto3 Session.
        schema: Glue database name.
        table: Table name.
        filters: fnmatch patterns, one per
            partition column. Empty means all.

    Returns:
        Mapping of S3 path to list of partition
        column values.
    """
    raw: Dict[str, List[str]] = (
        wr.catalog.get_partitions(
            database=schema, table=table,
            boto3_session=session,
        )
    )
    if not filters:
        return dict(raw)
    result: Dict[str, List[str]] = {}
    for path, vals in raw.items():
        if all(
            fnmatch.fnmatch(v, f)
            for v, f in zip(vals, filters)
        ):
            result[path] = list(vals)
    return result


def list_partitions(
    session: boto3.Session,
    table_name: str,
    *filters: str,
) -> List[Tuple[str, ...]]:
    """List partitions using awswrangler,
    optionally filtered via fnmatch.

    Args:
        session: boto3 Session.
        table_name: Fully qualified name
            (schema.table).
        *filters: Optional fnmatch patterns,
            one per partition column.

    Returns:
        List of tuples of partition values.
    """
    schema, table = table_name.split(".", 1)
    raw = _get_raw_partitions(
        session, schema, table, filters
    )
    return [tuple(v) for v in raw.values()]


def drop_partitions(
    session: boto3.Session,
    table_name: str,
    *filters: str,
) -> None:
    """Drop partitions and delete their S3 data.

    Deletes all S3 objects for each matching
    partition before removing the catalog
    entries.

    Args:
        session: boto3 Session.
        table_name: Fully qualified name
            (schema.table).
        *filters: fnmatch patterns, one per
            partition column.
    """
    schema, table = table_name.split(".", 1)
    raw = _get_raw_partitions(
        session, schema, table, filters
    )
    if not raw:
        return
    for path in raw:
        _delete_s3_prefix(path)
    wr.catalog.delete_partitions(
        table=table,
        database=schema,
        partitions_values=list(raw.values()),
        boto3_session=session,
    )
