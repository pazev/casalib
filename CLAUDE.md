# CLAUDE.md

## Project overview

`casalib` is a Python library that provides a dialect-agnostic abstraction layer over SQL databases. The goal is to let application code build and run queries without coupling to a specific SQL engine (Athena, BigQuery, Spark SQL, etc.).

The library lives under `src/casalib/` and currently has one subpackage: `sql_connection`.

## Architecture — `sql_connection`

The design is built around four collaborating classes and two abstract base classes:

```
Connection
  └── produces → Query / Table
                    └── uses → SqlDialectAbstract (type, not instance)
                    └── uses → WorkerAbstract (instance, optional)
```

### Abstract bases

- **`SqlDialectAbstract`** (`sql_abstract.py`) — SQL generation. Holds `input_query: str` and exposes methods that return `Query` objects (`select`, `agg`, `sample`, `enrich`, `get_diffs`, `op`, `last_partitions`, `get_duplicates`, `jsonify`). **Stored as a type (`Type[SqlDialectAbstract]`), not an instance.** Instantiated at call time with `dialect(input_query=...)`. Also has two classmethods for table name utilities: `util_table_name_has_fullname` and `util_table_name_split_schema`.

- **`WorkerAbstract`** (`worker_abstract.py`) — Database I/O. Handles `run_query`, `create_insert`, `create_ctas`, `get_query_metadata`, `get_table_metadata`, `drop`, `list_partitions`, `drop_partitions`. Concrete implementations wrap the actual database client.

### Concrete classes

- **`Connection`** (`connection.py`) — Entry point. Holds a dialect type and an optional worker. `.query(sql)` and `.table(name)` return fully wired `Query`/`Table` objects.

- **`Query`** (`query.py`) — A stored SQL string plus dialect type. Main operations: `.collect()`, `.create_insert()`, `.create_ctas()`, `.metadata()`. The `.q` property instantiates the dialect with the current query and returns a `_QueryBuilder` for chaining transformations (note: `_QueryBuilder` uses `__getattr__`, so IDE type hints will not work through it).

- **`Table`** (`table.py`) — Named table wrapper. Exposes `.query` (select all), `.metadata()`, `.list_partitions()`, `.drop_partitions()`, `.drop()`, `.collect()`.

- **`Metadata`** / **`TableInfo`** / **`QueryInfo`** (`metadata.py`) — Plain dataclasses carrying schema and partition information.

### Worker / dialect propagation pattern

All three of `Query`, `Table`, and `Connection` follow the same pattern for the worker:

```python
obj.set_worker(worker)   # sets worker_; accepts None
obj.worker               # raises RuntimeError if not set (Query/Table)
                         # returns None if not set (Connection)
```

`Connection` always calls `.set_worker(self.worker)` on every `Query`/`Table` it creates, even when `self.worker` is `None`.

### Generated column naming convention (`agg`)

When `SqlDialectAbstract.agg(...)` generates aggregated columns, the name must follow `col__aggregation` (double underscore), e.g. `amount__sum`, `id__count_distinct`.

## Key conventions

- `dialect` is always a **type** (`Type[SqlDialectAbstract]`), never an instance. Instantiate it as `dialect(input_query=...)` when calling methods.
- `worker` is optional at construction time; pass it via `set_worker()`.
- `list_partitions` in the worker returns `List[Tuple[str, ...]]` and accepts `*filters: str` (fnmatch patterns per partition column). Filtering happens inside the worker, not in `Table`.
- `drop_partitions` in the worker also accepts `*filters: str` directly.
- `Table.list_partitions` wraps the worker result in a `pd.DataFrame` using `metadata.table.partition_cols` keys as column names.

## Tooling

- **Linter:** pylint (min score 9.5, mccabe complexity plugin enabled, max line length 88)
- **Type checker:** mypy strict mode
- **Build:** setuptools
