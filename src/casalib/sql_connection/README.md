# `casalib.sql_connection`

In this module, we have some objects to deal with several
SQL like databases.

## Classes
`Dialect`: contains several methods to generate valid queries
for the target database.

`Worker`: contains methods to trully interact with the
    database, querying, creating/dropping tables, getting
    metadata.

`Query`: contains a query to be executed, with methods to
    generate some special queries from the `Dialect` and
    interact with the database using a `Worker`.

`Table`: contains a `table_name`, and present some methods
    to interact with the table (dropping the table,
    listing/dropping partitions, getting metadata and
    allowing to querying, creating a `Query` object).

`Connection`: object that has everything to work.


In the files we have the interfaces.
