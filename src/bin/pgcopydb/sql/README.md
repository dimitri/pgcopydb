# src/bin/pgcopydb/sql/

SQL query files for pgcopydb, together with the tooling that embeds them
in the binary as C string literals.

## Why this exists

pgcopydb issues slightly different SQL depending on the PostgreSQL server
version and on the active source filter type.  Keeping those variants as
raw SQL files lets you read and test them directly in psql.  At build time
they are compiled into the binary so that pgcopydb remains a single
self-contained executable.

## Files

| File | Purpose |
|------|---------|
| `*.sql` | SQL query variants — one file per variant, complete and standalone |
| `sql2c.awk` | POSIX awk script that converts `*.sql` → `sql_queries_data.inc` |
| `Makefile` | Drives `sql2c.awk`; invoked from `src/bin/pgcopydb/Makefile` via `$(MAKE) -C sql` |
| `sql_queries_data.inc` | Generated — one `static const char sql_NAME[]` array per `.sql` file |

`sql_queries_data.inc` is committed so that building from a fresh checkout
does not require running `make gen-sql`.

## Naming convention

The C symbol name for each file is `sql_` + the filename stem, with no
further transformation.  Filenames therefore use only `[a-z0-9_]` characters.

Examples:

```
list_databases.sql                     → sql_list_databases
list_source_tables_no_filter.sql       → sql_list_source_tables_no_filter
list_source_tables_no_filter_pg96.sql → sql_list_source_tables_no_filter_pg96
list_source_depend_excl.sql            → sql_list_source_depend_excl
```

Filter variants correspond to `SourceFilterType` enum values in `filtering.h`:

```c
typedef enum
{
    SOURCE_FILTER_TYPE_NONE = 0,   /* _no_filter   */
    SOURCE_FILTER_TYPE_INCL,       /* _incl        */
    SOURCE_FILTER_TYPE_EXCL,       /* _excl        */

    SOURCE_FILTER_TYPE_LIST_NOT_INCL,  /* _list_not_incl  */
    SOURCE_FILTER_TYPE_LIST_EXCL,      /* _list_excl      */

    SOURCE_FILTER_TYPE_EXCL_INDEX,     /* _excl_index     */
    SOURCE_FILTER_TYPE_LIST_EXCL_INDEX /* _list_excl_index */
} SourceFilterType;
```

Version variants (`_pg96`) cover PostgreSQL server versions before PG 10
(`pg_version_num < 100000`).

## Dispatch layer

`sql_queries_data.inc` only contains the string data.  The function that
selects the right string at runtime — based on `SourceFilterType` and
`pg_version_num` — is hand-written in `sql_queries.c` (one directory up),
which `#include`s this file.  `sql_queries.h` declares the public API.

## Adding a new variant

1. Create the `.sql` file here, named `<query>_<variant>.sql`.
2. Run `make -C src/bin/pgcopydb gen-sql` (or just `make` — it rebuilds
   `sql_queries_data.inc` automatically when any `.sql` file changes).
3. Add the corresponding `case` branch in `sql_queries.c`.
4. Commit the `.sql` file, the updated `sql_queries_data.inc`, and
   `sql_queries.c` together.
