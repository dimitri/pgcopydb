# tools/

Developer tooling for pgcopydb — scripts that participate in the build
process but are not shipped in the binary.

## sql2c.py

`sql2c.py` is a code generator that converts SQL query files into C source
code (`sql_queries.c` and `sql_queries.h`).  It exists because pgcopydb
needs to issue slightly different SQL depending on the PostgreSQL server
version and on the active source filter type, and maintaining those variants
as C string literals inside `.c` files is hard to read and impossible to run
directly in psql for debugging.

### SQL file layout

All SQL source files live under `src/bin/pgcopydb/sql/`.  Three query
shapes are supported:

```
sql/<query>.sql                          flat      — no version, no filter
sql/<query>/<version>.sql               versioned — pg-version dispatch only
sql/<query>/filter.map + <dim>/*.sql    filtered  — filter dispatch (± versions)
```

**Flat** queries are simple: one file, one generated function with signature
`bool pgcopydb_sql_<query>(const char **sql)`.

**Versioned** queries select a SQL variant based on the server's
`pg_version_num`.  Version specifiers in the filename stem:

| Stem       | Meaning                                |
|------------|----------------------------------------|
| `default`  | fallback — any version not matched above |
| `pg-96`    | before PG 10 (version < 100000)        |
| `pg10`     | PG 10.x only (100000–109999)           |
| `pg10-12`  | PG 10 through 12 (100000–129999)       |
| `pg12-`    | PG 12 and later (≥ 120000)             |

**Filtered** queries select a SQL variant based on the active
`SourceFilterType`.  A `filter.map` file in the query directory maps each
filter subdirectory name to the corresponding C enum constant:

```
# header filtering.h
# enum   SourceFilterType
no-filter     SOURCE_FILTER_TYPE_NONE
incl          SOURCE_FILTER_TYPE_INCL
excl          SOURCE_FILTER_TYPE_EXCL
list-not-incl SOURCE_FILTER_TYPE_LIST_NOT_INCL
list-excl     SOURCE_FILTER_TYPE_LIST_EXCL
```

The generator parses the named C header to resolve enum constant names to
their integer values, so filter.map entries stay in sync with the header
automatically.

**Preamble** — a `preamble.sql` file in a filtered query directory is
prepended verbatim to every filter variant's SQL string.  It is used for
large shared CTE blocks (see `list_source_depend/preamble.sql`).  Preamble
files are SQL fragments, not complete statements, and do not carry a
trailing `;`.

### Trailing semicolons

`.sql` files that are standalone queries carry a trailing `;` so they can
be copy-pasted directly into psql.  The generator strips the `;` before
embedding in C: `PQsendQuery` and `PQsendQueryParams` accept trailing
semicolons, but `PQprepare` rejects them (it treats a trailing `;` as an
empty second command).  Current call sites all use `PQsendQuery`, but
stripping keeps the embedded strings safe for the full libpq API.

Filter variant files that are assembled with a preamble are not standalone
queries (they reference CTE names from the preamble and depend on
`pg_temp` filter tables prepared at runtime by pgcopydb) — those files do
not carry a trailing `;`.

### Generated files

`sql_queries.h` and `sql_queries.c` are **committed** to the repository so
that the build does not require Python.  They must be regenerated whenever
any `.sql` file or `filter.map` changes.  The Makefile handles this
automatically via an explicit dependency rule; you can also regenerate
manually:

```bash
make -C src/bin/pgcopydb gen-sql
```

After regenerating, run the Docker-based formatter to keep CI happy:

```bash
docker run --rm -v "$(pwd):/workdir" -w /workdir \
    citus/stylechecker:no-py citus_indent
```

### print sub-command

The `print` sub-command assembles a complete, runnable SQL query from its
parts and writes it to stdout with a trailing `;`:

```bash
# no-filter variant (standalone, copy-pasteable):
python3 tools/sql2c.py print src/bin/pgcopydb/sql list_source_tables

# specific filter (preamble + variant assembled, then ';' appended):
python3 tools/sql2c.py print src/bin/pgcopydb/sql list_source_depend excl

# flat query:
python3 tools/sql2c.py print src/bin/pgcopydb/sql list_databases
```

For filtered queries, the filter argument defaults to `no-filter` when
omitted (because that variant does not depend on any `pg_temp` tables and
can be run in any psql session).
