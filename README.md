# pgcopydb

pgcopydb is a tool that automates running `pg_dump | pg_restore` between two
running Postgres servers. To make a copy of a database to another server as
quickly as possible, one would like to use the parallel options of `pg_dump`
and still be able to stream the data to as many `pg_restore` jobs.

The idea would be to use `pg_dump --jobs=N --format=directory
postgres://user@source/dbname | pg_restore --jobs=N --format=directory -d
postgres://user@target/dbname` in a way. This command line can't be made to
work, unfortunately, because `pg_dump --format=directory` writes to local
files and directories first, and then later `pg_restore --format=directory`
can be used to read from those files again.

When using `pgcopydb` it is possible to achieve the result outlined before
with this simple command line:

```bash
$ pgcopydb copy db --jobs=N --source postgres://user@source/dbname --target postgres://user@target/dbname
```

Then `pgcopydb` implements the following steps:

  1. `pgcopydb` produces `pre-data` section and the `post-data` sections of
     the dump using Postgres custom format.

  2. The `pre-data` section of the dump is restored on the target database,
     creating all the Postgres objects from the source database into the
     target database.
     
  3. `pgcopydb` gets the list of ordinary and partitioned tables and for
     each of them runs COPY the data from the source to the target in a
     dedicated sub-process, and starts and control the sub-processes until
     all the data has been copied over.
     
     Postgres catalog table pg_class is used to get the list of tables with
     data to copy around, and the `reltuples` is used to start with the
     tables with the greatest number of rows first, as an attempt to
     minimize the copy time.
     
  4. In each copy table sub-process, as soon as the data copying is done,
     then `pgcopydb` gets the list of index definitions attached to the
     current target table and creates them in parallel.
     
     The primary indexes are created as UNIQUE indexes at this stage.
     
     Then the PRIMARY KEY constraints are created USING the just built
     indexes. This two-steps approach allows the primary key index itself to
     be created in parallel with other indexes on the same table, avoiding
     an EXCLUSIVE LOCK while creating the index.
     
  5. Then VACUUM ANALYZE is run on each target table as soon as the data and
     indexes are all created.
     
  6. The final stage consists now of running the rest of the `post-data`
     section script for the whole database, and that's where the foreign key
     constraints and other elements are created.

     The `post-data` script is filtered out using the `pg_restore
     --use-list` option so that indexes and primary key constraints already
     created in step 4. are properly skipped now.
     
     This is done by the per-table sub-processes sharing the dump IDs of the
     `post-data` items they have created with the main process, which can
     then filter out the `pg_restore --list` output and comment the already
     created objects from there, by dump ID.


## Design Considerations (why oh why)

The reason why `pgcopydb` has been developed is mostly to allow two aspects
that are not possible to achieve directly with `pg_dump` and `pg_restore`,
and that requires just enough fiddling around that not many scripts have
been made available to automate around.

### Bypass intermediate files for the TABLE DATA

First aspect is that for `pg_dump` and `pg_restore` to implement concurrency
they need to write to an intermediate file first.

The [docs for
pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) say the
following about the `--jobs` parameter:

> You can only use this option with the directory output format because this
> is the only output format where multiple processes can write their data at
> the same time.

The [docs for
pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html) say
the following about the `--jobs` parameter:

> Only the custom and directory archive formats are supported with this
> option. The input must be a regular file or directory (not, for example, a
> pipe or standard input).

So the first idea with `pgcopydb` is to provide the `--jobs` concurrency and
bypass intermediate files (and directories) altogether, at least as far as
the actual TABLE DATA set is concerned.

The trick to achieve that is that `pgcopydb` must be able to connect to the
source database during the whole operation, when `pg_restore` may be used
from an export on-disk, without having to still be able to connect to the
source database. In the context of `pgcopydb` requiring access to the source
database is fine. In the context of `pg_restore`, it would not be
acceptable.

### For each table, build all indexes concurrently

The other aspect that `pg_dump` and `pg_restore` are not very smart about is
how they deal with the indexes that are used to support constraints, in
particular unique constraints and primary keys.

Those indexes are exported using the `ALTER TABLE` command directly. This is
fine because the command creates both the constraint and the underlying
index, so the schema in the end is found as expected.

That said, those `ALTER TABLE ... ADD CONSTRAINT` commands require a level
of locking that prevents any concurrency. As we can read on the [docs for
ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html):

> Although most forms of ADD table_constraint require an ACCESS EXCLUSIVE
> lock, ADD FOREIGN KEY requires only a SHARE ROW EXCLUSIVE lock. Note that
> ADD FOREIGN KEY also acquires a SHARE ROW EXCLUSIVE lock on the referenced
> table, in addition to the lock on the table on which the constraint is
> declared.

The trick is then to first issue a `CREATE UNIQUE INDEX` statement and when
the index has been built then issue a second command in the form of `ALTER
TABLE ... ADD CONSTRAINT ... PRIMARY KEY USING INDEX ...`, as in the
following example taken from the logs of actually running `pgcopydb`:

```
...
21:52:06 68898 INFO  COPY "demo"."tracking";
21:52:06 68899 INFO  COPY "demo"."client";
21:52:06 68899 INFO  Creating 2 indexes for table "demo"."client"
21:52:06 68906 INFO  CREATE UNIQUE INDEX client_pkey ON demo.client USING btree (client);
21:52:06 68907 INFO  CREATE UNIQUE INDEX client_pid_key ON demo.client USING btree (pid);
21:52:06 68898 INFO  Creating 1 indexes for table "demo"."tracking"
21:52:06 68908 INFO  CREATE UNIQUE INDEX tracking_pkey ON demo.tracking USING btree (client, ts);
21:52:06 68907 INFO  ALTER TABLE "demo"."client" ADD CONSTRAINT "client_pid_key" UNIQUE USING INDEX "client_pid_key";
21:52:06 68906 INFO  ALTER TABLE "demo"."client" ADD CONSTRAINT "client_pkey" PRIMARY KEY USING INDEX "client_pkey";
21:52:06 68908 INFO  ALTER TABLE "demo"."tracking" ADD CONSTRAINT "tracking_pkey" PRIMARY KEY USING INDEX "tracking_pkey";
...
```

This trick is worth a lot of performance gains on its own, as has been
discovered and experienced and appreciated by
[pgloader](https://github.com/dimitri/pgloader) users already.

## Dependencies

At run-time `pgcopydb` depends on the `pg_dump` and `pg_restore` tools being
available in the `PATH`. The tools version should match the Postgres version
of the target database.

When you have multiple versions of Postgres installed, consider exporting
the `PG_CONFIG` environment variable to the version you want to use.
`pgcopydb` then uses the `PG_CONFIG` from the path and runs `${PG_CONFIG}
--bindir` to find the `pg_dump` and `pg_restore` binaries it needs.

## Authors

* [Dimitri Fontaine](https://github.com/dimitri)

## License

Copyright (c) The PostgreSQL Global Development Group.

This project is licensed under the PostgreSQL License, see LICENSE file for details.

This project includes bundled third-party dependencies, see NOTICE file for details.
