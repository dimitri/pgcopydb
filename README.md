# pgcopydb

[![Documentation Status](https://readthedocs.org/projects/pgcopydb/badge/?version=latest)](https://pgcopydb.readthedocs.io/en/latest/?badge=latest)

## Introduction

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

Given that, pgcopydb then uses pg_dump and pg_restore for the schema parts
of the process, and implements its own data copying multi-process streaming
parts. Also, pgcopydb bypasses pg_restore index building and drives that
internally so that all indexes may be built concurrently.

## Base Copy and Change Data Capture

pgcopydb implements both the base copy of a database and also Change Data
Capture to allow replay of changes from the source database to the target
database. The Change Data Capture facility is implemented using Postgres
Logical Decoding infrastructure and the wal2json plugin.

The `pgcopydb follow` command implements a logical replication client for
the logical decoding plugin wal2json.

The `pgcopydb clone --follow` command implements a full solution for online
migration. Beware that online migrations involve a lot more complexities
when compared to offline migration. It is always a good idea to first
implement offline migration first. The command `pgcopydb clone` is used to
implement the offline migration approach.

## Documentation

Full documentation is available online, including manual pages of all the
pgcopydb sub-commands. Check out
[https://pgcopydb.readthedocs.io/](https://pgcopydb.readthedocs.io/en/latest/).

```
$ pgcopydb help
  pgcopydb
    clone     Clone an entire database from source to target
    fork      Clone an entire database from source to target
    follow    Replay changes from the source database to the target database
    snapshot  Create and export a snapshot on the source database
  + compare   Compare source and target databases
  + copy      Implement the data section of the database copy
  + dump      Dump database objects from a Postgres instance
  + restore   Restore database objects into a Postgres instance
  + list      List database objects from a Postgres instance
  + stream    Stream changes from the source database
    ping      Attempt to connect to the source and target instances
    help      Print help message
    version   Print pgcopydb version

  pgcopydb compare
    schema  Compare source and target schema
    data    Compare source and target data

  pgcopydb copy
    db           Copy an entire database from source to target
    roles        Copy the roles from the source instance to the target instance
    extensions   Copy the extensions from the source instance to the target instance
    schema       Copy the database schema from source to target
    data         Copy the data section from source to target
    table-data   Copy the data from all tables in database from source to target
    blobs        Copy the blob data from the source database to the target
    sequences    Copy the current value from all sequences in database from source to target
    indexes      Create all the indexes found in the source database in the target
    constraints  Create all the constraints found in the source database in the target

  pgcopydb dump
    schema     Dump source database schema as custom files in work directory
    pre-data   Dump source database pre-data schema as custom files in work directory
    post-data  Dump source database post-data schema as custom files in work directory
    roles      Dump source database roles as custom file in work directory

  pgcopydb restore
    schema      Restore a database schema from custom files to target database
    pre-data    Restore a database pre-data schema from custom file to target database
    post-data   Restore a database post-data schema from custom file to target database
    roles       Restore database roles from SQL file to target database
    parse-list  Parse pg_restore --list output from custom file

  pgcopydb list
    databases    List databases
    extensions   List all the source extensions to copy
    collations   List all the source collations to copy
    tables       List all the source tables to copy data from
    table-parts  List a source table copy partitions
    sequences    List all the source sequences to copy data from
    indexes      List all the indexes to create again after copying the data
    depends      List all the dependencies to filter-out
    schema       List the schema to migrate, formatted in JSON
    progress     List the progress

  pgcopydb stream
    setup      Setup source and target systems for logical decoding
    cleanup    Cleanup source and target systems for logical decoding
    prefetch   Stream JSON changes from the source database and transform them to SQL
    catchup    Apply prefetched changes from SQL files to the target database
    replay     Replay changes from the source to the target database, live
  + sentinel   Maintain a sentinel table on the source database
    receive    Stream changes from the source database
    transform  Transform changes from the source database into SQL commands
    apply      Apply changes from the source database into the target database

  pgcopydb stream sentinel
    create  Create the sentinel table on the source database
    drop    Drop the sentinel table on the source database
    get     Get the sentinel table values on the source database
  + set     Maintain a sentinel table on the source database

  pgcopydb stream sentinel set
    startpos  Set the sentinel start position LSN on the source database
    endpos    Set the sentinel end position LSN on the source database
    apply     Set the sentinel apply mode on the source database
    prefetch  Set the sentinel prefetch mode on the source database
```

## Example

When using `pgcopydb` it is possible to achieve the result outlined before
with this simple command line:

```bash
$ export PGCOPYDB_SOURCE_PGURI="postgres://user@source.host.dev/dbname"
$ export PGCOPYDB_TARGET_PGURI="postgres://role@target.host.dev/dbname"

$ pgcopydb clone --table-jobs 8 --index-jobs 2
```

A typical output from the command would contain lots of lines of logs, and
then a table summary with a line per table and some information (timing for
the table COPY, cumulative timing for the CREATE INDEX commands), and then
an overall summary that looks like the following:

```
18:26:35 77615 INFO  [SOURCE] Copying database from "port=54311 host=localhost dbname=pgloader"
18:26:35 77615 INFO  [TARGET] Copying database into "port=54311 dbname=plop"
18:26:35 77615 INFO  STEP 1: dump the source database schema (pre/post data)
18:26:35 77615 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section pre-data --file /tmp/pgcopydb/schema/pre.dump 'port=54311 host=localhost dbname=pgloader'
18:26:35 77615 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section post-data --file /tmp/pgcopydb/schema/post.dump 'port=54311 host=localhost dbname=pgloader'
18:26:36 77615 INFO  STEP 2: restore the pre-data section to the target database
18:26:36 77615 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54311 dbname=plop' /tmp/pgcopydb/schema/pre.dump
18:26:36 77615 INFO  STEP 3: copy data from source to target in sub-processes
18:26:36 77615 INFO  STEP 4: create indexes and constraints in parallel
18:26:36 77615 INFO  STEP 5: vacuum analyze each table
18:26:36 77615 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
18:26:36 77615 INFO  Fetched information for 56 tables
...
18:26:37 77615 INFO  STEP 6: restore the post-data section to the target database
18:26:37 77615 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54311 dbname=plop' --use-list /tmp/pgcopydb/schema/post.list /tmp/pgcopydb/schema/post.dump

  OID |   Schema |            Name | copy duration | indexes | create index duration
------+----------+-----------------+---------------+---------+----------------------
17085 |      csv |           track |          62ms |       1 |                  24ms
  ...
  ...

                                          Step   Connection    Duration   Concurrency
 ---------------------------------------------   ----------  ----------  ------------
                                   Dump Schema       source       884ms             1
                                Prepare Schema       target       405ms             1
 COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both       1s281         8 + 2
                             COPY (cumulative)         both       2s040             8
                     CREATE INDEX (cumulative)       target       381ms             2
                               Finalize Schema       target        29ms             1
 ---------------------------------------------   ----------  ----------  ------------
                     Total Wall Clock Duration         both       2s639         8 + 2
 ---------------------------------------------   ----------  ----------  ------------
```

## Installing pgcopydb

See our [documentation](https://pgcopydb.readthedocs.io/en/latest/install.html).

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

## Manual Steps

The `pgcopydb` command line also includes entry points that allows
implementing any step on its own.

  1. `pgcopydb snapshot &`
  2. `pgcopydb dump schema`
  3. `pgcopydb restore pre-data`
  4. `pgcopydb copy table-data`
  5. `pgcopydb copy blobs`
  6. `pgcopydb copy sequences`
  7. `pgcopydb copy indexes`
  8. `pgcopydb copy constraints`
  9. `pgcopydb restore post-data`
 10. `kill %1`

Using individual commands fails to provide the advanced concurrency
capabilities of the main `pgcopydb clone` command, so it is strongly
advised to prefer that main command.

Also when using separate commands, one has to consider the `--snapshot`
option that allows for consistent operations. A background process should
then export the snapshot and maintain a transaction opened for the duration
of the operations. See documentation for `pgcopydb snapshot`.

## Authors

* [Dimitri Fontaine](https://github.com/dimitri)

## License

Copyright (c) The PostgreSQL Global Development Group.

This project is licensed under the PostgreSQL License, see LICENSE file for details.

This project includes bundled third-party dependencies, see NOTICE file for details.
