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
     each of them runs a COPY FREEZE job as a sub-process, and starts and
     control the sub-processes until all the data has been copied over.
     
     Postgres catalog table pg_class is used to get the list of tables with
     data to copy around, and the `reltuples` is used to start with the
     tables with the greatest number of rows first, as an attempt to
     minimize the copy time.
     
  4. In each copy table sub-process, as soon as the data copying is done,
     then `pgcopydb` gets the list of index definitions attached to the
     current target table and creates them in parallel.
     
     The primary indexes are created as UNIQUE indexes at this stage.
     
     Then the PRIMARY KEY constraints are created USING the just built
     index, allowing the primary key index itself to be created in parallel
     with other indexes on the same table.
     
  6. Then VACUUM ANALYZE is run on the each target table as soon as the data
     and indexes are all created.
     
  7. The final stage consists now of running the rest of the `post-data`
     section script for the whole database, and that's where the foreign key
     constraints and other elements are created.

     The `post-data` script is filtered out using the `pg_restore
     --use-list` option so that indexes and primary key constraints already
     created in step 4. are properly skipped now.
     
     This is done by the per-table sub-processes sharing the dump IDs of the
     `post-data` items they have created with the main process, which can
     then filter out the `pg_restore --list` output and comment the already
     created objects from there, by dump ID.

## Dependencies

At run-time `pgcopydb` depends on the `pg_dump` and `pg_restore` tools being
available in the `PATH`. The tools version should match the Postgres version
of the target database.

## Authors

* [Dimitri Fontaine](https://github.com/dimitri)

## License

Copyright (c) The PostgreSQL Global Development Group.

This project is licensed under the PostgreSQL License, see LICENSE file for details.

This project includes bundled third-party dependencies, see NOTICE file for details.
