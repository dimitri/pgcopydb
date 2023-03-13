.. _pgcopydb:

pgcopydb
=========

pgcopydb - copy an entire Postgres database from source to target

Synopsis
--------

pgcopydb provides the following commands::

  pgcopydb
    clone    Clone an entire database from source to target
    fork     Clone an entire database from source to target
    follow   Replay changes from the source database to the target database
    snapshot  Create and exports a snapshot on the source database
  + copy     Implement the data section of the database copy
  + dump     Dump database objects from a Postgres instance
  + restore  Restore database objects into a Postgres instance
  + list     List database objects from a Postgres instance
  + stream   Stream changes from the source database
    help     print help message
    version  print pgcopydb version

Description
-----------

The pgcopydb command implements a full migration of an entire Postgres
database from a source instance to a target instance. Both the Postgres
instances must be available for the entire duration of the command.

The pgcopydb command also implements a full `Logical Decoding`__ client for
Postgres, allowing Change Data Capture to replay data changes (DML)
happening on the source database after the base copy snapshot. The pgcopydb
logical decoding client code is compatible with both `test_decoding`__ and
`wal2json`__ output plugins, and defaults to using test_decoding.

__ https://www.postgresql.org/docs/current/logicaldecoding.html
__ https://www.postgresql.org/docs/current/test-decoding.html
__ https://github.com/eulerto/wal2json/

pgcopydb help
-------------

The ``pgcopydb help`` command lists all the supported sub-commands:

::

   $ pgcopydb help
    pgcopydb
      clone     Clone an entire database from source to target
      fork      Clone an entire database from source to target
      follow    Replay changes from the source database to the target database
      copy-db   Clone an entire database from source to target
      snapshot  Create and exports a snapshot on the source database
    + copy      Implement the data section of the database copy
    + dump      Dump database objects from a Postgres instance
    + restore   Restore database objects into a Postgres instance
    + list      List database objects from a Postgres instance
    + stream    Stream changes from the source database
      ping      Copy the roles from the source instance to the target instance
      help      print help message
      version   print pgcopydb version

    pgcopydb copy
      db           Copy an entire database from source to target
      roles        Copy the roles from the source instance to the target instance
      extensions   Copy the extensions from the source instance to the target instance
      schema       Copy the database schema from source to target
      data         Copy the data section from source to target
      table-data   Copy the data from all tables in database from source to target
      blobs        Copy the blob data from ther source database to the target
      sequences    Copy the current value from all sequences in database from source to target
      indexes      Create all the indexes found in the source database in the target
      constraints  Create all the constraints found in the source database in the target

    pgcopydb dump
      schema     Dump source database schema as custom files in work directory
      pre-data   Dump source database pre-data schema as custom files in work directory
      post-data  Dump source database post-data schema as custom files in work directory
      roles      Dump source database roles as custome file in work directory

    pgcopydb restore
      schema      Restore a database schema from custom files to target database
      pre-data    Restore a database pre-data schema from custom file to target database
      post-data   Restore a database post-data schema from custom file to target database
      roles       Restore database roles from SQL file to target database
      parse-list  Parse pg_restore --list output from custom file

    pgcopydb list
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
      cleanup    cleanup source and target systems for logical decoding
      prefetch   Stream JSON changes from the source database and transform them to SQL
      catchup    Apply prefetched changes from SQL files to the target database
      replay     Replay changes from the source to the target database, live
    + create     Create resources needed for pgcopydb
    + drop       Drop resources needed for pgcopydb
    + sentinel   Maintain a sentinel table on the source database
      receive    Stream changes from the source database
      transform  Transform changes from the source database into SQL commands
      apply      Apply changes from the source database into the target database

    pgcopydb stream create
      slot    Create a replication slot in the source database
      origin  Create a replication origin in the target database

    pgcopydb stream drop
      slot    Drop a replication slot in the source database
      origin  Drop a replication origin in the target database

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

pgcopydb version
----------------

The ``pgcopydb version`` command outputs the version string of the version
of pgcopydb used, and can do that in the JSON format when using the
``--json`` option.

::

   $ pgcopydb version
   pgcopydb version 0.8
   compiled with PostgreSQL 12.12 on x86_64-apple-darwin16.7.0, compiled by Apple LLVM version 8.1.0 (clang-802.0.42), 64-bit
   compatible with Postgres 10, 11, 12, 13, and 14

In JSON:

::

   $ pgcopydb version --json
   {
       "pgcopydb": "0.8",
       "pg_major": "12",
       "pg_version": "12.12",
       "pg_version_str": "PostgreSQL 12.12 on x86_64-apple-darwin16.7.0, compiled by Apple LLVM version 8.1.0 (clang-802.0.42), 64-bit",
       "pg_version_num": 120012
   }

The details about the Postgres version applies to the version that's been
used to build pgcopydb from sources, so that's the version of the client
library ``libpq`` really.


pgcopydb ping
-------------

The ``pgcopydb ping`` command attempts to connect to both the source and the
target Postgres databases, concurrently.

::

   pgcopydb ping: Copy the roles from the source instance to the target instance
   usage: pgcopydb ping  --source ... --target ...

     --source              Postgres URI to the source database
     --target              Postgres URI to the target database

An example output looks like the following:

::

   $ pgcopydb ping
   18:04:48 84679 INFO   Running pgcopydb version 0.10.31.g7e5fbb8.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb"
   18:04:48 84683 INFO   Successfully could connect to target database at "postgres://@:/plop?"
   18:04:48 84682 INFO   Successfully could connect t source database at "postgres://@:/pagila?"

This command implements a retry policy (named *Decorrelated Jitter*) and can
be used in automation to make sure that the databases are ready to accept
connections.
