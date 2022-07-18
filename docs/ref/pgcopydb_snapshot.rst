.. _pgcopydb_snapshot:

pgcopydb snapshot
=================

pgcopydb snapshot - Create and exports a snapshot on the source database

The command ``pgcopydb snapshot`` connects to the source database and
executes a SQL query to export a snapshot. The obtained snapshot is both
printed on stdout and also in a file where other pgcopydb commands might
expect to find it.

::

   pgcopydb snapshot: Create and exports a snapshot on the source database
   usage: pgcopydb snapshot  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use

Options
-------

The following options are available to ``pgcopydb create`` and ``pgcopydb
drop`` subcommands:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--dir

  During its normal operations pgcopydb creates a lot of temporary files to
  track sub-processes progress. Temporary files are created in the directory
  location given by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  then to ``/tmp/pgcopydb``.

--snapshot

  Instead of exporting its own snapshot by calling the PostgreSQL function
  ``pg_export_snapshot()`` it is possible for pgcopydb to re-use an already
  exported snapshot.

--slot-name

  Logical replication slot name to use, default to ``pgcopydb``. The slot
  should be created within the same transaction snapshot as the initial data
  copy.

  Must be using the `wal2json`__ output plugin, available with
  format-version 2.

  __ https://github.com/eulerto/wal2json/

--origin

  Logical replication target system needs to track the transactions that
  have been applied already, so that in case we get disconnected or need to
  resume operations we can skip already replayed transaction.

  Postgres uses a notion of an origin node name as documented in
  `Replication Progress Tracking`__. This option allows to pick your own
  node name and defaults to "pgcopydb". Picking a different name is useful
  in some advanced scenarios like migrating several sources in the same
  target, where each source should have their own unique origin node name.

  __ https://www.postgresql.org/docs/current/replication-origins.html

--startpos

  Logical replication target system registers progress by assigning a
  current LSN to the ``--origin`` node name. When creating an origin on the
  target database system, it is required to provide the current LSN from the
  source database system, in order to properly bootstrap pgcopydb logical
  decoding.

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.

Examples
--------

Create a snapshot on the source database in the background:

::

   $ pgcopydb snapshot &
   [1] 72938
   17:31:52 72938 INFO  Running pgcopydb version 0.7.13.gcbf2d16.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/./src/bin/pgcopydb/pgcopydb"
   17:31:52 72938 INFO  Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   17:31:52 72938 INFO  Removing the stale pid file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/pgcopydb.aux.pid"
   17:31:52 72938 INFO  Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   17:31:52 72938 INFO  Exported snapshot "00000003-000CB5FE-1" from the source database
   00000003-000CB5FE-1

And when the process is done, stop maintaining the snapshot in the
background:

::

   $ kill %1
   17:31:56 72938 INFO  Asked to terminate, aborting
   [1]+  Done                    pgcopydb snapshot
