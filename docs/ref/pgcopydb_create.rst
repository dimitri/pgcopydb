.. _pgcopydb_create:

pgcopydb create
===============

::

   pgcopydb
   + create   Create resources needed for pgcopydb
   + drop     Drop resources needed for pgcopydb

::

  pgcopydb create
    snapshot  Create and exports a snapshot on the source database
    slot      Create a replication slot in the source database


::

  pgcopydb drop
    slot  Drop a replication slot in the source database

.. _pgcopydb_create_snapshot:

pgcopydb create snapshot
------------------------

pgcopydb create snapshot - Create and exports a snapshot on the source database

The command ``pgcopydb create snapshot`` connects to the source database and
executes a SQL query to export a snapshot. The obtained snapshot is both
printed on stdout and also in a file where other pgcopydb commands might
expect to find it.

::

   pgcopydb create snapshot: Create and exports a snapshot on the source database
   usage: pgcopydb create snapshot  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use

.. _pgcopydb_create_slot:

pgcopydb create slot
--------------------

pgcopydb create slot - Create a replication slot in the source database

The command ``pgcopydb create slot`` connects to the source database and
executes a SQL query to create a logical replication slot using the plugin
``wal2json``.

::

   pgcopydb create slot: Create a replication slot in the source database
   usage: pgcopydb create slot  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use
     --snapshot       Use snapshot obtained with pg_export_snapshot
     --slot-name      Use this Postgres replication slot name

.. _pgcopydb_drop_slot:

pgcopydb drop slot
------------------

pgcopydb drop slot - Drop a replication slot in the source database

The command ``pgcopydb drop slot`` connects to the source database and
executes a SQL query to drop the logical replication slot with the given
name (that defaults to ``pgcopydb``).

::

   pgcopydb drop slot: Drop a replication slot in the source database
   usage: pgcopydb drop slot  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use
     --slot-name      Use this Postgres replication slot name


Options
-------

The following options are available to ``pgcopydb create`` subcommands:

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

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.

Examples
--------

Create a snapshot on the source database in the background:

::

   $ ./src/bin/pgcopydb/pgcopydb create snapshot &
   [1] 72938
   ~/dev/PostgreSQL/pgcopydb 17:31:52 72938 INFO  Running pgcopydb version 0.7.13.gcbf2d16.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/./src/bin/pgcopydb/pgcopydb"
   17:31:52 72938 INFO  Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   17:31:52 72938 INFO  Removing the stale pid file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/pgcopydb.aux.pid"
   17:31:52 72938 INFO  Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   17:31:52 72938 INFO  Exported snapshot "00000003-000CB5FE-1" from the source database
   00000003-000CB5FE-1

Now use the snapshot and create a logical replication slot:

::

   $ pgcopydb create slot --snapshot 00000003-000000FC-1
   12:23:40 7553 INFO  Running pgcopydb version 0.7.14.gcf0ad9b from "/Users/dim/dev/PostgreSQL/pgcopydb/./src/bin/pgcopydb/pgcopydb"
   12:23:40 7553 INFO  Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   12:23:40 7553 INFO  Removing the stale pid file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/pgcopydb.pid"
   12:23:40 7553 INFO  Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   12:23:40 7553 INFO  Created logical replication slot "pgcopydb" with plugin "wal2json" at LSN 1/5C89E4D0

When it's time to drop the replication slot:

::

   $ pgcopydb drop slot
   12:23:50 7559 INFO  Running pgcopydb version 0.7.14.gcf0ad9b from "/Users/dim/dev/PostgreSQL/pgcopydb/./src/bin/pgcopydb/pgcopydb"
   12:23:50 7559 INFO  Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   12:23:50 7559 INFO  Removing the stale pid file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/pgcopydb.pid"
   12:23:50 7559 INFO  Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   12:23:50 7559 INFO  Dropping replication slot "pgcopydb"

And when the process is done, stop maintaining the snapshot in the
background:

::

   $ kill %1
   17:31:56 72938 INFO  Asked to terminate, aborting
   [1]+  Done                    ./src/bin/pgcopydb/pgcopydb create snapshot
