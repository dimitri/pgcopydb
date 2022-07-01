.. _pgcopydb_create:

pgcopydb create
===============

pgcopydb create - Create resources needed for pgcopybd

This command prefixes the following sub-commands:

::

  pgcopydb create
    snapshot  Create and exports a snapshot on the source database


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

Options
-------

The following options are available to ``pgcopydb dump schema``:

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

And when the process is done, stop maintaining the snapshot in the
background:

::

   $ kill %1
   17:31:56 72938 INFO  Asked to terminate, aborting
   [1]+  Done                    ./src/bin/pgcopydb/pgcopydb create snapshot
