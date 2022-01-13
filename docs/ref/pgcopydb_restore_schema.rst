.. _pgcopydb_restore_schema:

pgcopydb restore schema
=======================

pgcopydb restore schema - Restore a database schema from custom files to target database

Synopsis
--------

The command ``pgcopydb restore schema`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

::

   pgcopydb restore schema: Restore a database schema from custom files to target database
   usage: pgcopydb restore schema  --source <dir> --target <URI>

     --source          Directory where to find the schema custom files
     --target          Postgres URI to the source database
     --drop-if-exists  On the target database, clean-up from a previous run first


Description
-----------

The ``pgcopydb restore schema`` command implements the creation of SQL
objects in the target database, second and last steps of a full database
migration.

When the command runs, it calls ``pg_restore`` on the files found at the
expected location within the ``--target`` directory, which has typically
been created with the ``pgcopydb dump schema`` command.

Options
-------

The following options are available to ``pgcopydb restore schema``:

--source

  Source directory where to read pg_dump custom format schema files. This
  directory is expected to have been prepared with the ``pgcopydb dump
  schema`` command.

--target

  Connection string to the target Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

Environment
-----------

PGCOPYDB_TARGET_PGURI

  Connection string to the target Postgres instance. When ``--target`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_DROP_IF_EXISTS

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb uses the pg_restore options ``--clean --if-exists`` when
   creating the schema on the target Postgres instance.

Examples
--------

::

   $ PGCOPYDB_DROP_IF_EXISTS=on pgcopydb restore schema --source /tmp/target/ --target "port=54314 dbname=demo"
   09:54:37 20401 INFO  Restoring database from "/tmp/target/"
   09:54:37 20401 INFO  Restoring database into "port=54314 dbname=demo"
   09:54:37 20401 INFO  Found a stale pidfile at "/tmp/target//pgcopydb.pid"
   09:54:37 20401 WARN  Removing the stale pid file "/tmp/target//pgcopydb.pid"
   09:54:37 20401 INFO  Using pg_restore for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_restore"
   09:54:37 20401 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54314 dbname=demo' --clean --if-exists /tmp/target//schema/pre.dump
   09:54:38 20401 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54314 dbname=demo' --clean --if-exists --use-list /tmp/target//schema/post.list /tmp/target//schema/post.dump
