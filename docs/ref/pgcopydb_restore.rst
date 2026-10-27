.. _pgcopydb_restore:

pgcopydb restore
================

pgcopydb restore - Restore database objects into a Postgres instance

This command prefixes the following sub-commands:

.. include:: ../include/restore.rst

.. _pgcopydb_restore_schema:

pgcopydb restore schema
-----------------------

pgcopydb restore schema - Restore a database schema from custom files to target database

The command ``pgcopydb restore schema`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

.. include:: ../include/restore-schema.rst

.. _pgcopydb_restore_pre_data:

pgcopydb restore pre-data
-------------------------

pgcopydb restore pre-data - Restore a database pre-data schema from custom file to target database

The command ``pgcopydb restore pre-data`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

.. include:: ../include/restore-pre-data.rst

.. _pgcopydb_restore_post_data:

pgcopydb restore post-data
--------------------------

pgcopydb restore post-data - Restore a database post-data schema from custom file to target database

The command ``pgcopydb restore post-data`` uses pg_restore to create the SQL
schema definitions from the given ``pgcopydb dump schema`` export directory.
This command is not compatible with using Postgres files directly, it must
be fed with the directory output from the ``pgcopydb dump ...`` commands.

.. include:: ../include/restore-post-data.rst

.. _pgcopydb_restore_roles:

pgcopydb restore roles
----------------------

pgcopydb restore roles - Restore database roles from SQL file to target database

The command ``pgcopydb restore roles`` runs the commands from the SQL script
obtained from the command ``pgcopydb dump roles``. Roles that already exist
on the target database are skipped.

The ``pg_dumpall`` command issues two lines per role, the first one is a
``CREATE ROLE`` SQL command, the second one is an ``ALTER ROLE`` SQL
command. Both those lines are skipped when the role already exists on the
target database.

.. include:: ../include/restore-roles.rst

Description
-----------

The ``pgcopydb restore schema`` command implements the creation of SQL
objects in the target database, second and last steps of a full database
migration.

When the command runs, it calls ``pg_restore`` on the files found at the
expected location within the ``--target`` directory, which has typically
been created with the ``pgcopydb dump schema`` command.

The ``pgcopydb restore pre-data`` and ``pgcopydb restore post-data`` are
limiting their actions to the file with pre-data and post-data
in the source directory..

Options
-------

The following options are available to ``pgcopydb restore schema``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--target

  Connection string to the target Postgres instance.

--dir

  During its normal operations pgcopydb creates a lot of temporary files to
  track sub-processes progress. Temporary files are created in the directory
  specified by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  otherwise to ``/tmp/pgcopydb``.

--restore-jobs

  How many threads or processes can be used during pg_restore. A good option is
  to set this option to the count of CPU cores that are available on the
  Postgres target system.

  If this value is not set, we reuse the ``--index-jobs`` value. If that value
  is not set either, we use the the default value for ``--index-jobs``.

--drop-if-exists

  When restoring the schema on the target Postgres instance, ``pgcopydb``
  actually uses ``pg_restore``. When this options is specified, then the
  following pg_restore options are also used: ``--clean --if-exists``.

  This option is useful when the same command is run several times in a row,
  either to fix a previous mistake or for instance when used in a continuous
  integration system.

  This option causes ``DROP TABLE`` and ``DROP INDEX`` and other DROP
  commands to be used. Make sure you understand what you're doing here!

--no-owner

  Do not output commands to set ownership of objects to match the original
  database. By default, ``pg_restore`` issues ``ALTER OWNER`` or ``SET
  SESSION AUTHORIZATION`` statements to set ownership of created schema
  elements. These statements will fail unless the initial connection to the
  database is made by a superuser (or the same user that owns all of the
  objects in the script). With ``--no-owner``, any user name can be used for
  the initial connection, and this user will own all the created objects.

--filters <filename>

  This option allows to exclude table and indexes from the copy operations.
  See :ref:`filtering` for details about the expected file format and the
  filtering options available.

--skip-extensions

  Skip copying extensions from the source database to the target database.

  When used, schema that extensions depend-on are also skipped: it is
  expected that creating needed extensions on the target system is then the
  responsibility of another command (such as
  :ref:`pgcopydb_copy_extensions`), and schemas that extensions depend-on
  are part of that responsibility.

  Because creating extensions require superuser, this allows a multi-steps
  approach where extensions are dealt with superuser privileges, and then
  the rest of the pgcopydb operations are done without superuser privileges.

--skip-ext-comments

  Skip copying COMMENT ON EXTENSION commands. This is implicit when using
  --skip-extensions.

--restart

  When running the pgcopydb command again, if the work directory already
  contains information from a previous run, then the command refuses to
  proceed and delete information that might be used for diagnostics and
  forensics.

  In that case, the ``--restart`` option can be used to allow pgcopydb to
  delete traces from a previous run.

--resume

  When the pgcopydb command was terminated before completion, either by an
  interrupt signal (such as C-c or SIGTERM) or because it crashed, it is
  possible to resume the database migration.

  When resuming activity from a previous run, table data that was fully
  copied over to the target server is not sent again. Table data that was
  interrupted during the COPY has to be started from scratch even when using
  ``--resume``: the COPY command in Postgres is transactional and was rolled
  back.

  Same reasonning applies to the CREATE INDEX commands and ALTER TABLE
  commands that pgcopydb issues, those commands are skipped on a
  ``--resume`` run only if known to have run through to completion on the
  previous one.

  Finally, using ``--resume`` requires the use of ``--not-consistent``.

--not-consistent

  In order to be consistent, pgcopydb exports a Postgres snapshot by calling
  the `pg_export_snapshot()`__ function on the source database server. The
  snapshot is then re-used in all the connections to the source database
  server by using the ``SET TRANSACTION SNAPSHOT`` command.

  Per the Postgres documentation about ``pg_export_snapshot``:

    Saves the transaction's current snapshot and returns a text string
    identifying the snapshot. This string must be passed (outside the
    database) to clients that want to import the snapshot. The snapshot is
    available for import only until the end of the transaction that exported
    it.

  __ https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION-TABLE

  Now, when the pgcopydb process was interrupted (or crashed) on a previous
  run, it is possible to resume operations, but the snapshot that was
  exported does not exists anymore. The pgcopydb command can only resume
  operations with a new snapshot, and thus can not ensure consistency of the
  whole data set, because each run is now using their own snapshot.

--snapshot

  Instead of exporting its own snapshot by calling the PostgreSQL function
  ``pg_export_snapshot()`` it is possible for pgcopydb to re-use an already
  exported snapshot.

--verbose

  Increase current verbosity. The default level of verbosity is INFO. In
  ascending order pgcopydb knows about the following verbosity levels:
  FATAL, ERROR, WARN, INFO, NOTICE, DEBUG, TRACE.

--debug

  Set current verbosity to DEBUG level.

--trace

  Set current verbosity to TRACE level.

--quiet

  Set current verbosity to ERROR level.

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

First, using ``pgcopydb restore schema``

::

   $ PGCOPYDB_DROP_IF_EXISTS=on pgcopydb restore schema --source /tmp/target/ --target "port=54314 dbname=demo"
   07:45:10.626 39254 INFO   Using work dir "/tmp/pgcopydb"
   07:45:10.626 39254 INFO   Restoring database from existing files at "/tmp/pgcopydb"
   07:45:10.720 39254 INFO   Found 2 indexes (supporting 2 constraints) in the target database
   07:45:10.723 39254 INFO   Using pg_restore for Postgres "16.2" at "/usr/bin/pg_restore"
   07:45:10.723 39254 INFO   [TARGET] Restoring database into "postgres://postgres@127.0.0.1:5435/demo?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   07:45:10.737 39254 INFO   Drop tables on the target database, per --drop-if-exists
   07:45:10.750 39254 INFO    /usr/bin/pg_restore --dbname 'postgres://postgres@127.0.0.1:5435/demo?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60' --section pre-data --jobs 4 --clean --if-exists --use-list /tmp/pgcopydb/schema/pre-filtered.list /tmp/pgcopydb/schema/schema.dump
   07:45:10.803 39254 INFO    /usr/bin/pg_restore --dbname 'postgres://postgres@127.0.0.1:5435/demo?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60' --section post-data --jobs 4 --clean --if-exists --use-list /tmp/pgcopydb/schema/post-filtered.list /tmp/pgcopydb/schema/schema.dump

Then the ``pgcopydb restore pre-data`` and ``pgcopydb restore post-data``
would look the same with just a single call to pg_restore instead of the
both of them.

