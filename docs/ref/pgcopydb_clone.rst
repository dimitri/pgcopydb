pgcopydb clone
==============

The main pgcopydb operation is the clone operation, and for historical and
user friendlyness reasons three aliases are available that implement the
same operation:

::

  pgcopydb
    clone     Clone an entire database from source to target
    fork      Clone an entire database from source to target

.. _pgcopydb_clone:

pgcopydb clone
--------------

The command ``pgcopydb clone`` copies a database from the given source
Postgres instance to the target Postgres instance.

.. include:: ../include/clone.rst

.. _pgcopydb_fork:

pgcopydb fork
-------------

The command ``pgcopydb fork`` copies a database from the given source
Postgres instance to the target Postgres instance. This command is an alias
to the command ``pgcopydb clone`` seen above.

Description
-----------

The ``pgcopydb clone`` command implements both a base copy of a source
database into a target database and also a full `Logical Decoding`__ client
for the `wal2json`__ logical decoding plugin.

__ https://www.postgresql.org/docs/current/logicaldecoding.html
__ https://github.com/eulerto/wal2json/

Base copy, or the clone operation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``pgcopydb clone`` command implements the following steps:

  1. ``pgcopydb`` gets the list of ordinary and partitioned tables from a
     catalog query on the source database, and also the list of indexes, and
     the list of sequences with their current values.

     When filtering is used, the list of objects OIDs that are meant to be
     filtered out is built during this step.

  2. ``pgcopydb`` calls into ``pg_dump`` to produce the ``pre-data`` section
     and the ``post-data`` sections of the dump using Postgres custom
     format.

  3. The ``pre-data`` section of the dump is restored on the target database
     using the ``pg_restore`` command, creating all the Postgres objects
     from the source database into the target database.

     When filtering is used, the ``pg_restore --use-list`` feature is used
     to filter the list of objects to restore in this step.

     This step uses as many as ``--restore-jobs`` jobs for ``pg_restore`` to
     share the workload and restore the objects in parallel.

  4. Then as many as ``--table-jobs`` COPY sub-processes are started to
     share the workload and COPY the data from the source to the target
     database one table at a time, in a loop.

     A Postgres connection and a SQL query to the Postgres catalog table
     pg_class is used to get the list of tables with data to copy around,
     and the `reltuples` statistic is used to start with the tables with the
     greatest number of rows first, as an attempt to minimize the copy time.

  5. An auxiliary process loops through all the Large Objects found on the
     source database and copies its data parts over to the target database,
     much like pg_dump itself would.

     This step is much like ``pg_dump | pg_restore`` for large objects data
     parts, except that there isn't a good way to do just that with the
     tooling.

  6. As many as ``--index-jobs`` CREATE INDEX sub-processes are started to
     share the workload and build indexes. In order to make sure to start
     the CREATE INDEX commands only after the COPY operation has completed,
     a queue mechanism is used. As soon as a table data COPY has completed,
     all the indexes for the table are queued for processing by the CREATE
     INDEX sub-processes.

     The primary indexes are created as UNIQUE indexes at this stage.

  7. Then the PRIMARY KEY constraints are created USING the just built
     indexes. This two-steps approach allows the primary key index itself to
     be created in parallel with other indexes on the same table, avoiding
     an EXCLUSIVE LOCK while creating the index.

  8. As many as ``--table-jobs`` VACUUM ANALYZE sub-processes are started to
     share the workload. As soon as a table data COPY has completed, the
     table is queued for processing by the VACUUM ANALYZE sub-processes.

  9. An auxilliary process loops over the sequences on the source database and
     for each of them runs a separate query on the source to fetch the
     ``last_value`` and the ``is_called`` metadata the same way that pg_dump
     does.

     For each sequence, pgcopydb then calls ``pg_catalog.setval()`` on the
     target database with the information obtained on the source database.

  10. The final stage consists now of running the ``pg_restore`` command for
      the ``post-data`` section script for the whole database, and that's
      where the foreign key constraints and other elements are created.

      The *post-data* script is filtered out using the ``pg_restore
      --use-list`` option so that indexes and primary key constraints
      already created in steps 6 and 7 are properly skipped now.

      This step uses as many as ``--restore-jobs`` jobs for ``pg_restore`` to
      share the workload and restore the objects in parallel.

.. _superuser:

Postgres privileges, superuser, and dump and restore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Postgres has a notion of a superuser status that can be assigned to any role
in the system, and the default role *postgres* has this status. From the
`Role Attributes`__ documentation page we see that:

__ https://www.postgresql.org/docs/current/role-attributes.html

.. epigraph:: superuser status:

   *A database superuser bypasses all permission checks, except the right to
   log in. This is a dangerous privilege and should not be used carelessly;
   it is best to do most of your work as a role that is not a superuser. To
   create a new database superuser, use CREATE ROLE name SUPERUSER. You must
   do this as a role that is already a superuser.*

Some Postgres objects can only be created by superusers, and some read and
write operations are only allowed to superuser roles, such as the following
non-exclusive list:

  - Reading the `pg_authid`__ role password (even when encrypted) is
    restricted to roles with the superuser status. Reading this catalog
    table is done when calling ``pg_dumpall --roles-only`` so that the dump
    file can then be used to restore roles including their passwords.

    __ https://www.postgresql.org/docs/current/catalog-pg-authid.html

    It is possible to implement a pgcopydb migration that skips the
    passwords entirely when using the option ``--no-role-passwords``. In
    that case though authentication might fail until passwords have been
    setup again correctly.

  - Most of the available Postgres extensions, at least when being written
    in C, are then only allowed to be created by roles with superuser
    status.

    When such an extension contains `Extension Configuration Tables`__ and
    has been created with a role having superuser status, then the same
    superuser status is needed again to pg_dump and pg_restore that
    extension and its current configuration.

    __ https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES

When using pgcopydb it is possible to split your migration in privileged and
non-privileged parts, like in the following examples:

.. code-block:: bash
  :linenos:

   $ coproc ( pgcopydb snapshot )

   # first two commands would use a superuser role to connect
   $ pgcopydb copy roles --source ... --target ...
   $ pgcopydb copy extensions --source ... --target ...

   # now it's possible to use a non-superuser role to connect
   $ pgcopydb clone --skip-extensions --source ... --target ...

   $ kill -TERM ${COPROC_PID}
   $ wait ${COPROC_PID}

In such a script, the calls to :ref:`pgcopydb_copy_roles` and
:ref:`pgcopydb_copy_extensions` would be done with connection strings that
connects with a role having superuser status; and then the call to *pgcopydb
clone* would be done with a non-privileged role, typically the role that
owns the source and target databases.

.. warning::

   That said, there is currently a limitation in ``pg_dump`` that impacts
   pgcopydb. When an extension with configuration table has been installed
   as superuser, even the main ``pgcopydb clone`` operation has to be done
   with superuser status.

   That's because pg_dump filtering (here, there ``--exclude-table`` option)
   does not apply to extension members, and pg_dump does not provide a
   mechanism to exclude extensions.

.. _change_data_capture:

Change Data Capture using Postgres Logical Decoding
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using the ``--follow`` option the steps from the :ref:`pgcopydb_follow`
command are also run concurrently to the main copy. The Change Data Capture
is then automatically driven from a prefetch-only phase to the
prefetch-and-catchup phase, which is enabled as soon as the base copy is
done.

See the command :ref:`pgcopydb_stream_sentinel_set_endpos` to remote control
the follow parts of the command even while the command is already running.

The command :ref:`pgcopydb_stream_cleanup` must be used to free resources
created to support the change data capture process.

.. important::

   Make sure to read the documentation for :ref:`pgcopydb_follow` and the
   specifics about `Logical Replication Restrictions`__ as documented by
   Postgres.

   __ https://www.postgresql.org/docs/current/logical-replication-restrictions.html


.. _change_data_capture_example_1:

Change Data Capture Example 1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A simple approach to applying changes after the initial base copy has been
done follows:

.. code-block:: bash
  :linenos:

   $ pgcopydb clone --follow &

   # later when the application is ready to make the switch
   $ pgcopydb stream sentinel set endpos --current

   # later when the migration is finished, clean-up both source and target
   $ pgcopydb stream cleanup

.. _change_data_capture_example_2:

Change Data Capture Example 2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In some cases, it might be necessary to have more control over some of the
steps taken here. Given pgcopydb flexibility, it's possible to implement the
following steps:

  1. Grab a snapshot from the source database and hold an open Postgres
     connection for the duration of the base copy.

     In case of crash or other problems with the main operations, it's then
     possible to resume processing of the base copy and the applying of the
     changes with the same snapshot again.

     This step is also implemented when using ``pgcopydb clone --follow``.
     That said, if the command was interrupted (or crashed), then the
     snapshot would be lost.

  2. Setup the logical decoding within the snapshot obtained in the previous
     step, and the replication tracking on the target database.

     The following SQL objects are then created:

       - a replication slot on the source database,
       - a ``pgcopydb.sentinel`` table on the source database,
       - a replication origin on the target database.

     This step is also implemented when using ``pgcopydb clone --follow``.
     There is no way to implement Change Data Capture with pgcopydb and skip
     creating those SQL objects.

  3. Start the base copy of the source database, and prefetch logical
     decoding changes to ensure that we consume from the replication slot
     and allow the source database server to recycle its WAL files.

  4. Remote control the apply process to stop consuming changes and applying
     them on the target database.

  5. Re-sync the sequences to their now-current values.

     Sequences are not handled by Postgres logical decoding, so extra care
     needs to be implemented manually here.

     .. important::

        The next version of pgcopydb will include that step in the
        ``pgcopydb clone --snapshot`` command automatically, after it stops
        consuming changes and before the process terminates.

  6. Clean-up the specific resources created for supporting resumability of
     the whole process (replication slot on the source database, pgcopydb
     sentinel table on the source database, replication origin on the target
     database).

  7. Stop holding a snaphot on the source database by stopping the
     ``pgcopydb snapshot`` process left running in the background.

If the command ``pgcopydb clone --follow`` fails it's then possible to start
it again. It will automatically discover what was done successfully and what
needs to be done again because it failed or was interrupted (table copy,
index creation, resuming replication slot consuming, resuming applying
changes at the right LSN position, etc).

Here is an example implement the previous steps:

.. code-block:: bash
  :linenos:

   $ pgcopydb snapshot &

   $ pgcopydb stream setup

   $ pgcopydb clone --follow &

   # later when the application is ready to make the switch
   $ pgcopydb stream sentinel set endpos --current

   # when the follow process has terminated, re-sync the sequences
   $ pgcopydb copy sequences

   # later when the migration is finished, clean-up both source and target
   $ pgcopydb stream cleanup

   # now stop holding the snapshot transaction (adjust PID to your environment)
   $ kill %1


Options
-------

The following options are available to ``pgcopydb clone``:

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
  location given by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  then to ``/tmp/pgcopydb``.

--table-jobs

  How many tables can be processed in parallel.

  This limit only applies to the COPY operations, more sub-processes will be
  running at the same time that this limit while the CREATE INDEX operations
  are in progress, though then the processes are only waiting for the target
  Postgres instance to do all the work.

--index-jobs

  How many indexes can be built in parallel, globally. A good option is to
  set this option to the count of CPU cores that are available on the
  Postgres target system, minus some cores that are going to be used for
  handling the COPY operations.

--restore-jobs

  How many threads or processes can be used during pg_restore. A good option is
  to set this option to the count of CPU cores that are available on the
  Postgres target system.

  If this value is not set, we reuse the ``--index-jobs`` value. If that value
  is not set either, we use the the default value for ``--index-jobs``.

--large-object-jobs

  How many worker processes to start to copy Large Objects concurrently.

--split-tables-larger-than

   Allow :ref:`same_table_concurrency` when processing the source database.
   This environment variable value is expected to be a byte size, and bytes
   units B, kB, MB, GB, TB, PB, and EB are known.

--drop-if-exists

  When restoring the schema on the target Postgres instance, ``pgcopydb``
  actually uses ``pg_restore``. When this options is specified, then the
  following pg_restore options are also used: ``--clean --if-exists``.

  This option is useful when the same command is run several times in a row,
  either to fix a previous mistake or for instance when used in a continuous
  integration system.

  This option causes ``DROP TABLE`` and ``DROP INDEX`` and other DROP
  commands to be used. Make sure you understand what you're doing here!

--roles

  The option ``--roles`` add a preliminary step that copies the roles found
  on the source instance to the target instance. As Postgres roles are
  global object, they do not exist only within the context of a specific
  database, so all the roles are copied over when using this option.

  The ``pg_dumpall --roles-only`` is used to fetch the list of roles from
  the source database, and this command includes support for passwords. As a
  result, this operation requires the superuser privileges.

  See also :ref:`pgcopydb_copy_roles`.

--no-role-passwords

  Do not dump passwords for roles. When restored, roles will have a null
  password, and password authentication will always fail until the password
  is set. Since password values aren't needed when this option is specified,
  the role information is read from the catalog view pg_roles instead of
  pg_authid. Therefore, this option also helps if access to pg_authid is
  restricted by some security policy.

--no-owner

  Do not output commands to set ownership of objects to match the original
  database. By default, ``pg_restore`` issues ``ALTER OWNER`` or ``SET
  SESSION AUTHORIZATION`` statements to set ownership of created schema
  elements. These statements will fail unless the initial connection to the
  database is made by a superuser (or the same user that owns all of the
  objects in the script). With ``--no-owner``, any user name can be used for
  the initial connection, and this user will own all the created objects.

--skip-large-objects

  Skip copying large objects, also known as blobs, when copying the data
  from the source database to the target database.

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

--requirements <filename>

  This option allows to specify which version of an extension to install on
  the target database. The given filename is expected to be a JSON file, and
  the JSON contents must be an array of objects with the keys ``"name"`` and
  ``"version"``.

  The command ``pgcopydb list extension --requirements --json`` produces
  such a JSON file and can be used on the target database instance to get
  started.

  See also the command ``pgcopydb list extension --available-versions``.

  See also :ref:`pgcopydb_list_extensions`.

--skip-collations

  Skip copying collations from the source database to the target database.

  In some scenarios the list of collations provided by the Operating System
  on the source and target system might be different, and a mapping then
  needs to be manually installed before calling pgcopydb.

  Then this option allows pgcopydb to skip over collations and assume all
  the needed collations have been deployed on the target database already.

  See also :ref:`pgcopydb_list_collations`.

--skip-vacuum

  Skip running VACUUM ANALYZE on the target database once a table has been
  copied, its indexes have been created, and constraints installed.

--skip-db-properties

  Skip fetching database properties and copying them using the SQL command
  ``ALTER DATABASE ... SET name = value``. This is useful when the source
  and target database have a different set of properties, or when the target
  database is hosted in a way that disabled setting some of the properties
  that have been set on the source database, or also when copying these
  settings is not wanted.

--skip-split-by-ctid

  Skip splitting tables based on CTID during the copy operation. By default,
  pgcopydb splits large tables into smaller chunks based on the CTID column 
  if there isn't a unique integer column in the table. However, in some cases 
  you may want to skip this splitting process if the CTID range scan is slow 
  in the underlying system.

--filters <filename>

  This option allows to exclude table and indexes from the copy operations.
  See :ref:`filtering` for details about the expected file format and the
  filtering options available.

--fail-fast

  Abort early in case of error by sending the TERM signal to all the
  processes in the pgcopydb process group.

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

--follow

  When the ``--follow`` option is used then pgcopydb implements Change Data
  Capture as detailed in the manual page for :ref:`pgcopydb_follow` in
  parallel to the main copy database steps.

  The replication slot is created using the same snapshot as the main
  database copy operation, and the changes to the source database are
  prefetched only during the initial copy, then prefetched and applied in a
  catchup process.

  It is possible to give ``pgcopydb clone --follow`` a termination point
  (the LSN endpos) while the command is running with the command
  :ref:`pgcopydb_stream_sentinel_set_endpos`.

--plugin

  Logical decoding output plugin to use. The default is `test_decoding`__
  which ships with Postgres core itself, so is probably already available on
  your source server.

  It is possible to use `wal2json`__ instead. The support for wal2json is
  mostly historical in pgcopydb, it should not make a user visible
  difference whether you use the default test_decoding or wal2json.

  __ https://www.postgresql.org/docs/current/test-decoding.html
  __ https://github.com/eulerto/wal2json/

--wal2json-numeric-as-string

  When using the wal2json output plugin, it is possible to use the
  ``--wal2json-numeric-as-string`` option to instruct wal2json to output
  numeric values as strings and thus prevent some precision loss.

  You need to have a wal2json plugin version on source database that supports
  ``--numeric-data-types-as-string`` option to use this option.

  See also the documentation for `wal2json`__ regarding this option for details.

  __ https://github.com/eulerto/wal2json/pull/255

--slot-name

  Logical decoding slot name to use. Defaults to ``pgcopydb``. which is
  unfortunate when your use-case involves migrating more than one database
  from the source server.

--create-slot

  Instruct pgcopydb to create the logical replication slot to use.

--endpos

  Logical replication target LSN to use. Automatically stop replication and
  exit with normal exit status 0 when receiving reaches the specified LSN.
  If there's a record with LSN exactly equal to lsn, the record will be
  output.

  The ``--endpos`` option is not aware of transaction boundaries and may
  truncate output partway through a transaction. Any partially output
  transaction will not be consumed and will be replayed again when the slot
  is next read from. Individual messages are never truncated.

  See also documentation for `pg_recvlogical`__.

  __ https://www.postgresql.org/docs/current/app-pgrecvlogical.html

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

--verbose, --notice

  Increase current verbosity. The default level of verbosity is INFO. In
  ascending order pgcopydb knows about the following verbosity levels:
  FATAL, ERROR, WARN, INFO, NOTICE, SQL, DEBUG, TRACE.

--debug

  Set current verbosity to DEBUG level.

--trace

  Set current verbosity to TRACE level.

--quiet

  Set current verbosity to ERROR level.

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_TARGET_PGURI

  Connection string to the target Postgres instance. When ``--target`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_TABLE_JOBS

   Number of concurrent jobs allowed to run COPY operations in parallel.
   When ``--table-jobs`` is ommitted from the command line, then this
   environment variable is used.

PGCOPYDB_INDEX_JOBS

   Number of concurrent jobs allowed to run CREATE INDEX operations in
   parallel. When ``--index-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_RESTORE_JOBS

   Number of concurrent jobs allowed to run `pg_restore` operations in
   parallel. When ``--restore-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_LARGE_OBJECTS_JOBS

   Number of concurrent jobs allowed to copy Large Objects data in parallel.
   When ``--large-objects-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_SPLIT_TABLES_LARGER_THAN

   Allow :ref:`same_table_concurrency` when processing the source database.
   This environment variable value is expected to be a byte size, and bytes
   units B, kB, MB, GB, TB, PB, and EB are known.

   When ``--split-tables-larger-than`` is ommitted from the command line,
   then this environment variable is used.

PGCOPYDB_OUTPUT_PLUGIN

   Logical decoding output plugin to use. When ``--plugin`` is omitted from the
   command line, then this environment variable is used.

PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb uses the wal2json option ``--numeric-data-types-as-string``
   when using the wal2json output plugin.

   When ``--wal2json-numeric-as-string`` is ommitted from the command line
   then this environment variable is used.

PGCOPYDB_DROP_IF_EXISTS

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb uses the pg_restore options ``--clean --if-exists`` when
   creating the schema on the target Postgres instance.

   When ``--drop-if-exists`` is ommitted from the command line then this
   environment variable is used.

PGCOPYDB_FAIL_FAST

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb sends the TERM signal to all the processes in its process
   group as soon as one process terminates with a non-zero return code.

   When ``--fail-fast`` is ommitted from the command line then this
   environment variable is used.

PGCOPYDB_SKIP_VACUUM

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb skips the VACUUM ANALYZE jobs entirely, same as when using
   the ``--skip-vacuum`` option.

PGCOPYDB_SKIP_DB_PROPERTIES

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb skips the ALTER DATABASET SET properties commands that copy
   the setting from the source to the target database, same as when using
   the ``--skip-db-properties`` option.

PGCOPYDB_SKIP_CTID_SPLIT

  When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
  then pgcopydb skips the CTID split operation during the clone process,
  same as when using the ``--skip-split-by-ctid`` option.

PGCOPYDB_SNAPSHOT

  Postgres snapshot identifier to re-use, see also ``--snapshot``.

TMPDIR

  The pgcopydb command creates all its work files and directories in
  ``${TMPDIR}/pgcopydb``, and defaults to ``/tmp/pgcopydb``.

PGCOPYDB_LOG_TIME_FORMAT

  The logs time format defaults to ``%H:%M:%S`` when pgcopydb is used on an
  interactive terminal, and to ``%Y-%m-%d %H:%M:%S`` otherwise. This
  environment variable can be set to any format string other than the
  defaults.

  See documentation for strftime(3) for details about the format string. See
  documentation for isatty(3) for details about detecting if pgcopydb is run
  in an interactive terminal.

PGCOPYDB_LOG_JSON

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb formats its logs using JSON.

   ::

      {
        "timestamp": "2023-04-13 16:53:14",
        "pid": 87956,
        "error_level": 4,
        "error_severity": "INFO",
        "file_name": "main.c",
        "file_line_num": 165,
        "message": "Running pgcopydb version 0.11.19.g2290494.dirty from \"/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb\""
      }

PGCOPYDB_LOG_FILENAME

   When set to a filename (in a directory that must exists already) then
   pgcopydb writes its logs output to that filename in addition to the logs
   on the standard error output stream.

   If the file already exists, its content is overwritten. In other words
   the previous content would be lost when running the same command twice.

PGCOPYDB_LOG_JSON_FILE

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb formats its logs using JSON when writing to
   PGCOPYDB_LOG_FILENAME.

XDG_DATA_HOME

  The standard `XDG Base Directory Specification`__ defines several
  environment variables that allow controling where programs should store
  their files.

  __ https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html

  .. epigraph::

      *XDG_DATA_HOME defines the base directory relative to which user-specific
      data files should be stored. If $XDG_DATA_HOME is either not set or empty,
      a default equal to $HOME/.local/share should be used.*

  When using Change Data Capture (through ``--follow`` option and Postgres
  logical decoding with `wal2json`__) then pgcopydb pre-fetches changes in
  JSON files and transform them into SQL files to apply to the target
  database.

  __ https://github.com/eulerto/wal2json/

  These files are stored at the following location, tried in this order:

    1. when ``--dir`` is used, then pgcopydb uses the ``cdc`` subdirectory
       of the ``--dir`` location,

    2. when ``XDG_DATA_HOME`` is set in the environment, then pgcopydb uses
       that location,

    3. when neither of the previous settings have been used then pgcopydb
       defaults to using ``${HOME}/.local/share``.

Examples
--------

::

   $ export PGCOPYDB_SOURCE_PGURI=postgres://pagila:0wn3d@source/pagila
   $ export PGCOPYDB_TARGET_PGURI=postgres://pagila:0wn3d@target/pagila
   $ export PGCOPYDB_DROP_IF_EXISTS=on

   $ pgcopydb clone --table-jobs 8 --index-jobs 12
   14:49:01 22 INFO   Running pgcopydb version 0.13.38.g22e6544.dirty from "/usr/local/bin/pgcopydb"
   14:49:01 22 INFO   [SOURCE] Copying database from "postgres://pagila@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:49:01 22 INFO   [TARGET] Copying database into "postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:49:01 22 INFO   Exported snapshot "00000003-00000022-1" from the source database
   14:49:01 24 INFO   STEP 1: fetch source database tables, indexes, and sequences
   14:49:01 24 INFO   Fetched information for 3 extensions
   14:49:01 24 INFO   Splitting source candidate tables larger than 200 kB
   14:49:01 24 INFO   Table public.rental is 1224 kB large, 7 COPY processes will be used, partitioning on rental_id.
   14:49:01 24 INFO   Table public.film is 472 kB large, 3 COPY processes will be used, partitioning on film_id.
   14:49:01 24 INFO   Table public.film_actor is 264 kB large which is larger than --split-tables-larger-than 200 kB, and does not have a unique column of type integer: splitting by CTID
   14:49:01 24 INFO   Table public.film_actor is 264 kB large, 2 COPY processes will be used, partitioning on ctid.
   14:49:01 24 INFO   Table public.inventory is 264 kB large, 2 COPY processes will be used, partitioning on inventory_id.
   14:49:01 24 INFO   Fetched information for 21 tables, with an estimated total of 0 tuples and 3816 kB
   14:49:01 24 INFO   Fetched information for 54 indexes
   14:49:01 24 INFO   Fetching information for 13 sequences
   14:49:01 24 INFO   STEP 2: dump the source database schema (pre/post data)
   14:49:01 24 INFO    /usr/bin/pg_dump -Fc --snapshot 00000003-00000022-1 --section pre-data --file /tmp/pgcopydb/schema/pre.dump 'postgres://pagila@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60'
   14:49:01 24 INFO    /usr/bin/pg_dump -Fc --snapshot 00000003-00000022-1 --section post-data --file /tmp/pgcopydb/schema/post.dump 'postgres://pagila@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60'
   14:49:02 24 INFO   STEP 3: restore the pre-data section to the target database
   14:49:02 24 INFO    /usr/bin/pg_restore --dbname 'postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60' --single-transaction --use-list /tmp/pgcopydb/schema/pre-filtered.list /tmp/pgcopydb/schema/pre.dump
   14:49:02 24 INFO   STEP 6: starting 12 CREATE INDEX processes
   14:49:02 24 INFO   STEP 7: constraints are built by the CREATE INDEX processes
   14:49:02 24 INFO   STEP 8: starting 8 VACUUM processes
   14:49:02 24 INFO   STEP 9: reset sequences values
   14:49:02 51 INFO   STEP 5: starting 4 Large Objects workers
   14:49:02 30 INFO   STEP 4: starting 8 table data COPY processes
   14:49:02 52 INFO   Reset sequences values on the target database
   14:49:02 51 INFO   Added 0 large objects to the queue
   14:49:04 24 INFO   STEP 10: restore the post-data section to the target database
   14:49:04 24 INFO    /usr/bin/pg_restore --dbname 'postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60' --single-transaction --use-list /tmp/pgcopydb/schema/post-filtered.list /tmp/pgcopydb/schema/post.dump

     OID | Schema |             Name | copy duration | transmitted bytes | indexes | create index duration
   ------+--------+------------------+---------------+-------------------+---------+----------------------
   16880 | public |           rental |         160ms |            188 kB |       3 |                 230ms
   16880 | public |           rental |          77ms |            189 kB |       0 |                   0ms
   16880 | public |           rental |         105ms |            189 kB |       0 |                   0ms
   16880 | public |           rental |         107ms |            189 kB |       0 |                   0ms
   16880 | public |           rental |          97ms |            190 kB |       0 |                   0ms
   16880 | public |           rental |          82ms |            189 kB |       0 |                   0ms
   16880 | public |           rental |          81ms |            189 kB |       0 |                   0ms
   16758 | public |             film |         136ms |            112 kB |       5 |                 462ms
   16758 | public |             film |          52ms |            110 kB |       0 |                   0ms
   16758 | public |             film |          74ms |            111 kB |       0 |                   0ms
   16770 | public |       film_actor |          74ms |            5334 B |       0 |                   0ms
   16770 | public |       film_actor |          77ms |            156 kB |       0 |                   0ms
   16825 | public |        inventory |         106ms |             74 kB |       2 |                 586ms
   16825 | public |        inventory |         107ms |             76 kB |       0 |                   0ms
   16858 | public | payment_p2022_03 |          86ms |            137 kB |       4 |                 468ms
   16866 | public | payment_p2022_05 |          98ms |            136 kB |       4 |                 663ms
   16870 | public | payment_p2022_06 |         106ms |            134 kB |       4 |                 571ms
   16862 | public | payment_p2022_04 |         125ms |            129 kB |       4 |                 775ms
   16854 | public | payment_p2022_02 |         117ms |            121 kB |       4 |                 684ms
   16874 | public | payment_p2022_07 |         255ms |            118 kB |       1 |                 270ms
   16724 | public |         customer |         247ms |             55 kB |       4 |                 1s091
   16785 | public |          address |         128ms |             47 kB |       2 |                 132ms
   16795 | public |             city |         163ms |             23 kB |       2 |                 270ms
   16774 | public |    film_category |         172ms |             28 kB |       1 |                  47ms
   16850 | public | payment_p2022_01 |         166ms |             36 kB |       4 |                 679ms
   16738 | public |            actor |         399ms |            7999 B |       2 |                 116ms
   16748 | public |         category |         170ms |             526 B |       1 |                 200ms
   16805 | public |          country |          63ms |            3918 B |       1 |                 226ms
   16900 | public |            staff |         170ms |             272 B |       1 |                 114ms
   16832 | public |         language |         115ms |             276 B |       1 |                  68ms
   16911 | public |            store |          88ms |              58 B |       2 |                 185ms


                                                  Step   Connection    Duration    Transfer   Concurrency
    --------------------------------------------------   ----------  ----------  ----------  ------------
                                           Dump Schema       source        98ms                         1
      Catalog Queries (table ordering, filtering, etc)       source       687ms                         1
                                        Prepare Schema       target       667ms                         1
         COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both       1s256                    8 + 20
                                     COPY (cumulative)         both       4s003     2955 kB             8
                            Large Objects (cumulative)         both       877ms                         4
                CREATE INDEX, CONSTRAINTS (cumulative)       target       7s837                        12
                                       Finalize Schema       target       487ms                         1
    --------------------------------------------------   ----------  ----------  ----------  ------------
                             Total Wall Clock Duration         both       3s208                    8 + 20
    --------------------------------------------------   ----------  ----------  ----------  ------------
