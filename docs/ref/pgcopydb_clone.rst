pgcopydb clone
==============

::

   pgcopydb
     clone    Clone an entire database from source to target
     fork     Clone an entire database from source to target
     follow   Replay changes from the source database to the target database

.. _pgcopydb_clone:

pgcopydb clone
--------------

The command ``pgcopydb clone`` copies a database from the given source
Postgres instance to the target Postgres instance.

::

   pgcopydb clone: Clone an entire database from source to target
   usage: pgcopydb clone  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --table-jobs          Number of concurrent COPY jobs to run
     --index-jobs          Number of concurrent CREATE INDEX jobs to run
     --drop-if-exists      On the target database, clean-up from a previous run first
     --roles               Also copy roles found on source to target
     --no-owner            Do not set ownership of objects to match the original database
     --no-acl              Prevent restoration of access privileges (grant/revoke commands).
     --no-comments         Do not output commands to restore comments
     --skip-large-objects  Skip copying large objects (blobs)
     --filters <filename>  Use the filters defined in <filename>
     --restart             Allow restarting when temp files exist already
     --resume              Allow resuming operations after a failure
     --not-consistent      Allow taking a new snapshot on the source database
     --snapshot            Use snapshot obtained with pg_export_snapshot
     --follow              Implement logical decoding to replay changes
     --slot-name           Use this Postgres replication slot name
     --create-slot         Create the replication slot
     --origin              Use this Postgres replication origin node name
     --endpos              Stop replaying changes when reaching this LSN

.. _pgcopydb_fork:

pgcopydb fork
--------------

The command ``pgcopydb fork`` copies a database from the given source
Postgres instance to the target Postgres instance. This command is an alias
to the command ``pgcopydb clone`` seen above.

.. _pgcopydb_follow:

pgcopydb follow
---------------

Not yet implemented.

Description
-----------

The ``pgcopydb clone`` command implements the following steps:

  1. ``pgcopydb`` calls into ``pg_dump`` to produce the ``pre-data`` section
     and the ``post-data`` sections of the dump using Postgres custom
     format.

  2. The ``pre-data`` section of the dump is restored on the target database
     using the ``pg_restore`` command, creating all the Postgres objects
     from the source database into the target database.

  3. ``pgcopydb`` gets the list of ordinary and partitioned tables and for
     each of them runs COPY the data from the source to the target in a
     dedicated sub-process, and starts and control the sub-processes until
     all the data has been copied over.

     A Postgres connection and a SQL query to the Postgres catalog table
     pg_class is used to get the list of tables with data to copy around,
     and the `reltuples` is used to start with the tables with the greatest
     number of rows first, as an attempt to minimize the copy time.

  4. An auxiliary process is started concurrently to the main COPY workers.
     This auxiliary process loops through all the Large Objects found on the
     source database and copies its data parts over to the target database,
     much like pg_dump itself would.

     This step is much like ``pg_dump | pg_restore`` for large objects data
     parts, except that there isn't a good way to do just that with the
     tooling.

  5. In each copy table sub-process, as soon as the data copying is done,
     then ``pgcopydb`` gets the list of index definitions attached to the
     current target table and creates them in parallel.

     The primary indexes are created as UNIQUE indexes at this stage.

  6. Then the PRIMARY KEY constraints are created USING the just built
     indexes. This two-steps approach allows the primary key index itself to
     be created in parallel with other indexes on the same table, avoiding
     an EXCLUSIVE LOCK while creating the index.

  7. Then ``VACUUM ANALYZE`` is run on each target table as soon as the data
     and indexes are all created.

  8. Then pgcopydb gets the list of the sequences on the source database and
     for each of them runs a separate query on the source to fetch the
     ``last_value`` and the ``is_called`` metadata the same way that pg_dump
     does.

     For each sequence, pgcopydb then calls ``pg_catalog.setval()`` on the
     target database with the information obtained on the source database.

  9. The final stage consists now of running the ``pg_restore`` command for
     the ``post-data`` section script for the whole database, and that's
     where the foreign key constraints and other elements are created.

     The *post-data* script is filtered out using the ``pg_restore
     --use-list`` option so that indexes and primary key constraints already
     created in step 4. are properly skipped now.

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

  See also :ref:`pgcopydb_copy_roles`.

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

--filters <filename>

  This option allows to exclude table and indexes from the copy operations.
  See :ref:`filtering` for details about the expected file format and the
  filtering options available.

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

--slot-name

  Logical replication slot to use. At the moment pgcopydb doesn't know how
  to create the logical replication slot itself. The slot should be created
  within the same transaction snapshot as the initial data copy.

  Must be using the `wal2json`__ output plugin, available with
  format-version 2.

  __ https://github.com/eulerto/wal2json/

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

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_TARGET_PGURI

  Connection string to the target Postgres instance. When ``--target`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_TARGET_TABLE_JOBS

   Number of concurrent jobs allowed to run COPY operations in parallel.
   When ``--table-jobs`` is ommitted from the command line, then this
   environment variable is used.

PGCOPYDB_TARGET_INDEX_JOBS

   Number of concurrent jobs allowed to run CREATE INDEX operations in
   parallel. When ``--index-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_DROP_IF_EXISTS

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb uses the pg_restore options ``--clean --if-exists`` when
   creating the schema on the target Postgres instance.

PGCOPYDB_SNAPSHOT

  Postgres snapshot identifier to re-use, see also ``--snapshot``.

TMPDIR

  The pgcopydb command creates all its work files and directories in
  ``${TMPDIR}/pgcopydb``, and defaults to ``/tmp/pgcopydb``.

Examples
--------

::

   $ export PGCOPYDB_SOURCE_PGURI="port=54311 host=localhost dbname=pgloader"
   $ export PGCOPYDB_TARGET_PGURI="port=54311 dbname=plop"
   $ export PGCOPYDB_DROP_IF_EXISTS=on

   $ pgcopydb clone --table-jobs 8 --index-jobs 12
   10:04:49 29268 INFO  [SOURCE] Copying database from "port=54311 host=localhost dbname=pgloader"
   10:04:49 29268 INFO  [TARGET] Copying database into "port=54311 dbname=plop"
   10:04:49 29268 INFO  Found a stale pidfile at "/tmp/pgcopydb/pgcopydb.pid"
   10:04:49 29268 WARN  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   10:04:49 29268 WARN  Directory "/tmp/pgcopydb" already exists: removing it entirely
   10:04:49 29268 INFO  STEP 1: dump the source database schema (pre/post data)
   ...
   10:04:52 29268 INFO  STEP 3: copy data from source to target in sub-processes
   10:04:52 29268 INFO  STEP 4: create indexes and constraints in parallel
   10:04:52 29268 INFO  STEP 5: vacuum analyze each table
   10:04:52 29268 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
   10:04:52 29268 INFO  Fetched information for 56 tables
   ...
   10:04:53 29268 INFO  STEP 6: restore the post-data section to the target database
   ...

                                             Step   Connection    Duration   Concurrency
    ---------------------------------------------   ----------  ----------  ------------
                                      Dump Schema       source       1s275             1
                                   Prepare Schema       target       1s560             1
    COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both       1s095        8 + 12
                                COPY (cumulative)         both       2s645             8
                        CREATE INDEX (cumulative)       target       333ms            12
                                  Finalize Schema       target        29ms             1
    ---------------------------------------------   ----------  ----------  ------------
                        Total Wall Clock Duration         both       4s013        8 + 12
    ---------------------------------------------   ----------  ----------  ------------
