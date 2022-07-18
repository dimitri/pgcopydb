pgcopydb follow
===============

The command ``pgcopydb follow`` replays the database changes registered at
the source database with the logical decoding pluing `wal2json`__ into the
target database.

__ https://github.com/eulerto/wal2json/

.. _pgcopydb_follow:

pgcopydb follow
---------------

::

   pgcopydb follow: Replay changes from the source database to the target database
   usage: pgcopydb follow  --source ... --target ...

     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --filters <filename>  Use the filters defined in <filename>
     --restart             Allow restarting when temp files exist already
     --resume              Allow resuming operations after a failure
     --not-consistent      Allow taking a new snapshot on the source database
     --snapshot            Use snapshot obtained with pg_export_snapshot
     --slot-name           Use this Postgres replication slot name
     --create-slot         Create the replication slot
     --origin              Use this Postgres replication origin node name
     --endpos              Stop replaying changes when reaching this LSN

Description
-----------

This command runs two concurrent subproces.

  1. The first one pre-fetches the changes from the source database using
     the Postgres Logical Decoding protocol and save the JSON messages in
     local JSON files.

     The logical decoding plugin `wal2json`__ must be available on the
     source database system.

     __ https://github.com/eulerto/wal2json/

     Each time a JSON file is closed, an auxilliary process is started to
     transform the JSON file into a matching SQL file. This processing is
     done in the background, and the main receiver process only waits for
     the transformation process to be finished when there is a new JSON file
     to transform.

     In other words, only one such transform process can be started in the
     background, and the process is blocking when a second one could get
     started.

     The design model here is based on the assumption that receiving the
     next set of JSON messages that fills-up a whole JSON file is going to
     take more time than transforming the JSON file into an SQL file. When
     that assumption proves wrong, consider opening an issue on the github
     project for pgcopydb.

  2. The second process catches-up with changes happening on the source
     database by applying the SQL files to the target database system.

     The Postgres API for `Replication Progress Tracking`__ is used in that
     process so that we can skip already applied transactions at restart or
     resume.

     __ https://www.postgresql.org/docs/current//replication-origins.html

It is possible to start the ``pgcopydb follow`` command and then later,
while it's still running, set the LSN for the end position with the same
effect as using the command line option ``--endpos``, or switch from
prefetch mode only to prefetch and catchup mode. For that, see the commands
:ref:`pgcopydb_stream_sentinel_set_endpos`,
:ref:`pgcopydb_stream_sentinel_set_apply`, and
:ref:`pgcopydb_stream_sentinel_set_prefetch`.

Note that in many case the ``--endpos`` LSN position is not known at the
start of this command. Also before entering the *prefetch and apply* mode it
is important to make sure that the initial base copy is finished.

Finally, it is also possible to setup the streaming replication options
before using the ``pgcopydb follow`` command: see the
:ref:`pgcopydb_stream_setup` and :ref:`pgcopydb_stream_cleanup` commands.

Options
-------

The following options are available to ``pgcopydb follow``:

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

PGCOPYDB_SNAPSHOT

  Postgres snapshot identifier to re-use, see also ``--snapshot``.

TMPDIR

  The pgcopydb command creates all its work files and directories in
  ``${TMPDIR}/pgcopydb``, and defaults to ``/tmp/pgcopydb``.
