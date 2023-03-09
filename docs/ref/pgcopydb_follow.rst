pgcopydb follow
===============

The command ``pgcopydb follow`` replays the database changes registered at
the source database with the logical decoding plugin of your choice, either
the default `test_decoding`__ or `wal2json`__, into the target database.

__ https://www.postgresql.org/docs/current/test-decoding.html
__ https://github.com/eulerto/wal2json/


.. important::

   While the ``pgcopydb follow`` is a full client for logical decoding, the
   general use case involves using ``pgcopydb clone --follow`` as documented
   in :ref:`change_data_capture`.

When using Logical Decoding with pgcopydb or another tool, consider making
sure you're familiar with the `Logical Replication Restrictions`__ that
apply. In particular:

__ https://www.postgresql.org/docs/current/logical-replication-restrictions.html

 - DDL are not replicated.

   When using DDL for partition scheme maintenance, such as when using the
   `pg_partman`__ extension, then consider creating a week or a month of
   partitions in advance, so that creating new partitions does not happen
   during the migration window.

   __ https://github.com/pgpartman/pg_partman

 - Sequence data is not replicated.

   When using ``pgcopydb clone --follow`` (starting with pgcopydb version
   0.9) then the sequence data is synced at the end of the operation, after
   the cutover point implemented via the
   :ref:`pgcopydb_stream_sentinel_set_endpos`.

   Updating the sequences manually is also possible by running the command
   :ref:`pgcopydb_copy_sequences`.

 - Large Objects are not replicated.

See the Postgres documentation page for `Logical Replication Restrictions`__
to read the exhaustive list of restrictions.

__ https://www.postgresql.org/docs/current/logical-replication-restrictions.html

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
     --plugin              Output plugin to use (test_decoding, wal2json)
     --slot-name           Use this Postgres replication slot name
     --create-slot         Create the replication slot
     --origin              Use this Postgres replication origin node name
     --endpos              Stop replaying changes when reaching this LSN

Description
-----------

This command runs three concurrent subprocesses in two possible modes of
operation:

 * The first mode of operation is named *prefetch and catchup* where the
   changes from the source database are stored in intermediate JSON and SQL
   files to be later replayed one file at a time in the catchup process.

 * The second mode of operation is named *live replay* where the changes
   from the source database are streamed from the receiver process to the
   transform process using a Unix pipe, and then with the same mechanism
   from the transform process to the replay process.

Only one mode of operation may be active at any given time, and pgcopydb
automatically switches from one mode to the other one, in a loop.

The follow command always starts using the *prefetch and catchup* mode, and
as soon as the catchup process can't find the next SQL file to replay then
it exits, triggering the switch to the *live replay* mode. Before entering
the new mode, to make sure to replay all the changes that have been
received, pgcopydb implements an extra catchup phase without concurrent
activity.

Prefetch and Catchup
^^^^^^^^^^^^^^^^^^^^

In the *prefetch and catchup* mode of operations, the three processes are
implementing the following approach:

  1. The first process pre-fetches the changes from the source database
     using the Postgres Logical Decoding protocol and save the JSON messages
     in local JSON files.

  2. The second process transforms the JSON files into SQL. A Unix system V
     message queue is used to communicate LSN positions from the prefetch
     process to the transform process.

  3. The third process catches-up with changes happening on the source
     database by applying the SQL files to the target database system.

     The Postgres API for `Replication Progress Tracking`__ is used in that
     process so that we can skip already applied transactions at restart or
     resume.

     __ https://www.postgresql.org/docs/current//replication-origins.html

Live Replay
^^^^^^^^^^^

In the *live replay* mode of operations, the three processes are
implementing the following approach:

  1. The first process receives the changes from the source database using
     the Postgres Logical Decoding protocol and save the JSON messages in
     local JSON files.

     Additionnaly, the JSON changes are written to a Unix pipe shared with
     the transform process.

  2. The second process transforms the JSON lines into SQL. A Unix pipe is
     used to stream the JSON lines from the receive process to the transform
     process.

     The transform process in that mode still writes the changes to SQL
     files, so that it's still possible to catchup with received changes if
     the apply process is interrupted.

  3. The third process replays the changes happening on the source database
     by applying the SQL commands to the target database system. The SQL
     commands are read from the Unix pipe shared with the transform process.

     The Postgres API for `Replication Progress Tracking`__ is used in that
     process so that we can skip already applied transactions at restart or
     resume.

     __ https://www.postgresql.org/docs/current//replication-origins.html

Remote control of the follow command
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Replica Identity and lack of Primary Keys
-----------------------------------------

Postgres Logical Decoding works with replaying changes using SQL statements,
and for that exposes the concept of *Replica Identity* as described in the
documentation for the `ALTER TABLE ... REPLICA IDENTITY`__ command.

__ https://www.postgresql.org/docs/current/sql-altertable.html

To quote Postgres docs:

.. epigraph::

   *This form changes the information which is written to the write-ahead
   log to identify rows which are updated or deleted. In most cases, the old
   value of each column is only logged if it differs from the new value;
   however, if the old value is stored externally, it is always logged
   regardless of whether it changed. This option has no effect except when
   logical replication is in use.*

To support Change Data Capture with Postgres Logical Decoding for tables
that do not have a Primary Key, then it is necessary to use the ``ALTER
TABLE ... REPLICA IDENTITY`` command for those tables.

In practice the two following options are to be considered:

  - REPLICA IDENTITY USING INDEX index_name

	This form is prefered when a UNIQUE index exists for the table without a
	primary key. The index must be unique, not partial, not deferrable, and
	include only columns marked NOT NULL.

  - REPLICA IDENTITY FULL

	When this is used on a table, then the WAL records contain the old
	values of all columns in the row.

Logical Decoding Pre-Fetching
-----------------------------

When using ``pgcopydb clone --follow`` a logical replication slot is created
on the source database before the initial COPY, using the same Postgres
snapshot. This ensure data consistency.

Within the ``pgcopydb clone --follow`` approach, it is only possible to
start applying the changes from the source database after the initial COPY
has finished on the target database.

Also, from the Postgres documentation we read that `Postgres replication
slots`__ provide an automated way to ensure that the primary does not remove
WAL segments until they have been received by all standbys.

__ https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS

Accumulating WAL segments on the primary during the whole duration of the
initial COPY involves capacity hazards, which translate into potential *File
System is Full* errors on the WAL disk of the source database. It is crucial
to avoid such a situation.

This is why pgcopydb implements CDC pre-fetching. In parallel to the initial
COPY the command ``pgcopydb clone --follow`` pre-fetches the changes in
local JSON and SQL files. Those files are placed in the XDG_DATA_HOME
location, which could be a mount point for an infinite Blob Storage area.

The ``pgcopydb follow`` command is a convenience command that's available as
a logical decoding client, and it shares the same implementation as the
``pgcopydb clone --follow`` command. As a result, the pre-fetching strategy
is also relevant to the ``pgcopydb follow`` command.

The sentinel table, or the Remote Control
-----------------------------------------

To track progress and allow resuming of operations, pgcopydb uses a sentinel
table on the source database. The sentinel table consists of a single row
with the following fields:

::

   $ pgcopydb stream sentinel get
   startpos   1/8D173AF8
   endpos     0/0
   apply      disabled
   write_lsn  0/0
   flush_lsn  0/0
   replay_lsn 0/0

Note that you can use the command ``pgcopydb stream sentinel get --json`` to
fetch a JSON formatted output, such as the following:

.. code-block:: json

   {
     "startpos": "1/8D173AF8",
     "endpos": "1/8D173AF8",
     "apply": false,
     "write_lsn": "0/0",
     "flush_lsn": "0/0",
     "replay_lsn": "0/0"
   }

The first three fields (startpos, endpos, apply) are specific to pgcopydb,
then the following three fields (write_lsn, flush_lsn, replay_lsn) follow
the Postgres replication protocol as visible in the docs for the
`pg_stat_replication`__ function.

__ https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-VIEW

  - ``startpos``

    The startpos field is the current LSN on the source database at the time
    when the Change Data Capture is setup in pgcopydb, such as when using the
    :ref:`pgcopydb_stream_setup` command.

    Note that both the ``pgcopydb follow`` and the ``pgcopydb clone --follow``
    command implement the setup parts if the ``pgcopydb stream setup`` has not
    been used already.

  - ``endpos``

    The endpos field is last LSN position from the source database that
    pgcopydb replays. The command ``pgcopydb follow`` (or ``pgcopydb clone
    --follow``) stops when reaching beyond this LSN position.

    The ``endpos`` can be set at the start of the process, which is useful
    for unit testing, or while the command is running, which is useful in
    production to define a cutover point.

    To define the ``endpos`` while the command is running, use
    :ref:`pgcopydb_stream_sentinel_set_endpos`.

  - ``apply``

    The apply field is a boolean (enabled/disabled) that control the catchup
    process. The pgcopydb catchup process replays the changes only when the
    apply boolean is set to true.

    The ``pgcopydb clone --follow`` command automatically enables the apply
    field of the sentinel table as soon as the initial COPY is done.

    To manually control the apply field, use the
    :ref:`pgcopydb_stream_sentinel_set_apply` command.

  - ``write_lsn``

    The Postgres documentation for ``pg_stat_replication.write_lsn`` is:
    Last write-ahead log location written to disk by this standby server.

    In the pgcopydb case, the sentinel field write_lsn is the position that
    has been written to disk (as JSON) by the streaming process.

  - ``flush_lsn``

    The Postgres documentation for ``pg_stat_replication.flush_lsn`` is:
    Last write-ahead log location flushed to disk by this standby server

    In the pgcopydb case, the sentinel field flush_lsn is the position that
    has been written and then fsync'ed to disk (as JSON) by the streaming
    process.

  - ``replay_lsn``

    The Postgres documentation for ``pg_stat_replication.replay_lsn`` is:
    Last write-ahead log location replayed into the database on this standby server

    In the pgcopydb case, the sentinel field replay_lsn is the position that
    has been applied to the target database, as kept track from the WAL.json
    and then the WAL.sql files, and using the Postgres API for `Replication
    Progress Tracking`__.

    __ https://www.postgresql.org/docs/current//replication-origins.html

    The replay_lsn is also shared by the pgcopydb streaming process that
    uses the Postgres logical replication protocol, so the
    `pg_stat_replication`__ entry associated with the replication slot used
    by pgcopydb can be used to monitor replication lag.

    __ https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-VIEW

As the pgcopydb streaming processes maintain the sentinel table on the
source database, it is also possible to use it to keep track of the logical
replication progress.

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

--plugin

  Logical decoding output plugin to use. The default is `test_decoding`__
  which ships with Postgres core itself, so is probably already available on
  your source server.

  It is possible to use `wal2json`__ instead. The support for wal2json is
  mostly historical in pgcopydb, it should not make a user visible
  difference whether you use the default test_decoding or wal2json.

  __ https://www.postgresql.org/docs/current/test-decoding.html
  __ https://github.com/eulerto/wal2json/

--slot-name

  Logical decoding slot name to use. Defaults to ``pgcopydb``. which is
  unfortunate when your use-case involves migrating more than one database
  from the source server.

--create-slot

  Instruct pgcopydb to create the logical replication slot to use.

--endpos

  Logical decoding target LSN to use. Automatically stop replication and
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
  logical decoding) then pgcopydb pre-fetches changes in JSON files and
  transform them into SQL files to apply to the target database.

  These files are stored at the following location, tried in this order:

    1. when ``--dir`` is used, then pgcopydb uses the ``cdc`` subdirectory
       of the ``--dir`` location,

    2. when ``XDG_DATA_HOME`` is set in the environment, then pgcopydb uses
       that location,

    3. when neither of the previous settings have been used then pgcopydb
       defaults to using ``${HOME}/.local/share``.
