.. _pgcopydb_stream:

pgcopydb stream
===============

pgcopydb stream - Stream changes from source database

.. warning::

   **This mode of operations has been designed for unit testing only.**

   Consider using the :ref:`pgcopydb_clone` (with the ``--follow`` option)
   or the :ref:`pgcopydb_follow` command instead.

.. note::

   Some *pgcopydb stream* commands are still designed for normal operations,
   rather than unit testing only.

   The :ref:`pgcopydb_stream_sentinel_set_startpos`,
   :ref:`pgcopydb_stream_sentinel_set_endpos`,
   :ref:`pgcopydb_stream_sentinel_set_apply`, and
   :ref:`pgcopydb_stream_sentinel_set_prefetch` commands are necessary to
   communicate with the main ``pgcopydb clone --follow`` or ``pgcopydb
   follow`` process. See :ref:`change_data_capture_example_1` for a detailed
   example using :ref:`pgcopydb_stream_sentinel_set_endpos`.

   Also the commands :ref:`pgcopydb_stream_setup` and
   :ref:`pgcopydb_stream_cleanup` might be used directly in normal
   operations. See :ref:`change_data_capture_example_2` for a detailed
   example.

This command prefixes the following sub-commands:

::

  pgcopydb stream
    setup      Setup source and target systems for logical decoding
    cleanup    cleanup source and target systems for logical decoding
    prefetch   Stream JSON changes from the source database and transform them to SQL
    catchup    Apply prefetched changes from SQL files to the target database
    replay     Replay changes from the source to the target database, live
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

Those commands implement a part of the whole database replay operation as
detailed in section :ref:`pgcopydb_follow`. Only use those commands to debug
a specific part, or because you know that you just want to implement that
step.

.. note::

   The sub-commands ``stream setup`` then ``stream prefetch`` and ``stream
   catchup`` are higher level commands, that use internal information to
   know which files to process. Those commands also keep track of their
   progress.

   The sub-commands ``stream receive``, ``stream transform``, and ``stream
   apply`` are lower level interface that work on given files. Those
   commands still keep track of their progress, but have to be given more
   information to work.

.. _pgcopydb_stream_setup:

pgcopydb stream setup
---------------------

pgcopydb stream setup - Setup source and target systems for logical decoding

The command ``pgcopydb stream setup`` connects to the source database and
creates creates a ``pgcopydb.sentinel`` table, and then connects to the
target database and creates a replication origin positioned at the LSN
position of the logical decoding replication slot that must have been
created already. See :ref:`pgcopydb_snapshot` to create the replication slot
and export a snapshot.

::

   pgcopydb stream setup: Setup source and target systems for logical decoding
   usage: pgcopydb stream setup

     --source                      Postgres URI to the source database
     --target                      Postgres URI to the target database
     --dir                         Work directory to use
     --restart                     Allow restarting when temp files exist already
     --resume                      Allow resuming operations after a failure
     --not-consistent              Allow taking a new snapshot on the source database
     --snapshot                    Use snapshot obtained with pg_export_snapshot
     --plugin                      Output plugin to use (test_decoding, wal2json)
     --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin
     --slot-name                   Stream changes recorded by this slot
     --origin                      Name of the Postgres replication origin

.. _pgcopydb_stream_cleanup:

pgcopydb stream cleanup
-----------------------

pgcopydb stream cleanup - cleanup source and target systems for logical decoding

The command ``pgcopydb stream cleanup`` connects to the source and target
databases to delete the objects created in the ``pgcopydb stream setup``
step.

::

   pgcopydb stream cleanup: cleanup source and target systems for logical decoding
   usage: pgcopydb stream cleanup

     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --snapshot       Use snapshot obtained with pg_export_snapshot
     --slot-name      Stream changes recorded by this slot
     --origin         Name of the Postgres replication origin

.. _pgcopydb_stream_prefetch:

pgcopydb stream prefetch
------------------------

pgcopydb stream prefetch - Stream JSON changes from the source database and transform them to SQL

The command ``pgcopydb stream prefetch`` connects to the source database
using the logical replication protocl and the given replication slot.

The prefetch command receives the changes from the source database in a
streaming fashion, and writes them in a series of JSON files named the same
as their origin WAL filename (with the ``.json`` extension). Each time a
JSON file is closed, a subprocess is started to transform the JSON into an
SQL file.


::

   pgcopydb stream prefetch: Stream JSON changes from the source database and transform them to SQL
   usage: pgcopydb stream prefetch

     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes

.. _pgcopydb_stream_catchup:

pgcopydb stream catchup
-----------------------

pgcopydb stream catchup - Apply prefetched changes from SQL files to the target database

The command ``pgcopydb stream catchup`` connects to the target database and
applies changes from the SQL files that have been prepared with the
``pgcopydb stream prefetch`` command.


::

   pgcopydb stream catchup: Apply prefetched changes from SQL files to the target database
   usage: pgcopydb stream catchup

     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes
	 --origin         Name of the Postgres replication origin

.. _pgcopydb_stream_replay:

pgcopydb stream replay
----------------------

pgcopydb stream replay - Replay changes from the source to the target database, live

The command ``pgcopydb stream replay`` connects to the source database and
streams changes using the logical decoding protocol, and internally streams
those changes to a transform process and then a replay process, which
connects to the target database and applies SQL changes.

::

   pgcopydb stream replay: Replay changes from the source to the target database, live
   usage: pgcopydb stream replay

     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes
     --origin         Name of the Postgres replication origin


This command is equivalent to running the following script::

  pgcopydb stream receive --to-stdout
  | pgcopydb stream transform - -
  | pgcopydb stream apply -

.. _pgcopydb_stream_sentinel_create:

pgcopydb stream sentinel create
-------------------------------

pgcopydb stream sentinel create - Create the sentinel table on the source database

The ``pgcopydb.sentinel`` table allows to remote control the prefetch and
catchup processes of the logical decoding implementation in pgcopydb.

::

   pgcopydb stream sentinel create: Create the sentinel table on the source database
   usage: pgcopydb stream sentinel create

     --source      Postgres URI to the source database
     --startpos    Start replaying changes when reaching this LSN
     --endpos      Stop replaying changes when reaching this LSN


.. _pgcopydb_stream_sentinel_drop:

pgcopydb stream sentinel drop
-----------------------------

pgcopydb stream sentinel drop - Drop the sentinel table on the source database

The ``pgcopydb.sentinel`` table allows to remote control the prefetch and
catchup processes of the logical decoding implementation in pgcopydb.

::

   pgcopydb stream sentinel drop: Drop the sentinel table on the source database
   usage: pgcopydb stream sentinel drop

     --source      Postgres URI to the source database

.. _pgcopydb_stream_sentinel_get:

pgcopydb stream sentinel get
----------------------------

pgcopydb stream sentinel get - Get the sentinel table values on the source database

::

   pgcopydb stream sentinel get: Get the sentinel table values on the source database
   usage: pgcopydb stream sentinel get

     --source      Postgres URI to the source database
     --json        Format the output using JSON

.. _pgcopydb_stream_sentinel_set_startpos:

pgcopydb stream sentinel set startpos
-------------------------------------

pgcopydb stream sentinel set startpos - Set the sentinel start position LSN on the source database

::

   pgcopydb stream sentinel set startpos: Set the sentinel start position LSN on the source database
   usage: pgcopydb stream sentinel set startpos <start LSN>

     --source      Postgres URI to the source database

.. _pgcopydb_stream_sentinel_set_endpos:

pgcopydb stream sentinel set endpos
-----------------------------------

pgcopydb stream sentinel set endpos - Set the sentinel end position LSN on the source database

::

   pgcopydb stream sentinel set endpos: Set the sentinel end position LSN on the source database
   usage: pgcopydb stream sentinel set endpos <end LSN>

     --source      Postgres URI to the source database
     --current     Use pg_current_wal_flush_lsn() as the endpos


.. _pgcopydb_stream_sentinel_set_apply:

pgcopydb stream sentinel set apply
----------------------------------

pgcopydb stream sentinel set apply - Set the sentinel apply mode on the source database

::

   pgcopydb stream sentinel set apply: Set the sentinel apply mode on the source database
   usage: pgcopydb stream sentinel set apply

     --source      Postgres URI to the source database


.. _pgcopydb_stream_sentinel_set_prefetch:

pgcopydb stream sentinel set prefetch
-------------------------------------

pgcopydb stream sentinel set prefetch - Set the sentinel prefetch mode on the source database

::

   pgcopydb stream sentinel set prefetch: Set the sentinel prefetch mode on the source database
   usage: pgcopydb stream sentinel set prefetch

     --source      Postgres URI to the source database


.. _pgcopydb_stream_receive:

pgcopydb stream receive
-----------------------

pgcopydb stream receive - Stream changes from the source database

The command ``pgcopydb stream receive`` connects to the source database
using the logical replication protocl and the given replication slot.

The receive command receives the changes from the source database in a
streaming fashion, and writes them in a series of JSON files named the same
as their origin WAL filename (with the ``.json`` extension).

::

   pgcopydb stream receive: Stream changes from the source database
   usage: pgcopydb stream receive  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use
     --to-stdout      Stream logical decoding messages to stdout
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes


.. _pgcopydb_stream_transform:

pgcopydb stream transform
-------------------------

pgcopydb stream transform - Transform changes from the source database into SQL commands

The command ``pgcopydb stream transform`` transforms a JSON file as received
by the ``pgcopydb stream receive`` command into an SQL file with one query
per line.

::

   pgcopydb stream transform: Transform changes from the source database into SQL commands
   usage: pgcopydb stream transform  <json filename> <sql filename>

     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database

The command supports using ``-`` as the filename for either the JSON input
or the SQL output, or both. In that case reading from standard input and/or
writing to standard output is implemented, in a streaming fashion. A classic
use case is to use Unix Pipes, see :ref:`pgcopydb_stream_replay` too.

pgcopydb stream apply
---------------------

pgcopydb stream apply - Apply changes from the source database into the target database

The command ``pgcopydb stream apply`` applies a SQL file as prepared by the
``pgcopydb stream transform`` command in the target database. The apply
process tracks progress thanks to the Postgres API for `Replication Progress
Tracking`__.

__ https://www.postgresql.org/docs/current/replication-origins.html

::

   pgcopydb stream apply: Apply changes from the source database into the target database
   usage: pgcopydb stream apply <sql filename>

     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --origin         Name of the Postgres replication origin

This command supports using ``-`` as the filename to read from, and in that
case reads from the standard input in a streaming fashion instead.

Options
-------

The following options are available to ``pgcopydb stream`` sub-commands:

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

  Change Data Capture files are stored in the ``cdc`` sub-directory of the
  ``--dir`` option when provided, otherwise see XDG_DATA_HOME environment
  variable below.

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

  To be able to resume a streaming operation in a consistent way, all that's
  required is re-using the same replication slot as in previous run(s).

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

  Logical decoding slot name to use.

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

--startpos

  Logical replication target system registers progress by assigning a
  current LSN to the ``--origin`` node name. When creating an origin on the
  target database system, it is required to provide the current LSN from the
  source database system, in order to properly bootstrap pgcopydb logical
  decoding.

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

PGCOPYDB_OUTPUT_PLUGIN

  Logical decoding output plugin to use. When ``--plugin`` is omitted from the
  command line, then this environment variable is used.

PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING

  When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
  then pgcopydb uses the wal2json option ``--numeric-data-types-as-string``
  when using the wal2json output plugin.

  When ``--wal2json-numeric-as-string`` is ommitted from the command line
  then this environment variable is used.

TMPDIR

  The pgcopydb command creates all its work files and directories in
  ``${TMPDIR}/pgcopydb``, and defaults to ``/tmp/pgcopydb``.

XDG_DATA_HOME

  The pgcopydb command creates Change Data Capture files in the standard
  place XDG_DATA_HOME, which defaults to ``~/.local/share``. See the `XDG
  Base Directory Specification`__.

  __ https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html

Examples
--------

As an example here is the output generated from running the cdc test case,
where a replication slot is created before the initial copy of the data, and
then the following INSERT statement is executed:

.. code-block:: sql
  :linenos:

   begin;

   with r as
    (
      insert into rental(rental_date, inventory_id, customer_id, staff_id, last_update)
           select '2022-06-01', 371, 291, 1, '2022-06-01'
        returning rental_id, customer_id, staff_id
    )
    insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
         select customer_id, staff_id, rental_id, 5.99, '2020-06-01'
           from r;

   commit;

The command then looks like the following, where the ``--endpos`` has been
extracted by calling the ``pg_current_wal_lsn()`` SQL function:

::

   $ pgcopydb stream receive --slot-name test_slot --restart --endpos 0/236D668 -vv
   16:01:57 157 INFO  Running pgcopydb version 0.7 from "/usr/local/bin/pgcopydb"
   16:01:57 157 DEBUG copydb.c:406 Change Data Capture data is managed at "/var/lib/postgres/.local/share/pgcopydb"
   16:01:57 157 INFO  copydb.c:73 Using work dir "/tmp/pgcopydb"
   16:01:57 157 DEBUG pidfile.c:143 Failed to signal pid 34: No such process
   16:01:57 157 DEBUG pidfile.c:146 Found a stale pidfile at "/tmp/pgcopydb/pgcopydb.pid"
   16:01:57 157 INFO  pidfile.c:147 Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   16:01:57 157 INFO  copydb.c:254 Work directory "/tmp/pgcopydb" already exists
   16:01:57 157 INFO  copydb.c:258 A previous run has run through completion
   16:01:57 157 INFO  copydb.c:151 Removing directory "/tmp/pgcopydb"
   16:01:57 157 DEBUG copydb.c:445 rm -rf "/tmp/pgcopydb" && mkdir -p "/tmp/pgcopydb"
   16:01:57 157 DEBUG copydb.c:445 rm -rf "/tmp/pgcopydb/schema" && mkdir -p "/tmp/pgcopydb/schema"
   16:01:57 157 DEBUG copydb.c:445 rm -rf "/tmp/pgcopydb/run" && mkdir -p "/tmp/pgcopydb/run"
   16:01:57 157 DEBUG copydb.c:445 rm -rf "/tmp/pgcopydb/run/tables" && mkdir -p "/tmp/pgcopydb/run/tables"
   16:01:57 157 DEBUG copydb.c:445 rm -rf "/tmp/pgcopydb/run/indexes" && mkdir -p "/tmp/pgcopydb/run/indexes"
   16:01:57 157 DEBUG copydb.c:445 rm -rf "/var/lib/postgres/.local/share/pgcopydb" && mkdir -p "/var/lib/postgres/.local/share/pgcopydb"
   16:01:57 157 DEBUG pgsql.c:2476 starting log streaming at 0/0 (slot test_slot)
   16:01:57 157 DEBUG pgsql.c:485 Connecting to [source] "postgres://postgres@source:/postgres?password=****&replication=database"
   16:01:57 157 DEBUG pgsql.c:2009 IDENTIFY_SYSTEM: timeline 1, xlogpos 0/236D668, systemid 7104302452422938663
   16:01:57 157 DEBUG pgsql.c:3188 RetrieveWalSegSize: 16777216
   16:01:57 157 DEBUG pgsql.c:2547 streaming initiated
   16:01:57 157 INFO  stream.c:237 Now streaming changes to "/var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json"
   16:01:57 157 DEBUG stream.c:341 Received action B for XID 488 in LSN 0/236D638
   16:01:57 157 DEBUG stream.c:341 Received action I for XID 488 in LSN 0/236D178
   16:01:57 157 DEBUG stream.c:341 Received action I for XID 488 in LSN 0/236D308
   16:01:57 157 DEBUG stream.c:341 Received action C for XID 488 in LSN 0/236D638
   16:01:57 157 DEBUG pgsql.c:2867 pgsql_stream_logical: endpos reached at 0/236D668
   16:01:57 157 DEBUG stream.c:382 Flushed up to 0/236D668 in file "/var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json"
   16:01:57 157 INFO  pgsql.c:3030 Report write_lsn 0/236D668, flush_lsn 0/236D668
   16:01:57 157 DEBUG pgsql.c:3107 end position 0/236D668 reached by WAL record at 0/236D668
   16:01:57 157 DEBUG pgsql.c:408 Disconnecting from [source] "postgres://postgres@source:/postgres?password=****&replication=database"
   16:01:57 157 DEBUG stream.c:414 streamClose: closing file "/var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json"
   16:01:57 157 INFO  stream.c:171 Streaming is now finished after processing 4 messages


The JSON file then contains the following content, from the `wal2json`
logical replication plugin. Note that you're seeing diffent LSNs here
because each run produces different ones, and the captures have not all been
made from the same run.

::

   $ cat /var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json
   {"action":"B","xid":489,"timestamp":"2022-06-27 13:24:31.460822+00","lsn":"0/236F5A8","nextlsn":"0/236F5D8"}
   {"action":"I","xid":489,"timestamp":"2022-06-27 13:24:31.460822+00","lsn":"0/236F0E8","schema":"public","table":"rental","columns":[{"name":"rental_id","type":"integer","value":16050},{"name":"rental_date","type":"timestamp with time zone","value":"2022-06-01 00:00:00+00"},{"name":"inventory_id","type":"integer","value":371},{"name":"customer_id","type":"integer","value":291},{"name":"return_date","type":"timestamp with time zone","value":null},{"name":"staff_id","type":"integer","value":1},{"name":"last_update","type":"timestamp with time zone","value":"2022-06-01 00:00:00+00"}]}
   {"action":"I","xid":489,"timestamp":"2022-06-27 13:24:31.460822+00","lsn":"0/236F278","schema":"public","table":"payment_p2020_06","columns":[{"name":"payment_id","type":"integer","value":32099},{"name":"customer_id","type":"integer","value":291},{"name":"staff_id","type":"integer","value":1},{"name":"rental_id","type":"integer","value":16050},{"name":"amount","type":"numeric(5,2)","value":5.99},{"name":"payment_date","type":"timestamp with time zone","value":"2020-06-01 00:00:00+00"}]}
   {"action":"C","xid":489,"timestamp":"2022-06-27 13:24:31.460822+00","lsn":"0/236F5A8","nextlsn":"0/236F5D8"}

It's then possible to transform the JSON into SQL:


::

   $ pgcopydb stream transform  ./tests/cdc/000000010000000000000002.json /tmp/000000010000000000000002.sql

And the SQL file obtained looks like this:

::

   $ cat /tmp/000000010000000000000002.sql
   BEGIN; -- {"xid":489,"lsn":"0/236F5A8"}
   INSERT INTO "public"."rental" (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) VALUES (16050, '2022-06-01 00:00:00+00', 371, 291, NULL, 1, '2022-06-01 00:00:00+00');
   INSERT INTO "public"."payment_p2020_06" (payment_id, customer_id, staff_id, rental_id, amount, payment_date) VALUES (32099, 291, 1, 16050, 5.99, '2020-06-01 00:00:00+00');
   COMMIT; -- {"xid": 489,"lsn":"0/236F5A8"}
