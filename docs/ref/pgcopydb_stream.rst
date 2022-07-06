.. _pgcopydb_stream:

pgcopydb stream
===============

pgcopydb stream - Stream changes from source database

This command prefixes the following sub-commands:

::

  pgcopydb stream
    setup      Setup source and target systems for logical decoding
    prefetch   Stream JSON changes from the source database and transform them to SQL
    receive    Stream changes from the source database
    transform  Transform changes from the source database into SQL commands
    apply      Apply changes from the source database into the target database

Those commands implement a part of the whole database replay operation as
detailed in section :ref:`pgcopydb_follow`. Only use those commands to debug
a specific part, or because you know that you just want to implement that
step.

.. warning::

   Using the ``pgcopydb follow`` command or the command ``pgcopydb
   clone --follow`` is strongly advised.

   This mode of operations has been designed for unit testing.

This is still a work in progress. Stay tuned.

.. _pgcopydb_stream_setup:

pgcopydb stream setup
---------------------

pgcopydb stream setup - Setup source and target systems for logical decoding

The command ``pgcopydb stream setup`` connects to the source database and
create a replication slot using the logical decoding plugin `wal2json`__,
then connects to the target database and creates a replication origin
positioned at the LSN position of the just created replication slot.

__ https://github.com/eulerto/wal2json/


::

   pgcopydb stream setup: Setup source and target systems for logical decoding
   usage: pgcopydb stream setup  --source ... --target ... --dir ...

     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --dir            Work directory to use
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
using the logical replication protocl and the given replication slot, that
should be created with the logical decoding plugin `wal2json`__.

__ https://github.com/eulerto/wal2json/

The prefetch command receives the changes from the source database in a
streaming fashion, and writes them in a series of JSON files named the same
as their origin WAL filename (with the ``.json`` extension). Each time a
JSON file is closed, a subprocess is started to transform the JSON into an
SQL file.


::

   pgcopydb stream prefetch: Stream JSON changes from the source database and transform them to SQL
   usage: pgcopydb stream prefetch  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes

.. _pgcopydb_stream_receive:

pgcopydb stream receive
-----------------------

pgcopydb stream receive - Stream changes from the source database

The command ``pgcopydb stream receive`` connects to the source database
using the logical replication protocl and the given replication slot, that
should be created with the logical decoding plugin `wal2json`__.

__ https://github.com/eulerto/wal2json/

The receive command receives the changes from the source database in a
streaming fashion, and writes them in a series of JSON files named the same
as their origin WAL filename (with the ``.json`` extension).

::

   pgcopydb stream receive: Stream changes from the source database
   usage: pgcopydb stream receive  --source ...

     --source         Postgres URI to the source database
     --dir            Work directory to use
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
   usage: pgcopydb stream transform  [ --source ... ] <json filename> <sql filename>

     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database


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
   usage: pgcopydb stream apply  --target ... <sql filename>

     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --origin         Name of the Postgres replication origin


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

--slot-name

  Logical replication slot to use. At the moment pgcopydb doesn't know how
  to create the logical replication slot itself. The slot should be created
  within the same transaction snapshot as the initial data copy.

  Must be using the `wal2json`__ output plugin, available with
  format-version 2.

  __ https://github.com/eulerto/wal2json/

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
