.. _pgcopydb_stream:

pgcopydb stream
===============

pgcopydb stream - Stream changes from source database

This command prefixes the following sub-commands:

::

  pgcopydb stream
    receive    Stream changes from the source database
    transform  Transform changes from the source database into SQL commands
    apply      Apply changes from the source database into the target database

.. warning::

   Those commands are still experimental. The documentation will be expanded
   later when the integration is complete. Meanwhile, this is an Open Source
   project, consider contributing.

   Later, when this is implemented, it should be possible with pgcopydb to
   implement an online migration approach, using a classic multi-phases
   approach:

     0. create and export a snapshot to re-use in the three phases
     1. initial copy of schema and data visible in the exported snapshot
     2. capture data changes (DML) applied after the exported snapshot
     3. catch-up with the changes captured
     4. switch to low-lag streaming of the changes
     5. disconnect application from the old system, the source
     6. connect applications to the new system, the target

This is still a work in progress. Stay tuned.

.. _pgcopydb_stream_receive:

pgcopydb stream receive
-----------------------

pgcopydb stream receive - Stream changes from the source database

The command ``pgcopydb stream tables`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a stream of all the
tables to COPY the data from.

::

   pgcopydb stream receive: Stream changes from the source database
   usage: pgcopydb stream receive  --source ... 
   
     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes


Options
-------

The following options are available to ``pgcopydb stream receive``:

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
   16:01:57 157 INFO  stream.c:223 Now streaming changes to "/var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json"
   16:01:57 157 DEBUG stream.c:327 Received action B for XID 488 in LSN 0/236D638
   16:01:57 157 DEBUG stream.c:327 Received action I for XID 488 in LSN 0/236D178
   16:01:57 157 DEBUG stream.c:327 Received action I for XID 488 in LSN 0/236D308
   16:01:57 157 DEBUG stream.c:327 Received action C for XID 488 in LSN 0/236D638
   16:01:57 157 DEBUG pgsql.c:2867 pgsql_stream_logical: endpos reached at 0/236D668
   16:01:57 157 DEBUG stream.c:368 Flushed up to 0/236D668 in file "/var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json"
   16:01:57 157 INFO  pgsql.c:3030 Report write_lsn 0/236D668, flush_lsn 0/236D668
   16:01:57 157 DEBUG pgsql.c:3107 end position 0/236D668 reached by WAL record at 0/236D668
   16:01:57 157 DEBUG pgsql.c:408 Disconnecting from [source] "postgres://postgres@source:/postgres?password=****&replication=database"
   16:01:57 157 DEBUG stream.c:400 streamClose: closing file "/var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json"


The JSON file then contains the following content, from the `wal2json`
logical replication plugin:
   
::
   
   $ cat /var/lib/postgres/.local/share/pgcopydb/000000010000000000000002.json
   {"action":"B","xid":488,"lsn":"0/236D638","nextlsn":"0/236D668"}
   {"action":"I","xid":488,"lsn":"0/236D178","schema":"public","table":"rental","columns":[{"name":"rental_id","type":"integer","value":16050},{"name":"rental_date","type":"timestamp with time zone","value":"2022-06-01 00:00:00+00"},{"name":"inventory_id","type":"integer","value":371},{"name":"customer_id","type":"integer","value":291},{"name":"return_date","type":"timestamp with time zone","value":null},{"name":"staff_id","type":"integer","value":1},{"name":"last_update","type":"timestamp with time zone","value":"2022-06-01 00:00:00+00"}]}
   {"action":"I","xid":488,"lsn":"0/236D308","schema":"public","table":"payment_p2020_06","columns":[{"name":"payment_id","type":"integer","value":32099},{"name":"customer_id","type":"integer","value":291},{"name":"staff_id","type":"integer","value":1},{"name":"rental_id","type":"integer","value":16050},{"name":"amount","type":"numeric(5,2)","value":5.99},{"name":"payment_date","type":"timestamp with time zone","value":"2020-06-01 00:00:00+00"}]}
   {"action":"C","xid":488,"lsn":"0/236D638","nextlsn":"0/236D668"}

A pretty printed version of the JSON contents follows:
   
.. code-block:: json
  :linenos:

   {
     "action": "B",
     "xid": 488,
     "lsn": "0/236D948",
     "nextlsn": "0/236D978"
   }
   {
     "action": "I",
     "xid": 488,
     "lsn": "0/236D488",
     "schema": "public",
     "table": "rental",
     "columns": [
       {
         "name": "rental_id",
         "type": "integer",
         "value": 16050
       },
       {
         "name": "rental_date",
         "type": "timestamp with time zone",
         "value": "2022-06-01 00:00:00+00"
       },
       {
         "name": "inventory_id",
         "type": "integer",
         "value": 371
       },
       {
         "name": "customer_id",
         "type": "integer",
         "value": 291
       },
       {
         "name": "return_date",
         "type": "timestamp with time zone",
         "value": null
       },
       {
         "name": "staff_id",
         "type": "integer",
         "value": 1
       },
       {
         "name": "last_update",
         "type": "timestamp with time zone",
         "value": "2022-06-01 00:00:00+00"
       }
     ]
   }
   {
     "action": "I",
     "xid": 488,
     "lsn": "0/236D618",
     "schema": "public",
     "table": "payment_p2020_06",
     "columns": [
       {
         "name": "payment_id",
         "type": "integer",
         "value": 32099
       },
       {
         "name": "customer_id",
         "type": "integer",
         "value": 291
       },
       {
         "name": "staff_id",
         "type": "integer",
         "value": 1
       },
       {
         "name": "rental_id",
         "type": "integer",
         "value": 16050
       },
       {
         "name": "amount",
         "type": "numeric(5,2)",
         "value": 5.99
       },
       {
         "name": "payment_date",
         "type": "timestamp with time zone",
         "value": "2020-06-01 00:00:00+00"
       }
     ]
   }
   {
     "action": "C",
     "xid": 488,
     "lsn": "0/236D948",
     "nextlsn": "0/236D978"
   }