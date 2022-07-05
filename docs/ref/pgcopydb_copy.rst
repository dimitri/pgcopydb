.. _pgcopydb_copy:

pgcopydb copy
=============

pgcopydb copy - Implement the data section of the database copy

This command prefixes the following sub-commands:

::

  pgcopydb copy
    db           Copy an entire database from source to target
    roles        Copy the roles from the source instance to the target instance
    schema       Copy the database schema from source to target
    data         Copy the data section from source to target
    table-data   Copy the data from all tables in database from source to target
    blobs        Copy the blob data from ther source database to the target
    sequences    Copy the current value from all sequences in database from source to target
    indexes      Create all the indexes found in the source database in the target
    constraints  Create all the constraints found in the source database in the target

Those commands implement a part of the whole database copy operation as
detailed in section :ref:`pgcopydb_clone`. Only use those commands to debug
a specific part, or because you know that you just want to implement that
step.

.. warning::
   Using the ``pgcopydb clone`` command is strongly advised.

   This mode of operations is useful for debugging and advanced use cases
   only.

.. _pgcopydb_copy_db:

pgcopydb copy db
----------------

pgcopydb copy db - Copy an entire database from source to target

The command ``pgcopydb copy db`` is an alias for ``pgcopydb clone``. See
also :ref:`pgcopydb_clone`.

::

   pgcopydb copy db: Copy an entire database from source to target
   usage: pgcopydb copy db  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

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

.. _pgcopydb_copy_roles:

pgcopydb copy roles
-------------------

pgcopydb copy roles - Copy the roles from the source instance to the target instance

The command ``pgcopydb copy roles`` implements both
:ref:`pgcopydb_dump_roles` and then :ref:`pgcopydb_restore_roles`.

::

   pgcopydb copy roles: Copy the roles from the source instance to the target instance
   usage: pgcopydb copy roles  --source ... --target ...

     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use

.. note::

   In Postgres, roles are a global object. This means roles do not belong to
   any specific database, and as a result, even when the ``pgcopydb`` tool
   otherwise works only in the context of a specific database, this command
   is not limited to roles that are used within a single database.

When a role already exists on the target database, its restoring is entirely
skipped, which includes skipping both the ``CREATE ROLE`` and the ``ALTER
ROLE`` commands produced by ``pg_dumpall --roles-only``.

.. _pgcopydb_copy_schema:

pgcopydb copy schema
--------------------

pgcopydb copy schema - Copy the database schema from source to target

The command ``pgcopydb copy schema`` implements the schema only section of
the clone steps.

::

   pgcopydb copy schema: Copy the database schema from source to target
   usage: pgcopydb copy schema  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --filters <filename>  Use the filters defined in <filename>
     --restart             Allow restarting when temp files exist already
     --resume              Allow resuming operations after a failure
     --not-consistent      Allow taking a new snapshot on the source database
     --snapshot            Use snapshot obtained with pg_export_snapshot


.. _pgcopydb_copy_data:

pgcopydb copy data
------------------

pgcopydb copy data - Copy the data section from source to target

The command ``pgcopydb copy data`` implements the data section of the clone
steps.

::

   pgcopydb copy data: Copy the data section from source to target
   usage: pgcopydb copy data  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --table-jobs          Number of concurrent COPY jobs to run
     --index-jobs          Number of concurrent CREATE INDEX jobs to run
     --drop-if-exists      On the target database, clean-up from a previous run first
     --no-owner            Do not set ownership of objects to match the original database
     --skip-large-objects  Skip copying large objects (blobs)
     --restart             Allow restarting when temp files exist already
     --resume              Allow resuming operations after a failure
     --not-consistent      Allow taking a new snapshot on the source database
     --snapshot            Use snapshot obtained with pg_export_snapshot

.. note::

   The current command line has both the commands ``pgcopydb copy
   table-data`` and ``pgcopydb copy data``, which are looking quite similar
   but implement different steps. Be careful for now. This will change
   later.

The ``pgcopydb copy data`` command implements the following steps::

   $ pgcopydb copy table-data
   $ pgcopydb copy blobs
   $ pgcopydb copy indexes
   $ pgcopydb copy constraints
   $ pgcopydb copy sequences
   $ vacuumdb -z

Those steps are actually done concurrently to one another when that's
possible, in the same way as the main command ``pgcopydb clone`` would.
The only difference is that the ``pgcopydb clone`` command also prepares
and finishes the schema parts of the operations (pre-data, then post-data),
which the ``pgcopydb copy data`` command ignores.

.. _pgcopydb_copy_table_data:

pgcopydb copy table-data
------------------------

pgcopydb copy table-data - Copy the data from all tables in database from source to target

The command ``pgcopydb copy table-data`` fetches the list of tables from the
source database and runs a COPY TO command on the source database and sends
the result to the target database using a COPY FROM command directly,
avoiding disks entirely.

::

   pgcopydb copy table-data: Copy the data from all tables in database from source to target
   usage: pgcopydb copy table-data  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source          Postgres URI to the source database
     --target          Postgres URI to the target database
     --dir             Work directory to use
     --table-jobs      Number of concurrent COPY jobs to run
     --restart         Allow restarting when temp files exist already
     --resume          Allow resuming operations after a failure
     --not-consistent  Allow taking a new snapshot on the source database
     --snapshot        Use snapshot obtained with pg_export_snapshot

.. _pgcopydb_copy_blobs:

pgcopydb copy blobs
-------------------

pgcopydb copy blobs - Copy the blob data from ther source database to the target

The command ``pgcopydb copy blobs`` fetches list of large objects (aka
blobs) from the source database and copies their data parts to the target
database. By default the command assumes that the large objects metadata
have already been taken care of, because of the behaviour of
``pg_dump --section=pre-data``.

::

   pgcopydb copy blobs: Copy the blob data from ther source database to the target
   usage: pgcopydb copy blobs  --source ... --target ...

     --source          Postgres URI to the source database
     --target          Postgres URI to the target database
     --dir             Work directory to use
     --restart         Allow restarting when temp files exist already
     --resume          Allow resuming operations after a failure
     --not-consistent  Allow taking a new snapshot on the source database
     --snapshot        Use snapshot obtained with pg_export_snapshot
     --drop-if-exists  On the target database, drop and create large objects

.. _pgcopydb_copy_sequences:

pgcopydb copy sequences
-----------------------

pgcopydb copy sequences - Copy the current value from all sequences in database from source to target

The command ``pgcopydb copy sequences`` fetches the list of sequences from
the source database, then for each sequence fetches the ``last_value`` and
``is_called`` properties the same way pg_dump would on the source database,
and then for each sequence call ``pg_catalog.setval()`` on the target
database.

::

   pgcopydb copy sequences: Copy the current value from all sequences in database from source to target
   usage: pgcopydb copy sequences  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source          Postgres URI to the source database
     --target          Postgres URI to the target database
     --dir                 Work directory to use
     --restart         Allow restarting when temp files exist already
     --resume          Allow resuming operations after a failure
     --not-consistent  Allow taking a new snapshot on the source database

.. _pgcopydb_copy_indexes:

pgcopydb copy indexes
---------------------

pgcopydb copy indexes - Create all the indexes found in the source database in the target

The command ``pgcopydb copy indexes`` fetches the list of indexes from the
source database and runs each index CREATE INDEX statement on the target
database. The statements for the index definitions are modified to include
IF NOT EXISTS and allow for skipping indexes that already exist on the
target database.

::

   pgcopydb copy indexes: Create all the indexes found in the source database in the target
   usage: pgcopydb copy indexes  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source          Postgres URI to the source database
     --target          Postgres URI to the target database
     --dir                 Work directory to use
	 --index-jobs      Number of concurrent CREATE INDEX jobs to run
     --restart         Allow restarting when temp files exist already
     --resume          Allow resuming operations after a failure
     --not-consistent  Allow taking a new snapshot on the source database

.. _pgcopydb_copy_constraints:

pgcopydb copy constraints
-------------------------

pgcopydb copy constraints - Create all the constraints found in the source database in the target

The command ``pgcopydb copy constraints`` fetches the list of indexes from
the source database and runs each index ALTER TABLE ... ADD CONSTRAINT ...
USING INDEX statement on the target database.

The indexes must already exist, and the command will fail if any constraint
is found existing already on the target database.

::

   pgcopydb copy indexes: Create all the indexes found in the source database in the target
   usage: pgcopydb copy indexes  --source ... --target ... [ --table-jobs ... --index-jobs ... ]

     --source          Postgres URI to the source database
     --target          Postgres URI to the target database
     --dir                 Work directory to use
     --restart         Allow restarting when temp files exist already
     --resume          Allow resuming operations after a failure
     --not-consistent  Allow taking a new snapshot on the source data

Description
-----------

These commands allow implementing a specific step of the pgcopydb operations
at a time. It's useful mainly for debugging purposes, though some advanced
and creative usage can be made from the commands.

The target schema is not created, so it needs to have been taken care of
first. It is possible to use the commands :ref:`pgcopydb_dump_schema` and
then :ref:`pgcopydb_restore_pre_data` to prepare your target database.

To implement the same operations as a ``pgcopydb clone`` command would,
use the following recipe:

::

   $ export PGCOPYDB_SOURCE_PGURI="postgres://user@source/dbname"
   $ export PGCOPYDB_TARGET_PGURI="postgres://user@target/dbname"

   $ pgcopydb dump schema
   $ pgcopydb restore pre-data --resume --not-consistent
   $ pgcopydb copy table-data --resume --not-consistent
   $ pgcopydb copy sequences --resume --not-consistent
   $ pgcopydb copy indexes --resume --not-consistent
   $ pgcopydb copy constraints --resume --not-consistent
   $ vacuumdb -z
   $ pgcopydb restore post-data --resume --not-consistent

The main ``pgcopydb clone`` is still better at concurrency than doing
those steps manually, as it will create the indexes for any given table as
soon as the table-data section is finished, without having to wait until the
last table-data has been copied over. Same applies to constraints, and then
vacuum analyze.

Options
-------

The following options are available to ``pgcopydb copy`` sub-commands:

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

--skip-large-objects

  Skip copying large objects, also known as blobs, when copying the data
  from the source database to the target database.

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

Let's export the Postgres databases connection strings to make it easy to
re-use them all along:

::

   $ export PGCOPYDB_SOURCE_PGURI="port=54311 host=localhost dbname=pgloader"
   $ export PGCOPYDB_TARGET_PGURI="port=54311 dbname=plop"

Now, first dump the schema:

::

   $ pgcopydb dump schema
   15:24:24 75511 INFO  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   15:24:24 75511 WARN  Directory "/tmp/pgcopydb" already exists: removing it entirely
   15:24:24 75511 INFO  Dumping database from "port=54311 host=localhost dbname=pgloader"
   15:24:24 75511 INFO  Dumping database into directory "/tmp/pgcopydb"
   15:24:24 75511 INFO  Using pg_dump for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_dump"
   15:24:24 75511 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section pre-data --file /tmp/pgcopydb/schema/pre.dump 'port=54311 host=localhost dbname=pgloader'
   15:24:25 75511 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section post-data --file /tmp/pgcopydb/schema/post.dump 'port=54311 host=localhost dbname=pgloader'

Now restore the pre-data schema on the target database, cleaning up the
already existing objects if any, which allows running this test scenario
again and again. It might not be what you want to do in your production
target instance though!

::

   PGCOPYDB_DROP_IF_EXISTS=on pgcopydb restore pre-data --no-owner
   15:24:29 75591 INFO  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   15:24:29 75591 INFO  Restoring database from "/tmp/pgcopydb"
   15:24:29 75591 INFO  Restoring database into "port=54311 dbname=plop"
   15:24:29 75591 INFO  Using pg_restore for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_restore"
   15:24:29 75591 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54311 dbname=plop' --clean --if-exists --no-owner /tmp/pgcopydb/schema/pre.dump


Then copy the data over:

::

   $ pgcopydb copy table-data --resume --not-consistent
   15:24:36 75688 INFO  [SOURCE] Copying database from "port=54311 host=localhost dbname=pgloader"
   15:24:36 75688 INFO  [TARGET] Copying database into "port=54311 dbname=plop"
   15:24:36 75688 INFO  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   15:24:36 75688 INFO  STEP 3: copy data from source to target in sub-processes
   15:24:36 75688 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
   15:24:36 75688 INFO  Fetched information for 56 tables
   ...
                                             Step   Connection    Duration   Concurrency
    ---------------------------------------------   ----------  ----------  ------------
                                      Dump Schema       source         0ms             1
                                   Prepare Schema       target         0ms             1
    COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both         0ms         4 + 4
                                COPY (cumulative)         both       1s140             4
                        CREATE INDEX (cumulative)       target         0ms             4
                                  Finalize Schema       target         0ms             1
    ---------------------------------------------   ----------  ----------  ------------
                        Total Wall Clock Duration         both       2s143         4 + 4
    ---------------------------------------------   ----------  ----------  ------------


And now create the indexes on the target database, using the index
definitions from the source database:

::

   $ pgcopydb copy indexes --resume --not-consistent
   15:24:40 75918 INFO  [SOURCE] Copying database from "port=54311 host=localhost dbname=pgloader"
   15:24:40 75918 INFO  [TARGET] Copying database into "port=54311 dbname=plop"
   15:24:40 75918 INFO  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   15:24:40 75918 INFO  STEP 4: create indexes in parallel
   15:24:40 75918 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
   15:24:40 75918 INFO  Fetched information for 56 tables
   15:24:40 75930 INFO  Creating 2 indexes for table "csv"."partial"
   15:24:40 75922 INFO  Creating 1 index for table "csv"."track"
   15:24:40 75931 INFO  Creating 1 index for table "err"."errors"
   15:24:40 75928 INFO  Creating 1 index for table "csv"."blocks"
   15:24:40 75925 INFO  Creating 1 index for table "public"."track_full"
   15:24:40 76037 INFO  CREATE INDEX IF NOT EXISTS partial_b_idx ON csv.partial USING btree (b);
   15:24:40 76036 INFO  CREATE UNIQUE INDEX IF NOT EXISTS track_pkey ON csv.track USING btree (trackid);
   15:24:40 76035 INFO  CREATE UNIQUE INDEX IF NOT EXISTS partial_a_key ON csv.partial USING btree (a);
   15:24:40 76038 INFO  CREATE UNIQUE INDEX IF NOT EXISTS errors_pkey ON err.errors USING btree (a);
   15:24:40 75987 INFO  Creating 1 index for table "public"."xzero"
   15:24:40 75969 INFO  Creating 1 index for table "public"."csv_escape_mode"
   15:24:40 75985 INFO  Creating 1 index for table "public"."udc"
   15:24:40 75965 INFO  Creating 1 index for table "public"."allcols"
   15:24:40 75981 INFO  Creating 1 index for table "public"."serial"
   15:24:40 76039 INFO  CREATE INDEX IF NOT EXISTS blocks_ip4r_idx ON csv.blocks USING gist (iprange);
   15:24:40 76040 INFO  CREATE UNIQUE INDEX IF NOT EXISTS track_full_pkey ON public.track_full USING btree (trackid);
   15:24:40 75975 INFO  Creating 1 index for table "public"."nullif"
   15:24:40 76046 INFO  CREATE UNIQUE INDEX IF NOT EXISTS xzero_pkey ON public.xzero USING btree (a);
   15:24:40 76048 INFO  CREATE UNIQUE INDEX IF NOT EXISTS udc_pkey ON public.udc USING btree (b);
   15:24:40 76047 INFO  CREATE UNIQUE INDEX IF NOT EXISTS csv_escape_mode_pkey ON public.csv_escape_mode USING btree (id);
   15:24:40 76049 INFO  CREATE UNIQUE INDEX IF NOT EXISTS allcols_pkey ON public.allcols USING btree (a);
   15:24:40 76052 INFO  CREATE UNIQUE INDEX IF NOT EXISTS nullif_pkey ON public."nullif" USING btree (id);
   15:24:40 76050 INFO  CREATE UNIQUE INDEX IF NOT EXISTS serial_pkey ON public.serial USING btree (a);

                                             Step   Connection    Duration   Concurrency
    ---------------------------------------------   ----------  ----------  ------------
                                      Dump Schema       source         0ms             1
                                   Prepare Schema       target         0ms             1
    COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both         0ms         4 + 4
                                COPY (cumulative)         both       619ms             4
                        CREATE INDEX (cumulative)       target       1s023             4
                                  Finalize Schema       target         0ms             1
    ---------------------------------------------   ----------  ----------  ------------
                        Total Wall Clock Duration         both       400ms         4 + 4
    ---------------------------------------------   ----------  ----------  ------------

Now re-create the constraints (primary key, unique constraints) from the
source database schema into the target database:

::

   $ pgcopydb copy constraints --resume --not-consistent
   15:24:43 76095 INFO  [SOURCE] Copying database from "port=54311 host=localhost dbname=pgloader"
   15:24:43 76095 INFO  [TARGET] Copying database into "port=54311 dbname=plop"
   15:24:43 76095 INFO  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   15:24:43 76095 INFO  STEP 4: create constraints
   15:24:43 76095 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
   15:24:43 76095 INFO  Fetched information for 56 tables
   15:24:43 76099 INFO  ALTER TABLE "csv"."track" ADD CONSTRAINT "track_pkey" PRIMARY KEY USING INDEX "track_pkey";
   15:24:43 76107 INFO  ALTER TABLE "csv"."partial" ADD CONSTRAINT "partial_a_key" UNIQUE USING INDEX "partial_a_key";
   15:24:43 76102 INFO  ALTER TABLE "public"."track_full" ADD CONSTRAINT "track_full_pkey" PRIMARY KEY USING INDEX "track_full_pkey";
   15:24:43 76142 INFO  ALTER TABLE "public"."allcols" ADD CONSTRAINT "allcols_pkey" PRIMARY KEY USING INDEX "allcols_pkey";
   15:24:43 76157 INFO  ALTER TABLE "public"."serial" ADD CONSTRAINT "serial_pkey" PRIMARY KEY USING INDEX "serial_pkey";
   15:24:43 76161 INFO  ALTER TABLE "public"."xzero" ADD CONSTRAINT "xzero_pkey" PRIMARY KEY USING INDEX "xzero_pkey";
   15:24:43 76146 INFO  ALTER TABLE "public"."csv_escape_mode" ADD CONSTRAINT "csv_escape_mode_pkey" PRIMARY KEY USING INDEX "csv_escape_mode_pkey";
   15:24:43 76154 INFO  ALTER TABLE "public"."nullif" ADD CONSTRAINT "nullif_pkey" PRIMARY KEY USING INDEX "nullif_pkey";
   15:24:43 76159 INFO  ALTER TABLE "public"."udc" ADD CONSTRAINT "udc_pkey" PRIMARY KEY USING INDEX "udc_pkey";
   15:24:43 76108 INFO  ALTER TABLE "err"."errors" ADD CONSTRAINT "errors_pkey" PRIMARY KEY USING INDEX "errors_pkey";

                                             Step   Connection    Duration   Concurrency
    ---------------------------------------------   ----------  ----------  ------------
                                      Dump Schema       source         0ms             1
                                   Prepare Schema       target         0ms             1
    COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both         0ms         4 + 4
                                COPY (cumulative)         both       605ms             4
                        CREATE INDEX (cumulative)       target       1s023             4
                                  Finalize Schema       target         0ms             1
    ---------------------------------------------   ----------  ----------  ------------
                        Total Wall Clock Duration         both       415ms         4 + 4
    ---------------------------------------------   ----------  ----------  ------------

The next step is a VACUUM ANALYZE on each table that's been just filled-in
with the data, and for that we can just use the `vacuumdb`__ command from
Postgres:

__ https://www.postgresql.org/docs/current/app-vacuumdb.html

::

   $ vacuumdb --analyze --dbname "$PGCOPYDB_TARGET_PGURI" --jobs 4
   vacuumdb: vacuuming database "plop"

Finally we can restore the post-data section of the schema:

::

   $ pgcopydb restore post-data --resume --not-consistent
   15:24:50 76328 INFO  Removing the stale pid file "/tmp/pgcopydb/pgcopydb.pid"
   15:24:50 76328 INFO  Restoring database from "/tmp/pgcopydb"
   15:24:50 76328 INFO  Restoring database into "port=54311 dbname=plop"
   15:24:50 76328 INFO  Using pg_restore for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_restore"
   15:24:50 76328 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_restore --dbname 'port=54311 dbname=plop' --use-list /tmp/pgcopydb/schema/post.list /tmp/pgcopydb/schema/post.dump
