Concurrency
===========

The reason why ``pgcopydb`` has been developed is mostly to allow two
aspects that are not possible to achieve directly with ``pg_dump`` and
``pg_restore``, and that requires just enough fiddling around that not many
scripts have been made available to automate around.

.. _pgcopydb_concurrency:

Notes about concurrency
-----------------------

The pgcopydb too implements many operations concurrently to one another, by
ways of using the ``fork()`` system call. This means that pgcopydb creates
sub-processes that each handle a part of the work.

The process tree then looks like the following::

  $ pgcopydb clone --follow --table-jobs 4 --index-jobs 4 --large-objects-jobs 4
   + pgcopydb clone worker
      + pgcopydb copy supervisor [ --table-jobs 4 ]
        - pgcopydb copy queue worker
        - pgcopydb copy worker
        - pgcopydb copy worker
        - pgcopydb copy worker
        - pgcopydb copy worker
 
      + pgcopydb blob metadata worker [ --large-objects-jobs 4 ]
        - pgcopydb blob data worker
        - pgcopydb blob data worker
        - pgcopydb blob data worker
        - pgcopydb blob data worker

      + pgcopydb index supervisor [ --index-jobs 4 ]
        - pgcopydb index/constraints worker
        - pgcopydb index/constraints worker
        - pgcopydb index/constraints worker
        - pgcopydb index/constraints worker

      + pgcopydb vacuum supervisor [ --table-jobs 4 ]
        - pgcopydb vacuum analyze worker
        - pgcopydb vacuum analyze worker
        - pgcopydb vacuum analyze worker
        - pgcopydb vacuum analyze worker

      + pgcopydb sequences reset worker

   + pgcopydb follow worker [ --follow ]
     - pgcopydb stream receive
     - pgcopydb stream transform
     - pgcopydb stream catchup

We see that when using ``pgcopydb clone --follow --table-jobs 4 --index-jobs
4 --large-objects-jobs 4`` then pgcopydb creates 27 sub-processes.

The 27 total is counted from:

 - 1 clone worker + 1 copy supervisor + 1 copy queue worker + 4 copy
   workers + 1 blob metadata worker + 4 blob data workers + 1 index
   supervisor + 4 index workers + 1 vacuum supervisor + 4 vacuum workers + 1
   sequence reset worker

   that's 1 + 1 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 4 + 1 = 23

 - 1 follow worker + 1 stream receive + 1 stream transform + 1 stream catchup

   that's 1 + 1 + 1 + 1 = 4

 - that's 23 + 4 = 27 total

Here is a description of the process tree:

 * When starting with the TABLE DATA copying step, then pgcopydb creates as
   many sub-processes as specified by the ``--table-jobs`` command line
   option (or the environment variable ``PGCOPYDB_TABLE_JOBS``), and an
   extra process is created to send the table to the queue and handle
   TRUNCATE commands for COPY-partitioned tables.

 * A single sub-process is created by pgcopydb to copy the Postgres Large
   Objects (BLOBs) metadata found on the source database to the target
   database, and as many as ``--large-objects-jobs`` processes are started
   to copy the large object data.

 * To drive the index and constraint build on the target database, pgcopydb
   creates as many sub-processes as specified by the ``--index-jobs``
   command line option (or the environment variable
   ``PGCOPYDB_INDEX_JOBS``).

   It is possible with Postgres to create several indexes for the same table
   in parallel, for that, the client just needs to open a separate database
   connection for each index and run each CREATE INDEX command in its own
   connection, at the same time. In pgcopydb this is implemented by running
   one sub-process per index to create.

   The ``--index-jobs`` option is global for the whole operation, so that
   it's easier to setup to the count of available CPU cores on the target
   Postgres instance. Usually, a given CREATE INDEX command uses 100% of a
   single core.

 * To drive the VACUUM ANALYZE workload on the target database pgcopydb
   creates as many sub-processes as specified by the ``--table-jobs``
   command line option.

 * To reset sequences in parallel to COPYing the table data, pgcopydb
   creates a single dedicated sub-process.

 * When using the ``--follow`` option then another sub-process leader is
   created to handle the three Change Data Capture processes.

    - One process implements :ref:`pgcopydb_stream_receive` to fetch changes
      in the JSON format and pre-fetch them in JSON files.

    - As soon as JSON file is completed, the pgcopydb stream transform
      worker transforms the JSON file into SQL, as if by calling the command
      :ref:`pgcopydb_stream_transform`.

    - Another process implements :ref:`pgcopydb_stream_catchup` to apply SQL
      changes to the target Postgres instance. This process loops over
      querying the pgcopydb sentinel table until the apply mode has been
      enabled, and then loops over the SQL files and run the queries from
      them.

.. _index_concurrency:

For each table, build all indexes concurrently
----------------------------------------------

pgcopydb takes the extra step and makes sure to create all your indexes in
parallel to one-another, going the extra mile when it comes to indexes that
are associated with a constraint.

Postgres introduced the configuration parameter `synchronize_seqscans`__ in
version 8.3, eons ago. It is on by default and allows the following
behavior:

__ https://postgresqlco.nf/doc/en/param/synchronize_seqscans/

.. admonition:: From the PostgreSQL documentation

  This allows sequential scans of large tables to synchronize with each
  other, so that concurrent scans read the same block at about the same time
  and hence share the I/O workload.

The other aspect that ``pg_dump`` and ``pg_restore`` are not very smart about is
how they deal with the indexes that are used to support constraints, in
particular unique constraints and primary keys.

Those indexes are exported using the ``ALTER TABLE`` command directly. This is
fine because the command creates both the constraint and the underlying
index, so the schema in the end is found as expected.

That said, those ``ALTER TABLE ... ADD CONSTRAINT`` commands require a level
of locking that prevents any concurrency. As we can read on the `docs for
ALTER TABLE`__:

__ https://www.postgresql.org/docs/current/sql-altertable.html

.. admonition:: From the PostgreSQL documentation

  Although most forms of ADD table_constraint require an ACCESS EXCLUSIVE
  lock, ADD FOREIGN KEY requires only a SHARE ROW EXCLUSIVE lock. Note that
  ADD FOREIGN KEY also acquires a SHARE ROW EXCLUSIVE lock on the referenced
  table, in addition to the lock on the table on which the constraint is
  declared.

The trick is then to first issue a ``CREATE UNIQUE INDEX`` statement and when
the index has been built then issue a second command in the form of ``ALTER
TABLE ... ADD CONSTRAINT ... PRIMARY KEY USING INDEX ...``, as in the
following example taken from the logs of actually running ``pgcopydb``::

  21:52:06 68898 INFO  COPY "demo"."tracking";
  21:52:06 68899 INFO  COPY "demo"."client";
  21:52:06 68899 INFO  Creating 2 indexes for table "demo"."client"
  21:52:06 68906 INFO  CREATE UNIQUE INDEX client_pkey ON demo.client USING btree (client);
  21:52:06 68907 INFO  CREATE UNIQUE INDEX client_pid_key ON demo.client USING btree (pid);
  21:52:06 68898 INFO  Creating 1 indexes for table "demo"."tracking"
  21:52:06 68908 INFO  CREATE UNIQUE INDEX tracking_pkey ON demo.tracking USING btree (client, ts);
  21:52:06 68907 INFO  ALTER TABLE "demo"."client" ADD CONSTRAINT "client_pid_key" UNIQUE USING INDEX "client_pid_key";
  21:52:06 68906 INFO  ALTER TABLE "demo"."client" ADD CONSTRAINT "client_pkey" PRIMARY KEY USING INDEX "client_pkey";
  21:52:06 68908 INFO  ALTER TABLE "demo"."tracking" ADD CONSTRAINT "tracking_pkey" PRIMARY KEY USING INDEX "tracking_pkey";

This trick is worth a lot of performance gains on its own, as has been
discovered and experienced and appreciated by `pgloader`__ users already.

__ https://github.com/dimitri/pgloader

.. _same_table_concurrency:

Same-table Concurrency
----------------------

In some database schema design, it happens that most of the database size
on-disk is to be found in a single giant table, or a short list of giant
tables. When this happens, the concurrency model that is implemented with
``--table-jobs`` still allocates a single process to COPY all the data from
the source table.

Same-table concurrency allows pgcopydb to use more than one process at the
same time to process a single source table. The data is then logically
partitionned (on the fly) and split between processes:

  - To fetch the data from the source database, the COPY processes then use
    SELECT queries like in the following example:

    ::

       COPY (SELECT * FROM source.table WHERE id BETWEEN      1 AND 123456)
       COPY (SELECT * FROM source.table WHERE id BETWEEN 123457 AND 234567)
       COPY (SELECT * FROM source.table WHERE id BETWEEN 234568 AND 345678)
       ...

    This is only possible when the source.table has at least one column of
    an integer type (``int2``, ``int4``, and ``int8`` are supported) and
    with a UNIQUE or PRIMARY KEY constraint. We must make sure that any
    given row is selected only once overall to avoid introducing duplicates
    on the target database.

    When a table is missing such a primary key column of an integer data
    type, pgcopydb then automatically resorts to using CTID based
    comparisons. See `Postgres documentation section about System Columns`__
    for more information about Postgres CTIDs.

    __ https://www.postgresql.org/docs/current/ddl-system-columns.html

    The COPY processes then use the SELECT queries like in the following
    example:

    ::

       COPY (SELECT * FROM source.table WHERE ctid >= '(0,0)'::tid and ctid < '(5925,0)'::tid)
       COPY (SELECT * FROM source.table WHERE ctid >= '(5925,0)'::tid and ctid < '(11850,0)'::tid)
       COPY (SELECT * FROM source.table WHERE ctid >= '(11850,0)'::tid and ctid < '(17775,0)'::tid)
       COPY (SELECT * FROM source.table WHERE ctid >= '(17775,0)'::tid and ctid < '(23698,0)'::tid)
       COPY (SELECT * FROM source.table WHERE ctid >= '(23698,0)'::tid)


  - To decide if a table COPY processing should be split, the command line
    option ``split-tables-larger-than`` is used, or the environment variable
    ``PGCOPYDB_SPLIT_TABLES_LARGER_THAN``.

    The expected value is either a plain number of bytes, or a
    pretty-printed number of bytes such as ``250 GB``.

    When using this option, then tables that have at least this amount of
    data and also a candidate key for the COPY partitioning are then
    distributed among a number of COPY processes.

    The number of COPY processes is computed by dividing the table size by
    the threshold set with the split option. For example, if the threshold
    is 250 GB then a 400 GB table is going to be distributed among 2 COPY
    processes.

    The command :ref:`pgcopydb_list_table_parts` may be used to list the
    COPY partitioning that pgcopydb computes given a source table and a
    threshold.

Significant differences when using same-table COPY concurrency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When same-table concurrency happens for a source table, some operations are
not implemented as they would have been without same-table concurrency.
Specifically:

  - TRUNCATE and COPY FREEZE Postgres optimisation

    When using a single COPY process, it's then possible to TRUNCATE the
    target table in the same transaction as the COPY command, as in the
    following syntethic example:

    ::

       BEGIN;
       TRUNCATE table ONLY;
       COPY table FROM stdin WITH (FREEZE);
       COMMIT

    This technique allows Postgres to implement several optimisations, doing
    work during the COPY that would otherwise need to happen later when
    executing the first queries on the table.

    When using same-table concurrency then we have several transactions
    happening concurrently on the target system that are copying data from
    the source table. This means that we have to TRUNCATE separately and the
    FREEZE option can not be used.

  - CREATE INDEX and VACUUM

    Even when same-table COPY concurrency is enabled, creating the indexes
    on the target system only happens after the whole data set has been
    copied over. This means that only when the last process is done with
    the COPYing then this process will take care of the indexes and the
    *vacuum analyze* operation.

Same-table COPY concurrency performance limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, it might be that same-table concurrency is not effective at all in
some use cases. Here is a list of limitations to have in mind when selecting
to use this feature:

  - Network Bandwidth

    The most common performance bottleneck relevant to database migrations
    is the network bandwidth. When the bandwidth is saturated (used in full)
    then same-table concurrency will provide no performance benefits.

  - Disks IOPS

    The second most command performance bottleneck relevant to database
    migrations is disks IOPS and, in the Cloud, burst capacity. When the
    disk bandwidth is used in full, then same-table concurrency will provide
    no performance benefits.

    Source database system uses read IOPS, target database system uses both
    read and write IOPS (copying the data writes to disk, creating the
    indexes both read table data from disk and then write index data to
    disk).

  - On-disk data organisation

    When using a single COPY process, the target system may fill-in the
    Postgres table in a clustered way, using each disk page in full before
    opening the next one, in a sequential fashion.

    When using same-table COPY concurrency, then the target Postgres system
    needs to handle concurrent writes to the same table, resulting in a
    possibly less effective disk usage.

    How that may impact your application performance is to be tested.

  - synchronize_seqscans

    Postgres implemented this option back in version 8.3. The option is now
    documented in the `Version and Platform Compatibility`__ section.

    __ https://www.postgresql.org/docs/current/runtime-config-compatible.html

    The documentation reads:

    .. epigraph::

        This allows sequential scans of large tables to synchronize with
        each other, so that concurrent scans read the same block at about
        the same time and hence share the I/O workload.

    The impact on performance when having concurrent COPY processes reading
    the same source table at the same time is to be assessed. At the moment
    there is no option in pgcopydb to `SET synchronize_seqscans TO off` when
    using same-table COPY concurrency.

    Use your usual Postgres configuration editing for testing.

