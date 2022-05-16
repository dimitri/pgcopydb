Introduction to pgcopydb
========================

pgcopydb is a tool that automates running ``pg_dump -jN | pg_restore -jN``
between two running Postgres servers. To make a copy of a database to
another server as quickly as possible, one would like to use the parallel
options of ``pg_dump`` and still be able to stream the data to as many
``pg_restore`` jobs.

When using ``pgcopydb`` it is possible to achieve the result outlined before
with this simple command line::

  $ export PGCOPYDB_SOURCE_PGURI="postgres://user@source.host.dev/dbname"
  $ export PGCOPYDB_TARGET_PGURI="postgres://role@target.host.dev/dbname"

  $ pgcopydb copy-db --table-jobs 4 --index-jobs 4

How to copy a Postgres database
-------------------------------

Then pgcopydb implements the following steps:

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

Notes about concurrency
-----------------------

In the previous steps list, the idea of executing some of the tasks
concurrently to one another is introduced. The concurrency is implemented by
ways of using the ``fork()`` system call, so pgcopydb creates sub-processes
that each handle a part of the work.

The process tree then looks like the following:

  - main process
	  - per-table COPY DATA process
		  - per-index CREATE INDEX process
		  - another index
		  - a third one on the same table
	  - another table to COPY DATA from source to target
		  - with another index

When starting with the TABLE DATA copying step, then pgcopydb creates as
many sub-processes as specified by the ``--table-jobs`` command line option
(or the environment variable ``PGCOPYDB_TARGET_TABLE_JOBS``).

Then as soon as the COPY command is done, another sub-process can be
created. At this time in the process, pgcopydb might be running more
sub-processes than has been setup. The setup limits how many of those
sub-processes are concurrently executing a COPY command.

The process that's implementing the COPY command now turns its attention to
the building of the indexes attached to the given table. That's because the
CREATE INDEX command only consumes resources (CPU, memory, etc) on the
target Postgres instance server, the pgcopydb process just sends the command
and wait until completion.

It is possible with Postgres to create several indexes for the same table in
parallel, for that, the client just needs to open a separate database
connection for each index and run each CREATE INDEX command in its own
connection, at the same time. In pgcopydb this is implemented by running one
sub-process per index to create.

The command line option ``--index-jobs`` is used to limit how many CREATE
INDEX commands are running at any given time --- by using a Unix semaphore.
So when running with ``--index-jobs 2`` and when a specific table has 3
indexes attached to it, then the 3rd index creation is blocked until another
index is finished.

Postgres introduced the configuration parameter `synchronize_seqscans`__ in
version 8.3, eons ago. It is on by default and allows the following
behavior:

__ https://postgresqlco.nf/doc/en/param/synchronize_seqscans/

  This allows sequential scans of large tables to synchronize with each
  other, so that concurrent scans read the same block at about the same time
  and hence share the I/O workload.

That's why pgcopydb takes the extra step and makes sure to create all your
indexes in parallel to one-another, going the extra mile when it comes to
indexes that are associated with a constraint, as detailed in our section
:ref:`index_concurrency`.

That said, the index jobs setup is global for the whole pgcopydb operation
rather than per-table. It means that in some cases, indexes for the same
table might be created in a sequential fashion, depending on exact timing of
the other index builds.

The ``--index-jobs`` option has been made global so that it's easier to
setup to the count of available CPU cores on the target Postgres instance.
Usually, a given CREATE INDEX command uses 100% of a single core.
