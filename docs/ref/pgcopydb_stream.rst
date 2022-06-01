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

  
