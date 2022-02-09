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
     --slot-name      Stream changes recorded by this slot

Options
-------

The following options are available to ``pgcopydb stream receive``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--slot-name

  Logical replication slot to use. At the moment pgcopydb doesn't know how
  to create the logical replication slot itself. The slot should be created
  within the same transaction snapshot as the initial data copy.

  Must be using the `wal2json`__ output plugin, available with
  format-version 2.

  __ https://github.com/eulerto/wal2json/

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.
