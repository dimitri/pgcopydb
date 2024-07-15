Welcome to pgcopydb's documentation!
====================================

The `pgcopydb`__ project is an Open Source Software project. The development
happens at `https://github.com/dimitri/pgcopydb`__ and is public: everyone
is welcome to participate by opening issues, pull requests, giving feedback,
etc.

Remember that the first steps are to actually play with the ``pgcopydb``
command, then read the entire available documentation (after all, I took the
time to write it), and then to address the community in a kind and polite
way — the same way you would expect people to use when addressing you.

__ https://github.com/dimitri/pgcopydb
__ https://github.com/dimitri/pgcopydb


How to copy a Postgres database
-------------------------------

pgcopydb is a tool that automates copying a PostgreSQL database to another
server. Main use case for pgcopydb is migration to a new Postgres system,
either for new hardware, new architecture, or new Postgres major version.

The idea would be to run ``pg_dump -jN | pg_restore -jN`` between two
running Postgres servers. To make a copy of a database to another server as
quickly as possible, one would like to use the parallel options of
``pg_dump`` and still be able to stream the data to as many ``pg_restore``
jobs. Unfortunately, this approach cannot be implemented by using ``pg_dump`` and
``pg_restore`` directly, see :ref:`bypass_intermediate_files`.

When using ``pgcopydb`` it is possible to achieve both concurrency and
streaming with this simple command line::

  $ export PGCOPYDB_SOURCE_PGURI="postgres://user@source.host.dev/dbname"
  $ export PGCOPYDB_TARGET_PGURI="postgres://role@target.host.dev/dbname"

  $ pgcopydb clone --table-jobs 4 --index-jobs 4

See the manual page for :ref:`pgcopydb_clone` for detailed information about
how the command is implemented along with many other supported options.

Main pgcopydb features
----------------------

Bypass intermediate files
    When using ``pg_dump`` and ``pg_restore`` with the ``-jobs`` option, the
    table data is first copied to files on-disk before being read again and
    sent to the target server. pgcopydb avoids those steps and instead
    streams the COPY buffers from the source to the target with zero
    processing.

Use COPY FREEZE
    Postgres has an optimization which reduces post-migration vacuum work by
    marking the imported rows as frozen already during the import, that's the
    FREEZE option to the VACUUM command. pgcopydb uses that option, unless
    when using same-table concurrency.

Create Index Concurrency
    When creating an index on a table, Postgres has to implement a full
    sequential scan to read all the rows. Implemented in Postgres 8.3 is the
    `synchronize_seqscans`__ optimization where a single such on-disk read
    is able to feed several SQL commands running concurrently in different
    client sessions.

    pgcopydb takes benefit of this feature by running many CREATE INDEX commands on
    the same table at the same time. This number is limited by the
    ``--index-jobs`` option.

    __ https://www.postgresql.org/docs/current/runtime-config-compatible.html#GUC-SYNCHRONIZE-SEQSCANS

Same Table Concurrency
    When migrating a very large table, it might be beneficial to *partition*
    the table and run several COPY commands, distributing the source data
    using a non-overlapping WHERE clause. pgcopydb implements that approach
    with the ``split-table-larger-than`` option.

Change Data Capture
    The simplest and safest way to migrate a database to a new Postgres
    server requires a maintenance window duration that's dependent on the
    size of the data to migrate.

    Sometimes the migration context needs to reduce that downtime window.
    For these advanced and complex cases, pgcopydb embeds a full replication
    solution using the Postgres Logical Decoding low-level APIs, available
    since Postgres 9.4.

    See the reference manual for the ``pgcopydb fork --follow`` command.

.. toctree::
   :hidden:
   :caption: Getting Started

   intro
   tutorial
   install

.. toctree::
   :hidden:
   :caption: Design Considerations

   features
   concurrency
   resume

.. toctree::
   :hidden:
   :caption: Reference Manual

   ref/pgcopydb
   ref/pgcopydb_clone
   ref/pgcopydb_follow
   ref/pgcopydb_snapshot
   ref/pgcopydb_compare
   ref/pgcopydb_copy
   ref/pgcopydb_dump
   ref/pgcopydb_restore
   ref/pgcopydb_list
   ref/pgcopydb_stream
   ref/pgcopydb_config
