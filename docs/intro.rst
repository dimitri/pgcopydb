Introduction to pgcopydb
========================

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

Feature Matrix
--------------

Here is a comparison of the features available when using ``pg_dump`` and
``pg_restore`` directly versus when using ``pgcopydb`` to handle the database copying:

==============================   ========  =====================
Feature                          pgcopydb   pg_dump ; pg_restore
==============================   ========  =====================
Single-command operation          ✓         ✗
Snapshot consistency              ✓         ✓
Ability to resume partial run     ✓         ✗
Advanced filtering                ✓         ✓
Tables concurrency                ✓         ✓
Same-table concurrency            ✓         ✗
Index concurrency                 ✓         ✓
Constraint index concurrency      ✓         ✗
Schema                            ✓         ✓
Large Objects                     ✓         ✓
Vacuum Analyze                    ✓         ✗
Copy Freeze                       ✓         ✗
Roles                             ✓         ✗ (needs pg_dumpall)
Tablespaces                       ✗         ✗ (needs pg_dumpall)
Follow changes                    ✓         ✗
==============================   ========  =====================

Refer to the documentation about :ref:`config` for its *Advanced filtering*
capabilities.

pgcopydb uses pg_dump and pg_restore
------------------------------------

The implementation of ``pgcopydb`` actually calls into the ``pg_dump`` and
``pg_restore`` binaries to handle a large part of the work, such as the pre-data
and post-data sections. Refer to `pg_dump docs`__ for more information about the
three sections supported.

__ https://www.postgresql.org/docs/current/app-pgdump.html

After using ``pg_dump`` to obtain the pre-data and the post-data parts, then
``pgcopydb`` restores the pre-data parts to the target Postgres instance using
``pg_restore``.

``pgcopydb`` then uses SQL commands and the `COPY streaming protocol`__ to
migrate the table contents, the large objects data, and to VACUUM ANALYZE
tables as soon as the data becomes available on the target instance.

__ https://www.postgresql.org/docs/current/sql-copy.html

Then ``pgcopydb`` uses SQL commands to build the indexes on the target Postgres
instance, as detailed in the design doc :ref:`index_concurrency`. This
allows to include *constraint indexes* such as Primary Keys in the list of
indexes built at the same time.

Change Data Capture, or fork and follow
---------------------------------------

It is also possible with ``pgcopydb`` to implement Change Data Capture and
replay data modifications happening on the source database to the target
database. See the :ref:`pgcopydb_follow` command and the ``pgcopydb clone
--follow`` command line option at :ref:`pgcopydb_clone` in the manual.

The simplest possible implementation of *online migration* with pgcopydb,
where changes being made to the source Postgres instance database are
replayed on the target system, looks like the following:

.. code-block:: bash
  :linenos:

   $ pgcopydb clone --follow &

   # later when the application is ready to make the switch
   $ pgcopydb stream sentinel set endpos --current

   # later when the migration is finished, clean-up both source and target
   $ pgcopydb stream cleanup
