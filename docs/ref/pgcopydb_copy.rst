.. _pgcopydb_copy:

pgcopydb copy
=============

pgcopydb copy - Implement the data section of the database copy

This command prefixes the following sub-commands:

.. include:: ../include/copy.rst

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

.. include:: ../include/copy-db.rst

.. _pgcopydb_copy_roles:

pgcopydb copy roles
-------------------

pgcopydb copy roles - Copy the roles from the source instance to the target instance

The command ``pgcopydb copy roles`` implements both
:ref:`pgcopydb_dump_roles` and then :ref:`pgcopydb_restore_roles`.

.. include:: ../include/copy-roles.rst

.. note::

   In Postgres, roles are a global object. This means roles do not belong to
   any specific database, and as a result, even when the ``pgcopydb`` tool
   otherwise works only in the context of a specific database, this command
   is not limited to roles that are used within a single database.

When a role already exists on the target database, its restoring is entirely
skipped, which includes skipping both the ``CREATE ROLE`` and the ``ALTER
ROLE`` commands produced by ``pg_dumpall --roles-only``.

The ``pg_dumpall --roles-only`` is used to fetch the list of roles from the
source database, and this command includes support for passwords. As a
result, this operation requires the superuser privileges.

.. _pgcopydb_copy_extensions:

pgcopydb copy extensions
------------------------

pgcopydb copy extensions - Copy the extensions from the source instance to the target instance

The command ``pgcopydb copy extensions`` gets a list of the extensions
installed on the source database, and for each of them run the SQL command
CREATE EXTENSION IF NOT EXISTS.

.. include:: ../include/copy-extensions.rst

When copying extensions, this command also takes care of copying any
`Extension Configuration Tables`__ user-data to the target database.

__ https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES

.. _pgcopydb_copy_schema:

pgcopydb copy schema
--------------------

pgcopydb copy schema - Copy the database schema from source to target

The command ``pgcopydb copy schema`` implements the schema only section of
the clone steps.

.. include:: ../include/copy-schema.rst

.. _pgcopydb_copy_data:

pgcopydb copy data
------------------

pgcopydb copy data - Copy the data section from source to target

The command ``pgcopydb copy data`` implements the data section of the clone
steps.

.. include:: ../include/copy-data.rst

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

.. include:: ../include/copy-table-data.rst

.. _pgcopydb_copy_blobs:

pgcopydb copy blobs
-------------------

pgcopydb copy blobs - Copy the blob data from the source database to the target

The command ``pgcopydb copy blobs`` fetches list of large objects (aka
blobs) from the source database and copies their data parts to the target
database. By default the command assumes that the large objects metadata
have already been taken care of, because of the behaviour of
``pg_dump --section=pre-data``.

.. include:: ../include/copy-blobs.rst

.. _pgcopydb_copy_sequences:

pgcopydb copy sequences
-----------------------

pgcopydb copy sequences - Copy the current value from all sequences in database from source to target

The command ``pgcopydb copy sequences`` fetches the list of sequences from
the source database, then for each sequence fetches the ``last_value`` and
``is_called`` properties the same way pg_dump would on the source database,
and then for each sequence call ``pg_catalog.setval()`` on the target
database.

.. include:: ../include/copy-sequences.rst

.. _pgcopydb_copy_indexes:

pgcopydb copy indexes
---------------------

pgcopydb copy indexes - Create all the indexes found in the source database in the target

The command ``pgcopydb copy indexes`` fetches the list of indexes from the
source database and runs each index CREATE INDEX statement on the target
database. The statements for the index definitions are modified to include
IF NOT EXISTS and allow for skipping indexes that already exist on the
target database.

.. include:: ../include/copy-indexes.rst

.. _pgcopydb_copy_constraints:

pgcopydb copy constraints
-------------------------

pgcopydb copy constraints - Create all the constraints found in the source database in the target

The command ``pgcopydb copy constraints`` fetches the list of indexes from
the source database and runs each index ALTER TABLE ... ADD CONSTRAINT ...
USING INDEX statement on the target database.

The indexes must already exist, and the command will fail if any constraint
is found existing already on the target database.

.. include:: ../include/copy-constraints.rst

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
  specified by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  otherwise to ``/tmp/pgcopydb``.

--no-role-passwords

  Do not dump passwords for roles. When restored, roles will have a null
  password, and password authentication will always fail until the password
  is set. Since password values aren't needed when this option is specified,
  the role information is read from the catalog view pg_roles instead of
  pg_authid. Therefore, this option also helps if access to pg_authid is
  restricted by some security policy.

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

--large-object-jobs

  How many worker processes to start to copy Large Objects concurrently.

--split-tables-larger-than

   Allow :ref:`same_table_concurrency` when processing the source database.
   This environment variable value is expected to be a byte size, and bytes
   units B, kB, MB, GB, TB, PB, and EB are known.

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

--use-copy-binary

  Use the COPY WITH (FORMAT BINARY) instead of the COPY command. 

  See also documentation for `COPY`__.

  __ https://www.postgresql.org/docs/current/sql-copy.html

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

PGCOPYDB_TABLE_JOBS

   Number of concurrent jobs allowed to run COPY operations in parallel.
   When ``--table-jobs`` is ommitted from the command line, then this
   environment variable is used.

PGCOPYDB_INDEX_JOBS

   Number of concurrent jobs allowed to run CREATE INDEX operations in
   parallel. When ``--index-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_RESTORE_JOBS

   Number of concurrent jobs allowed to run `pg_restore` operations in
   parallel. When ``--restore-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_LARGE_OBJECTS_JOBS

   Number of concurrent jobs allowed to copy Large Objects data in parallel.
   When ``--large-objects-jobs`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_SPLIT_TABLES_LARGER_THAN

   Allow :ref:`same_table_concurrency` when processing the source database.
   This environment variable value is expected to be a byte size, and bytes
   units B, kB, MB, GB, TB, PB, and EB are known.

   When ``--split-tables-larger-than`` is ommitted from the command line,
   then this environment variable is used.

PGCOPYDB_SPLIT_MAX_PARTS

   Limit the maximum number of parts when :ref:`same_table_concurrency` is
   used. When ``--split-max-parts`` is ommitted from the command line, then this
   environment variable is used.

PGCOPYDB_ESTIMATE_TABLE_SIZES

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb estimates the size of tables to determine whether or not to
   split tables. This option is only useful when querying the relation sizes on
   source database is costly.

   When ``--estimate-table-sizes`` is ommitted from the command line, then
   this environment variable is used.

PGCOPYDB_DROP_IF_EXISTS

   When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
   then pgcopydb uses the pg_restore options ``--clean --if-exists`` when
   creating the schema on the target Postgres instance.

PGCOPYDB_SNAPSHOT

  Postgres snapshot identifier to re-use, see also ``--snapshot``.

PGCOPYDB_USE_COPY_BINARY

  When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
  then pgcopydb uses the COPY WITH (FORMAT BINARY) instead of the COPY
  command, same as when using the ``--use-copy-binary`` option.

TMPDIR

  The pgcopydb command creates all its work files and directories in
  ``${TMPDIR}/pgcopydb``, and defaults to ``/tmp/pgcopydb``.

Examples
--------

Let's export the Postgres databases connection strings to make it easy to
re-use them all along:

::

   $ export PGCOPYDB_SOURCE_PGURI=postgres://pagila:0wn3d@source/pagila
   $ export PGCOPYDB_TARGET_PGURI=postgres://pagila:0wn3d@target/pagila

Now, first dump the schema:

::

   $ pgcopydb dump schema
   08:27:48.633 44371 INFO   Using work dir "/tmp/pgcopydb"
   08:27:48.634 44371 INFO   Dumping database from "postgres://pagila:0wn3d@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   08:27:48.634 44371 INFO   Dumping database into directory "/tmp/pgcopydb"
   08:27:48.634 44371 INFO   Using pg_dump for Postgres "16.2" at "/usr/bin/pg_dump"
   08:27:48.971 44371 INFO   Fetched information for 5 tables (including 0 tables split in 0 partitions total), with an estimated total of 1000 thousands tuples and 128 MB on-disk
   08:27:48.978 44371 INFO   Fetched information for 4 indexes (supporting 4 constraints)
   08:27:48.983 44371 INFO   Fetching information for 1 sequences
   08:27:48.996 44371 INFO   Fetched information for 1 extensions
   08:27:49.072 44371 INFO   Found 5 indexes (supporting 5 constraints) in the target database
   08:27:49.078 44371 INFO    /usr/bin/pg_dump -Fc --section=pre-data --section=post-data --file /tmp/pgcopydb/schema/schema.dump 'postgres://pagila:0wn3d@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60'

Now restore the pre-data schema on the target database, cleaning up the
already existing objects if any, which allows running this test scenario
again and again. It might not be what you want to do in your production
target instance though!

::

   $ PGCOPYDB_DROP_IF_EXISTS=on pgcopydb restore pre-data --no-owner --resume --not-consistent
   08:30:13.621 44597 INFO   Using work dir "/tmp/pgcopydb"
   08:30:13.621 44597 INFO   Restoring database from existing files at "/tmp/pgcopydb"
   08:30:13.706 44597 INFO   Found 5 indexes (supporting 5 constraints) in the target database
   08:30:13.710 44597 INFO   Using pg_restore for Postgres "16.2" at "/usr/bin/pg_restore"
   08:30:13.711 44597 INFO   [TARGET] Restoring database into "postgres://pagila:0wn3d@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   08:30:13.730 44597 INFO   Drop tables on the target database, per --drop-if-exists
   08:30:13.774 44597 INFO    /usr/bin/pg_restore --dbname 'postgres://pagila:0wn3d@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60' --section pre-data --jobs 4 --clean --if-exists --no-owner --use-list /tmp/pgcopydb/schema/pre-filtered.list /tmp/pgcopydb/schema/schema.dump

Then copy the data over:

::

   $ pgcopydb copy table-data --resume --not-consistent
   08:34:02.813 44834 INFO   [SOURCE] Copying database from "postgres://pagila:0wn3d@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   08:34:02.813 44834 INFO   [TARGET] Copying database into "postgres://pagila:0wn3d@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   08:34:02.861 44834 INFO   Using work dir "/tmp/pgcopydb"
   08:34:02.862 44834 INFO   Copy data from source to target in sub-processes
   08:34:02.863 44834 INFO   Re-using catalog caches
   08:34:02.863 44834 INFO   STEP 4: starting 4 table-data COPY processes

And now create the indexes on the target database, using the index
definitions from the source database:

::

   $ pgcopydb copy indexes --resume --not-consistent
   14:28:53 47 INFO   Running pgcopydb version 0.13.38.g22e6544.dirty from "/usr/local/bin/pgcopydb"
   14:28:53 47 INFO   [SOURCE] Copying database from "postgres://pagila@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:28:53 47 INFO   [TARGET] Copying database into "postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:28:53 47 INFO   Schema dump for pre-data and post-data section have been done
   14:28:53 47 INFO   Pre-data schema has been restored on the target instance
   14:28:53 47 INFO   All the table data has been copied to the target instance
   14:28:53 47 INFO   All the indexes have been copied to the target instance
   14:28:53 47 INFO   Fetched information for 54 indexes
   14:28:53 47 INFO   Creating 54 indexes in the target database using 4 processes

                                                  Step   Connection    Duration    Transfer   Concurrency
    --------------------------------------------------   ----------  ----------  ----------  ------------
                                           Dump Schema       source         0ms                         1
      Catalog Queries (table ordering, filtering, etc)       source         0ms                         1
                                        Prepare Schema       target         0ms                         1
         COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both         0ms                     4 + 8
                                     COPY (cumulative)         both         0ms         0 B             4
                            Large Objects (cumulative)         both                                     4
                CREATE INDEX, CONSTRAINTS (cumulative)       target         0ms                         4
                                       Finalize Schema       target         0ms                         1
    --------------------------------------------------   ----------  ----------  ----------  ------------
                             Total Wall Clock Duration         both       696ms                     4 + 8
    --------------------------------------------------   ----------  ----------  ----------  ------------


Now re-create the constraints (primary key, unique constraints) from the
source database schema into the target database:

::

   $ pgcopydb copy constraints --resume --not-consistent
   14:28:54 53 INFO   Running pgcopydb version 0.13.38.g22e6544.dirty from "/usr/local/bin/pgcopydb"
   14:28:54 53 INFO   [SOURCE] Copying database from "postgres://pagila@source/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:28:54 53 INFO   [TARGET] Copying database into "postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:28:54 53 INFO   Schema dump for pre-data and post-data section have been done
   14:28:54 53 INFO   Pre-data schema has been restored on the target instance
   14:28:54 53 INFO   All the table data has been copied to the target instance
   14:28:54 53 INFO   All the indexes have been copied to the target instance
   14:28:54 53 INFO   Create constraints
   14:28:54 53 INFO   Fetched information for 54 indexes
   14:28:54 53 INFO   Creating 54 indexes in the target database using 4 processes

                                                  Step   Connection    Duration    Transfer   Concurrency
    --------------------------------------------------   ----------  ----------  ----------  ------------
                                           Dump Schema       source         0ms                         1
      Catalog Queries (table ordering, filtering, etc)       source         0ms                         1
                                        Prepare Schema       target         0ms                         1
         COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both         0ms                     4 + 8
                                     COPY (cumulative)         both         0ms         0 B             4
                            Large Objects (cumulative)         both                                     4
                CREATE INDEX, CONSTRAINTS (cumulative)       target         0ms                         4
                                       Finalize Schema       target         0ms                         1
    --------------------------------------------------   ----------  ----------  ----------  ------------
                             Total Wall Clock Duration         both       283ms                     4 + 8
    --------------------------------------------------   ----------  ----------  ----------  ------------


The next step is a VACUUM ANALYZE on each table that's been just filled-in
with the data, and for that we can just use the `vacuumdb`__ command from
Postgres:

__ https://www.postgresql.org/docs/current/app-vacuumdb.html

::

   $ vacuumdb --analyze --dbname "$PGCOPYDB_TARGET_PGURI" --jobs 4
   vacuumdb: vacuuming database "pagila"

Finally we can restore the post-data section of the schema:

::

   $ pgcopydb restore post-data --resume --not-consistent
   14:28:54 60 INFO   Running pgcopydb version 0.13.38.g22e6544.dirty from "/usr/local/bin/pgcopydb"
   14:28:54 60 INFO   Schema dump for pre-data and post-data section have been done
   14:28:54 60 INFO   Pre-data schema has been restored on the target instance
   14:28:54 60 INFO   All the table data has been copied to the target instance
   14:28:54 60 INFO   All the indexes have been copied to the target instance
   14:28:54 60 INFO   Restoring database from existing files at "/tmp/pgcopydb"
   14:28:54 60 INFO   Using pg_restore for Postgres "16.1" at "/usr/bin/pg_restore"
   14:28:54 60 INFO   [TARGET] Restoring database into "postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
   14:28:55 60 INFO    /usr/bin/pg_restore --dbname 'postgres://pagila@target/pagila?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60' --single-transaction --use-list /tmp/pgcopydb/schema/post-filtered.list /tmp/pgcopydb/schema/schema.dump
