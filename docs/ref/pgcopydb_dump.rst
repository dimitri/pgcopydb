.. _pgcopydb_dump:

pgcopydb dump
=============

pgcopydb dump - Dump database objects from a Postgres instance

This command prefixes the following sub-commands:

::

   pgcopydb dump
     schema     Dump source database schema as custom files in target directory
     pre-data   Dump source database pre-data schema as custom files in target directory
     post-data  Dump source database post-data schema as custom files in target directory


.. _pgcopydb_dump_schema:

pgcopydb dump schema
--------------------

pgcopydb dump schema - Dump source database schema as custom files in target directory

The command ``pgcopydb dump schema`` uses pg_dump to export SQL schema
definitions from the given source Postgres instance.

::

   pgcopydb dump schema: Dump source database schema as custom files in target directory
   usage: pgcopydb dump schema  --source <URI> --target <dir>

     --source          Postgres URI to the source database
     --target          Directory where to save the dump files

.. _pgcopydb_dump_pre_data:

pgcopydb dump pre-data
----------------------

pgcopydb dump pre-data - Dump source database pre-data schema as custom files in target directory

The command ``pgcopydb dump pre-data`` uses pg_dump to export SQL schema
*pre-data* definitions from the given source Postgres instance.

::

   pgcopydb dump pre-data: Dump source database pre-data schema as custom files in target directory
   usage: pgcopydb dump schema  --source <URI> --target <dir>

     --source          Postgres URI to the source database
     --target          Directory where to save the dump files

.. _pgcopydb_dump_post_data:

pgcopydb dump post-data
-----------------------

pgcopydb dump post-data - Dump source database post-data schema as custom files in target directory

The command ``pgcopydb dump post-data`` uses pg_dump to export SQL schema
*post-data* definitions from the given source Postgres instance.

::

   pgcopydb dump post-data: Dump source database post-data schema as custom files in target directory
   usage: pgcopydb dump schema  --source <URI> --target <dir>

     --source          Postgres URI to the source database
     --target          Directory where to save the dump files


Description
-----------

The ``pgcopydb dump schema`` command implements the first step of the full
database migration and fetches the schema definitions from the source
database.

When the command runs, it calls ``pg_dump`` to get first the pre-data schema
output in a Postgres custom file, and then again to get the post-data schema
output in another Postgres custom file.

The output files are written to the ``schema`` sub-directory of the
``--target`` directory.

The ``pgcopydb dump pre-data`` and ``pgcopydb dump post-data`` are limiting
their action to respectively the pre-data and the post-data sections of the
pg_dump.

Options
-------

The following options are available to ``pgcopydb dump schema``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--target

  Target directory where to write output and temporary files.

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.

Examples
--------

First, using ``pgcopydb dump schema``

::

   $ pgcopydb dump schema --source "port=5501 dbname=demo" --target /tmp/target
   09:35:21 3926 INFO  Dumping database from "port=5501 dbname=demo"
   09:35:21 3926 INFO  Dumping database into directory "/tmp/target"
   09:35:21 3926 INFO  Found a stale pidfile at "/tmp/target/pgcopydb.pid"
   09:35:21 3926 WARN  Removing the stale pid file "/tmp/target/pgcopydb.pid"
   09:35:21 3926 INFO  Using pg_dump for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_dump"
   09:35:21 3926 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section pre-data --file /tmp/target/schema/pre.dump 'port=5501 dbname=demo'
   09:35:22 3926 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section post-data --file /tmp/target/schema/post.dump 'port=5501 dbname=demo'


Once the previous command is finished, the pg_dump output files can be found
in ``/tmp/target/schema`` and are named ``pre.dump`` and ``post.dump``.
Other files and directories have been created.

::

   $ find /tmp/target
   /tmp/target
   /tmp/target/pgcopydb.pid
   /tmp/target/schema
   /tmp/target/schema/post.dump
   /tmp/target/schema/pre.dump
   /tmp/target/run
   /tmp/target/run/tables
   /tmp/target/run/indexes

Then we have almost the same thing when using the other forms.

We can see that ``pgcopydb dump pre-data`` only does the pre-data section of
the dump.

::

   $ pgcopydb dump pre-data --source "port=5501 dbname=demo" --target /tmp/target
   09:35:21 3926 INFO  Dumping database from "port=5501 dbname=demo"
   09:35:21 3926 INFO  Dumping database into directory "/tmp/target"
   09:35:21 3926 INFO  Found a stale pidfile at "/tmp/target/pgcopydb.pid"
   09:35:21 3926 WARN  Removing the stale pid file "/tmp/target/pgcopydb.pid"
   09:35:21 3926 INFO  Using pg_dump for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_dump"
   09:35:21 3926 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section pre-data --file /tmp/target/schema/pre.dump 'port=5501 dbname=demo'

And then ``pgcopydb dump post-data`` only does the post-data section of the
dump.

::

   $ pgcopydb dump post-data --source "port=5501 dbname=demo" --target /tmp/target
   09:35:21 3926 INFO  Dumping database from "port=5501 dbname=demo"
   09:35:21 3926 INFO  Dumping database into directory "/tmp/target"
   09:35:21 3926 INFO  Found a stale pidfile at "/tmp/target/pgcopydb.pid"
   09:35:21 3926 WARN  Removing the stale pid file "/tmp/target/pgcopydb.pid"
   09:35:21 3926 INFO  Using pg_dump for Postgres "12.9" at "/Applications/Postgres.app/Contents/Versions/12/bin/pg_dump"
   09:35:21 3926 INFO   /Applications/Postgres.app/Contents/Versions/12/bin/pg_dump -Fc --section post-data --file /tmp/target/schema/post.dump 'port=5501 dbname=demo'
