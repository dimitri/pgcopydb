.. _pgcopydb_dump_pre_data:

pgcopydb dump pre-data
======================

pgcopydb dump pre-data - Dump source database pre-data schema as custom files in target directory

Synopsis
--------

The command ``pgcopydb dump pre-data`` uses pg_dump to export SQL schema
*pre-data* definitions from the given source Postgres instance.

::

   pgcopydb dump pre-data: Dump source database pre-data schema as custom files in target directory
   usage: pgcopydb dump schema  --source <URI> --target <dir>

     --source          Postgres URI to the source database
     --target          Directory where to save the dump files


Description
-----------



Options
-------

The following options are available to ``pgcopydb dump pre-data``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--target

  Connection string to the target Postgres instance.

Environment
-----------

PGCOPYDB_SOURCE_PGURI

  Connection string to the source Postgres instance. When ``--source`` is
  ommitted from the command line, then this environment variable is used.

PGCOPYDB_TARGET_PGURI

  Connection string to the target Postgres instance. When ``--target`` is
  ommitted from the command line, then this environment variable is used.
