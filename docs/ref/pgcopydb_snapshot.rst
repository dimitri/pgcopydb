.. _pgcopydb_snapshot:

pgcopydb snapshot
=================

pgcopydb snapshot - Create and export a snapshot on the source database

The command ``pgcopydb snapshot`` connects to the source database and
executes a SQL query to export a snapshot. The obtained snapshot is both
printed on stdout and also in a file where other pgcopydb commands might
expect to find it.

.. include:: ../include/snapshot.rst

Options
-------

The following options are available to ``pgcopydb snapshot``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--dir

  During its normal operations pgcopydb creates a lot of temporary files to
  track sub-processes progress. Temporary files are created in the directory
  specified by this option, or defaults to
  ``${TMPDIR}/pgcopydb`` when the environment variable is set, or
  otherwise to ``/tmp/pgcopydb``.

--follow

  When the ``--follow`` option is used then pgcopydb implements Change Data
  Capture as detailed in the manual page for :ref:`pgcopydb_follow` in
  parallel to the main copy database steps.

  The replication slot is created using the Postgres replication protocol
  command CREATE_REPLICATION_SLOT, which then exports the snapshot being
  used in that command.

--plugin

  Logical decoding output plugin to use. The default is `test_decoding`__
  which ships with Postgres core itself, so is probably already available on
  your source server.

  It is possible to use `wal2json`__ instead. The support for wal2json is
  mostly historical in pgcopydb, it should not make a user visible
  difference whether you use the default test_decoding or wal2json.

  __ https://www.postgresql.org/docs/current/test-decoding.html
  __ https://github.com/eulerto/wal2json/

--wal2json-numeric-as-string

  When using the wal2json output plugin, it is possible to use the
  ``--wal2json-numeric-as-string`` option to instruct wal2json to output
  numeric values as strings and thus prevent some precision loss.

  You need to have a wal2json plugin version on source database that supports
  ``--numeric-data-types-as-string`` option to use this option.

  See also the documentation for `wal2json`__ regarding this option for details.

  __ https://github.com/eulerto/wal2json/pull/255

--slot-name

  Logical decoding slot name to use.

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

PGCOPYDB_OUTPUT_PLUGIN

  Logical decoding output plugin to use. When ``--plugin`` is omitted from the
  command line, then this environment variable is used.

PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING

  When true (or *yes*, or *on*, or 1, same input as a Postgres boolean)
  then pgcopydb uses the wal2json option ``--numeric-data-types-as-string``
  when using the wal2json output plugin.

  When ``--wal2json-numeric-as-string`` is ommitted from the command line
  then this environment variable is used.

Examples
--------

Create a snapshot on the source database in the background:

::

   $ pgcopydb snapshot &
   [1] 72938
   17:31:52 72938 INFO  Running pgcopydb version 0.7.13.gcbf2d16.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/./src/bin/pgcopydb/pgcopydb"
   17:31:52 72938 INFO  Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   17:31:52 72938 INFO  Removing the stale pid file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/pgcopydb.aux.pid"
   17:31:52 72938 INFO  Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   17:31:52 72938 INFO  Exported snapshot "00000003-000CB5FE-1" from the source database
   00000003-000CB5FE-1

And when the process is done, stop maintaining the snapshot in the
background:

::

   $ kill %1
   17:31:56 72938 INFO  Asked to terminate, aborting
   [1]+  Done                    pgcopydb snapshot
