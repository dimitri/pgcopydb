.. _pgcopydb:

pgcopydb
=========

pgcopydb - copy an entire Postgres database from source to target

Synopsis
--------

pgcopydb provides the following commands

.. include:: ../include/pgcopydb.rst

Description
-----------

The pgcopydb command implements a full migration of an entire Postgres
database from a source instance to a target instance. Both the Postgres
instances must be available for the entire duration of the command.

The pgcopydb command also implements a full `Logical Decoding`__ client for
Postgres, allowing Change Data Capture to replay data changes (DML)
happening on the source database after the base copy snapshot. The pgcopydb
logical decoding client code is compatible with both `test_decoding`__ and
`wal2json`__ output plugins, and defaults to using test_decoding.

__ https://www.postgresql.org/docs/current/logicaldecoding.html
__ https://www.postgresql.org/docs/current/test-decoding.html
__ https://github.com/eulerto/wal2json/

pgcopydb help
-------------

The ``pgcopydb help`` command lists all the supported sub-commands:

.. include:: ../include/help.rst

pgcopydb version
----------------

The ``pgcopydb version`` command outputs the version string of the version
of pgcopydb used, and can do that in the JSON format when using the
``--json`` option.

::

   $ pgcopydb version
   pgcopydb version 0.13.1.g868ad77
   compiled with PostgreSQL 13.11 (Debian 13.11-0+deb11u1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
   compatible with Postgres 10, 11, 12, 13, 14, and 15

In JSON:

::

   $ pgcopydb version --json
   {
       "pgcopydb": "0.13.1.g868ad77",
       "pg_major": "13",
       "pg_version": "13.11 (Debian 13.11-0+deb11u1)",
       "pg_version_str": "PostgreSQL 13.11 (Debian 13.11-0+deb11u1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit",
       "pg_version_num": 130011
   }

The details about the Postgres version applies to the version that's been
used to build pgcopydb from sources, so that's the version of the client
library ``libpq`` really.


pgcopydb ping
-------------

The ``pgcopydb ping`` command attempts to connect to both the source and the
target Postgres databases, concurrently.

.. include:: ../include/ping.rst

An example output looks like the following:

::

   $ pgcopydb ping
   18:04:48 84679 INFO   Running pgcopydb version 0.10.31.g7e5fbb8.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb"
   18:04:48 84683 INFO   Successfully could connect to target database at "postgres://@:/plop?"
   18:04:48 84682 INFO   Successfully could connect to source database at "postgres://@:/pagila?"

This command implements a retry policy (named *Decorrelated Jitter*) and can
be used in automation to make sure that the databases are ready to accept
connections.
