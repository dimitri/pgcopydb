.. _pgcopydb_list:

pgcopydb list
=============

pgcopydb list - List database objects from a Postgres instance

This command prefixes the following sub-commands:

::

  pgcopydb list
    extensions   List all the source extensions to copy
    collations   List all the source collations to copy
    tables       List all the source tables to copy data from
    table-parts  List a source table copy partitions
    sequences    List all the source sequences to copy data from
    indexes      List all the indexes to create again after copying the data
    depends      List all the dependencies to filter-out
    schema       List the schema to migrate, formatted in JSON
    progress     List the progress


.. _pgcopydb_list_extensions:

pgcopydb list extensions
------------------------

pgcopydb list extensions - List all the source extensions to copy

The command ``pgcopydb list extensions`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
extensions to COPY to the target database.

::

   pgcopydb list extensions: List all the source extensions to copy
   usage: pgcopydb list extensions  --source ...

     --source            Postgres URI to the source database

.. _pgcopydb_list_collations:

pgcopydb list collations
------------------------

pgcopydb list collations - List all the source collations to copy

The command ``pgcopydb list collations`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
collations to COPY to the target database.

::

   pgcopydb list collations: List all the source collations to copy
   usage: pgcopydb list collations  --source ...

     --source            Postgres URI to the source database

The SQL query that is used lists the database collation, and then any
non-default collation that's used in a user column or a user index.

.. _pgcopydb_list_tables:

pgcopydb list tables
--------------------

pgcopydb list tables - List all the source tables to copy data from

The command ``pgcopydb list tables`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
tables to COPY the data from.

::

   pgcopydb list tables: List all the source tables to copy data from
   usage: pgcopydb list tables  --source ...

     --source            Postgres URI to the source database
     --filter <filename> Use the filters defined in <filename>
     --cache             Cache table size in relation pgcopydb.pgcopydb_table_size
     --drop-cache        Drop relation pgcopydb.pgcopydb_table_size
     --list-skipped      List only tables that are setup to be skipped
     --without-pkey      List only tables that have no primary key

The ``--cache`` option allows caching the `pg_table_size()`__ result in the
newly created table ``pgcopydb.pgcopydb_table_size``. This is only useful in
Postgres deployments where this computation is quite slow, and when the
pgcopydb operation is going to be run multiple times.

__ https://www.postgresql.org/docs/15/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE

.. _pgcopydb_list_table_parts:

pgcopydb list table-parts
-------------------------

pgcopydb list table-parts - List a source table copy partitions

The command ``pgcopydb list table-parts`` connects to the source database
and executes a SQL query using the Postgres catalogs to get detailed
information about the given source table, and then another SQL query to
compute how to split this source table given the size threshold argument.

::

   pgcopydb list table-parts: List a source table copy partitions
   usage: pgcopydb list table-parts  --source ...

     --source                    Postgres URI to the source database
     --schema-name               Name of the schema where to find the table
     --table-name                Name of the target table
     --split-tables-larger-than  Size threshold to consider partitioning

.. _pgcopydb_list_sequences:

pgcopydb list sequences
-----------------------

pgcopydb list sequences - List all the source sequences to copy data from

The command ``pgcopydb list sequences`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
sequences to COPY the data from.

::

   pgcopydb list sequences: List all the source sequences to copy data from
   usage: pgcopydb list sequences  --source ...

     --source            Postgres URI to the source database
     --filter <filename> Use the filters defined in <filename>
     --list-skipped      List only tables that are setup to be skipped

.. _pgcopydb_list_indexes:

pgcopydb list indexes
---------------------

pgcopydb list indexes - List all the indexes to create again after copying the data

The command ``pgcopydb list indexes`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
indexes to COPY the data from.

::

  pgcopydb list indexes: List all the indexes to create again after copying the data
  usage: pgcopydb list indexes  --source ... [ --schema-name [ --table-name ] ]

    --source            Postgres URI to the source database
    --schema-name       Name of the schema where to find the table
    --table-name        Name of the target table
    --filter <filename> Use the filters defined in <filename>
    --list-skipped      List only tables that are setup to be skipped

.. _pgcopydb_list_depends:

pgcopydb list depends
---------------------

pgcopydb list depends - List all the dependencies to filter-out

The command ``pgcopydb list depends`` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
objects that depend on excluded objects from the filtering rules.

::

   pgcopydb list depends: List all the dependencies to filter-out
   usage: pgcopydb list depends  --source ... [ --schema-name [ --table-name ] ]

     --source            Postgres URI to the source database
     --schema-name       Name of the schema where to find the table
     --table-name        Name of the target table
     --filter <filename> Use the filters defined in <filename>
     --list-skipped      List only tables that are setup to be skipped


.. _pgcopydb_list_schema:

pgcopydb list schema
--------------------

pgcopydb list schema - List the schema to migrate, formatted in JSON

The command ``pgcopydb list schema`` connects to the source database and
executes a SQL queries using the Postgres catalogs to get a list of the
tables, indexes, and sequences to migrate. The command then outputs a JSON
formatted string that contains detailed information about all those objects.

::

   pgcopydb list schema: List the schema to migrate, formatted in JSON
   usage: pgcopydb list schema  --source ...

     --source            Postgres URI to the source database
     --filter <filename> Use the filters defined in <filename>


.. _pgcopydb_list_progress:

pgcopydb list progress
----------------------

pgcopydb list progress - List the progress

The command ``pgcopydb list progress`` reads the ``schema.json`` file in the
work directory, parses it, and then computes how many tables and indexes are
planned to be copied and created on the target database, how many have been
done already, and how many are in-progress.

When using the option ``--json`` the JSON formatted output also includes a
list of all the tables and indexes that are currently being processed.

::

    pgcopydb list progress: List the progress
    usage: pgcopydb list progress  --source ...

      --source  Postgres URI to the source database
      --json    Format the output using JSON


Options
-------

The following options are available to ``pgcopydb dump schema``:

--source

  Connection string to the source Postgres instance. See the Postgres
  documentation for `connection strings`__ for the details. In short both
  the quoted form ``"host=... dbname=..."`` and the URI form
  ``postgres://user@host:5432/dbname`` are supported.

  __ https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

--schema-name

  Filter indexes from a given schema only.

--table-name

  Filter indexes from a given table only (use ``--schema-name`` to fully
  qualify the table).

--without-pkey

  List only tables from the source database when they have no primary key
  attached to their schema.

--filter <filename>

  This option allows to skip objects in the list operations. See
  :ref:`filtering` for details about the expected file format and the
  filtering options available.

--list-skipped

  Instead of listing objects that are selected for copy by the filters
  installed with the ``--filter`` option, list the objects that are going to
  be skipped when using the filters.

--json

  The output of the command is formatted in JSON, when supported. Ignored
  otherwise.

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

Examples
--------

Listing the tables:

::

   $ pgcopydb list tables
   14:35:18 13827 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
   14:35:19 13827 INFO  Fetched information for 56 tables
        OID |          Schema Name |           Table Name |  Est. Row Count |    On-disk size
   ---------+----------------------+----------------------+-----------------+----------------
      17085 |                  csv |                track |            3503 |          544 kB
      17098 |             expected |                track |            3503 |          544 kB
      17290 |             expected |           track_full |            3503 |          544 kB
      17276 |               public |           track_full |            3503 |          544 kB
      17016 |             expected |            districts |             440 |           72 kB
      17007 |               public |            districts |             440 |           72 kB
      16998 |                  csv |               blocks |             460 |           48 kB
      17003 |             expected |               blocks |             460 |           48 kB
      17405 |                  csv |              partial |               7 |           16 kB
      17323 |                  err |               errors |               0 |           16 kB
      16396 |             expected |              allcols |               0 |           16 kB
      17265 |             expected |                  csv |               0 |           16 kB
      17056 |             expected |      csv_escape_mode |               0 |           16 kB
      17331 |             expected |               errors |               0 |           16 kB
      17116 |             expected |                group |               0 |           16 kB
      17134 |             expected |                 json |               0 |           16 kB
      17074 |             expected |             matching |               0 |           16 kB
      17201 |             expected |               nullif |               0 |           16 kB
      17229 |             expected |                nulls |               0 |           16 kB
      17417 |             expected |              partial |               0 |           16 kB
      17313 |             expected |              reg2013 |               0 |           16 kB
      17437 |             expected |               serial |               0 |           16 kB
      17247 |             expected |                 sexp |               0 |           16 kB
      17378 |             expected |                test1 |               0 |           16 kB
      17454 |             expected |                  udc |               0 |           16 kB
      17471 |             expected |                xzero |               0 |           16 kB
      17372 |               nsitra |                test1 |               0 |           16 kB
      16388 |               public |              allcols |               0 |           16 kB
      17256 |               public |                  csv |               0 |           16 kB
      17047 |               public |      csv_escape_mode |               0 |           16 kB
      17107 |               public |                group |               0 |           16 kB
      17125 |               public |                 json |               0 |           16 kB
      17065 |               public |             matching |               0 |           16 kB
      17192 |               public |               nullif |               0 |           16 kB
      17219 |               public |                nulls |               0 |           16 kB
      17307 |               public |              reg2013 |               0 |           16 kB
      17428 |               public |               serial |               0 |           16 kB
      17238 |               public |                 sexp |               0 |           16 kB
      17446 |               public |                  udc |               0 |           16 kB
      17463 |               public |                xzero |               0 |           16 kB
      17303 |             expected |              copyhex |               0 |      8192 bytes
      17033 |             expected |           dateformat |               0 |      8192 bytes
      17366 |             expected |                fixed |               0 |      8192 bytes
      17041 |             expected |              jordane |               0 |      8192 bytes
      17173 |             expected |           missingcol |               0 |      8192 bytes
      17396 |             expected |             overflow |               0 |      8192 bytes
      17186 |             expected |              tab_csv |               0 |      8192 bytes
      17213 |             expected |                 temp |               0 |      8192 bytes
      17299 |               public |              copyhex |               0 |      8192 bytes
      17029 |               public |           dateformat |               0 |      8192 bytes
      17362 |               public |                fixed |               0 |      8192 bytes
      17037 |               public |              jordane |               0 |      8192 bytes
      17164 |               public |           missingcol |               0 |      8192 bytes
      17387 |               public |             overflow |               0 |      8192 bytes
      17182 |               public |              tab_csv |               0 |      8192 bytes
      17210 |               public |                 temp |               0 |      8192 bytes

Listing a table list of COPY partitions:

::

   $ pgcopydb list table-parts --table-name rental --split-at 300kB
   16:43:26 73794 INFO  Running pgcopydb version 0.8.8.g0838291.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb"
   16:43:26 73794 INFO  Listing COPY partitions for table "public"."rental" in "postgres://@:/pagila?"
   16:43:26 73794 INFO  Table "public"."rental" COPY will be split 5-ways
         Part |        Min |        Max |      Count
   -----------+------------+------------+-----------
          1/5 |          1 |       3211 |       3211
          2/5 |       3212 |       6422 |       3211
          3/5 |       6423 |       9633 |       3211
          4/5 |       9634 |      12844 |       3211
          5/5 |      12845 |      16049 |       3205


Listing the indexes:

::

   $ pgcopydb list indexes
   14:35:07 13668 INFO  Listing indexes in "port=54311 host=localhost dbname=pgloader"
   14:35:07 13668 INFO  Fetching all indexes in source database
   14:35:07 13668 INFO  Fetched information for 12 indexes
        OID |     Schema |           Index Name |         conname |                Constraint | DDL
   ---------+------------+----------------------+-----------------+---------------------------+---------------------
      17002 |        csv |      blocks_ip4r_idx |                 |                           | CREATE INDEX blocks_ip4r_idx ON csv.blocks USING gist (iprange)
      17415 |        csv |        partial_b_idx |                 |                           | CREATE INDEX partial_b_idx ON csv.partial USING btree (b)
      17414 |        csv |        partial_a_key |   partial_a_key |                UNIQUE (a) | CREATE UNIQUE INDEX partial_a_key ON csv.partial USING btree (a)
      17092 |        csv |           track_pkey |      track_pkey |     PRIMARY KEY (trackid) | CREATE UNIQUE INDEX track_pkey ON csv.track USING btree (trackid)
      17329 |        err |          errors_pkey |     errors_pkey |           PRIMARY KEY (a) | CREATE UNIQUE INDEX errors_pkey ON err.errors USING btree (a)
      16394 |     public |         allcols_pkey |    allcols_pkey |           PRIMARY KEY (a) | CREATE UNIQUE INDEX allcols_pkey ON public.allcols USING btree (a)
      17054 |     public | csv_escape_mode_pkey | csv_escape_mode_pkey |          PRIMARY KEY (id) | CREATE UNIQUE INDEX csv_escape_mode_pkey ON public.csv_escape_mode USING btree (id)
      17199 |     public |          nullif_pkey |     nullif_pkey |          PRIMARY KEY (id) | CREATE UNIQUE INDEX nullif_pkey ON public."nullif" USING btree (id)
      17435 |     public |          serial_pkey |     serial_pkey |           PRIMARY KEY (a) | CREATE UNIQUE INDEX serial_pkey ON public.serial USING btree (a)
      17288 |     public |      track_full_pkey | track_full_pkey |     PRIMARY KEY (trackid) | CREATE UNIQUE INDEX track_full_pkey ON public.track_full USING btree (trackid)
      17452 |     public |             udc_pkey |        udc_pkey |           PRIMARY KEY (b) | CREATE UNIQUE INDEX udc_pkey ON public.udc USING btree (b)
      17469 |     public |           xzero_pkey |      xzero_pkey |           PRIMARY KEY (a) | CREATE UNIQUE INDEX xzero_pkey ON public.xzero USING btree (a)


Listing the schema in JSON:

::

   $ pgcopydb list schema --split-at 200kB

This gives the following JSON output:

.. code-block:: json
   :linenos:

   {
       "setup": {
           "snapshot": "00000003-00051AAE-1",
           "source_pguri": "postgres:\/\/@:\/pagila?",
           "target_pguri": "postgres:\/\/@:\/plop?",
           "table-jobs": 4,
           "index-jobs": 4,
           "split-tables-larger-than": 204800
       },
       "tables": [
           {
               "oid": 317934,
               "schema": "public",
               "name": "rental",
               "reltuples": 16044,
               "bytes": 1253376,
               "bytes-pretty": "1224 kB",
               "exclude-data": false,
               "restore-list-name": "public rental postgres",
               "part-key": "rental_id",
               "parts": [
                   {
                       "number": 1,
                       "total": 7,
                       "min": 1,
                       "max": 2294,
                       "count": 2294
                   },
                   {
                       "number": 2,
                       "total": 7,
                       "min": 2295,
                       "max": 4588,
                       "count": 2294
                   },
                   {
                       "number": 3,
                       "total": 7,
                       "min": 4589,
                       "max": 6882,
                       "count": 2294
                   },
                   {
                       "number": 4,
                       "total": 7,
                       "min": 6883,
                       "max": 9176,
                       "count": 2294
                   },
                   {
                       "number": 5,
                       "total": 7,
                       "min": 9177,
                       "max": 11470,
                       "count": 2294
                   },
                   {
                       "number": 6,
                       "total": 7,
                       "min": 11471,
                       "max": 13764,
                       "count": 2294
                   },
                   {
                       "number": 7,
                       "total": 7,
                       "min": 13765,
                       "max": 16049,
                       "count": 2285
                   }
               ]
           },
           {
               "oid": 317818,
               "schema": "public",
               "name": "film",
               "reltuples": 1000,
               "bytes": 483328,
               "bytes-pretty": "472 kB",
               "exclude-data": false,
               "restore-list-name": "public film postgres",
               "part-key": "film_id",
               "parts": [
                   {
                       "number": 1,
                       "total": 3,
                       "min": 1,
                       "max": 334,
                       "count": 334
                   },
                   {
                       "number": 2,
                       "total": 3,
                       "min": 335,
                       "max": 668,
                       "count": 334
                   },
                   {
                       "number": 3,
                       "total": 3,
                       "min": 669,
                       "max": 1000,
                       "count": 332
                   }
               ]
           },
           {
               "oid": 317920,
               "schema": "public",
               "name": "payment_p2020_04",
               "reltuples": 6754,
               "bytes": 434176,
               "bytes-pretty": "424 kB",
               "exclude-data": false,
               "restore-list-name": "public payment_p2020_04 postgres",
               "part-key": ""
           },
           {
               "oid": 317916,
               "schema": "public",
               "name": "payment_p2020_03",
               "reltuples": 5644,
               "bytes": 368640,
               "bytes-pretty": "360 kB",
               "exclude-data": false,
               "restore-list-name": "public payment_p2020_03 postgres",
               "part-key": ""
           },
           {
               "oid": 317830,
               "schema": "public",
               "name": "film_actor",
               "reltuples": 5462,
               "bytes": 270336,
               "bytes-pretty": "264 kB",
               "exclude-data": false,
               "restore-list-name": "public film_actor postgres",
               "part-key": ""
           },
           {
               "oid": 317885,
               "schema": "public",
               "name": "inventory",
               "reltuples": 4581,
               "bytes": 270336,
               "bytes-pretty": "264 kB",
               "exclude-data": false,
               "restore-list-name": "public inventory postgres",
               "part-key": "inventory_id",
               "parts": [
                   {
                       "number": 1,
                       "total": 2,
                       "min": 1,
                       "max": 2291,
                       "count": 2291
                   },
                   {
                       "number": 2,
                       "total": 2,
                       "min": 2292,
                       "max": 4581,
                       "count": 2290
                   }
               ]
           },
           {
               "oid": 317912,
               "schema": "public",
               "name": "payment_p2020_02",
               "reltuples": 2312,
               "bytes": 163840,
               "bytes-pretty": "160 kB",
               "exclude-data": false,
               "restore-list-name": "public payment_p2020_02 postgres",
               "part-key": ""
           },
           {
               "oid": 317784,
               "schema": "public",
               "name": "customer",
               "reltuples": 599,
               "bytes": 106496,
               "bytes-pretty": "104 kB",
               "exclude-data": false,
               "restore-list-name": "public customer postgres",
               "part-key": "customer_id"
           },
           {
               "oid": 317845,
               "schema": "public",
               "name": "address",
               "reltuples": 603,
               "bytes": 98304,
               "bytes-pretty": "96 kB",
               "exclude-data": false,
               "restore-list-name": "public address postgres",
               "part-key": "address_id"
           },
           {
               "oid": 317908,
               "schema": "public",
               "name": "payment_p2020_01",
               "reltuples": 1157,
               "bytes": 98304,
               "bytes-pretty": "96 kB",
               "exclude-data": false,
               "restore-list-name": "public payment_p2020_01 postgres",
               "part-key": ""
           },
           {
               "oid": 317855,
               "schema": "public",
               "name": "city",
               "reltuples": 600,
               "bytes": 73728,
               "bytes-pretty": "72 kB",
               "exclude-data": false,
               "restore-list-name": "public city postgres",
               "part-key": "city_id"
           },
           {
               "oid": 317834,
               "schema": "public",
               "name": "film_category",
               "reltuples": 1000,
               "bytes": 73728,
               "bytes-pretty": "72 kB",
               "exclude-data": false,
               "restore-list-name": "public film_category postgres",
               "part-key": ""
           },
           {
               "oid": 317798,
               "schema": "public",
               "name": "actor",
               "reltuples": 200,
               "bytes": 49152,
               "bytes-pretty": "48 kB",
               "exclude-data": false,
               "restore-list-name": "public actor postgres",
               "part-key": "actor_id"
           },
           {
               "oid": 317924,
               "schema": "public",
               "name": "payment_p2020_05",
               "reltuples": 182,
               "bytes": 40960,
               "bytes-pretty": "40 kB",
               "exclude-data": false,
               "restore-list-name": "public payment_p2020_05 postgres",
               "part-key": ""
           },
           {
               "oid": 317808,
               "schema": "public",
               "name": "category",
               "reltuples": 0,
               "bytes": 16384,
               "bytes-pretty": "16 kB",
               "exclude-data": false,
               "restore-list-name": "public category postgres",
               "part-key": "category_id"
           },
           {
               "oid": 317865,
               "schema": "public",
               "name": "country",
               "reltuples": 109,
               "bytes": 16384,
               "bytes-pretty": "16 kB",
               "exclude-data": false,
               "restore-list-name": "public country postgres",
               "part-key": "country_id"
           },
           {
               "oid": 317946,
               "schema": "public",
               "name": "staff",
               "reltuples": 0,
               "bytes": 16384,
               "bytes-pretty": "16 kB",
               "exclude-data": false,
               "restore-list-name": "public staff postgres",
               "part-key": "staff_id"
           },
           {
               "oid": 378280,
               "schema": "pgcopydb",
               "name": "sentinel",
               "reltuples": 1,
               "bytes": 8192,
               "bytes-pretty": "8192 bytes",
               "exclude-data": false,
               "restore-list-name": "pgcopydb sentinel dim",
               "part-key": ""
           },
           {
               "oid": 317892,
               "schema": "public",
               "name": "language",
               "reltuples": 0,
               "bytes": 8192,
               "bytes-pretty": "8192 bytes",
               "exclude-data": false,
               "restore-list-name": "public language postgres",
               "part-key": "language_id"
           },
           {
               "oid": 317928,
               "schema": "public",
               "name": "payment_p2020_06",
               "reltuples": 0,
               "bytes": 8192,
               "bytes-pretty": "8192 bytes",
               "exclude-data": false,
               "restore-list-name": "public payment_p2020_06 postgres",
               "part-key": ""
           },
           {
               "oid": 317957,
               "schema": "public",
               "name": "store",
               "reltuples": 0,
               "bytes": 8192,
               "bytes-pretty": "8192 bytes",
               "exclude-data": false,
               "restore-list-name": "public store postgres",
               "part-key": "store_id"
           }
       ],
       "indexes": [
           {
               "oid": 378283,
               "schema": "pgcopydb",
               "name": "sentinel_expr_idx",
               "isPrimary": false,
               "isUnique": true,
               "columns": "",
               "sql": "CREATE UNIQUE INDEX sentinel_expr_idx ON pgcopydb.sentinel USING btree ((1))",
               "restore-list-name": "pgcopydb sentinel_expr_idx dim",
               "table": {
                   "oid": 378280,
                   "schema": "pgcopydb",
                   "name": "sentinel"
               }
           },
           {
               "oid": 318001,
               "schema": "public",
               "name": "idx_actor_last_name",
               "isPrimary": false,
               "isUnique": false,
               "columns": "last_name",
               "sql": "CREATE INDEX idx_actor_last_name ON public.actor USING btree (last_name)",
               "restore-list-name": "public idx_actor_last_name postgres",
               "table": {
                   "oid": 317798,
                   "schema": "public",
                   "name": "actor"
               }
           },
           {
               "oid": 317972,
               "schema": "public",
               "name": "actor_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "actor_id",
               "sql": "CREATE UNIQUE INDEX actor_pkey ON public.actor USING btree (actor_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317798,
                   "schema": "public",
                   "name": "actor"
               },
               "constraint": {
                   "oid": 317973,
                   "name": "actor_pkey",
                   "sql": "PRIMARY KEY (actor_id)"
               }
           },
           {
               "oid": 317974,
               "schema": "public",
               "name": "address_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "address_id",
               "sql": "CREATE UNIQUE INDEX address_pkey ON public.address USING btree (address_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317845,
                   "schema": "public",
                   "name": "address"
               },
               "constraint": {
                   "oid": 317975,
                   "name": "address_pkey",
                   "sql": "PRIMARY KEY (address_id)"
               }
           },
           {
               "oid": 318003,
               "schema": "public",
               "name": "idx_fk_city_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "city_id",
               "sql": "CREATE INDEX idx_fk_city_id ON public.address USING btree (city_id)",
               "restore-list-name": "public idx_fk_city_id postgres",
               "table": {
                   "oid": 317845,
                   "schema": "public",
                   "name": "address"
               }
           },
           {
               "oid": 317976,
               "schema": "public",
               "name": "category_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "category_id",
               "sql": "CREATE UNIQUE INDEX category_pkey ON public.category USING btree (category_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317808,
                   "schema": "public",
                   "name": "category"
               },
               "constraint": {
                   "oid": 317977,
                   "name": "category_pkey",
                   "sql": "PRIMARY KEY (category_id)"
               }
           },
           {
               "oid": 317978,
               "schema": "public",
               "name": "city_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "city_id",
               "sql": "CREATE UNIQUE INDEX city_pkey ON public.city USING btree (city_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317855,
                   "schema": "public",
                   "name": "city"
               },
               "constraint": {
                   "oid": 317979,
                   "name": "city_pkey",
                   "sql": "PRIMARY KEY (city_id)"
               }
           },
           {
               "oid": 318004,
               "schema": "public",
               "name": "idx_fk_country_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "country_id",
               "sql": "CREATE INDEX idx_fk_country_id ON public.city USING btree (country_id)",
               "restore-list-name": "public idx_fk_country_id postgres",
               "table": {
                   "oid": 317855,
                   "schema": "public",
                   "name": "city"
               }
           },
           {
               "oid": 317980,
               "schema": "public",
               "name": "country_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "country_id",
               "sql": "CREATE UNIQUE INDEX country_pkey ON public.country USING btree (country_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317865,
                   "schema": "public",
                   "name": "country"
               },
               "constraint": {
                   "oid": 317981,
                   "name": "country_pkey",
                   "sql": "PRIMARY KEY (country_id)"
               }
           },
           {
               "oid": 318024,
               "schema": "public",
               "name": "idx_last_name",
               "isPrimary": false,
               "isUnique": false,
               "columns": "last_name",
               "sql": "CREATE INDEX idx_last_name ON public.customer USING btree (last_name)",
               "restore-list-name": "public idx_last_name postgres",
               "table": {
                   "oid": 317784,
                   "schema": "public",
                   "name": "customer"
               }
           },
           {
               "oid": 318002,
               "schema": "public",
               "name": "idx_fk_address_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "address_id",
               "sql": "CREATE INDEX idx_fk_address_id ON public.customer USING btree (address_id)",
               "restore-list-name": "public idx_fk_address_id postgres",
               "table": {
                   "oid": 317784,
                   "schema": "public",
                   "name": "customer"
               }
           },
           {
               "oid": 317982,
               "schema": "public",
               "name": "customer_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "customer_id",
               "sql": "CREATE UNIQUE INDEX customer_pkey ON public.customer USING btree (customer_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317784,
                   "schema": "public",
                   "name": "customer"
               },
               "constraint": {
                   "oid": 317983,
                   "name": "customer_pkey",
                   "sql": "PRIMARY KEY (customer_id)"
               }
           },
           {
               "oid": 318023,
               "schema": "public",
               "name": "idx_fk_store_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "store_id",
               "sql": "CREATE INDEX idx_fk_store_id ON public.customer USING btree (store_id)",
               "restore-list-name": "public idx_fk_store_id postgres",
               "table": {
                   "oid": 317784,
                   "schema": "public",
                   "name": "customer"
               }
           },
           {
               "oid": 318009,
               "schema": "public",
               "name": "idx_fk_original_language_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "original_language_id",
               "sql": "CREATE INDEX idx_fk_original_language_id ON public.film USING btree (original_language_id)",
               "restore-list-name": "public idx_fk_original_language_id postgres",
               "table": {
                   "oid": 317818,
                   "schema": "public",
                   "name": "film"
               }
           },
           {
               "oid": 318026,
               "schema": "public",
               "name": "idx_title",
               "isPrimary": false,
               "isUnique": false,
               "columns": "title",
               "sql": "CREATE INDEX idx_title ON public.film USING btree (title)",
               "restore-list-name": "public idx_title postgres",
               "table": {
                   "oid": 317818,
                   "schema": "public",
                   "name": "film"
               }
           },
           {
               "oid": 318000,
               "schema": "public",
               "name": "film_fulltext_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "fulltext",
               "sql": "CREATE INDEX film_fulltext_idx ON public.film USING gist (fulltext)",
               "restore-list-name": "public film_fulltext_idx postgres",
               "table": {
                   "oid": 317818,
                   "schema": "public",
                   "name": "film"
               }
           },
           {
               "oid": 317988,
               "schema": "public",
               "name": "film_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "film_id",
               "sql": "CREATE UNIQUE INDEX film_pkey ON public.film USING btree (film_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317818,
                   "schema": "public",
                   "name": "film"
               },
               "constraint": {
                   "oid": 317989,
                   "name": "film_pkey",
                   "sql": "PRIMARY KEY (film_id)"
               }
           },
           {
               "oid": 318008,
               "schema": "public",
               "name": "idx_fk_language_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "language_id",
               "sql": "CREATE INDEX idx_fk_language_id ON public.film USING btree (language_id)",
               "restore-list-name": "public idx_fk_language_id postgres",
               "table": {
                   "oid": 317818,
                   "schema": "public",
                   "name": "film"
               }
           },
           {
               "oid": 317984,
               "schema": "public",
               "name": "film_actor_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "actor_id,film_id",
               "sql": "CREATE UNIQUE INDEX film_actor_pkey ON public.film_actor USING btree (actor_id, film_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317830,
                   "schema": "public",
                   "name": "film_actor"
               },
               "constraint": {
                   "oid": 317985,
                   "name": "film_actor_pkey",
                   "sql": "PRIMARY KEY (actor_id, film_id)"
               }
           },
           {
               "oid": 318006,
               "schema": "public",
               "name": "idx_fk_film_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "film_id",
               "sql": "CREATE INDEX idx_fk_film_id ON public.film_actor USING btree (film_id)",
               "restore-list-name": "public idx_fk_film_id postgres",
               "table": {
                   "oid": 317830,
                   "schema": "public",
                   "name": "film_actor"
               }
           },
           {
               "oid": 317986,
               "schema": "public",
               "name": "film_category_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "film_id,category_id",
               "sql": "CREATE UNIQUE INDEX film_category_pkey ON public.film_category USING btree (film_id, category_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317834,
                   "schema": "public",
                   "name": "film_category"
               },
               "constraint": {
                   "oid": 317987,
                   "name": "film_category_pkey",
                   "sql": "PRIMARY KEY (film_id, category_id)"
               }
           },
           {
               "oid": 318025,
               "schema": "public",
               "name": "idx_store_id_film_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "film_id,store_id",
               "sql": "CREATE INDEX idx_store_id_film_id ON public.inventory USING btree (store_id, film_id)",
               "restore-list-name": "public idx_store_id_film_id postgres",
               "table": {
                   "oid": 317885,
                   "schema": "public",
                   "name": "inventory"
               }
           },
           {
               "oid": 317990,
               "schema": "public",
               "name": "inventory_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "inventory_id",
               "sql": "CREATE UNIQUE INDEX inventory_pkey ON public.inventory USING btree (inventory_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317885,
                   "schema": "public",
                   "name": "inventory"
               },
               "constraint": {
                   "oid": 317991,
                   "name": "inventory_pkey",
                   "sql": "PRIMARY KEY (inventory_id)"
               }
           },
           {
               "oid": 317992,
               "schema": "public",
               "name": "language_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "language_id",
               "sql": "CREATE UNIQUE INDEX language_pkey ON public.language USING btree (language_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317892,
                   "schema": "public",
                   "name": "language"
               },
               "constraint": {
                   "oid": 317993,
                   "name": "language_pkey",
                   "sql": "PRIMARY KEY (language_id)"
               }
           },
           {
               "oid": 318010,
               "schema": "public",
               "name": "idx_fk_payment_p2020_01_customer_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_01_customer_id ON public.payment_p2020_01 USING btree (customer_id)",
               "restore-list-name": "public idx_fk_payment_p2020_01_customer_id postgres",
               "table": {
                   "oid": 317908,
                   "schema": "public",
                   "name": "payment_p2020_01"
               }
           },
           {
               "oid": 318029,
               "schema": "public",
               "name": "payment_p2020_01_customer_id_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX payment_p2020_01_customer_id_idx ON public.payment_p2020_01 USING btree (customer_id)",
               "restore-list-name": "public payment_p2020_01_customer_id_idx postgres",
               "table": {
                   "oid": 317908,
                   "schema": "public",
                   "name": "payment_p2020_01"
               }
           },
           {
               "oid": 318012,
               "schema": "public",
               "name": "idx_fk_payment_p2020_01_staff_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "staff_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_01_staff_id ON public.payment_p2020_01 USING btree (staff_id)",
               "restore-list-name": "public idx_fk_payment_p2020_01_staff_id postgres",
               "table": {
                   "oid": 317908,
                   "schema": "public",
                   "name": "payment_p2020_01"
               }
           },
           {
               "oid": 318013,
               "schema": "public",
               "name": "idx_fk_payment_p2020_02_customer_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_02_customer_id ON public.payment_p2020_02 USING btree (customer_id)",
               "restore-list-name": "public idx_fk_payment_p2020_02_customer_id postgres",
               "table": {
                   "oid": 317912,
                   "schema": "public",
                   "name": "payment_p2020_02"
               }
           },
           {
               "oid": 318014,
               "schema": "public",
               "name": "idx_fk_payment_p2020_02_staff_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "staff_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_02_staff_id ON public.payment_p2020_02 USING btree (staff_id)",
               "restore-list-name": "public idx_fk_payment_p2020_02_staff_id postgres",
               "table": {
                   "oid": 317912,
                   "schema": "public",
                   "name": "payment_p2020_02"
               }
           },
           {
               "oid": 318030,
               "schema": "public",
               "name": "payment_p2020_02_customer_id_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX payment_p2020_02_customer_id_idx ON public.payment_p2020_02 USING btree (customer_id)",
               "restore-list-name": "public payment_p2020_02_customer_id_idx postgres",
               "table": {
                   "oid": 317912,
                   "schema": "public",
                   "name": "payment_p2020_02"
               }
           },
           {
               "oid": 318016,
               "schema": "public",
               "name": "idx_fk_payment_p2020_03_staff_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "staff_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_03_staff_id ON public.payment_p2020_03 USING btree (staff_id)",
               "restore-list-name": "public idx_fk_payment_p2020_03_staff_id postgres",
               "table": {
                   "oid": 317916,
                   "schema": "public",
                   "name": "payment_p2020_03"
               }
           },
           {
               "oid": 318031,
               "schema": "public",
               "name": "payment_p2020_03_customer_id_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX payment_p2020_03_customer_id_idx ON public.payment_p2020_03 USING btree (customer_id)",
               "restore-list-name": "public payment_p2020_03_customer_id_idx postgres",
               "table": {
                   "oid": 317916,
                   "schema": "public",
                   "name": "payment_p2020_03"
               }
           },
           {
               "oid": 318015,
               "schema": "public",
               "name": "idx_fk_payment_p2020_03_customer_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_03_customer_id ON public.payment_p2020_03 USING btree (customer_id)",
               "restore-list-name": "public idx_fk_payment_p2020_03_customer_id postgres",
               "table": {
                   "oid": 317916,
                   "schema": "public",
                   "name": "payment_p2020_03"
               }
           },
           {
               "oid": 318032,
               "schema": "public",
               "name": "payment_p2020_04_customer_id_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX payment_p2020_04_customer_id_idx ON public.payment_p2020_04 USING btree (customer_id)",
               "restore-list-name": "public payment_p2020_04_customer_id_idx postgres",
               "table": {
                   "oid": 317920,
                   "schema": "public",
                   "name": "payment_p2020_04"
               }
           },
           {
               "oid": 318018,
               "schema": "public",
               "name": "idx_fk_payment_p2020_04_staff_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "staff_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_04_staff_id ON public.payment_p2020_04 USING btree (staff_id)",
               "restore-list-name": "public idx_fk_payment_p2020_04_staff_id postgres",
               "table": {
                   "oid": 317920,
                   "schema": "public",
                   "name": "payment_p2020_04"
               }
           },
           {
               "oid": 318017,
               "schema": "public",
               "name": "idx_fk_payment_p2020_04_customer_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_04_customer_id ON public.payment_p2020_04 USING btree (customer_id)",
               "restore-list-name": "public idx_fk_payment_p2020_04_customer_id postgres",
               "table": {
                   "oid": 317920,
                   "schema": "public",
                   "name": "payment_p2020_04"
               }
           },
           {
               "oid": 318019,
               "schema": "public",
               "name": "idx_fk_payment_p2020_05_customer_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_05_customer_id ON public.payment_p2020_05 USING btree (customer_id)",
               "restore-list-name": "public idx_fk_payment_p2020_05_customer_id postgres",
               "table": {
                   "oid": 317924,
                   "schema": "public",
                   "name": "payment_p2020_05"
               }
           },
           {
               "oid": 318020,
               "schema": "public",
               "name": "idx_fk_payment_p2020_05_staff_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "staff_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_05_staff_id ON public.payment_p2020_05 USING btree (staff_id)",
               "restore-list-name": "public idx_fk_payment_p2020_05_staff_id postgres",
               "table": {
                   "oid": 317924,
                   "schema": "public",
                   "name": "payment_p2020_05"
               }
           },
           {
               "oid": 318033,
               "schema": "public",
               "name": "payment_p2020_05_customer_id_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX payment_p2020_05_customer_id_idx ON public.payment_p2020_05 USING btree (customer_id)",
               "restore-list-name": "public payment_p2020_05_customer_id_idx postgres",
               "table": {
                   "oid": 317924,
                   "schema": "public",
                   "name": "payment_p2020_05"
               }
           },
           {
               "oid": 318022,
               "schema": "public",
               "name": "idx_fk_payment_p2020_06_staff_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "staff_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_06_staff_id ON public.payment_p2020_06 USING btree (staff_id)",
               "restore-list-name": "public idx_fk_payment_p2020_06_staff_id postgres",
               "table": {
                   "oid": 317928,
                   "schema": "public",
                   "name": "payment_p2020_06"
               }
           },
           {
               "oid": 318034,
               "schema": "public",
               "name": "payment_p2020_06_customer_id_idx",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX payment_p2020_06_customer_id_idx ON public.payment_p2020_06 USING btree (customer_id)",
               "restore-list-name": "public payment_p2020_06_customer_id_idx postgres",
               "table": {
                   "oid": 317928,
                   "schema": "public",
                   "name": "payment_p2020_06"
               }
           },
           {
               "oid": 318021,
               "schema": "public",
               "name": "idx_fk_payment_p2020_06_customer_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "customer_id",
               "sql": "CREATE INDEX idx_fk_payment_p2020_06_customer_id ON public.payment_p2020_06 USING btree (customer_id)",
               "restore-list-name": "public idx_fk_payment_p2020_06_customer_id postgres",
               "table": {
                   "oid": 317928,
                   "schema": "public",
                   "name": "payment_p2020_06"
               }
           },
           {
               "oid": 318028,
               "schema": "public",
               "name": "idx_unq_rental_rental_date_inventory_id_customer_id",
               "isPrimary": false,
               "isUnique": true,
               "columns": "rental_date,inventory_id,customer_id",
               "sql": "CREATE UNIQUE INDEX idx_unq_rental_rental_date_inventory_id_customer_id ON public.rental USING btree (rental_date, inventory_id, customer_id)",
               "restore-list-name": "public idx_unq_rental_rental_date_inventory_id_customer_id postgres",
               "table": {
                   "oid": 317934,
                   "schema": "public",
                   "name": "rental"
               }
           },
           {
               "oid": 317994,
               "schema": "public",
               "name": "rental_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "rental_id",
               "sql": "CREATE UNIQUE INDEX rental_pkey ON public.rental USING btree (rental_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317934,
                   "schema": "public",
                   "name": "rental"
               },
               "constraint": {
                   "oid": 317995,
                   "name": "rental_pkey",
                   "sql": "PRIMARY KEY (rental_id)"
               }
           },
           {
               "oid": 318007,
               "schema": "public",
               "name": "idx_fk_inventory_id",
               "isPrimary": false,
               "isUnique": false,
               "columns": "inventory_id",
               "sql": "CREATE INDEX idx_fk_inventory_id ON public.rental USING btree (inventory_id)",
               "restore-list-name": "public idx_fk_inventory_id postgres",
               "table": {
                   "oid": 317934,
                   "schema": "public",
                   "name": "rental"
               }
           },
           {
               "oid": 317996,
               "schema": "public",
               "name": "staff_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "staff_id",
               "sql": "CREATE UNIQUE INDEX staff_pkey ON public.staff USING btree (staff_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317946,
                   "schema": "public",
                   "name": "staff"
               },
               "constraint": {
                   "oid": 317997,
                   "name": "staff_pkey",
                   "sql": "PRIMARY KEY (staff_id)"
               }
           },
           {
               "oid": 318027,
               "schema": "public",
               "name": "idx_unq_manager_staff_id",
               "isPrimary": false,
               "isUnique": true,
               "columns": "manager_staff_id",
               "sql": "CREATE UNIQUE INDEX idx_unq_manager_staff_id ON public.store USING btree (manager_staff_id)",
               "restore-list-name": "public idx_unq_manager_staff_id postgres",
               "table": {
                   "oid": 317957,
                   "schema": "public",
                   "name": "store"
               }
           },
           {
               "oid": 317998,
               "schema": "public",
               "name": "store_pkey",
               "isPrimary": true,
               "isUnique": true,
               "columns": "store_id",
               "sql": "CREATE UNIQUE INDEX store_pkey ON public.store USING btree (store_id)",
               "restore-list-name": "",
               "table": {
                   "oid": 317957,
                   "schema": "public",
                   "name": "store"
               },
               "constraint": {
                   "oid": 317999,
                   "name": "store_pkey",
                   "sql": "PRIMARY KEY (store_id)"
               }
           }
       ],
       "sequences": [
           {
               "oid": 317796,
               "schema": "public",
               "name": "actor_actor_id_seq",
               "last-value": 200,
               "is-called": true,
               "restore-list-name": "public actor_actor_id_seq postgres"
           },
           {
               "oid": 317843,
               "schema": "public",
               "name": "address_address_id_seq",
               "last-value": 605,
               "is-called": true,
               "restore-list-name": "public address_address_id_seq postgres"
           },
           {
               "oid": 317806,
               "schema": "public",
               "name": "category_category_id_seq",
               "last-value": 16,
               "is-called": true,
               "restore-list-name": "public category_category_id_seq postgres"
           },
           {
               "oid": 317853,
               "schema": "public",
               "name": "city_city_id_seq",
               "last-value": 600,
               "is-called": true,
               "restore-list-name": "public city_city_id_seq postgres"
           },
           {
               "oid": 317863,
               "schema": "public",
               "name": "country_country_id_seq",
               "last-value": 109,
               "is-called": true,
               "restore-list-name": "public country_country_id_seq postgres"
           },
           {
               "oid": 317782,
               "schema": "public",
               "name": "customer_customer_id_seq",
               "last-value": 599,
               "is-called": true,
               "restore-list-name": "public customer_customer_id_seq postgres"
           },
           {
               "oid": 317816,
               "schema": "public",
               "name": "film_film_id_seq",
               "last-value": 1000,
               "is-called": true,
               "restore-list-name": "public film_film_id_seq postgres"
           },
           {
               "oid": 317883,
               "schema": "public",
               "name": "inventory_inventory_id_seq",
               "last-value": 4581,
               "is-called": true,
               "restore-list-name": "public inventory_inventory_id_seq postgres"
           },
           {
               "oid": 317890,
               "schema": "public",
               "name": "language_language_id_seq",
               "last-value": 6,
               "is-called": true,
               "restore-list-name": "public language_language_id_seq postgres"
           },
           {
               "oid": 317902,
               "schema": "public",
               "name": "payment_payment_id_seq",
               "last-value": 32099,
               "is-called": true,
               "restore-list-name": "public payment_payment_id_seq postgres"
           },
           {
               "oid": 317932,
               "schema": "public",
               "name": "rental_rental_id_seq",
               "last-value": 16050,
               "is-called": true,
               "restore-list-name": "public rental_rental_id_seq postgres"
           },
           {
               "oid": 317944,
               "schema": "public",
               "name": "staff_staff_id_seq",
               "last-value": 2,
               "is-called": true,
               "restore-list-name": "public staff_staff_id_seq postgres"
           },
           {
               "oid": 317955,
               "schema": "public",
               "name": "store_store_id_seq",
               "last-value": 2,
               "is-called": true,
               "restore-list-name": "public store_store_id_seq postgres"
           }
       ]
   }


Listing current progress (log lines removed):

::

   $ pgcopydb list progress 2>/dev/null
                |  Total Count |  In Progress |         Done
   -------------+--------------+--------------+-------------
         Tables |           21 |            4 |            7
        Indexes |           48 |           14 |            7


Listing current progress, in JSON:

::

   $ pgcopydb list progress --json 2>/dev/null
   {
       "table-jobs": 4,
       "index-jobs": 4,
       "tables": {
           "total": 21,
           "done": 9,
           "in-progress": [
               {
                   "oid": 317908,
                   "schema": "public",
                   "name": "payment_p2020_01",
                   "reltuples": 1157,
                   "bytes": 98304,
                   "bytes-pretty": "96 kB",
                   "exclude-data": false,
                   "restore-list-name": "public payment_p2020_01 postgres",
                   "part-key": "",
                   "process": {
                       "pid": 75159,
                       "start-time-epoch": 1662476249,
                       "start-time-string": "2022-09-06 16:57:29 CEST",
                       "command": "COPY \"public\".\"payment_p2020_01\""
                   }
               },
               {
                   "oid": 317855,
                   "schema": "public",
                   "name": "city",
                   "reltuples": 600,
                   "bytes": 73728,
                   "bytes-pretty": "72 kB",
                   "exclude-data": false,
                   "restore-list-name": "public city postgres",
                   "part-key": "city_id",
                   "process": {
                       "pid": 75157,
                       "start-time-epoch": 1662476249,
                       "start-time-string": "2022-09-06 16:57:29 CEST",
                       "command": "COPY \"public\".\"city\""
                   }
               }
           ]
       },
          "indexes": {
           "total": 48,
           "done": 39,
           "in-progress": [
               {
                   "oid": 378283,
                   "schema": "pgcopydb",
                   "name": "sentinel_expr_idx",
                   "isPrimary": false,
                   "isUnique": true,
                   "columns": "",
                   "sql": "CREATE UNIQUE INDEX sentinel_expr_idx ON pgcopydb.sentinel USING btree ((1))",
                   "restore-list-name": "pgcopydb sentinel_expr_idx dim",
                   "table": {
                       "oid": 378280,
                       "schema": "pgcopydb",
                       "name": "sentinel"
                   },
                   "process": {
                       "pid": 74372,
                       "start-time-epoch": 1662476080,
                       "start-time-string": "2022-09-06 16:54:40 CEST"
                   }
               },
               {
                   "oid": 317980,
                   "schema": "public",
                   "name": "country_pkey",
                   "isPrimary": true,
                   "isUnique": true,
                   "columns": "country_id",
                   "sql": "CREATE UNIQUE INDEX country_pkey ON public.country USING btree (country_id)",
                   "restore-list-name": "public country_pkey postgres",
                   "table": {
                       "oid": 317865,
                       "schema": "public",
                       "name": "country"
                   },
                   "constraint": {
                       "oid": 317981,
                       "name": "country_pkey",
                       "sql": "PRIMARY KEY (country_id)",
                       "restore-list-name": ""
                   },
                   "process": {
                       "pid": 74358,
                       "start-time-epoch": 1662476080,
                       "start-time-string": "2022-09-06 16:54:40 CEST"
                   }
               },
               {
                   "oid": 317996,
                   "schema": "public",
                   "name": "staff_pkey",
                   "isPrimary": true,
                   "isUnique": true,
                   "columns": "staff_id",
                   "sql": "CREATE UNIQUE INDEX staff_pkey ON public.staff USING btree (staff_id)",
                   "restore-list-name": "public staff_pkey postgres",
                   "table": {
                       "oid": 317946,
                       "schema": "public",
                       "name": "staff"
                   },
                   "constraint": {
                       "oid": 317997,
                       "name": "staff_pkey",
                       "sql": "PRIMARY KEY (staff_id)",
                       "restore-list-name": ""
                   },
                   "process": {
                       "pid": 74368,
                       "start-time-epoch": 1662476080,
                       "start-time-string": "2022-09-06 16:54:40 CEST"
                   }
               }
           ]
       }
   }
