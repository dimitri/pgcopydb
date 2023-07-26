.. _pgcopydb_compare:

pgcopydb compare
=================

pgcopydb compare - Compare source and target databases

The command ``pgcopydb compare`` connects to the source and target databases
and executes SQL queries to get Postgres catalog information about the
table, indexes and sequences that are migrated.

The tool then compares either the schema definitions or the data contents of
the selected tables, and report success by means of an Unix return code of
zero.

At the moment, the ``pgcopydb compare`` tool is pretty limited in terms of
schema support: it only covers what pgcopydb needs to know about the
database schema, which isn't much.

::

   pgcopydb compare: Compare source and target databases

   Available commands:
     pgcopydb compare
       schema  Compare source and target schema
       data    Compare source and target data

.. _pgcopydb_compare_schema:

pgcopydb compare schema
-----------------------

pgcopydb compare schema - Compare source and target schema

The command ``pgcopydb compare schema`` connects to the source and target
databases and executes SQL queries using the Postgres catalogs to get a list
of tables, indexes, constraints and sequences there.

::

   pgcopydb compare schema: Compare source and target schema
   usage: pgcopydb compare schema  --source ...

     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --dir            Work directory to use


.. _pgcopydb_compare_data:

pgcopydb compare data
---------------------

pgcopydb compare data - Compare source and target data

The command ``pgcopydb compare data`` connects to the source and target
databases and executes SQL queries using the Postgres catalogs to get a list
of tables, indexes, constraints and sequences there.

Then it uses a SQL query with the following template to compute the row
count and a checksum for each table::

  select count(1) as cnt,
         sum(hashtext(_COLS_::text)::bigint) as chksum
     from only _TABLE_

Running such a query on a large table can take a lot of time.

::

   pgcopydb compare data: Compare source and target data
   usage: pgcopydb compare data  --source ...

     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --dir            Work directory to use


Options
-------

The following options are available to ``pgcopydb create`` and ``pgcopydb
drop`` subcommands:

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

Examples
--------

Comparing pgcopydb limited understanding of the schema:

::

   $ pgcopydb compare schema --notice
   15:08:47 24072 INFO   Running pgcopydb version 0.12.28.g34343c8.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb"
   15:08:47 24072 NOTICE Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   15:08:47 24072 NOTICE Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   15:08:47 24072 INFO   A previous run has run through completion
   15:08:47 24072 INFO   SOURCE: Connecting to "postgres:///pagila"
   15:08:47 24072 INFO   Fetched information for 1 extensions
   15:08:47 24072 INFO   Fetched information for 25 tables, with an estimated total of 5179  tuples and 190 MB
   15:08:48 24072 INFO   Fetched information for 49 indexes
   15:08:48 24072 INFO   Fetching information for 16 sequences
   15:08:48 24072 NOTICE Skipping target catalog preparation
   15:08:48 24072 NOTICE Storing migration schema in JSON file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/compare/source-schema.json"
   15:08:48 24072 INFO   TARGET: Connecting to "postgres:///plop"
   15:08:48 24072 INFO   Fetched information for 6 extensions
   15:08:48 24072 INFO   Fetched information for 25 tables, with an estimated total of 5219  tuples and 190 MB
   15:08:48 24072 INFO   Fetched information for 49 indexes
   15:08:48 24072 INFO   Fetching information for 16 sequences
   15:08:48 24072 NOTICE Skipping target catalog preparation
   15:08:48 24072 NOTICE Storing migration schema in JSON file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/compare/target-schema.json"
   15:08:48 24072 INFO   [SOURCE] table: 25 index: 49 sequence: 16
   15:08:48 24072 INFO   [TARGET] table: 25 index: 49 sequence: 16
   15:08:48 24072 NOTICE Matched table "public"."test": 1 columns ok, 0 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."rental": 7 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."film": 14 columns ok, 5 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."film_actor": 3 columns ok, 2 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."inventory": 4 columns ok, 2 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_03": 6 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_05": 6 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_06": 6 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_04": 6 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_02": 6 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_07": 6 columns ok, 0 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."customer": 10 columns ok, 4 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."address": 8 columns ok, 2 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."city": 4 columns ok, 2 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."film_category": 3 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."payment_p2022_01": 6 columns ok, 3 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."actor": 4 columns ok, 2 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."bar": 2 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."bin": 2 columns ok, 0 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."category": 3 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."country": 3 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."foo": 2 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."staff": 11 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."language": 3 columns ok, 1 indexes ok
   15:08:48 24072 NOTICE Matched table "public"."store": 4 columns ok, 2 indexes ok
   15:08:48 24072 NOTICE Matched sequence "public"."actor_actor_id_seq" (last value 200)
   15:08:48 24072 NOTICE Matched sequence "public"."address_address_id_seq" (last value 605)
   15:08:48 24072 NOTICE Matched sequence "public"."bar_id_seq" (last value 1)
   15:08:48 24072 NOTICE Matched sequence "public"."bin_id_seq" (last value 17)
   15:08:48 24072 NOTICE Matched sequence "public"."category_category_id_seq" (last value 16)
   15:08:48 24072 NOTICE Matched sequence "public"."city_city_id_seq" (last value 600)
   15:08:48 24072 NOTICE Matched sequence "public"."country_country_id_seq" (last value 109)
   15:08:48 24072 NOTICE Matched sequence "public"."customer_customer_id_seq" (last value 599)
   15:08:48 24072 NOTICE Matched sequence "public"."film_film_id_seq" (last value 1000)
   15:08:48 24072 NOTICE Matched sequence "public"."foo_id_seq" (last value 1)
   15:08:48 24072 NOTICE Matched sequence "public"."inventory_inventory_id_seq" (last value 4581)
   15:08:48 24072 NOTICE Matched sequence "public"."language_language_id_seq" (last value 6)
   15:08:48 24072 NOTICE Matched sequence "public"."payment_payment_id_seq" (last value 32102)
   15:08:48 24072 NOTICE Matched sequence "public"."rental_rental_id_seq" (last value 16053)
   15:08:48 24072 NOTICE Matched sequence "public"."staff_staff_id_seq" (last value 2)
   15:08:48 24072 NOTICE Matched sequence "public"."store_store_id_seq" (last value 2)
   15:08:48 24072 INFO   pgcopydb schema inspection is successful

Comparing data:

::

   $ pgcopydb compare data
   15:09:31 24090 INFO   Running pgcopydb version 0.12.28.g34343c8.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb"
   15:09:31 24090 INFO   A previous run has run through completion
   15:09:31 24090 INFO   SOURCE: Connecting to "postgres:///pagila"
   15:09:31 24090 INFO   Fetched information for 1 extensions
   15:09:31 24090 INFO   Fetched information for 25 tables, with an estimated total of 5179  tuples and 190 MB
   15:09:31 24090 INFO   Fetched information for 49 indexes
   15:09:31 24090 INFO   Fetching information for 16 sequences
   15:09:31 24090 INFO   TARGET: Connecting to "postgres:///plop"
   15:09:31 24090 INFO   Fetched information for 6 extensions
   15:09:31 24090 INFO   Fetched information for 25 tables, with an estimated total of 5219  tuples and 190 MB
   15:09:31 24090 INFO   Fetched information for 49 indexes
   15:09:31 24090 INFO   Fetching information for 16 sequences
   15:09:31 24090 INFO   Comparing data for 25 tables
   15:09:34 24090 INFO   pgcopydb data inspection is successful
                       Table Name |            Row Count |             Checksum
   -------------------------------+----------------------+---------------------
                  "public"."test" |              5173525 |     fffffe0eda6e8ed6
                "public"."rental" |                16044 |            a9e94a0fd
                  "public"."film" |                 1000 |            6c09234f3
            "public"."film_actor" |                 5462 |            62de3e446
             "public"."inventory" |                 4581 |            b8cd676ea
      "public"."payment_p2022_03" |                 2713 |              83be351
      "public"."payment_p2022_05" |                 2677 |           1f7db109e6
      "public"."payment_p2022_06" |                 2654 |           136e71d157
      "public"."payment_p2022_04" |                 2547 |     ffffffee3cc184de
      "public"."payment_p2022_02" |                 2401 |            46630a420
      "public"."payment_p2022_07" |                 2334 |            41ab5db80
              "public"."customer" |                  599 |     fffffffd9f34bcc0
               "public"."address" |                  603 |     fffffffe2feecfad
                  "public"."city" |                  600 |            408b30b2b
         "public"."film_category" |                 1000 |     fffffff7416d4e14
      "public"."payment_p2022_01" |                  723 |     fffffffb62e13a74
                 "public"."actor" |                  200 |             59093ce3
                   "public"."bar" |                    1 |             4b05576b
                   "public"."bin" |                   17 |     ffffffff8f6be7b1
              "public"."category" |                   16 |     fffffffd669034f7
               "public"."country" |                  109 |     fffffffd359c2b94
                   "public"."foo" |                    2 |             6bc8e3ff
                 "public"."staff" |                    2 |     ffffffff97467951
              "public"."language" |                    6 |            1922751a8
                 "public"."store" |                    2 |             441cc744
