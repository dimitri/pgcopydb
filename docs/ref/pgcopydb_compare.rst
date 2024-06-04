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

.. include:: ../include/compare.rst

.. _pgcopydb_compare_schema:

pgcopydb compare schema
-----------------------

pgcopydb compare schema - Compare source and target schema

The command ``pgcopydb compare schema`` connects to the source and target
databases and executes SQL queries using the Postgres catalogs to get a list
of tables, indexes, constraints and sequences there.

.. include:: ../include/compare-schema.rst

.. _pgcopydb_compare_data:

pgcopydb compare data
---------------------

pgcopydb compare data - Compare source and target data

The command ``pgcopydb compare data`` connects to the source and target
databases and executes SQL queries using the Postgres catalogs to get a list
of tables, indexes, constraints and sequences there.

Then it uses a SQL query with the following template to compute the row
count and a checksum for each table::

    /*
     * Compute the hashtext of every single row in the table, and aggregate the
     * results as a sum of bigint numbers. Because the sum of bigint could
     * overflow to numeric, the aggregated sum is then hashed into an MD5
     * value: bigint is 64 bits, MD5 is 128 bits.
     *
     * Also, to lower the chances of a collision, include the row count in the
     * computation of the MD5 by appending it to the input string of the MD5
     * function.
     */
    select count(1) as cnt,
           md5(
             format(
               '%%s-%%s',
               sum(hashtext(__COLS__::text)::bigint),
               count(1)
             )
           )::uuid as chksum
    from only __TABLE__

Running such a query on a large table can take a lot of time.

.. include:: ../include/compare-data.rst

Options
-------

The following options are available to ``pgcopydb compare schema`` and ``pgcopydb
compare data`` subcommands:

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

PGCOPYDB_TARGET_PGURI

  Connection string to the target Postgres instance. When ``--target`` is
  ommitted from the command line, then this environment variable is used.

Examples
--------

Comparing pgcopydb limited understanding of the schema:

::

   $ pgcopydb compare schema --notice
   INFO   Running pgcopydb version 0.12.28.g34343c8.dirty from "/Users/dim/dev/PostgreSQL/pgcopydb/src/bin/pgcopydb/pgcopydb"
   NOTICE Using work dir "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb"
   NOTICE Work directory "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb" already exists
   INFO   A previous run has run through completion
   INFO   SOURCE: Connecting to "postgres:///pagila"
   INFO   Fetched information for 1 extensions
   INFO   Fetched information for 25 tables, with an estimated total of 5179  tuples and 190 MB
   INFO   Fetched information for 49 indexes
   INFO   Fetching information for 16 sequences
   NOTICE Skipping target catalog preparation
   NOTICE Storing migration schema in JSON file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/compare/source-schema.json"
   INFO   TARGET: Connecting to "postgres:///plop"
   INFO   Fetched information for 6 extensions
   INFO   Fetched information for 25 tables, with an estimated total of 5219  tuples and 190 MB
   INFO   Fetched information for 49 indexes
   INFO   Fetching information for 16 sequences
   NOTICE Skipping target catalog preparation
   NOTICE Storing migration schema in JSON file "/var/folders/d7/zzxmgs9s16gdxxcm0hs0sssw0000gn/T//pgcopydb/compare/target-schema.json"
   INFO   [SOURCE] table: 25 index: 49 sequence: 16
   INFO   [TARGET] table: 25 index: 49 sequence: 16
   NOTICE Matched table "public"."test": 1 columns ok, 0 indexes ok
   NOTICE Matched table "public"."rental": 7 columns ok, 3 indexes ok
   NOTICE Matched table "public"."film": 14 columns ok, 5 indexes ok
   NOTICE Matched table "public"."film_actor": 3 columns ok, 2 indexes ok
   NOTICE Matched table "public"."inventory": 4 columns ok, 2 indexes ok
   NOTICE Matched table "public"."payment_p2022_03": 6 columns ok, 3 indexes ok
   NOTICE Matched table "public"."payment_p2022_05": 6 columns ok, 3 indexes ok
   NOTICE Matched table "public"."payment_p2022_06": 6 columns ok, 3 indexes ok
   NOTICE Matched table "public"."payment_p2022_04": 6 columns ok, 3 indexes ok
   NOTICE Matched table "public"."payment_p2022_02": 6 columns ok, 3 indexes ok
   NOTICE Matched table "public"."payment_p2022_07": 6 columns ok, 0 indexes ok
   NOTICE Matched table "public"."customer": 10 columns ok, 4 indexes ok
   NOTICE Matched table "public"."address": 8 columns ok, 2 indexes ok
   NOTICE Matched table "public"."city": 4 columns ok, 2 indexes ok
   NOTICE Matched table "public"."film_category": 3 columns ok, 1 indexes ok
   NOTICE Matched table "public"."payment_p2022_01": 6 columns ok, 3 indexes ok
   NOTICE Matched table "public"."actor": 4 columns ok, 2 indexes ok
   NOTICE Matched table "public"."bar": 2 columns ok, 1 indexes ok
   NOTICE Matched table "public"."bin": 2 columns ok, 0 indexes ok
   NOTICE Matched table "public"."category": 3 columns ok, 1 indexes ok
   NOTICE Matched table "public"."country": 3 columns ok, 1 indexes ok
   NOTICE Matched table "public"."foo": 2 columns ok, 1 indexes ok
   NOTICE Matched table "public"."staff": 11 columns ok, 1 indexes ok
   NOTICE Matched table "public"."language": 3 columns ok, 1 indexes ok
   NOTICE Matched table "public"."store": 4 columns ok, 2 indexes ok
   NOTICE Matched sequence "public"."actor_actor_id_seq" (last value 200)
   NOTICE Matched sequence "public"."address_address_id_seq" (last value 605)
   NOTICE Matched sequence "public"."bar_id_seq" (last value 1)
   NOTICE Matched sequence "public"."bin_id_seq" (last value 17)
   NOTICE Matched sequence "public"."category_category_id_seq" (last value 16)
   NOTICE Matched sequence "public"."city_city_id_seq" (last value 600)
   NOTICE Matched sequence "public"."country_country_id_seq" (last value 109)
   NOTICE Matched sequence "public"."customer_customer_id_seq" (last value 599)
   NOTICE Matched sequence "public"."film_film_id_seq" (last value 1000)
   NOTICE Matched sequence "public"."foo_id_seq" (last value 1)
   NOTICE Matched sequence "public"."inventory_inventory_id_seq" (last value 4581)
   NOTICE Matched sequence "public"."language_language_id_seq" (last value 6)
   NOTICE Matched sequence "public"."payment_payment_id_seq" (last value 32102)
   NOTICE Matched sequence "public"."rental_rental_id_seq" (last value 16053)
   NOTICE Matched sequence "public"."staff_staff_id_seq" (last value 2)
   NOTICE Matched sequence "public"."store_store_id_seq" (last value 2)
   INFO   pgcopydb schema inspection is successful

Comparing data:

::

   $ pgcopydb compare data
   INFO   A previous run has run through completion
   INFO   SOURCE: Connecting to "postgres:///pagila"
   INFO   Fetched information for 1 extensions
   INFO   Fetched information for 25 tables, with an estimated total of 5179  tuples and 190 MB
   INFO   Fetched information for 49 indexes
   INFO   Fetching information for 16 sequences
   INFO   TARGET: Connecting to "postgres:///plop"
   INFO   Fetched information for 6 extensions
   INFO   Fetched information for 25 tables, with an estimated total of 5219  tuples and 190 MB
   INFO   Fetched information for 49 indexes
   INFO   Fetching information for 16 sequences
   INFO   Comparing data for 25 tables
   ERROR  Table "public"."test" has 5173526 rows on source, 5173525 rows on target
   ERROR  Table "public"."test" has checksum be66f291-2774-9365-400c-1ccd5160bdf on source, 8be89afa-bceb-f501-dc7b-0538dc17fa3 on target
   ERROR  Table "public"."foo" has 3 rows on source, 2 rows on target
   ERROR  Table "public"."foo" has checksum a244eba3-376b-75e6-6720-e853b485ef6 on source, 594ae64d-2216-f687-2f11-45cbd9c7153 on target
                       Table Name | ! |                      Source Checksum |                      Target Checksum
   -------------------------------+---+--------------------------------------+-------------------------------------
                  "public"."test" | ! |  be66f291-2774-9365-400c-1ccd5160bdf |  8be89afa-bceb-f501-dc7b-0538dc17fa3
                "public"."rental" |   |  e7dfabf3-baa8-473a-8fd3-76d59e56467 |  e7dfabf3-baa8-473a-8fd3-76d59e56467
                  "public"."film" |   |  c5058d1e-aaf4-f058-6f1e-76d5db63da9 |  c5058d1e-aaf4-f058-6f1e-76d5db63da9
            "public"."film_actor" |   |  7534654a-0bcd-cb27-1a2e-ccd524899a9 |  7534654a-0bcd-cb27-1a2e-ccd524899a9
             "public"."inventory" |   |  72f9afd8-0064-3642-acd7-9ee1f444efe |  72f9afd8-0064-3642-acd7-9ee1f444efe
      "public"."payment_p2022_03" |   |  dc73311a-2ea2-e933-da80-123b44d06b9 |  dc73311a-2ea2-e933-da80-123b44d06b9
      "public"."payment_p2022_05" |   |  e788bf50-9809-9896-8110-91816edcc04 |  e788bf50-9809-9896-8110-91816edcc04
      "public"."payment_p2022_06" |   |  5f650b4c-d491-37ac-6d91-dc2ae484600 |  5f650b4c-d491-37ac-6d91-dc2ae484600
      "public"."payment_p2022_04" |   |  02beb400-1b82-c9ba-8fe9-690eca2e635 |  02beb400-1b82-c9ba-8fe9-690eca2e635
      "public"."payment_p2022_02" |   |  97154691-488e-9a36-9a4b-4da7b62dbc0 |  97154691-488e-9a36-9a4b-4da7b62dbc0
      "public"."payment_p2022_07" |   |  c6fdf7ef-4382-b301-41c3-1d190149dc5 |  c6fdf7ef-4382-b301-41c3-1d190149dc5
              "public"."customer" |   |  11973c6a-6df3-c502-5495-64f42e0386c |  11973c6a-6df3-c502-5495-64f42e0386c
               "public"."address" |   |  8c701dbf-c1ba-f386-a9ae-c3f6e478ba7 |  8c701dbf-c1ba-f386-a9ae-c3f6e478ba7
                  "public"."city" |   |  f23ad758-f94a-a8fd-8c3f-25fedcadb06 |  f23ad758-f94a-a8fd-8c3f-25fedcadb06
         "public"."film_category" |   |  4b04cfee-e1bc-718d-d890-afdcd6729ce |  4b04cfee-e1bc-718d-d890-afdcd6729ce
      "public"."payment_p2022_01" |   |  fde341ed-0f3f-23bd-dedd-4e92c5a8e55 |  fde341ed-0f3f-23bd-dedd-4e92c5a8e55
                 "public"."actor" |   |  b5ea389d-140f-10b4-07b9-a80d634d86b |  b5ea389d-140f-10b4-07b9-a80d634d86b
                   "public"."bar" |   |  a7cae1c8-ed66-63ba-1b93-7ba7570ef63 |  a7cae1c8-ed66-63ba-1b93-7ba7570ef63
                   "public"."bin" |   |  6832546a-333b-3bdb-fdf2-325cc7a028a |  6832546a-333b-3bdb-fdf2-325cc7a028a
              "public"."category" |   |  082f9cf9-92ab-6d6c-c74a-feb577611cc |  082f9cf9-92ab-6d6c-c74a-feb577611cc
               "public"."country" |   |  a3a0dd4f-68e0-4ca5-33d2-05c9fd60c34 |  a3a0dd4f-68e0-4ca5-33d2-05c9fd60c34
                   "public"."foo" | ! |  a244eba3-376b-75e6-6720-e853b485ef6 |  594ae64d-2216-f687-2f11-45cbd9c7153
                 "public"."staff" |   |  3eb5f007-7160-81ba-5aa5-973de3f5c3d |  3eb5f007-7160-81ba-5aa5-973de3f5c3d
              "public"."language" |   |  58aa8132-11ae-f3bc-fa82-c773bba2032 |  58aa8132-11ae-f3bc-fa82-c773bba2032
                 "public"."store" |   |  d8477e63-0661-90a4-03fa-fcc26a95865 |  d8477e63-0661-90a4-03fa-fcc26a95865
