.. _pgcopydb_list:

pgcopydb list
=============

pgcopydb list - List database objects from a Postgres instance

This command prefixes the following sub-commands:

::

  pgcopydb list
    tables     List all the source tables to copy data from
    sequences  List all the source sequences to copy data from
    indexes    List all the indexes to create again after copying the data


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

     --source          Postgres URI to the source database
     --without-pkey    List only tables that have no primary key

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

     --source          Postgres URI to the source database

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

    --source          Postgres URI to the source database
    --schema-name     Name of the schema where to find the table
    --table-name      Name of the target table


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
