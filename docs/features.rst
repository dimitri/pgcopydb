Features Highlights
===================

``pgcopydb`` project was started to allow certain improvements and considerations
which were otherwise not possible to achieve directly with ``pg_dump`` and
``pg_restore`` commands. Below are the details of what ``pgcopydb`` can achieve.

.. _bypass_intermediate_files:

Bypass intermediate files for the TABLE DATA
--------------------------------------------

First aspect is that for ``pg_dump`` and ``pg_restore`` to implement
concurrency, they need to write to an intermediate file first.

The `docs for pg_dump`__ say the following about the ``--jobs`` parameter:

__ https://www.postgresql.org/docs/current/app-pgdump.html

.. admonition:: From the PostgreSQL documentation

  You can only use this option with the directory output format because this
  is the only output format where multiple processes can write their data at
  the same time.

The `docs for pg_restore`__ say the following about the ``--jobs``
parameter:

__ https://www.postgresql.org/docs/current/app-pgrestore.html

.. admonition:: From the PostgreSQL documentation

  Only the custom and directory archive formats are supported with this
  option. The input must be a regular file or directory (not, for example, a
  pipe or standard input).

So the first idea with ``pgcopydb`` is to provide the ``--jobs`` concurrency and
bypass intermediate files (and directories) altogether, at least as far as
the actual TABLE DATA set is concerned.

The trick to achieve that is that ``pgcopydb`` must be able to connect to the
source database during the whole operation, whereas ``pg_restore`` may be used
from an export on-disk, without having to still be able to connect to the
source database. In the context of ``pgcopydb``, requiring access to the source
database is fine. In the context of ``pg_restore``, it would not be
acceptable.

Large-Objects Support
---------------------

The `Postgres Large-Objects`__ API is nobody's favorite, though the
trade-offs implemented in that API are found to be very useful by many application
developers. In the context dump and restore, Postgres separates the large
objects metadata from the large object contents.

__ https://www.postgresql.org/docs/current/largeobjects.html

Specifically, the metadata consists of a large-object OID and ACLs, and is
considered to be part of the pre-data section of a Postgres dump.

This means that pgcopydb relies on ``pg_dump`` to import the large object
metadata from the source to the target Postgres server, but then implements
its own logic to migrate the large objects contents, using several worker
processes depending on the setting of the command-line option
``--large-objects-jobs``.

Concurrency
-----------

A major feature of pgcopydb is how concurrency is implemented, including
options to obtain same-table COPY concurrency. See the
:ref:`pgcopydb_concurrency` chapter of the documentation for more information.

Change Data Capture
-------------------

pgcopydb implements full Postgres replication solution based on the
lower-level API for `Postgres Logical Decoding`__. This allows pgcopydb to
be compatible with old versions of Postgres, starting with version 9.4.

__ https://www.postgresql.org/docs/current/logicaldecoding.html

Always do a test migration first without the ``--follow`` option to have an
idea of the downtime window needed for your very own case. This will inform
your decision about using the Change Data Capture mode, which makes a
migration a lot more complex to drive to success.

PostgreSQL Logical Decoding Client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The replication client of pgcopydb has been designed to be able to fetch
changes from the source Postgres instance concurrently to the initial COPY
of the data. Three worker processes are created to handle the logical
decoding client:

  - The **streaming** process fetches data from the Postgres replication slot
    using the Postgres replication protocol.

  - The **transform** process transforms the data fetched from an intermediate
    JSON format into a derivative of the SQL language. In *prefetch mode*
    this is implemented as a batch operation; in *replay mode* this is done
    in a streaming fashion, one line at a time, reading from a unix pipe.

  - The **apply** process then applies the SQL script to the target Postgres
    database system and uses Postgres APIs for `Replication Progress
    Tracking`__.

    __ https://www.postgresql.org/docs/current//replication-origins.html

During the initial COPY phase of operations, pgcopydb follow runs in
prefetch mode and does not apply changes yet. After the initial COPY is
done, then pgcopy replication system enters a loop that switches between the
following two modes of operation:

  1. In **prefetch mode**, changes are stored to JSON files on-disk, the
     transform process operates on files when a SWITCH occurs, and the apply
     process catches-up with changes on-disk by applying one file at time.

     When the next file to apply does not exists (yet), then the 3 transform
     worker processes stop and the main follow supervisor process then
     switches to *replay mode*.

  2. In **replay mode** changes are streamed from the streaming worker
     process to the transform worker process using a Unix PIPE mechanism,
     and the obtained SQL statements are sent to the replay worker process
     using another Unix PIPE.

     Changes are then replayed in a streaming fashion, end-to-end, with a
     transaction granularity.

The internal SQL-like script format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Postgres Logical Decoding API does not provide a CDC format, instead it
allows Postgres extension developers to implement *logical decoding output
plugins*. The Postgres core distribution implements such an output plugin
named `test_decoding`__. Another commonly used output plugin is named
`wal2json`__.

__ https://www.postgresql.org/docs/16/test-decoding.html
__ https://github.com/eulerto/wal2json

pgcopydb is compatible with both ``test_decoding`` and ``wal2json`` plugins. 
As a user it's possible to choose an output plugin with the ``--plugin``
command-line option.

The output plugin compatibility means that pgcopydb has to implement code to
parse the output plugin syntax and make sense of it. Internally, the
messages from the output plugin are stored by pgcopydb in a `JSON Lines`__
formatted file, where each line is a JSON record with decoded metadata about
the changes and the output plugin message, as-is.

__ https://jsonlines.org

This JSON Lines format is transformed into SQL scripts. At first, pgcopydb
would just use SQL for the intermediate format, but then support for 
`prepared statements`__ was added  as an optimization. This means that our SQL
script uses commands such as the following examples::

  PREPARE d33a643f AS INSERT INTO public.rental ("rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14);
  EXECUTE d33a643f["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00","16051","2022-06-01 00:00:00+00","373","293",null,"2","2022-06-01 00:00:00+00"];

__ https://www.postgresql.org/docs/current/sql-prepare.html

As you can see in the example, pgcopydb is now able to use a single INSERT
statement with multiple VALUES, which is a huge performance boost. In order
to simplify pgcopydb parsing of the SQL syntax, the choice was made to
format the EXECUTE argument list as a JSON array, which does not comply with
the actual SQL syntax, but is simple and fast to process.

Finally, it's not possible for the transform process to anticipate the
actual session management of the apply process, so SQL statements are always
included with both the PREPARE and the EXECUTE steps. The pgcopydb apply
code knows how to skip PREPARing again, of course.

Unfortunately that means that our SQL files are not actually using SQL
syntax and can't be processed as-is with any SQL client software. At the
moment either using :ref:`pgcopydb_stream_apply` or writing your own
processing code is required.

.. _catalogs:

Internal Catalogs (SQLite)
--------------------------

To be able to implement pgcopydb operations, a list of SQL objects such as
tables, indexes, constraints and sequences is needed internally. While
pgcopydb used to handle such a list as an array in-memory, with also a
hash-table for direct lookup (by oid and by *restore list name*), in some
cases the source database contains so many objects that these arrays do not
fit in memory.

As pgcopydb is written in C, the current best approach to handle an array of
objects that needs to spill to disk and supports direct lookup is actually
the SQLite library, file format, and embedded database engine.

That's why the current version of pgcopydb uses SQLite to handle its
catalogs.

Internally pgcopydb stores metadata information in three different catalogs,
all found in the ``${TMPDIR}/pgcopydb/schema/`` directory by default, unless
using the recommended ``--dir`` option.

  - The **source** catalog registers metadata about the source database, and
    also some metadata about the pgcopydb context, consistency, and
    progress.

  - The **filters** catalog is only used when the ``--filters`` option is
    provided, and it registers metadata about the objects in the source database
    that are going to be skipped.

    This is necessary because the filtering is implemented using the
    ``pg_restore --list`` and ``pg_restore --use-list`` options. The
    Postgres archive Table Of Contents format contains an object OID and its
    *restore list name*, and pgcopydb needs to be able to lookup for that
    OID or name in its filtering catalogs.

  - The **target** catalog registers metadata about the target database,
    such as the list of roles, the list of schemas, or the list of already
    existing constraints found on the target database.

