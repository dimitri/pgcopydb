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
of the data. Two worker processes are created to handle the logical decoding
client:

  - The **receive** process fetches data from the Postgres replication slot
    using the Postgres replication protocol and stores the decoded messages
    into the *output table* of a SQLite database (the ``*-output.db`` file).

  - The **apply** process reads from the output table, transforms the decoded
    messages into parameterized SQL statements (stored in the *stmt* and
    *replay* tables of the ``*-replay.db`` file), and applies those statements
    to the target Postgres database system, using Postgres APIs for
    `Replication Progress Tracking`__.

    __ https://www.postgresql.org/docs/current//replication-origins.html

The transform step is no longer a separate worker process: it is now done
*inline* by the apply process, which reads the output table and produces the
replay statements as part of the same catchup loop. There is no standalone
``stream transform`` command anymore.

The SQLite-based CDC pipeline uses a unified **catchup mode** for all
operations. Both worker processes (receive, apply) can restart in the same
mode every time. The SQLite databases provide inter-process communication,
eliminating the need for complex mode-switching or Unix pipes.

During the initial COPY phase of operations, pgcopydb follow starts the two
worker processes which then:

  1. **Receive** writes raw logical decoding messages to the output table in
     the ``*-output.db`` file.
  2. **Apply** continuously reads from the output table, transforms entries
     into parameterized SQL in the ``*-replay.db`` file (stmt and replay
     tables), and applies those changes to the target database, tracking
     progress via replication origins.

This pipelined approach allows both phases to operate concurrently, with the
SQLite databases acting as a durable intermediate store and providing
transaction-level synchronization between processes. Changes are applied with
transaction granularity, and the system can be safely paused/resumed from the
last applied LSN position.

Internal CDC Storage Format
^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
parse the output plugin syntax and make sense of it. Internally, pgcopydb
stores the decoded messages in two SQLite databases: the ``*-output.db`` file
holds the raw decoded stream, and the ``*-replay.db`` file holds the
transformed SQL statements. Together they use three main tables:

- **output table** (in ``*-output.db``): Stores the raw decoded messages from
  the logical decoding output plugin, with metadata about the logical operation
  and a normalized copy of the output plugin message.

- **replay table** (in ``*-replay.db``): Stores the transformed SQL statements,
  with references to the prepared statements in the stmt table.

- **stmt table** (in ``*-replay.db``): A dictionary of prepared statements that
  can be reused across multiple EXECUTE calls, with the actual SQL and a hash
  for fast lookup.

The apply process converts the output table entries into parameterized SQL
using `prepared statements`__ as an optimization, as part of its inline
transform step. This means that pgcopydb efficiently reuses prepared statements
across multiple rows, avoiding the overhead of re-preparing identical
statements.

__ https://www.postgresql.org/docs/current/sql-prepare.html

For example, a single INSERT template might be prepared once and then executed
multiple times with different parameter values, providing a huge performance
boost compared to individual INSERT statements.

The pgcopydb apply process reads from the replay table and executes the
statements on the target database. It automatically handles statement caching
and knows how to skip re-preparing statements that have already been prepared
in the current session.

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

