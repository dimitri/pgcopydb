Design Considerations
=====================

The reason why ``pgcopydb`` has been developed is mostly to allow two
aspects that are not possible to achieve directly with ``pg_dump`` and
``pg_restore``, and that requires just enough fiddling around that not many
scripts have been made available to automate around.

Bypass intermediate files for the TABLE DATA
--------------------------------------------

First aspect is that for ``pg_dump`` and ``pg_restore`` to implement
concurrency they need to write to an intermediate file first.

The `docs for pg_dump`__ say the following about the ``--jobs`` parameter:

__ https://www.postgresql.org/docs/current/app-pgdump.html

  You can only use this option with the directory output format because this
  is the only output format where multiple processes can write their data at
  the same time.

The `docs for pg_restore`__ say the following about the ``--jobs``
parameter:

__ https://www.postgresql.org/docs/current/app-pgrestore.html

  Only the custom and directory archive formats are supported with this
  option. The input must be a regular file or directory (not, for example, a
  pipe or standard input).

So the first idea with ``pgcopydb`` is to provide the ``--jobs`` concurrency and
bypass intermediate files (and directories) altogether, at least as far as
the actual TABLE DATA set is concerned.

The trick to achieve that is that ``pgcopydb`` must be able to connect to the
source database during the whole operation, when ``pg_restore`` may be used
from an export on-disk, without having to still be able to connect to the
source database. In the context of ``pgcopydb`` requiring access to the source
database is fine. In the context of ``pg_restore``, it would not be
acceptable.

.. _index_concurrency:

For each table, build all indexes concurrently
----------------------------------------------

The other aspect that ``pg_dump`` and ``pg_restore`` are not very smart about is
how they deal with the indexes that are used to support constraints, in
particular unique constraints and primary keys.

Those indexes are exported using the ``ALTER TABLE`` command directly. This is
fine because the command creates both the constraint and the underlying
index, so the schema in the end is found as expected.

That said, those ``ALTER TABLE ... ADD CONSTRAINT`` commands require a level
of locking that prevents any concurrency. As we can read on the `docs for
ALTER TABLE`__:

__ https://www.postgresql.org/docs/current/sql-altertable.html

  Although most forms of ADD table_constraint require an ACCESS EXCLUSIVE
  lock, ADD FOREIGN KEY requires only a SHARE ROW EXCLUSIVE lock. Note that
  ADD FOREIGN KEY also acquires a SHARE ROW EXCLUSIVE lock on the referenced
  table, in addition to the lock on the table on which the constraint is
  declared.

The trick is then to first issue a ``CREATE UNIQUE INDEX`` statement and when
the index has been built then issue a second command in the form of ``ALTER
TABLE ... ADD CONSTRAINT ... PRIMARY KEY USING INDEX ...``, as in the
following example taken from the logs of actually running ``pgcopydb``::

  21:52:06 68898 INFO  COPY "demo"."tracking";
  21:52:06 68899 INFO  COPY "demo"."client";
  21:52:06 68899 INFO  Creating 2 indexes for table "demo"."client"
  21:52:06 68906 INFO  CREATE UNIQUE INDEX client_pkey ON demo.client USING btree (client);
  21:52:06 68907 INFO  CREATE UNIQUE INDEX client_pid_key ON demo.client USING btree (pid);
  21:52:06 68898 INFO  Creating 1 indexes for table "demo"."tracking"
  21:52:06 68908 INFO  CREATE UNIQUE INDEX tracking_pkey ON demo.tracking USING btree (client, ts);
  21:52:06 68907 INFO  ALTER TABLE "demo"."client" ADD CONSTRAINT "client_pid_key" UNIQUE USING INDEX "client_pid_key";
  21:52:06 68906 INFO  ALTER TABLE "demo"."client" ADD CONSTRAINT "client_pkey" PRIMARY KEY USING INDEX "client_pkey";
  21:52:06 68908 INFO  ALTER TABLE "demo"."tracking" ADD CONSTRAINT "tracking_pkey" PRIMARY KEY USING INDEX "tracking_pkey";

This trick is worth a lot of performance gains on its own, as has been
discovered and experienced and appreciated by `pgloader`__ users already.

__ https://github.com/dimitri/pgloader
