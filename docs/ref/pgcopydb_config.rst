.. _config:

pgcopydb configuration
======================

Manual page for the configuration of pgcopydb. The ``pgcopydb`` command
accepts sub-commands and command line options, see the manual for those
commands for details. The only setup that ``pgcopydb`` commands accept is
the filtering.

.. _filtering:

Filtering
---------

Filtering allows to skip some object definitions and data when copying from
the source to the target database. The pgcopydb commands that accept the
option ``--filter`` (or ``--filters``) expect an existing filename as the
option argument. The given filename is read in the INI file format, but only
uses sections and option keys. Option values are not used.

Here is an inclusion based filter configuration example:

.. code-block:: ini
  :linenos:

  [include-only-table]
  public.allcols
  public.csv
  public.serial
  public.xzero

  [exclude-index]
  public.foo_gin_tsvector

  [exclude-table-data]
  public.csv

Here is an exclusion based filter configuration example:

.. code-block:: ini
  :linenos:

  [exclude-schema]
  foo
  bar
  expected

  [exclude-table]
  "schema"."name"
  schema.othername
  err.errors
  public.serial

  [exclude-index]
  schema.indexname

  [exclude-table-data]
  public.bar
  nsitra.test1

Filtering can be done with pgcopydb by using the following rules, which are
also the name of the sections of the INI file.

include-only-table
^^^^^^^^^^^^^^^^^^

This section allows listing the exclusive list of the source tables to copy
to the target database. No other table will be processed by pgcopydb.

Each line in that section should be a schema-qualified table name. `Postgres
identifier quoting rules`__ can be used to avoid ambiguity.

__ https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

When the section ``include-only-table`` is used in the filtering
configuration then the sections ``exclude-schema`` and ``exclude-table`` are
disallowed. We would not know how to handle tables that exist on the source
database and are not part of any filter.

exclude-schema
^^^^^^^^^^^^^^

This section allows adding schemas (Postgres namespaces) to the exclusion
filters. All the tables that belong to any listed schema in this section are
going to be ignored by the pgcopydb command.

This section is not allowed when the section ``include-only-table`` is
used.

exclude-table
^^^^^^^^^^^^^

This section allows to add a list of qualified table names to the exclusion
filters. All the tables that are listed in the ``exclude-table`` section are
going to be ignored by the pgcopydb command.

This section is not allowed when the section ``include-only-table`` is
used.

exclude-index
^^^^^^^^^^^^^

This section allows to add a list of qualified index names to the exclusion
filters. It is then possible for pgcopydb to operate on a table and skip a
single index definition that belong to a table that is still processed.

exclude-table-data
^^^^^^^^^^^^^^^^^^

This section allows to skip copying the data from a list of qualified table
names. The schema, index, constraints, etc of the table are still copied
over.

Reviewing and Debugging the filters
-----------------------------------

Filtering a ``pg_restore`` archive file is done through rewriting the
archive catalog obtained with ``pg_restore --list``. That's a little hackish
at times, and we also have to deal with dependencies in pgcopydb itself.

The following commands can be used to explore a set of filtering rules:

  - :ref:`pgcopydb_list_depends`
  - :ref:`pgcopydb_restore_parse_list`
