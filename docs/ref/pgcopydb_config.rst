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

  [include-only-schema]
  public
  ~/^audit_/

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
  public.~/^tmp_/

  [exclude-index]
  schema.indexname

  [exclude-table-data]
  public.bar
  nsitra.test1

  [exclude-extension]
  aiven_extras

Pattern matching
^^^^^^^^^^^^^^^^

In any filter section that accepts schema-qualified names, each name component
(schema or table) may be written as a regular-expression pattern using the
syntax ``~/pattern/`` instead of a literal name.  The ``~/`` prefix and
trailing ``/`` are the delimiters; everything between them is a POSIX extended
regular expression matched with ``regcomp(3)`` / ``regexec(3)``.

Patterns may appear as the schema part, the name part, or both:

.. code-block:: ini

  [exclude-table]
  ; exact schema, regex table — excludes all tables starting with "tmp_" in public
  public.~/^tmp_/

  ; regex schema, exact table — excludes "audit_log" in any schema starting with "log"
  ~/^log/.audit_log

  ; regex schema, regex table — excludes any table ending with "_bak" in any "archive" schema
  ~/archive/.~/_bak$/

For ``[exclude-schema]`` and ``[include-only-schema]`` entries are bare schema
names and the whole entry is treated as the pattern:

.. code-block:: ini

  [exclude-schema]
  ; excludes every schema whose name starts with "tmp_"
  ~/^tmp_/

  [include-only-schema]
  public
  ; also include every schema whose name matches "audit_YYYY" (four digits)
  ~/^audit_[0-9]{4}$/

A pattern is matched against each object name from the source database catalog.
Entries that resolve to zero matching objects are silently ignored; pgcopydb
does not treat an unmatched pattern as an error.

Filtering can be done with pgcopydb by using the following rules, which are
also the name of the sections of the INI file.

include-only-table
^^^^^^^^^^^^^^^^^^

This section allows listing the exclusive list of the source tables to copy
to the target database. No other table will be processed by pgcopydb.

Each line in that section should be a schema-qualified table name. `Postgres
identifier quoting rules`__ can be used to avoid ambiguity.  Either the schema
component or the table component (or both) may be a ``~/pattern/`` regex
instead of a literal name; see `Pattern matching`_ above.

__ https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

When the section ``include-only-table`` is used in the filtering
configuration then the sections ``exclude-schema`` and ``exclude-table`` are
disallowed. We would not know how to handle tables that exist on the source
database and are not part of any filter.

NOTE: Materialized views are also considered as tables during the filtering.

exclude-schema
^^^^^^^^^^^^^^

This section allows adding schemas (Postgres namespaces) to the exclusion
filters. All the tables that belong to any listed schema in this section are
going to be ignored by the pgcopydb command.

Each line is a bare schema name or a ``~/pattern/`` regex that is matched
against schema names; see `Pattern matching`_ above.

This section is not allowed when the section ``include-only-table`` is
used.

include-only-schema
^^^^^^^^^^^^^^^^^^^

This section restricts processing to the listed schemas: only tables that
belong to one of the named schemas are copied; all others are skipped.  This
is a syntactic shortcut for excluding every other schema without having to
list them all under ``exclude-schema``.

Despite the name, this section is implemented as an exclusion filter.

Each line is a bare schema name or a ``~/pattern/`` regex that is matched
against schema names; see `Pattern matching`_ above.

This section is not allowed when the section ``exclude-schema`` is used.

exclude-table
^^^^^^^^^^^^^

This section allows to add a list of qualified table names to the exclusion
filters. All the tables that are listed in the ``exclude-table`` section are
going to be ignored by the pgcopydb command.

Each line is a schema-qualified table name.  Either the schema component or
the table component (or both) may be a ``~/pattern/`` regex; see `Pattern
matching`_ above.

This section is not allowed when the section ``include-only-table`` is
used.

NOTE: Materialized views are also considered as tables during the filtering.

exclude-index
^^^^^^^^^^^^^

This section allows to add a list of qualified index names to the exclusion
filters. It is then possible for pgcopydb to operate on a table and skip a
single index definition that belong to a table that is still processed.

Each line is a schema-qualified index name.  Either the schema component or
the index-name component (or both) may be a ``~/pattern/`` regex; see
`Pattern matching`_ below.

exclude-table-data
^^^^^^^^^^^^^^^^^^

This section allows to skip copying the data from a list of qualified table
names. The schema, index, constraints, etc of the table are still copied
over.

Each line is a schema-qualified table name.  Either the schema component or
the table component (or both) may be a ``~/pattern/`` regex; see `Pattern
matching`_ above.

NOTE: Materialized views are also considered as tables during the filtering.

exclude-extension
^^^^^^^^^^^^^^^^^

This section allows to skip specific PostgreSQL extensions by name. The
listed extensions are not created on the target database and their
configuration tables are not copied.

Each entry is a bare extension name (no schema prefix), one per line::

  [exclude-extension]
  aiven_extras
  timescaledb

This section is not allowed when the section ``include-only-extension`` is
used at the same time.

include-only-extension
^^^^^^^^^^^^^^^^^^^^^^

This section restricts extension handling to only the listed extensions.
All other extensions found in the source database are skipped — they are
not created on the target and their configuration tables are not copied.

Each entry is a bare extension name (no schema prefix), one per line::

  [include-only-extension]
  postgis
  uuid-ossp

This section is not allowed when the section ``exclude-extension`` is
used at the same time, and it cannot be combined with ``--skip-extensions``
(which skips all extensions).

Reviewing and Debugging the filters
-----------------------------------

Filtering a ``pg_restore`` archive file is done through rewriting the
archive catalog obtained with ``pg_restore --list``. That's a little hackish
at times, and we also have to deal with dependencies in pgcopydb itself.

The following commands can be used to explore a set of filtering rules:

  - :ref:`pgcopydb_list_depends`
  - :ref:`pgcopydb_restore_parse_list`
