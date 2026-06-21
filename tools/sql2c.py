#!/usr/bin/env python3
"""
tools/sql2c.py — SQL-to-C generator for pgcopydb

Reads SQL files from a directory tree and emits two C files:
    <output>.h  — enum types and function declarations
    <output>.c  — static string constants and dispatch functions

Directory layout
----------------
    <sql-dir>/<query>/[<N>-<dim>/]<version>.sql

  <query>     Function name suffix; becomes pgcopydb_sql_<query>().
  <N>-<dim>   Optional numbered filter-dimension directory.  N is the
              integer enum value; <dim> becomes the enum label.
              When absent the query takes no filter argument.
  <version>   Version specifier (without .sql):

                default    — matches any server version (lowest priority)
                pg-96      — server version < 100000 (before PG 10)
                pg10       — 100000 <= version <= 109999 (PG 10.x)
                pg10-12    — 100000 <= version <= 129999 (PG 10 through 12)
                pg12-      — version >= 120000 (PG 12 and later)

Generated API (Option B — enum + 3-arg)
-----------------------------------------
  typedef enum SqlListSourceTablesFilter {
      SQL_LIST_SOURCE_TABLES_NONE = 0,
      ...
  } SqlListSourceTablesFilter;

  bool pgcopydb_sql_list_source_tables(int pg_version,
                                        SqlListSourceTablesFilter filter,
                                        const char **sql);
"""

import os
import re
import sys
from pathlib import Path
from collections import OrderedDict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_c_ident(s):
    return re.sub(r'[^a-zA-Z0-9]', '_', s).strip('_')


def camel(s):
    """list_source_tables -> ListSourceTables"""
    return ''.join(w.capitalize() for w in s.split('_'))


def parse_version_specifier(ver):
    """Return (min_ver, max_ver) for a version filename stem.

    (0, 0) means no constraint (default, lowest priority).
    0 for min means no lower bound; 0 for max means no upper bound.
    """
    if ver == 'default':
        return (0, 0)

    m = re.fullmatch(r'pg(-?)(\d+)(?:-(\d*))?(-?)', ver)
    if not m:
        raise ValueError(f"Cannot parse version specifier: {ver!r}")

    leading_dash  = m.group(1)   # '-' in pg-96
    first         = m.group(2)   # '96', '10', '12'
    mid_second    = m.group(3)   # '12' in pg10-12; '' in pg12-; None in pg10
    trailing_dash = m.group(4)   # '-' in pg12-

    def to_min(s):
        n = int(s)
        if 90 <= n <= 99:
            # Old two-digit notation: "96" means PG 9.6 -> version 90600
            return (n // 10) * 10000 + (n % 10) * 100
        return n * 10000             # PG 10 -> 100000, PG 12 -> 120000

    def to_max_inclusive(s):
        n = int(s)
        if 90 <= n <= 99:
            # "96" -> include all of PG 9.x -> everything before PG 10 = 99999
            return ((n // 10) + 1) * 10000 - 1
        return n * 10000 + 9999      # PG 10 -> 109999, PG 12 -> 129999

    if leading_dash and not trailing_dash:
        # pg-96: no min, max inclusive of that version's family
        return (0, to_max_inclusive(first))

    if not leading_dash and trailing_dash and mid_second == '':
        # pg12-: min = PG 12, no max
        return (to_min(first), 0)

    if not leading_dash and mid_second is not None and mid_second != '':
        # pg10-12: range
        return (to_min(first), to_max_inclusive(mid_second))

    if not leading_dash and mid_second is None:
        # pg10: exactly that major version
        return (to_min(first), to_max_inclusive(first))

    raise ValueError(f"Unhandled version specifier: {ver!r}")


def version_priority(v):
    """Sort key: most constrained first, default last."""
    mn, mx = v['min'], v['max']
    has_min = mn > 0
    has_max = mx > 0
    if has_min and has_max:
        return (0, -mn)
    if has_min or has_max:
        return (1, -max(mn, mx))
    return (2, 0)


def c_string_lines(path):
    """Read a SQL file and return a list of C string literal lines."""
    lines = []
    with open(path) as f:
        content = f.read()
    for line in content.split('\n'):
        escaped = line.replace('\\', '\\\\').replace('"', '\\"')
        lines.append(f'    "{escaped}\\n"')
    # Strip trailing empty-line entries that would add a bare \n at end
    while lines and lines[-1] == '    "\\n"':
        lines.pop()
    return lines


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover(sql_dir):
    """Walk sql_dir and build an ordered dict of query info."""
    sql_dir = Path(sql_dir)
    queries = OrderedDict()

    for qdir in sorted(sql_dir.iterdir()):
        if not qdir.is_dir():
            continue
        query = qdir.name
        qdims = sorted(d for d in qdir.iterdir() if d.is_dir())
        qfiles = sorted(qdir.glob('*.sql'))

        if qdims:
            # Has numbered dimension subdirectories
            dims = []
            for dimdir in qdims:
                m = re.fullmatch(r'(\d+)-(.*)', dimdir.name)
                if not m:
                    raise ValueError(
                        f"Dimension directory must match N-name: {dimdir}")
                enum_val = int(m.group(1))
                label    = m.group(2)
                versions = []
                for sqlfile in sorted(dimdir.glob('*.sql')):
                    ver = sqlfile.stem
                    mn, mx = parse_version_specifier(ver)
                    versions.append(
                        {'ver': ver, 'min': mn, 'max': mx, 'path': sqlfile})
                versions.sort(key=version_priority)
                dims.append({'name': dimdir.name, 'label': label,
                             'enum_val': enum_val, 'versions': versions})
            dims.sort(key=lambda d: d['enum_val'])
            queries[query] = {'has_dims': True, 'dims': dims}

        elif qfiles:
            versions = []
            for sqlfile in qfiles:
                ver = sqlfile.stem
                mn, mx = parse_version_specifier(ver)
                versions.append(
                    {'ver': ver, 'min': mn, 'max': mx, 'path': sqlfile})
            versions.sort(key=version_priority)
            queries[query] = {'has_dims': False, 'versions': versions}

    return queries


# ---------------------------------------------------------------------------
# Code emission
# ---------------------------------------------------------------------------

HEADER_TOP = """\
/*
 * GENERATED FILE - do not edit.
 * Source: src/bin/pgcopydb/sql/
 * Run 'make gen-sql' after modifying .sql files.
 */

#ifndef PGCOPYDB_SQL_QUERIES_H
#define PGCOPYDB_SQL_QUERIES_H

#include <stdbool.h>

"""

HEADER_BOT = """\
#endif  /* PGCOPYDB_SQL_QUERIES_H */
"""

SOURCE_TOP = """\
/*
 * GENERATED FILE - do not edit.
 * Source: src/bin/pgcopydb/sql/
 * Run 'make gen-sql' after modifying .sql files.
 */

#include "sql_queries.h"

"""


def emit_version_dispatch(f, varname_prefix, versions, indent):
    """Emit if-chain for version dispatch, returning via *sql."""
    i = indent
    for v in versions:
        mn, mx = v['min'], v['max']
        vname = f"{varname_prefix}__{to_c_ident(v['ver'])}"
        if mn == 0 and mx == 0:
            # default — always matches, write unconditionally (fallback)
            f.write(f"{i}*sql = {vname};\n")
            f.write(f"{i}return true;\n")
            return   # nothing after default
        elif mn > 0 and mx > 0:
            cond = f"pg_version >= {mn} && pg_version <= {mx}"
        elif mx > 0:
            cond = f"pg_version <= {mx}"
        else:
            cond = f"pg_version >= {mn}"
        f.write(f"{i}if ({cond})\n")
        f.write(f"{i}{{\n")
        f.write(f"{i}    *sql = {vname};\n")
        f.write(f"{i}    return true;\n")
        f.write(f"{i}}}\n")

    # If we get here there was no default — caller returns false
    f.write(f"{i}return false;\n")


def generate(sql_dir, hdr_path, src_path):
    queries = discover(sql_dir)

    # ---------------------------------------------------------------- header
    with open(hdr_path, 'w') as h:
        h.write(HEADER_TOP)

        for query, info in queries.items():
            qident = to_c_ident(query)
            sep = '/' * 62
            h.write(f"/{sep}/\n")
            h.write(f"/* {query:<60} */\n")
            h.write(f"/{sep}/\n\n")

            if info['has_dims']:
                enum_type = f"Sql{camel(qident)}Filter"
                h.write(f"typedef enum {enum_type}\n{{\n")
                for dim in info['dims']:
                    const = (f"SQL_{qident.upper()}_"
                             f"{to_c_ident(dim['label']).upper()}")
                    h.write(f"    {const} = {dim['enum_val']},\n")
                h.write(f"}} {enum_type};\n\n")

                pad = ' ' * (len("bool pgcopydb_sql_") + len(qident) + 1)
                h.write(f"bool pgcopydb_sql_{qident}(int pg_version,\n")
                h.write(f"{pad}{enum_type} filter,\n")
                h.write(f"{pad}const char **sql);\n\n")
            else:
                pad = ' ' * (len("bool pgcopydb_sql_") + len(qident) + 1)
                h.write(f"bool pgcopydb_sql_{qident}(int pg_version,\n")
                h.write(f"{pad}const char **sql);\n\n")

        h.write(HEADER_BOT)

    # ---------------------------------------------------------------- source
    with open(src_path, 'w') as c:
        c.write(SOURCE_TOP)

        for query, info in queries.items():
            qident = to_c_ident(query)
            sep = '/' * 62
            c.write(f"/{sep}/\n")
            c.write(f"/* {query:<60} */\n")
            c.write(f"/{sep}/\n\n")

            if info['has_dims']:
                enum_type = f"Sql{camel(qident)}Filter"

                # Static string constants
                for dim in info['dims']:
                    for v in dim['versions']:
                        vident = to_c_ident(v['ver'])
                        vname  = (f"sql__{qident}"
                                  f"__{to_c_ident(dim['label'])}__{vident}")
                        c.write(f"/* {query}/{dim['name']}/{v['ver']}.sql */\n")
                        c.write(f"static const char {vname}[] =\n")
                        for line in c_string_lines(v['path']):
                            c.write(line + '\n')
                        c.write(";\n\n")

                # Function
                c.write(f"bool\n")
                c.write(f"pgcopydb_sql_{qident}(int pg_version,\n")
                pad = ' ' * (len("pgcopydb_sql_") + len(qident) + 1)
                c.write(f"{pad}{enum_type} filter,\n")
                c.write(f"{pad}const char **sql)\n")
                c.write("{\n")
                c.write("    switch ((int) filter)\n")
                c.write("    {\n")

                for dim in info['dims']:
                    c.write(f"        case {dim['enum_val']}: /* {dim['label']} */\n")
                    c.write("        {\n")
                    vprefix = (f"sql__{qident}"
                               f"__{to_c_ident(dim['label'])}")
                    emit_version_dispatch(c, vprefix, dim['versions'],
                                         indent="            ")
                    c.write("        }\n\n")

                c.write("        default:\n")
                c.write("            break;\n")
                c.write("    }\n")
                c.write("    return false;\n")
                c.write("}\n\n")

            else:
                # No filter dimension — just version dispatch
                for v in info['versions']:
                    vident = to_c_ident(v['ver'])
                    vname  = f"sql__{qident}__{vident}"
                    c.write(f"/* {query}/{v['ver']}.sql */\n")
                    c.write(f"static const char {vname}[] =\n")
                    for line in c_string_lines(v['path']):
                        c.write(line + '\n')
                    c.write(";\n\n")

                c.write(f"bool\n")
                c.write(f"pgcopydb_sql_{qident}(int pg_version, const char **sql)\n")
                c.write("{\n")
                vprefix = f"sql__{qident}"
                emit_version_dispatch(c, vprefix, info['versions'],
                                      indent="    ")
                c.write("}\n\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <sql-dir> <output.h> <output.c>",
              file=sys.stderr)
        sys.exit(1)
    generate(sys.argv[1], sys.argv[2], sys.argv[3])
