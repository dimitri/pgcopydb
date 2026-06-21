#!/usr/bin/env python3
"""
tools/sql2c.py — SQL-to-C generator for pgcopydb

Reads SQL files from a directory tree and emits two generated C files:
    <output>.h  — function declarations
    <output>.c  — static string constants and dispatch functions

Directory layout
----------------
    <sql-dir>/<query>/[<dim>/]<version>.sql

  <query>     maps to C function pgcopydb_sql_<query>()
  <dim>       optional filter-dimension subdirectory.
              When present the query directory must also contain
              a filter.map file that defines the mapping from
              directory names to C enum constants.
  <version>   version specifier (filename stem, without .sql):

                default    — matches any server version (lowest priority,
                             used as fallback when no specific variant matches)
                pg-96      — server version < 100000 (before PG 10)
                pg10       — 100000 <= version <= 109999 (PG 10.x only)
                pg10-12    — 100000 <= version <= 129999 (PG 10 through 12)
                pg12-      — version >= 120000 (PG 12 and later)

filter.map format
-----------------
    # comment
    # include <header.h>    — header to include in generated sql_queries.h
    # type <TypeName>       — C type for the filter parameter
    <dir-name>  <C-constant>

Example (src/bin/pgcopydb/sql/list_source_tables/filter.map):
    # include filtering.h
    # type SourceFilterType
    no-filter     SOURCE_FILTER_TYPE_NONE
    incl          SOURCE_FILTER_TYPE_INCL
    excl          SOURCE_FILTER_TYPE_EXCL
    list-not-incl SOURCE_FILTER_TYPE_LIST_NOT_INCL
    list-excl     SOURCE_FILTER_TYPE_LIST_EXCL

Generated API
-------------
When a filter.map is present the generated function uses the declared
C type for the filter parameter (no cast needed at the call site):

    bool pgcopydb_sql_list_source_tables(int pg_version,
                                          SourceFilterType filter,
                                          const char **sql);

When there is no filter.map the function takes only pg_version:

    bool pgcopydb_sql_simple_query(int pg_version, const char **sql);

When there are no version variants (only default.sql) the pg_version
parameter is omitted:

    bool pgcopydb_sql_simple_query(SourceFilterType filter,
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


def c_string_lines(path, preamble=None):
    """Read a SQL file and return a list of C string literal lines.

    If preamble is provided (a string), it is prepended before the file
    content — used for a shared query-level preamble.sql across filters.
    """
    with open(path) as f:
        content = f.read()
    full = (preamble or '') + content
    lines = []
    for line in full.split('\n'):
        escaped = line.replace('\\', '\\\\').replace('"', '\\"')
        lines.append(f'    "{escaped}\\n"')
    # Drop trailing empty-line entries
    while lines and lines[-1] == '    "\\n"':
        lines.pop()
    return lines


def parse_filter_map(path):
    """Parse a filter.map file.

    Returns dict with keys:
        'entries'  — list of (dir_name, int_value) in file order

    File format:
        # comment line (ignored)
        <dir-name>  <integer-value>
    """
    result = {'entries': []}
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    result['entries'].append((parts[0], int(parts[1])))
                except ValueError:
                    raise ValueError(
                        f"{path}: expected integer value, got {parts[1]!r}")
    return result


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover(sql_dir):
    """Walk sql_dir and return an ordered dict of query info."""
    sql_dir = Path(sql_dir)
    queries = OrderedDict()

    for qdir in sorted(sql_dir.iterdir()):
        if not qdir.is_dir():
            continue
        query = qdir.name
        qdims  = sorted(d for d in qdir.iterdir() if d.is_dir())
        qfiles = sorted(qdir.glob('*.sql'))
        fmap_path = qdir / 'filter.map'

        if qdims and fmap_path.exists():
            # Has filter dimension directories with an explicit integer mapping
            fmap = parse_filter_map(fmap_path)

            # Optional preamble prepended to every filter's SQL
            preamble_path = qdir / 'preamble.sql'
            preamble = preamble_path.read_text() if preamble_path.exists() else None

            dims = []
            for dir_name, int_val in fmap['entries']:
                dimdir = qdir / dir_name
                if not dimdir.is_dir():
                    raise FileNotFoundError(
                        f"filter.map references {dir_name!r} but"
                        f" {dimdir} does not exist")
                versions = []
                for sqlfile in sorted(dimdir.glob('*.sql')):
                    ver = sqlfile.stem
                    mn, mx = parse_version_specifier(ver)
                    versions.append(
                        {'ver': ver, 'min': mn, 'max': mx,
                         'path': sqlfile})
                versions.sort(key=version_priority)
                dims.append({'name': dir_name, 'int_val': int_val,
                             'versions': versions})

            has_version_dispatch = any(
                any(v['min'] > 0 or v['max'] > 0 for v in d['versions'])
                for d in dims
            )

            queries[query] = {
                'has_dims': True,
                'dims': dims,
                'preamble': preamble,
                'has_version_dispatch': has_version_dispatch,
            }

        elif qfiles:
            # Only version files, no filter dimension
            versions = []
            for sqlfile in qfiles:
                ver = sqlfile.stem
                mn, mx = parse_version_specifier(ver)
                versions.append(
                    {'ver': ver, 'min': mn, 'max': mx, 'path': sqlfile})
            versions.sort(key=version_priority)
            has_version_dispatch = any(
                v['min'] > 0 or v['max'] > 0 for v in versions)
            queries[query] = {
                'has_dims': False,
                'versions': versions,
                'has_version_dispatch': has_version_dispatch,
            }

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

SOURCE_TOP_TMPL = """\
/*
 * GENERATED FILE - do not edit.
 * Source: src/bin/pgcopydb/sql/
 * Run 'make gen-sql' after modifying .sql files.
 */

#include "sql_queries.h"
"""

SEP = '/' * 62


def emit_version_dispatch(f, varname_prefix, versions, indent):
    """Emit if-chain for version dispatch, writing to *sql."""
    i = indent
    for v in versions:
        mn, mx = v['min'], v['max']
        vname = f"{varname_prefix}__{to_c_ident(v['ver'])}"
        if mn == 0 and mx == 0:
            # default — unconditional fallback
            f.write(f"{i}*sql = {vname};\n")
            f.write(f"{i}return true;\n")
            return
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
    f.write(f"{i}return false;\n")


def generate(sql_dir, hdr_path, src_path):
    queries = discover(sql_dir)

    # ---------------------------------------------------------------- header
    # The header only uses `int filter` — no external type dependency.
    with open(hdr_path, 'w') as h:
        h.write(HEADER_TOP)
        h.write('\n')

        for query, info in queries.items():
            qident = to_c_ident(query)
            h.write(f"/{SEP}/\n")
            h.write(f"/* {query:<60} */\n")
            h.write(f"/{SEP}/\n\n")

            pad = ' ' * (len("bool pgcopydb_sql_") + len(qident) + 1)

            if info['has_dims']:
                has_ver = info['has_version_dispatch']
                if has_ver:
                    h.write(f"bool pgcopydb_sql_{qident}(int pg_version,\n")
                    h.write(f"{pad}int filter,\n")
                    h.write(f"{pad}const char **sql);\n\n")
                else:
                    h.write(f"bool pgcopydb_sql_{qident}(int filter,\n")
                    h.write(f"{pad}const char **sql);\n\n")

            else:
                # No filter dimension
                has_ver = info['has_version_dispatch']
                if has_ver:
                    h.write(f"bool pgcopydb_sql_{qident}(int pg_version,\n")
                    h.write(f"{pad}const char **sql);\n\n")
                else:
                    h.write(f"bool pgcopydb_sql_{qident}(const char **sql);\n\n")

        h.write(HEADER_BOT)

    # ---------------------------------------------------------------- source
    with open(src_path, 'w') as c:
        c.write(SOURCE_TOP_TMPL)
        c.write('\n\n')

        for query, info in queries.items():
            qident = to_c_ident(query)
            c.write(f"/{SEP}/\n")
            c.write(f"/* {query:<60} */\n")
            c.write(f"/{SEP}/\n\n")

            if info['has_dims']:
                has_ver = info['has_version_dispatch']
                preamble = info.get('preamble')

                # Static string constants (all dims × all versions)
                for dim in info['dims']:
                    for v in dim['versions']:
                        vident = to_c_ident(v['ver'])
                        vname  = (f"sql__{qident}"
                                  f"__{to_c_ident(dim['name'])}__{vident}")
                        c.write(
                            f"/* {query}/{dim['name']}/{v['ver']}.sql */\n")
                        c.write(f"static const char {vname}[] =\n")
                        for line in c_string_lines(v['path'], preamble):
                            c.write(line + '\n')
                        c.write(";\n\n")

                # Function
                c.write("bool\n")
                pad = ' ' * (len("pgcopydb_sql_") + len(qident) + 1)
                if has_ver:
                    c.write(f"pgcopydb_sql_{qident}(int pg_version,\n")
                    c.write(f"{pad}int filter,\n")
                    c.write(f"{pad}const char **sql)\n")
                else:
                    c.write(f"pgcopydb_sql_{qident}(int filter,\n")
                    c.write(f"{pad}const char **sql)\n")
                c.write("{\n")
                c.write("    switch (filter)\n")
                c.write("    {\n")

                for dim in info['dims']:
                    int_val = dim['int_val']
                    c.write(f"        case {int_val}: /* {dim['name']} */\n")
                    c.write("        {\n")
                    vprefix = (f"sql__{qident}"
                               f"__{to_c_ident(dim['name'])}")
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
                has_ver = info['has_version_dispatch']

                for v in info['versions']:
                    vident = to_c_ident(v['ver'])
                    vname  = f"sql__{qident}__{vident}"
                    c.write(f"/* {query}/{v['ver']}.sql */\n")
                    c.write(f"static const char {vname}[] =\n")
                    for line in c_string_lines(v['path']):
                        c.write(line + '\n')
                    c.write(";\n\n")

                c.write("bool\n")
                if has_ver:
                    c.write(f"pgcopydb_sql_{qident}(int pg_version,\n")
                    pad = ' ' * (len("pgcopydb_sql_") + len(qident) + 1)
                    c.write(f"{pad}const char **sql)\n")
                else:
                    c.write(f"pgcopydb_sql_{qident}(const char **sql)\n")
                c.write("{\n")
                if has_ver:
                    vprefix = f"sql__{qident}"
                    emit_version_dispatch(c, vprefix, info['versions'],
                                         indent="    ")
                else:
                    # Only a default — no version check needed
                    v = info['versions'][0]
                    vident = to_c_ident(v['ver'])
                    vname = f"sql__{qident}__{vident}"
                    c.write(f"    *sql = {vname};\n")
                    c.write("    return true;\n")
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
