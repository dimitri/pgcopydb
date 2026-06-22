#!/usr/bin/env python3
"""
tools/sql2c.py — SQL-to-C generator for pgcopydb

Reads SQL files from a directory tree and emits two generated C files:
    <output>.h  — function declarations
    <output>.c  — static string constants and dispatch functions

Directory layout
----------------
    <sql-dir>/<query>.sql                    — flat file (no pg_version, no filter)
    <sql-dir>/<query>/<version>.sql          — version-dispatch only
    <sql-dir>/<query>/filter.map + <dim>/    — filter dispatch (optionally + versions)

  <query>     maps to C function pgcopydb_sql_<query>()
  <version>   version specifier (filename stem, without .sql):

                default    — matches any server version (fallback)
                pg-96      — server version < 100000 (before PG 10)
                pg10       — 100000 <= version <= 109999 (PG 10.x only)
                pg10-12    — 100000 <= version <= 129999 (PG 10 through 12)
                pg12-      — version >= 120000 (PG 12 and later)

filter.map format
-----------------
    # comment
    # header <file.h>         — C header containing the enum (relative to sql-dir/..)
    # enum   <TypedefName>    — typedef name of the enum (for documentation)
    <dir-name>  <C-constant>  — e.g.  no-filter  SOURCE_FILTER_TYPE_NONE

  Values may also be bare integers (e.g. for ad-hoc use), but enum constant
  names are preferred — the generator resolves them by parsing the C header.

  When a filter.map is present, the generated function signature is:
    bool pgcopydb_sql_<query>(int filter, const char **sql);          (no versions)
    bool pgcopydb_sql_<query>(int pg_version, int filter,             (with versions)
                               const char **sql);

  The caller passes a C enum value directly; C's implicit enum→int promotion
  means no cast is needed.

preamble.sql
------------
  An optional <query>/preamble.sql file whose content is prepended to every
  filter variant's SQL string constant.  Used for shared CTE prefixes.
"""

import os
import re
import sys
from pathlib import Path
from collections import OrderedDict


# ---------------------------------------------------------------------------
# C header enum parser
# ---------------------------------------------------------------------------

def parse_c_enum(header_path, typedef_name):
    """Parse a C header and return {constant_name: integer_value} for the enum.

    Handles both named-tag and anonymous typedef enums:
        typedef enum TagName { ... } TypedefName;
        typedef enum           { ... } TypedefName;
    Handles explicit initialisers (FOO = 5), implicit increments, and blank
    lines / comments inside the enum body.
    """
    with open(header_path) as f:
        src = f.read()

    # Strip block comments
    src = re.sub(r'/\*.*?\*/', ' ', src, flags=re.DOTALL)
    # Strip line comments
    src = re.sub(r'//[^\n]*', '', src)

    # Find the enum body whose typedef name matches.
    # [^{}]* prevents the match from spanning multiple brace pairs when
    # the same header defines more than one typedef enum.
    pat = (r'typedef\s+enum\s*(?:\w+\s*)?\{'
           r'([^{}]*)'
           r'\}\s*' + re.escape(typedef_name) + r'\s*;')
    m = re.search(pat, src)
    if not m:
        raise ValueError(
            f"Enum typedef {typedef_name!r} not found in {header_path}")

    body = m.group(1)
    result = {}
    current = 0
    for token in re.split(r',', body):
        token = token.strip()
        if not token:
            continue
        m2 = re.match(r'(\w+)\s*=\s*(-?\d+)', token)
        if m2:
            name = m2.group(1)
            current = int(m2.group(2))
        else:
            m3 = re.match(r'(\w+)', token)
            if not m3:
                continue
            name = m3.group(1)
        result[name] = current
        current += 1

    return result


# ---------------------------------------------------------------------------
# Version specifier parsing
# ---------------------------------------------------------------------------

def parse_version_specifier(ver):
    """Return (min_ver, max_ver) inclusive for a version filename stem.

    (0, 0) = no constraint (default / fallback).
    0 in min = no lower bound; 0 in max = no upper bound.
    """
    if ver == 'default':
        return (0, 0)

    m = re.fullmatch(r'pg(-?)(\d+)(?:-(\d*))?(-?)', ver)
    if not m:
        raise ValueError(f"Cannot parse version specifier: {ver!r}")

    leading_dash  = m.group(1)
    first         = m.group(2)
    mid_second    = m.group(3)  # '' in pg12-, '12' in pg10-12, None in pg10
    trailing_dash = m.group(4)

    def to_min(s):
        n = int(s)
        return ((n // 10) * 10000 + (n % 10) * 100) if 90 <= n <= 99 else n * 10000

    def to_max_inclusive(s):
        n = int(s)
        return (((n // 10) + 1) * 10000 - 1) if 90 <= n <= 99 else (n * 10000 + 9999)

    if leading_dash and not trailing_dash:
        return (0, to_max_inclusive(first))
    if not leading_dash and trailing_dash and mid_second == '':
        return (to_min(first), 0)
    if not leading_dash and mid_second is not None and mid_second != '':
        return (to_min(first), to_max_inclusive(mid_second))
    if not leading_dash and mid_second is None:
        return (to_min(first), to_max_inclusive(first))

    raise ValueError(f"Unhandled version specifier: {ver!r}")


def version_priority(v):
    """Sort key: most constrained variants first, default last."""
    mn, mx = v['min'], v['max']
    if mn > 0 and mx > 0:
        return (0, -mn)
    if mn > 0 or mx > 0:
        return (1, -max(mn, mx))
    return (2, 0)


# ---------------------------------------------------------------------------
# filter.map parser
# ---------------------------------------------------------------------------

def parse_filter_map(path, header_dir=None):
    """Parse a filter.map file.

    Returns {'entries': [(dir_name, int_value), ...]} in file order.

    Values in the map may be:
      - bare integers   (0, 1, 2, …)
      - C enum constants (SOURCE_FILTER_TYPE_NONE, …)

    When enum constants are used the map must include:
        # header <file.h>      — relative to header_dir (the parent of sql-dir)
        # enum   <TypedefName> — typedef name of the enum
    """
    header_file = None
    enum_typedef = None
    raw_entries = []

    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#'):
                m = re.match(r'#\s*header\s+(\S+)', line)
                if m:
                    header_file = m.group(1).strip()
                    continue
                m = re.match(r'#\s*enum\s+(\S+)', line)
                if m:
                    enum_typedef = m.group(1).strip()
                    continue
                continue
            parts = line.split()
            if len(parts) >= 2:
                raw_entries.append((parts[0], parts[1]))

    # Resolve values — try int first, fall back to enum lookup
    enum_cache = {}

    def resolve(val_str):
        try:
            return int(val_str)
        except ValueError:
            pass
        # It's an enum constant name
        if val_str not in enum_cache:
            if not header_file:
                raise ValueError(
                    f"{path}: constant {val_str!r} requires "
                    f"'# header <file.h>' directive")
            if not header_dir:
                raise ValueError(
                    f"{path}: header_dir not provided to parse_filter_map")
            hdr_path = Path(header_dir) / header_file
            enum_cache.update(parse_c_enum(hdr_path, enum_typedef))
        if val_str not in enum_cache:
            raise ValueError(
                f"{path}: constant {val_str!r} not found in "
                f"enum {enum_typedef!r} in {header_file}")
        return enum_cache[val_str]

    return {'entries': [(d, resolve(v)) for d, v in raw_entries]}


# ---------------------------------------------------------------------------
# C string emission
# ---------------------------------------------------------------------------

def c_string_lines(path, preamble=None):
    """Read a .sql file and return a list of C string literal lines.

    Each line of the file becomes one C string literal of the form:
        "    <content>\\n"

    If preamble (a string) is provided its content is prepended before the
    file's own content — used for shared CTE prefixes (preamble.sql).
    """
    with open(path) as f:
        content = f.read()
    full = (preamble or '') + content
    lines = []
    for line in full.split('\n'):
        escaped = line.replace('\\', '\\\\').replace('"', '\\"')
        lines.append(f'    "{escaped}\\n"')
    # Drop trailing blank-line entries
    while lines and lines[-1] == '    "\\n"':
        lines.pop()
    return lines


def to_c_ident(s):
    return re.sub(r'[^a-zA-Z0-9]', '_', s).strip('_')


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover(sql_dir):
    """Walk sql_dir and return an OrderedDict of query_name → info dict.

    Three query shapes are recognised:

        flat      sql_dir/<query>.sql
        versioned sql_dir/<query>/<version>.sql  (no filter.map)
        filtered  sql_dir/<query>/filter.map + <dim>/<version>.sql
    """
    sql_dir = Path(sql_dir)
    header_dir = sql_dir.parent   # src/bin/pgcopydb/
    items = {}                    # name → info dict

    # ---- flat .sql files at the top level ----
    for sqlfile in sorted(sql_dir.glob('*.sql')):
        query = sqlfile.stem
        items[query] = {'shape': 'flat', 'path': sqlfile}

    # ---- subdirectory-based queries ----
    for qdir in sorted(p for p in sql_dir.iterdir() if p.is_dir()):
        query = qdir.name
        qdims    = sorted(d for d in qdir.iterdir() if d.is_dir())
        qfiles   = sorted(qdir.glob('*.sql'))
        fmap_path = qdir / 'filter.map'

        if qdims and fmap_path.exists():
            fmap = parse_filter_map(fmap_path, header_dir)
            preamble_path = qdir / 'preamble.sql'
            preamble = preamble_path.read_text() if preamble_path.exists() else None

            dims = []
            for dir_name, int_val in fmap['entries']:
                dimdir = qdir / dir_name
                if not dimdir.is_dir():
                    raise FileNotFoundError(
                        f"{fmap_path}: references {dir_name!r} but "
                        f"{dimdir} does not exist")
                versions = []
                for sqlfile in sorted(dimdir.glob('*.sql')):
                    mn, mx = parse_version_specifier(sqlfile.stem)
                    versions.append(
                        {'ver': sqlfile.stem, 'min': mn, 'max': mx,
                         'path': sqlfile})
                versions.sort(key=version_priority)
                dims.append({'name': dir_name, 'int_val': int_val,
                             'versions': versions})

            has_ver = any(
                any(v['min'] > 0 or v['max'] > 0 for v in d['versions'])
                for d in dims)
            items[query] = {
                'shape': 'filtered',
                'dims': dims,
                'preamble': preamble,
                'has_ver': has_ver,
            }

        elif qfiles:
            # Version-dispatch only (no filter dimension)
            versions = []
            for sqlfile in qfiles:
                mn, mx = parse_version_specifier(sqlfile.stem)
                versions.append(
                    {'ver': sqlfile.stem, 'min': mn, 'max': mx,
                     'path': sqlfile})
            versions.sort(key=version_priority)
            has_ver = any(v['min'] > 0 or v['max'] > 0 for v in versions)
            items[query] = {
                'shape': 'versioned',
                'versions': versions,
                'has_ver': has_ver,
            }

    return OrderedDict(sorted(items.items()))


# ---------------------------------------------------------------------------
# Code generation
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

SEP = '/' * 62


def emit_version_dispatch(f, varname_prefix, versions, indent):
    """Emit if-chain for pg_version dispatch; sets *sql and returns."""
    i = indent
    for v in versions:
        mn, mx = v['min'], v['max']
        vname = f"{varname_prefix}__{to_c_ident(v['ver'])}"
        if mn == 0 and mx == 0:
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
    with open(hdr_path, 'w') as h:
        h.write(HEADER_TOP)

        for query, info in queries.items():
            qident = to_c_ident(query)
            pad = ' ' * (len("bool pgcopydb_sql_") + len(qident) + 1)
            h.write(f"/{SEP}/\n")
            h.write(f"/* {query:<60} */\n")
            h.write(f"/{SEP}/\n\n")

            shape = info['shape']
            if shape == 'flat':
                h.write(f"bool pgcopydb_sql_{qident}(const char **sql);\n\n")

            elif shape == 'versioned':
                if info['has_ver']:
                    h.write(f"bool pgcopydb_sql_{qident}(int pg_version,\n")
                    h.write(f"{pad}const char **sql);\n\n")
                else:
                    h.write(f"bool pgcopydb_sql_{qident}(const char **sql);\n\n")

            else:  # filtered
                if info['has_ver']:
                    h.write(f"bool pgcopydb_sql_{qident}(int pg_version,\n")
                    h.write(f"{pad}int filter,\n")
                    h.write(f"{pad}const char **sql);\n\n")
                else:
                    h.write(f"bool pgcopydb_sql_{qident}(int filter,\n")
                    h.write(f"{pad}const char **sql);\n\n")

        h.write(HEADER_BOT)

    # ---------------------------------------------------------------- source
    with open(src_path, 'w') as c:
        c.write(SOURCE_TOP)

        for query, info in queries.items():
            qident = to_c_ident(query)
            pad = ' ' * (len("pgcopydb_sql_") + len(qident) + 1)
            c.write(f"/{SEP}/\n")
            c.write(f"/* {query:<60} */\n")
            c.write(f"/{SEP}/\n\n")

            shape = info['shape']

            if shape == 'flat':
                vname = f"sql__{qident}"
                c.write(f"/* {query}.sql */\n")
                c.write(f"static const char {vname}[] =\n")
                for line in c_string_lines(info['path']):
                    c.write(line + '\n')
                c.write(";\n\n")
                c.write("bool\n")
                c.write(f"pgcopydb_sql_{qident}(const char **sql)\n")
                c.write("{\n")
                c.write(f"    *sql = {vname};\n")
                c.write("    return true;\n")
                c.write("}\n\n")

            elif shape == 'versioned':
                for v in info['versions']:
                    vident = to_c_ident(v['ver'])
                    vname  = f"sql__{qident}__{vident}"
                    c.write(f"/* {query}/{v['ver']}.sql */\n")
                    c.write(f"static const char {vname}[] =\n")
                    for line in c_string_lines(v['path']):
                        c.write(line + '\n')
                    c.write(";\n\n")

                c.write("bool\n")
                if info['has_ver']:
                    c.write(f"pgcopydb_sql_{qident}(int pg_version,\n")
                    c.write(f"{pad}const char **sql)\n")
                    c.write("{\n")
                    emit_version_dispatch(c, f"sql__{qident}",
                                         info['versions'], "    ")
                else:
                    v = info['versions'][0]
                    vname = f"sql__{qident}__{to_c_ident(v['ver'])}"
                    c.write(f"pgcopydb_sql_{qident}(const char **sql)\n")
                    c.write("{\n")
                    c.write(f"    *sql = {vname};\n")
                    c.write("    return true;\n")
                c.write("}\n\n")

            else:  # filtered
                preamble = info.get('preamble')
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

                c.write("bool\n")
                if info['has_ver']:
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
                    c.write(f"        case {dim['int_val']}: "
                            f"/* {dim['name']} */\n")
                    c.write("        {\n")
                    vprefix = f"sql__{qident}__{to_c_ident(dim['name'])}"
                    emit_version_dispatch(c, vprefix, dim['versions'],
                                         "            ")
                    c.write("        }\n\n")

                c.write("        default:\n")
                c.write("            break;\n")
                c.write("    }\n")
                c.write("    return false;\n")
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
