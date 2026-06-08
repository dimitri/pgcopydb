#!/usr/bin/env python3
"""
verify.py <check-name>

Verification checks for the cdc-file-rotation test.  Each check is a plain
function; the routing table at the bottom maps names to functions.  Exit 0
on success, 1 on the first failure (with a clear message on stderr).

Environment variables used:
  XDG_DATA_HOME          pgcopydb CDC share directory (default: /var/lib/postgres/.local/share)
  TMPDIR                 pgcopydb schema directory    (default: /tmp)
  PGCOPYDB_SOURCE_PGURI  source Postgres connection string
  PGCOPYDB_TARGET_PGURI  target Postgres connection string
"""

import glob
import os
import sqlite3
import subprocess
import sys

# ── paths ─────────────────────────────────────────────────────────────────────

SHAREDIR = os.path.join(
    os.environ.get("XDG_DATA_HOME", "/var/lib/postgres/.local/share"),
    "pgcopydb",
)
SOURCE_DB = os.path.join(
    os.environ.get("TMPDIR", "/tmp"),
    "pgcopydb", "schema", "source.db",
)

# ── helpers ───────────────────────────────────────────────────────────────────

def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)

def check(condition, msg):
    if not condition:
        fail(msg)
    print(f"ok: {msg}")

def output_files():
    return sorted(glob.glob(os.path.join(SHAREDIR, "*-output.db")))

def pg_count(uri, table):
    result = subprocess.run(
        ["psql", "-AtqX", "-d", uri, "-c", f"select count(*) from {table}"],
        capture_output=True, text=True, check=True,
    )
    return int(result.stdout.strip())

# ── checks ────────────────────────────────────────────────────────────────────

def check_output_rotation():
    """At least 2 output.db files must exist — rotation happened."""
    files = output_files()
    check(len(files) >= 2, f"at least 2 output.db files (got {len(files)})")


def check_cdc_files():
    """cdc_files must track every output.db: all closed except the last one."""
    files = output_files()
    con = sqlite3.connect(SOURCE_DB)
    total, closed, open_ = con.execute(
        "select count(*),"
        "       sum(done_time_epoch is not null),"
        "       sum(done_time_epoch is null)"
        "  from cdc_files"
    ).fetchone()
    con.close()

    print(f"cdc_files: total={total} closed={closed} open={open_}")
    check(open_ == 1,
          f"exactly 1 open cdc_file (got {open_})")
    check(closed >= 1,
          f"at least 1 closed cdc_file (got {closed})")
    check(total == len(files),
          f"cdc_files rows ({total}) matches output.db count ({len(files)})")


def check_large_txn_atomicity():
    """All 100 large-txn rows must land in exactly one output.db file."""
    hits = []
    for path in output_files():
        try:
            con = sqlite3.connect(path)
            (n,) = con.execute(
                "select count(*) from output"
                " where message like '%large-txn-row%'"
            ).fetchone()
            con.close()
        except sqlite3.DatabaseError:
            n = 0
        if n:
            print(f"  {os.path.basename(path)}: {n} large-txn rows")
            hits.append((path, n))

    total = sum(n for _, n in hits)
    check(total == 100,
          f"100 large-txn rows total across all files (got {total})")
    check(len(hits) == 1,
          f"large-txn rows in exactly 1 file (spread across {len(hits)})")


def check_row_counts():
    """Row counts on source and target must match (20 small + 100 large = 120)."""
    src_uri = os.environ["PGCOPYDB_SOURCE_PGURI"]
    tgt_uri = os.environ["PGCOPYDB_TARGET_PGURI"]
    src = pg_count(src_uri, "rotation_test")
    tgt = pg_count(tgt_uri, "rotation_test")
    print(f"rotation_test: source={src} target={tgt}")
    check(src == tgt,  f"source and target row counts match ({src} vs {tgt})")
    check(src == 120,  f"expected 120 rows (got {src})")


# ── routing ───────────────────────────────────────────────────────────────────

CHECKS = {
    "output-rotation":    check_output_rotation,
    "cdc-files":          check_cdc_files,
    "large-txn-atomicity": check_large_txn_atomicity,
    "row-counts":         check_row_counts,
}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in CHECKS:
        names = "\n  ".join(CHECKS)
        print(f"usage: verify.py <check>\n\navailable checks:\n  {names}", file=sys.stderr)
        sys.exit(1)

    CHECKS[sys.argv[1]]()
