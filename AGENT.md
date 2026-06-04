# pgcopydb — Agent Orientation Guide

pgcopydb automates `pg_dump | pg_restore` between two **live** PostgreSQL
servers, adding parallelism for COPY, index creation, and optional Change Data
Capture (CDC via logical decoding) for zero-downtime online migrations.

---

## Quick Orient (run at session start)

```bash
git log --oneline -10   # recent commits — understand what changed
git status              # uncommitted edits or staged hunks
git stash list          # any stashed work in progress
```

---

## Two Migration Modes

| Mode | Command | When to use | Relevant tests |
|------|---------|-------------|---------------|
| Offline (snapshot) | `pgcopydb clone` | Source can tolerate a brief write pause | `pagila`, `unit`, `blobs`, `filtering` |
| Online (CDC) | `pgcopydb clone --follow` | Zero-downtime; source keeps accepting writes | `cdc-*`, `follow-*` |

Understanding which mode is involved narrows the relevant source files and test suite immediately.

---

## Repository Layout

```
src/bin/pgcopydb/   Main binary — all C source (~50 files)
src/bin/lib/        Vendored libs: parson, sqlite3, uthash, log.c, subcommands.c, pg utils
tests/              20+ Docker-based integration suites + unit regression tests
docs/               Sphinx RST documentation
ci/                 CI scripts (banned.h.sh, Dockerfile.docs.template)
```

---

## Code Architecture

### Core modules

| File | Responsibility |
|------|---------------|
| `main.c` | Entry point — signal/logging init, dispatch to CLI parser |
| `cli_root.c` | Command tree: clone, follow, copy, stream, list, … |
| `copydb.c` / `copydb.h` | Central `CopyDataSpec` struct; top-level orchestration |
| `schema.c` | SQL queries to discover tables/indexes/sequences/extensions on source |
| `catalog.c` | SQLite-backed internal state: progress, resume, timing |
| `table-data.c` | Parallel COPY supervisor + worker pool |
| `indexes.c` | Parallel `CREATE INDEX` + `ALTER TABLE … USING INDEX` |
| `follow.c` | CDC orchestration: replication slot, snapshot export |
| `ld_stream.c` | Reads wal2json / test_decoding JSON from source WAL |
| `ld_transform.c` | JSON → SQL transformation with table/schema filtering |
| `ld_apply.c` | Executes transformed SQL on target |
| `sentinel.c` | Tracks receive/transform/apply LSNs in `pgcopydb.sentinel` table |
| `pgsql.c` | libpq abstraction with retry/backoff (`PGSQL` struct) |
| `filtering.c` | Include/exclude rules with dependency resolution |
| `queue_utils.c` | System V message queues for work distribution across workers |
| `lock_utils.c` | System V semaphores protecting shared resources |

### Process tree

```
pgcopydb clone
├── clone worker
│   ├── copy supervisor + N copy workers       (PGCOPYDB_TABLE_JOBS)
│   ├── blob metadata worker + N blob workers
│   ├── index supervisor + N index workers     (PGCOPYDB_INDEX_JOBS)
│   ├── vacuum supervisor + N vacuum workers
│   └── sequences reset worker
└── follow worker  (only with --follow)
    ├── stream receive   (ld_stream.c)
    ├── stream transform (ld_transform.c)
    └── stream apply     (ld_apply.c)
```

### Key data structure

`CopyDataSpec` (`copydb.h`) is the root context threaded through all
orchestration functions:

- connection strings, work-directory paths, filters
- `DatabaseCatalog sourceDB / targetDB` (SQLite catalogs)
- work queues + job counts
- options: `follow`, `resume`, `consistent`, `failFast`

### CLI → implementation call chain

For `pgcopydb clone`:
```
cli_clone()           cli_clone_follow.c:129   parse options, init CopyDataSpec
  copydb_prepare_snapshot()                    export snapshot on source
  start_clone_process()    :484                fork() clone worker
    cloneDB()              :530
      copydb_dump_source_schema()              pg_dump pre-data
      copydb_target_prepare_schema()           pg_restore pre-data
      copydb_copy_all_table_data()  table-data.c   COPY workers via queue
      copydb_index_all_tables()     indexes.c       CREATE INDEX workers
      copydb_target_finalize_schema()          pg_restore post-data
  start_follow_process()   :661   (only --follow)
    followDB()             follow.c             stream receive/transform/apply
```

For any `pgcopydb stream` sub-command, entry is `cli_stream.c` → `ld_stream.c`.

### Module dependency snapshot

```
cli_clone_follow.c
  └── copydb.c           (orchestration, CopyDataSpec)
        ├── schema.c     (source schema discovery via libpq)
        ├── catalog.c    (SQLite state — singleton, locked via lock_utils.c)
        ├── table-data.c → queue_utils.c  (COPY workers)
        ├── indexes.c    → queue_utils.c  (CREATE INDEX workers)
        ├── follow.c
        │     ├── ld_stream.c    (WAL JSON reader)
        │     ├── ld_transform.c (JSON → SQL, uses catalog.c for schema lookup)
        │     ├── ld_apply.c     (executes SQL on target)
        │     └── ld_store.c     (SQLite CDC pipeline, feeds transform+apply)
        └── pgsql.c      (libpq wrapper used by everything)
```

### Logging macros (defined in `src/bin/lib/log/src/log.h`)

```c
log_debug(...)   // -vvv / --debug
log_notice(...)  // --notice
log_info(...)    // default level
log_warn(...)
log_error(...)   // non-fatal, returns false up the call chain
log_fatal(...)   // exits the process
```

Grep pattern to find the failure path for a symptom:
```bash
grep -n "log_error\|log_fatal" src/bin/pgcopydb/<suspected_file>.c
```

Boolean return convention: almost every function returns `bool`; `false` means
failure and the caller checks with `if (!fn()) { return false; }`. Follow the
`false` chain up to find where an error is first logged.

---

## Work Directory Layout

Default root: `$TMPDIR/pgcopydb` (set in `tests/paths.env` to `/var/run/pgcopydb`).

```
/tmp/pgcopydb/
├── pgcopydb.pid              # lock — delete to allow a fresh run without --resume
├── pgcopydb.service.pid      # follow-mode service pid
├── snapshot                  # exported snapshot name (text)
├── schema.json               # full source schema as JSON
├── summary.json              # timing summary written at end
├── schema/
│   ├── source.db             # SQLite: source catalog (tables, indexes, sizes)
│   ├── filter.db             # SQLite: filter rules
│   └── target.db             # SQLite: target catalog (post-restore state)
└── cdc/
    ├── origin                # replication origin name
    ├── tli                   # timeline ID
    ├── wal_segment_size      # WAL segment size from source
    └── lsn.json              # last written/applied LSN checkpoint
```

To force a fully clean run (ignore previous partial state):
```bash
rm -rf /tmp/pgcopydb   # or whatever $TMPDIR/pgcopydb resolves to
```

To keep state for `--resume` debugging, leave the directory intact and re-run
with `--resume`. The SQLite catalogs retain per-table progress.

---

## Build

**Prerequisites**: PostgreSQL dev headers (via `pg_config`), `libgc` (Boehm
GC), `libncurses`, standard C toolchain, Docker + Docker Compose.

```bash
# Compile the binary natively
make bin
# → src/bin/pgcopydb/pgcopydb

# Debug build (-Og, full symbols)
DEBUG=1 make bin

# Build the Docker image used by all tests
make build                   # defaults to PGVERSION=16
PGVERSION=17 make build

# Clean
make clean
```

---

## Test Environment

### How tests work

Every suite under `tests/<name>/` is a Docker Compose project with three
services:

| Service | Role |
|---------|------|
| `source` | PostgreSQL source instance |
| `target` | PostgreSQL target instance |
| `test`   | Container that runs `copydb.sh` |

Shared env files (auto-loaded by compose):

| File | Contents |
|------|---------|
| `tests/postgres.env` | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST_AUTH_METHOD` |
| `tests/uris.env` | `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGSSLMODE` |
| `tests/paths.env` | `TMPDIR=/var/run/pgcopydb`, `XDG_DATA_HOME=/var/run/pgcopydb/cdc` |

### Run tests

```bash
# All tests (builds Docker image first)
make tests

# Single suite
make tests/pagila
make tests/unit
make tests/cdc-wal2json

# Specific PostgreSQL version
PGVERSION=17 make tests/pagila

# Available suites
pagila  pagila-multi-steps  blobs  unit  filtering  extensions
partitioned-target  cdc-wal2json  cdc-test-decoding  cdc-endpos-mid-txn
cdc-low-level  cdc-transform-apply  cdc-replica-identity-index
cdc-partitioned-target  follow-wal2json  follow-9.6  follow-data-only
endpos-in-multi-wal-txn  timescaledb  pagila-standby
```

### Interactive debugging inside a test container

```bash
# For CDC/follow tests — create the required named volume first
docker volume create cdc-wal2json   # (or whichever suite needs it)

# Drop into a shell with source+target Postgres already running
cd tests/pagila && make attach
cd tests/cdc-wal2json && make attach

# Inside the container:
pgcopydb ping                        # verify source/target reachability
bash /usr/src/pgcopydb/copydb.sh     # run the test script manually
pgcopydb --debug clone ...           # run individual commands
```

### Volume cleanup between CDC/follow runs

```bash
cd tests/cdc-wal2json
make fix-volumes    # wipes /var/run/pgcopydb inside the container
make down           # tear down compose stack
```

### Unit / regression tests (`tests/unit/`)

Follows a pg_regress-style pattern:

1. `setup/setup.sql` — creates source schema
2. `pgcopydb fork --skip-collations --fail-fast --debug` — copies to target
3. For each `sql/*.sql`: runs against target, diffs with `expected/*.out`
4. For each `script/*.sh`: runs script, diffs output (ignoring `INFO`/`WARN` lines)

---

## Reproducing a Bug

### General workflow

1. Identify the closest test suite for the failing scenario.
2. `make tests/<suite>` — confirm it fails.
3. `cd tests/<suite> && make attach` — get an interactive shell.
4. Reproduce manually with `--debug` or `--notice`.
5. Inspect SQLite catalog and CDC files (see below).
6. Iterate: edit source, rebuild image (`make build`), re-run.

### Useful pgcopydb diagnostic flags

```bash
--debug          # DEBUG-level logging (very verbose)
--notice         # NOTICE-level logging (recommended starting point)
--fail-fast      # abort on first error instead of continuing
--not-consistent # skip snapshot export (faster for non-CDC debugging)
--resume         # continue from partial run; lets you inspect catalog state
```

### Inspecting internal state

```bash
# SQLite catalog — progress, table list, timing stats
sqlite3 ${TMPDIR:-/var/run/pgcopydb}/pgcopydb.db
.tables
SELECT * FROM s_table LIMIT 10;
SELECT * FROM timings;

# Sentinel table on source — CDC LSN tracking
psql ${PGCOPYDB_SOURCE_PGURI} -c "SELECT * FROM pgcopydb.sentinel"

# CDC intermediate stream files (JSON from WAL, SQL ready to apply)
ls ${XDG_DATA_HOME:-/var/run/pgcopydb/cdc}/
```

### Banned API check

```bash
sh ./ci/banned.h.sh
```

### Attaching a debugger

The test containers ship with `gdb`. pgcopydb uses `fork()` for workers, so
attach to the child PID, not the parent.

```bash
# Inside a make attach shell — find worker PIDs
ps aux | grep pgcopydb

# Attach gdb to a running worker (child process)
gdb -p <pid>
(gdb) backtrace

# Alternatively, build with DEBUG=1 and run under gdb from the start
DEBUG=1 make bin
gdb --args src/bin/pgcopydb/pgcopydb clone --debug --source ... --target ...
(gdb) set follow-fork-mode child   # follow into workers automatically
(gdb) run
```

On macOS with lldb:
```bash
lldb -- src/bin/pgcopydb/pgcopydb clone --debug ...
(lldb) settings set target.process.follow-fork-mode child
(lldb) run
```

---

## Non-obvious Gotchas

- **`wal2json` must be installed on the source PostgreSQL server**, not locally.
  The error when it's missing is a generic replication slot creation failure —
  check `shared_preload_libraries` on source.

- **`wal_level = logical` required on source for CDC.** If it's `replica` or
  `minimal`, slot creation silently fails with a confusing message. Check with:
  `psql $SOURCE -c "SHOW wal_level"`.

- **Work directory is reused on re-run.** If `$TMPDIR/pgcopydb/pgcopydb.pid`
  exists from a prior run, pgcopydb refuses to start unless you pass `--resume`
  or delete the directory. This is intentional — stale state is preserved for
  resume, not silently discarded.

- **`pagila` is the canonical regression baseline.** When fixing a bug, verify
  `make tests/pagila` still passes. It exercises schema copy, data copy, indexes,
  sequences, extensions, blobs, and roles end-to-end.

- **Adding a regression case:** add a `.sql` file to `tests/unit/sql/` and its
  expected output to `tests/unit/expected/`. This is the right way to pin a fix
  so it doesn't regress.

- **Active TODOs in the source** (known limitations):
  - `catalog.c`: IDENTITY columns in UPDATE not yet supported
  - `ld_transform.c`: replica identity index UPDATE/DELETE, generated column
    handling, and parameter limit check for COPY operations all have open TODOs
  - `ld_stream.c`: `WalSegSz` hardcoded — tracked for removal

- **Open issues / recent changes:** `CHANGELOG.md` tracks releases.
  `git log --oneline -20` is the fastest way to see what changed recently.
  GitHub issues: https://github.com/dimitri/pgcopydb/issues

---

## Code Style

```bash
# Auto-format all C sources
make indent          # runs citus_indent (requires uncrustify)

# Check only (what CI runs)
citus_indent --check --diff

# Install citus_indent
sudo apt-get install uncrustify
git clone https://github.com/citusdata/tools.git && cd tools && make uncrustify/.install

# Recommended: git pre-commit hook
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
citus_indent --check --diff || { citus_indent --diff; exit 1; }
EOF
chmod +x .git/hooks/pre-commit
```

---

## Documentation

```bash
make docs           # rebuild Sphinx HTML + man pages
make update-docs    # sync CLI --help text into docs (run after changing options)
make check-docs     # verify docs build via Docker
```

---

## CI Matrix

| Workflow | File | Trigger | Versions |
|----------|------|---------|---------|
| PR/push  | `.github/workflows/run-tests.yml` | push/PR to main | PG16 + PG18 |
| Nightly  | `.github/workflows/nightly.yml`   | daily 02:00 UTC  | PG16/17/18 × all tests |

Simulate CI locally:

```bash
TEST=pagila PGVERSION=16 make tests/pagila
sh ./ci/banned.h.sh
```
