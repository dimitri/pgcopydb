# PostgreSQL 17 & 18 Combined Fixes Branch

Branch: `pg17-18-combined-fixes`

## Summary

This branch combines multiple PRs and fixes to add PostgreSQL 17 and 18 support to pgcopydb.

## Included Fixes

| Commit | Source | Fix | Issues |
|--------|--------|-----|--------|
| 3efdbd7 | PR #926 | Catalog mismatch when mixing filtered/unfiltered commands | #869 |
| 3764886 | teknogeek0 | PG17/18 Docker support + CI test matrix | #928 |
| 992b40e | teknogeek0 | Dynamic PostgreSQL version in CI tests | - |
| 0bd3b37 | teknogeek0 | PG17.6+ `\restrict`/`\unrestrict` metacommand handling + Docker ARG fix | #922, CVE-2025-8714 |
| f55faa3 | PR #927 | Transaction fix for `clone --follow --snapshot` | - |
| 3b071fa | Our fix | Proper PG18 snprintf.c using SIZEOF_LONG (replaces workaround) | #916 |
| e10098d | PR #924 | Fix crash with `clone --follow --filter` ("database source is already in use") | #829, #871, #910 |

## PostgreSQL 18 Build Fix Details

PostgreSQL 18 removed the `HAVE_LONG_INT_64` and `HAVE_LONG_LONG_INT_64` macros from `pg_config.h`.

Our fix updates `src/bin/lib/pg/snprintf.c` to use `SIZEOF_LONG` and `SIZEOF_LONG_LONG` comparisons instead, matching PostgreSQL 18's own `src/port/snprintf.c`. This is backwards compatible with PG14-17.

## PostgreSQL 17.6+ Security Fix

PostgreSQL 17.6 added `\restrict` and `\unrestrict` metacommands to `pg_dumpall` output to mitigate CVE-2025-8714. pgcopydb now skips these lines when restoring roles.

## Known Issues NOT Fixed

These issues remain open and may affect your use case:

| Issue | Severity | Problem | Workaround |
|-------|----------|---------|------------|
| #931 | **CRITICAL** | String `"null"` becomes actual `NULL` in follow mode with `test_decoding` | Use `wal2json` output plugin |
| #915 | HIGH | Large objects fail on PG17 | Avoid BLOBs or use older PG version |
| #930 | HIGH | OID collision breaks filtering | Avoid `--filter` on affected databases |
| #919 | MEDIUM | JSON column errors in CDC mode | Avoid JSON columns in replicated tables |

## CI Status

This branch includes CI configuration to test against PostgreSQL 16, 17, and 18.

## Links

- Branch: https://github.com/jmealo/pgcopydb/tree/pg17-18-combined-fixes
- CI Actions: https://github.com/jmealo/pgcopydb/actions
