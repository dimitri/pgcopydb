---
--- pgcopydb tests/cdc-partitioned-target/dml-truncate.sql
---
--- DML phase 2: TRUNCATE the flat source. ld_transform writes this as
--- "TRUNCATE ONLY ...", which Postgres rejects against the partitioned
--- target unless ld_apply rewrites it. After replay the target must be
--- empty.
---

truncate only partitioned_target.events;
