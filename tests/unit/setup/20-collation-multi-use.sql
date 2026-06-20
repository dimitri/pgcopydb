--
-- Regression test for https://github.com/dimitri/pgcopydb/issues/889
--
-- Two views that both reference the same built-in "C" collation cause
-- schema_list_collations() to return two rows with the same colloid (one per
-- view column) but different description values.  UNION does not collapse them
-- because the description differs, so catalog_add_s_coll() receives two INSERT
-- attempts for the same OID and fails with a SQLite PRIMARY KEY constraint
-- error.
--
-- The fix wraps the UNION in SELECT DISTINCT ON (colloid) so that only one row
-- per collation OID is inserted into s_coll.
--
CREATE VIEW v_coll_test AS SELECT 'foo' COLLATE "C";
CREATE VIEW v_coll_test2 AS SELECT * FROM v_coll_test;
