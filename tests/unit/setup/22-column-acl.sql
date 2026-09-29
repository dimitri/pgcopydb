-- Column-level ACL and COMMENT entries carry a composite "COLUMN <table>.<col>"
-- tag in the pg_restore TOC. Regression test for the ARCHIVE_TAG_COLUMN mapping.
CREATE TABLE column_acl_test (
    id   integer,
    name text
);

GRANT SELECT (id) ON column_acl_test TO PUBLIC;
COMMENT ON COLUMN column_acl_test.name IS 'column comment regression for the COLUMN archive TOC tag';
