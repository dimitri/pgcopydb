-- Column-level ACL and COMMENT entries carry a composite "COLUMN <table>.<col>"
-- tag in the pg_restore TOC. Regression test for the ARCHIVE_TAG_COLUMN mapping.
CREATE TABLE column_acl_test (
    id   integer,
    name text
);

GRANT SELECT (id) ON column_acl_test TO PUBLIC;
COMMENT ON COLUMN column_acl_test.name IS 'column comment regression for the COLUMN archive TOC tag';

-- pg_dump quotes the TOC tag names with fmtId(): exercise a dot, spaces and
-- an escaped double-quote in the table name, and a dot in the column name.
CREATE TABLE "column.acl ""quoted"" test" (
    "id.col" integer,
    name     text
);

GRANT SELECT ("id.col") ON "column.acl ""quoted"" test" TO PUBLIC;
COMMENT ON COLUMN "column.acl ""quoted"" test"."id.col" IS 'quoted column comment';

-- the only table kept by the filtering test in 10-column-acl-filtering.sh
CREATE TABLE column_acl_kept (
    id   integer,
    name text
);

GRANT SELECT (id) ON column_acl_kept TO PUBLIC;
COMMENT ON COLUMN column_acl_kept.name IS 'kept column comment';
