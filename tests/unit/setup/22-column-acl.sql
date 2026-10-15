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

-- the only table kept by the include-only filtering test in
-- 10-acl-comment-filtering.sh
CREATE TABLE column_acl_kept (
    id   integer,
    name text
);

GRANT SELECT (id) ON column_acl_kept TO PUBLIC;
COMMENT ON COLUMN column_acl_kept.name IS 'kept column comment';

-- Table-level ACL and COMMENT entries, and COMMENT entries of objects that
-- belong to a table, filtered out with their table in 10-acl-comment-filtering.sh
CREATE TABLE acl_gone (
    id   serial PRIMARY KEY,
    note text CONSTRAINT acl_gone_note_check CHECK (note <> '')
);

CREATE INDEX acl_gone_note_idx ON acl_gone (note);

CREATE FUNCTION acl_gone_trg_fn() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;

CREATE TRIGGER acl_gone_trg BEFORE INSERT ON acl_gone
   FOR EACH ROW EXECUTE FUNCTION acl_gone_trg_fn();

CREATE POLICY acl_gone_pol ON acl_gone USING (true);
CREATE RULE acl_gone_rule AS ON UPDATE TO acl_gone DO ALSO NOTHING;

COMMENT ON TABLE acl_gone IS 'table comment';
GRANT SELECT ON acl_gone TO PUBLIC;
COMMENT ON INDEX acl_gone_note_idx IS 'index comment';
COMMENT ON CONSTRAINT acl_gone_note_check ON acl_gone IS 'constraint comment';
COMMENT ON TRIGGER acl_gone_trg ON acl_gone IS 'trigger comment';
COMMENT ON POLICY acl_gone_pol ON acl_gone IS 'policy comment';
COMMENT ON RULE acl_gone_rule ON acl_gone IS 'rule comment';
COMMENT ON SEQUENCE acl_gone_id_seq IS 'sequence comment';
GRANT USAGE ON SEQUENCE acl_gone_id_seq TO PUBLIC;

