--
-- cdc-transform-apply/source_schema.sql
--
-- Minimal SQLite seed for source.db.  Load this after `pgcopydb stream init`
-- has initialised the schema DDL, for example:
--
--   sqlite3 /path/to/schema/source.db < source_schema.sql
--
-- The transform step uses source.db to look up attisprimary / attisreplident
-- when parsing test_decoding UPDATE messages that carry no old-key: section
-- (the normal behaviour for REPLICA IDENTITY DEFAULT when only non-PK
-- columns change).
--
-- Table: public.items (id bigserial PK, name text)
--   oid 16385 is an arbitrary placeholder; the lookup is by nspname+relname.
--

-- s_table
INSERT INTO s_table(oid, qname, nspname, relname, amname,
                    restore_list_name, relpages, reltuples, exclude_data,
                    part_key)
VALUES (16385, '"public"."items"', 'public', 'items', 'heap',
        'public items', 1, 3, 0, NULL);

-- s_attr: id  bigint  attnum=1  PK
INSERT INTO s_attr(oid, attnum, attypid, attname,
                   attisprimary, attisreplident, attisgenerated)
VALUES (16385, 1, 20, 'id', 1, 0, 0);

-- s_attr: name  text  attnum=2  not PK
INSERT INTO s_attr(oid, attnum, attypid, attname,
                   attisprimary, attisreplident, attisgenerated)
VALUES (16385, 2, 25, 'name', 0, 0, 0);
