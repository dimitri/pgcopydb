--
-- Regression test for issues #501 and #484:
-- REFRESH MATERIALIZED VIEW fails during pg_restore because the
-- search_path is empty at restore time, but the matview definition
-- embeds an unqualified table name inside a text argument to ts_stat().
--
-- ts_stat() accepts a SQL query as text.  PostgreSQL cannot schema-qualify
-- the table name inside that string at CREATE MATERIALIZED VIEW time — it is
-- opaque text data from the parser's perspective.  At REFRESH time the
-- embedded query is evaluated with whatever search_path is in effect; when
-- pg_restore runs REFRESH with an empty search_path the table is not found.
--
-- The dependency tree below also exercises the parallelism / ordering
-- requirements that a pgcopydb-owned REFRESH must eventually satisfy:
--
--   public.mv_word_stats   (Root 1 — ts_stat bug, unqualified table name)
--   mvtest.mv_author_summary (Root 2 — simple aggregate, no ordering issue)
--   mvtest.mv_tag_stats    (Root 3 — independent, parallel to roots 1+2)
--         |
--         └── Root 1 + Root 2 ──> mvtest.mv_combined  (Level 2)
--                                           |
--                                           └──> mvtest.mv_final  (Level 3)
--

CREATE SCHEMA mvtest;

CREATE TABLE mvtest.documents (
    id    serial PRIMARY KEY,
    title text   NOT NULL,
    body  text
);

CREATE TABLE mvtest.authors (
    id   serial PRIMARY KEY,
    name text   NOT NULL
);

CREATE TABLE mvtest.tags (
    id    serial PRIMARY KEY,
    label text   NOT NULL
);

INSERT INTO mvtest.documents (title, body) VALUES
    ('PostgreSQL Full Text Search', 'postgres full text search tsearch vectors'),
    ('Advanced Database Queries',   'database query optimization explain analyze'),
    ('Indexing Strategies',         'btree hash gin gist index postgres performance');

INSERT INTO mvtest.authors (name) VALUES ('Alice'), ('Bob'), ('Carol');

INSERT INTO mvtest.tags (label) VALUES ('postgres'), ('database'), ('index');

-- Set search_path so that unqualified names resolve to mvtest.
-- CREATE MATERIALIZED VIEW ... WITH DATA populates via ExecCreateTableAs(),
-- which runs in the current session context and sees this search_path.
-- pg_dump will later emit SET search_path = '' before running REFRESH,
-- which is exactly the condition that triggers the bug on the target side.
--
-- Note: REFRESH MATERIALIZED VIEW (ExecRefreshMatView) forces search_path=''
-- since PostgreSQL 17 for security hardening — so we populate the source
-- matviews at CREATE time (WITH DATA), not via explicit REFRESH, to stay
-- compatible with PG17+.
SET search_path TO mvtest, public;

------------------------------------------------------------------------
-- Root 1: public.mv_word_stats
--
-- Uses ts_stat() whose argument is a plain text string.  The table name
-- "documents" inside the string is NOT qualified by Postgres at CREATE
-- time — it stays as the bare word.  REFRESH works only when search_path
-- includes mvtest, which pg_restore does not do.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW public.mv_word_stats AS
SELECT word, ndoc, nentry
FROM ts_stat(
    'SELECT to_tsvector(''simple'',
            coalesce(title, '''') || '' '' || coalesce(body, ''''))
     FROM documents'
)
ORDER BY ndoc DESC, nentry DESC, word;

------------------------------------------------------------------------
-- Root 2: mvtest.mv_author_summary
--
-- Fully qualified; no search_path dependency.  Independent of Root 1.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_author_summary AS
SELECT name,
       row_number() OVER (ORDER BY name) AS rn
FROM   mvtest.authors
ORDER  BY name;

------------------------------------------------------------------------
-- Root 3: mvtest.mv_tag_stats
--
-- Independent of all other matviews; should be refreshable in parallel
-- with the Root-1 / Root-2 chain.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_tag_stats AS
SELECT label,
       length(label) AS label_len
FROM   mvtest.tags
ORDER  BY label;

------------------------------------------------------------------------
-- Level 2: mvtest.mv_combined
--
-- Depends on both Root 1 (public.mv_word_stats) and
-- Root 2 (mvtest.mv_author_summary).  Must be refreshed only after
-- both roots are populated.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_combined AS
SELECT w.word,
       a.name   AS author,
       a.rn     AS author_rank,
       w.ndoc,
       w.nentry
FROM   public.mv_word_stats       w
       CROSS JOIN mvtest.mv_author_summary a
ORDER  BY w.ndoc DESC, w.word, a.rn;

------------------------------------------------------------------------
-- Level 3: mvtest.mv_final
--
-- Depends on Level 2 (mvtest.mv_combined) only.
-- Three levels deep from the root, testing that the dependency chain
-- is followed correctly.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_final AS
SELECT word,
       count(*)    AS author_count,
       sum(ndoc)   AS total_ndoc,
       sum(nentry) AS total_nentry
FROM   mvtest.mv_combined
GROUP  BY word
ORDER  BY total_ndoc DESC, word;

RESET search_path;
