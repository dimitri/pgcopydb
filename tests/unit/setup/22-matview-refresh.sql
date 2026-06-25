--
-- Regression test for issues #501 and #484:
-- REFRESH MATERIALIZED VIEW fails during pg_restore because the
-- search_path is empty at restore time, but the matview definition
-- embeds an unqualified table name inside a text argument to ts_stat().
--
-- ts_stat() accepts a SQL query as text.  PostgreSQL cannot schema-qualify
-- the table name inside that string at CREATE MATERIALIZED VIEW time -- it is
-- opaque text data from the parser's perspective.  At REFRESH time the
-- embedded query is evaluated with whatever search_path is in effect; when
-- pg_restore runs REFRESH with an empty search_path the table is not found.
--
-- The dependency tree below also exercises the dependency ordering that
-- pgcopydb must respect when it drives REFRESH directly:
--
--   public.mv_word_stats   (Root 1 - ts_stat bug, unqualified table name)
--   mvtest.mv_author_summary (Root 2 - simple aggregate, no ordering issue)
--   mvtest.mv_tag_stats    (Root 3 - independent)
--         |
--         +-- Root 1 + Root 2 --> mvtest.mv_combined  (Level 2)
--                                           |
--                                           +--> mvtest.mv_final  (Level 3)
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
-- pg_dump will emit SET search_path = '' before running REFRESH, which
-- is exactly the condition that triggers the bug.
SET search_path TO mvtest, public;

------------------------------------------------------------------------
-- Root 1: public.mv_word_stats
--
-- Uses ts_stat() whose argument is a plain text string.  The table name
-- "documents" inside the string is NOT qualified by Postgres at CREATE
-- time -- it stays as the bare word.  REFRESH works only when search_path
-- includes mvtest.  pg_restore does not restore that search_path.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW public.mv_word_stats AS
SELECT word, ndoc, nentry
FROM ts_stat(
    'SELECT to_tsvector(''simple'',
            coalesce(title, '''') || '' '' || coalesce(body, ''''))
     FROM documents'
)
ORDER BY ndoc DESC, nentry DESC, word
WITH NO DATA;

------------------------------------------------------------------------
-- Root 2: mvtest.mv_author_summary
--
-- Fully qualified; no search_path dependency.  Independent of Root 1.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_author_summary AS
SELECT name,
       row_number() OVER (ORDER BY name) AS rn
FROM   mvtest.authors
ORDER  BY name
WITH NO DATA;

------------------------------------------------------------------------
-- Root 3: mvtest.mv_tag_stats
--
-- Independent of all other matviews.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_tag_stats AS
SELECT label,
       length(label) AS label_len
FROM   mvtest.tags
ORDER  BY label
WITH NO DATA;

------------------------------------------------------------------------
-- Level 2: mvtest.mv_combined
--
-- Depends on both Root 1 (public.mv_word_stats) and
-- Root 2 (mvtest.mv_author_summary).  Must be refreshed after both.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_combined AS
SELECT w.word,
       a.name   AS author,
       a.rn     AS author_rank,
       w.ndoc,
       w.nentry
FROM   public.mv_word_stats       w
       CROSS JOIN mvtest.mv_author_summary a
ORDER  BY w.ndoc DESC, w.word, a.rn
WITH NO DATA;

------------------------------------------------------------------------
-- Level 3: mvtest.mv_final
--
-- Depends on Level 2 (mvtest.mv_combined) only.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mvtest.mv_final AS
SELECT word,
       count(*)    AS author_count,
       sum(ndoc)   AS total_ndoc,
       sum(nentry) AS total_nentry
FROM   mvtest.mv_combined
GROUP  BY word
ORDER  BY total_ndoc DESC, word
WITH NO DATA;

-- Populate all matviews on source (search_path is still mvtest,public here).
REFRESH MATERIALIZED VIEW public.mv_word_stats;
REFRESH MATERIALIZED VIEW mvtest.mv_author_summary;
REFRESH MATERIALIZED VIEW mvtest.mv_tag_stats;
REFRESH MATERIALIZED VIEW mvtest.mv_combined;
REFRESH MATERIALIZED VIEW mvtest.mv_final;

RESET search_path;

--
-- Set the database-level search_path so that new connections on the target
-- (including the fresh libpq connections opened by pgcopydb vacuum workers
-- when they run REFRESH MATERIALIZED VIEW) can resolve the unqualified table
-- name "documents" embedded inside public.mv_word_stats.
--
-- pgcopydb restores ALTER DATABASE SET ... during pre-data, so this setting
-- is in effect when vacuum workers connect to the target during the data phase.
--
ALTER DATABASE postgres SET search_path TO mvtest, public, """abc""";
