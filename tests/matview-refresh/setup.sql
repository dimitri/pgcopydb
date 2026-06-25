--
-- Regression test for issues #501 and #484.
--
-- The bug: pg_restore runs REFRESH MATERIALIZED VIEW with an empty
-- search_path (SET search_path = ''), so any matview whose definition
-- embeds an unqualified table name in a ts_stat() argument string fails
-- with "relation X does not exist".
--
-- The fix: pgcopydb takes ownership of REFRESH MATERIALIZED VIEW and runs
-- each one from its own fresh libpq connection, which inherits the
-- database-level search_path (ALTER DATABASE SET search_path) that
-- pg_restore already restored during pre-data.
--
-- PostgreSQL 17 introduced RestrictSearchPath() inside RefreshMatViewByOid(),
-- which forces search_path to 'pg_catalog, pg_temp' during any REFRESH
-- (and also during CREATE MATERIALIZED VIEW ... WITH DATA).  On PG17+ a
-- matview with an unqualified table name in ts_stat() therefore cannot be
-- populated at all, so the original bug scenario is PG <= 16 only.
--
-- This fixture exercises the general REFRESH orchestration that pgcopydb
-- provides on all supported versions: dependency ordering across a
-- three-level matview tree and parallel execution via vacuum workers.
--
--   public.mv_word_stats      (Root 1)
--   mvtest.mv_author_summary  (Root 2 — independent)
--   mvtest.mv_tag_stats       (Root 3 — independent, parallel to roots 1+2)
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

------------------------------------------------------------------------
-- Root 1: public.mv_word_stats
--
-- Uses ts_stat() with a fully-qualified table name so the fixture works
-- on all supported PostgreSQL versions.  On PG <= 16, the original bug
-- (#484 / #501) was that an *unqualified* name here would fail when
-- pg_restore ran REFRESH with SET search_path = ''.  On PG17+,
-- RefreshMatViewByOid() forces search_path = 'pg_catalog, pg_temp'
-- unconditionally, so an unqualified name cannot even be used to
-- populate a matview at create time.
------------------------------------------------------------------------
CREATE MATERIALIZED VIEW public.mv_word_stats AS
SELECT word, ndoc, nentry
FROM ts_stat(
    'SELECT to_tsvector(''simple'',
            coalesce(title, '''') || '' '' || coalesce(body, ''''))
     FROM mvtest.documents'
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

