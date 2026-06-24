-- SQLite query used by catalog_filters_as_json().
-- $1 : filter type string (e.g. 'source', 'target')
--
-- Produces the same JSON object as filters_as_json() in filtering.c but reads
-- from f_schema / f_table instead of in-memory SourceFilters.  Two CTE
-- aggregate passes (one per table) replace twelve correlated subqueries.
--
-- NULLIF(..., '[]') converts an empty json_group_array result to NULL so that
-- the kv CTE's WHERE clause suppresses keys for empty categories, matching the
-- filters_as_json() behaviour of only emitting a key when count > 0.
--
-- json(v) in json_group_object() marks pre-serialised JSON text (the output of
-- json_group_array or json_quote) as already-parsed so it is embedded as a
-- JSON value rather than double-encoded as a string.
WITH
sa AS (
    SELECT
        NULLIF(json_group_array(nspname)
            FILTER (WHERE section = 'incl_schema' AND nspname    IS NOT NULL), '[]') AS incl_exact,
        NULLIF(json_group_array(nspname_re)
            FILTER (WHERE section = 'incl_schema' AND nspname_re IS NOT NULL), '[]') AS incl_re,
        NULLIF(json_group_array(nspname)
            FILTER (WHERE section = 'excl_schema' AND nspname    IS NOT NULL), '[]') AS excl_exact,
        NULLIF(json_group_array(nspname_re)
            FILTER (WHERE section = 'excl_schema' AND nspname_re IS NOT NULL), '[]') AS excl_re
      FROM f_schema
),
ta AS (
    SELECT
        NULLIF(json_group_array(
            json_object('schema', nspname, 'name', relname))
            FILTER (WHERE section = 'excl_table'
                      AND nspname_re IS NULL AND relname_re IS NULL),  '[]') AS excl_ee,
        NULLIF(json_group_array(
            CASE
              WHEN nspname    IS NOT NULL AND relname_re IS NOT NULL
                THEN json_object('schema',    nspname,    'name-re',  relname_re)
              WHEN nspname_re IS NOT NULL AND relname    IS NOT NULL
                THEN json_object('schema-re', nspname_re, 'name',     relname)
              ELSE       json_object('schema-re', nspname_re, 'name-re',  relname_re)
            END)
            FILTER (WHERE section = 'excl_table'
                      AND (nspname_re IS NOT NULL OR relname_re IS NOT NULL)), '[]') AS excl_pat,

        NULLIF(json_group_array(
            json_object('schema', nspname, 'name', relname))
            FILTER (WHERE section = 'xdat_table'
                      AND nspname_re IS NULL AND relname_re IS NULL),  '[]') AS xdat_ee,
        NULLIF(json_group_array(
            CASE
              WHEN nspname    IS NOT NULL AND relname_re IS NOT NULL
                THEN json_object('schema',    nspname,    'name-re',  relname_re)
              WHEN nspname_re IS NOT NULL AND relname    IS NOT NULL
                THEN json_object('schema-re', nspname_re, 'name',     relname)
              ELSE       json_object('schema-re', nspname_re, 'name-re',  relname_re)
            END)
            FILTER (WHERE section = 'xdat_table'
                      AND (nspname_re IS NOT NULL OR relname_re IS NOT NULL)), '[]') AS xdat_pat,

        NULLIF(json_group_array(
            json_object('schema', nspname, 'name', relname))
            FILTER (WHERE section = 'excl_index'
                      AND nspname_re IS NULL AND relname_re IS NULL),  '[]') AS excl_idx_ee,
        NULLIF(json_group_array(
            CASE
              WHEN nspname    IS NOT NULL AND relname_re IS NOT NULL
                THEN json_object('schema',    nspname,    'name-re',  relname_re)
              WHEN nspname_re IS NOT NULL AND relname    IS NOT NULL
                THEN json_object('schema-re', nspname_re, 'name',     relname)
              ELSE       json_object('schema-re', nspname_re, 'name-re',  relname_re)
            END)
            FILTER (WHERE section = 'excl_index'
                      AND (nspname_re IS NOT NULL OR relname_re IS NOT NULL)), '[]') AS excl_idx_pat,

        NULLIF(json_group_array(
            json_object('schema', nspname, 'name', relname))
            FILTER (WHERE section = 'incl_table'
                      AND nspname_re IS NULL AND relname_re IS NULL),  '[]') AS incl_ee,
        NULLIF(json_group_array(
            CASE
              WHEN nspname    IS NOT NULL AND relname_re IS NOT NULL
                THEN json_object('schema',    nspname,    'name-re',  relname_re)
              WHEN nspname_re IS NOT NULL AND relname    IS NOT NULL
                THEN json_object('schema-re', nspname_re, 'name',     relname)
              ELSE       json_object('schema-re', nspname_re, 'name-re',  relname_re)
            END)
            FILTER (WHERE section = 'incl_table'
                      AND (nspname_re IS NOT NULL OR relname_re IS NOT NULL)), '[]') AS incl_pat
      FROM f_table
),
kv(k, v) AS (
                   SELECT 'type',                        json_quote($1)
    UNION ALL      SELECT 'include-only-schema',         sa.incl_exact    FROM sa WHERE sa.incl_exact    IS NOT NULL
    UNION ALL      SELECT 'include-only-schema-pattern', sa.incl_re       FROM sa WHERE sa.incl_re       IS NOT NULL
    UNION ALL      SELECT 'exclude-schema',              sa.excl_exact    FROM sa WHERE sa.excl_exact    IS NOT NULL
    UNION ALL      SELECT 'exclude-schema-pattern',      sa.excl_re       FROM sa WHERE sa.excl_re       IS NOT NULL
    UNION ALL      SELECT 'exclude-table',               ta.excl_ee       FROM ta WHERE ta.excl_ee       IS NOT NULL
    UNION ALL      SELECT 'exclude-table-pattern',       ta.excl_pat      FROM ta WHERE ta.excl_pat      IS NOT NULL
    UNION ALL      SELECT 'exclude-table-data',          ta.xdat_ee       FROM ta WHERE ta.xdat_ee       IS NOT NULL
    UNION ALL      SELECT 'exclude-table-data-pattern',  ta.xdat_pat      FROM ta WHERE ta.xdat_pat      IS NOT NULL
    UNION ALL      SELECT 'exclude-index',               ta.excl_idx_ee   FROM ta WHERE ta.excl_idx_ee   IS NOT NULL
    UNION ALL      SELECT 'exclude-index-pattern',       ta.excl_idx_pat  FROM ta WHERE ta.excl_idx_pat  IS NOT NULL
    UNION ALL      SELECT 'include-only-table',          ta.incl_ee       FROM ta WHERE ta.incl_ee       IS NOT NULL
    UNION ALL      SELECT 'include-only-table-pattern',  ta.incl_pat      FROM ta WHERE ta.incl_pat      IS NOT NULL
)
SELECT json_group_object(k, json(v)) FROM kv;
