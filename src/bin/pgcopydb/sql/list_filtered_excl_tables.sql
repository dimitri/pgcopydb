-- Returns tables that ARE excluded by exclude-schema or exclude-table rules;
-- these are the objects filtered out when exclusion filters are active.
-- $1/$2: incl_exact/incl_re schema arrays (for include-only-schema pass-through).
-- $3..$10: paired text[] arrays for EE/ER/RE/RR exclude-table patterns.
-- A table is returned when it fails the schema include filter OR matches
-- an exclude-table pattern.
WITH filters AS (
    SELECT
        -- schema-level: include-only exact/regex (table is excluded if schema not included)
        $1::text[] AS incl_schema_exact,
        $2::text[] AS incl_schema_re,
        -- schema-level: exclude exact/regex
        $3::text[] AS excl_schema_exact,
        $4::text[] AS excl_schema_re,
        -- exclude table: exact nsp + exact rel
        $5::text[] AS excl_ee_nsp, $6::text[] AS excl_ee_rel,
        -- exclude table: exact nsp + regex rel
        $7::text[] AS excl_er_nsp, $8::text[] AS excl_er_rel_re,
        -- exclude table: regex nsp + exact rel
        $9::text[] AS excl_re_nsp_re, $10::text[] AS excl_re_rel,
        -- exclude table: regex nsp + regex rel
        $11::text[] AS excl_rr_nsp_re, $12::text[] AS excl_rr_rel_re
)
SELECT c.oid,
       format('%I', n.nspname) AS nspname,
       format('%I', c.relname) AS relname,
       c.relkind AS relkind,
       pg_am.amname,
       c.relpages, c.reltuples::bigint,
       c.relpages * current_setting('block_size')::bigint AS bytesestimate,
       pg_size_pretty(c.relpages * current_setting('block_size')::bigint),
       false AS excludedata,
       format('%s %s %s',
              regexp_replace(n.nspname, '[\n\r]', ' '),
              regexp_replace(c.relname, '[\n\r]', ' '),
              regexp_replace(auth.rolname, '[\n\r]', ' ')),
       CASE WHEN pkeys.attname IS NOT NULL
            THEN format('%I', pkeys.attname)
       END AS partkey

  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
  LEFT JOIN pg_catalog.pg_am ON c.relam = pg_am.oid
  JOIN pg_roles auth ON auth.oid = c.relowner,
       filters f

-- find a copy partition key candidate
  LEFT JOIN LATERAL (
      SELECT indrelid, indexrelid, a.attname
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_attribute a
          ON a.attrelid = c.oid AND attnum = indkey[0]
       WHERE x.indrelid = c.oid
         AND (indisprimary OR indisunique)
         AND array_length(indkey::integer[], 1) = 1
         AND atttypid IN ('smallint'::regtype, 'int'::regtype, 'bigint'::regtype)
    ORDER BY NOT indisprimary, NOT indisunique
       LIMIT 1
  ) AS pkeys ON true

 WHERE c.relkind IN ('r', 'p', 'm') AND c.relpersistence IN ('p', 'u')
   AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
   AND n.nspname !~ 'pgcopydb'
   -- table is in the excluded set if its schema is excluded OR if the table itself
   -- matches an exclude-table entry
   AND (
       -- schema not in include-only set (when include-only-schema is active)
       (f.incl_schema_exact IS NOT NULL AND n.nspname <> ALL(f.incl_schema_exact))
    OR (f.incl_schema_re IS NOT NULL AND NOT (n.nspname ~ ANY(f.incl_schema_re)))
    OR -- schema matches an explicit exclude-schema rule
       EXISTS (SELECT 1 FROM unnest(f.excl_schema_exact) AS t(nsp)
               WHERE n.nspname = t.nsp)
    OR (f.excl_schema_re IS NOT NULL AND n.nspname ~ ANY(f.excl_schema_re))
    OR -- table matches an explicit exclude-table rule
       EXISTS (SELECT 1 FROM unnest(f.excl_ee_nsp, f.excl_ee_rel) AS t(nsp, rel)
               WHERE n.nspname = t.nsp AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.excl_er_nsp, f.excl_er_rel_re) AS t(nsp, rel_re)
               WHERE n.nspname = t.nsp AND c.relname ~ t.rel_re)
    OR EXISTS (SELECT 1 FROM unnest(f.excl_re_nsp_re, f.excl_re_rel) AS t(nsp_re, rel)
               WHERE n.nspname ~ t.nsp_re AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.excl_rr_nsp_re, f.excl_rr_rel_re) AS t(nsp_re, rel_re)
               WHERE n.nspname ~ t.nsp_re AND c.relname ~ t.rel_re)
   )
   -- extension guard
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
        WHERE d.classid = 'pg_class'::regclass
          AND d.objid = c.oid AND d.deptype = 'e'
   )

 ORDER BY n.nspname, c.relname;
