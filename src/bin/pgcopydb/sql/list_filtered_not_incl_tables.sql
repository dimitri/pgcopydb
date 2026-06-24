-- Returns tables that are NOT in the include-only-table list; these are the
-- objects filtered out when include-only-table constraints are active.
-- $1..$8: paired text[] arrays for EE/ER/RE/RR include-only-table patterns.
WITH filters AS (
    SELECT
        -- include-only table: exact nsp + exact rel
        $1::text[] AS incl_ee_nsp,  $2::text[] AS incl_ee_rel,
        -- include-only table: exact nsp + regex rel
        $3::text[] AS incl_er_nsp,  $4::text[] AS incl_er_rel_re,
        -- include-only table: regex nsp + exact rel
        $5::text[] AS incl_re_nsp_re, $6::text[] AS incl_re_rel,
        -- include-only table: regex nsp + regex rel
        $7::text[] AS incl_rr_nsp_re, $8::text[] AS incl_rr_rel_re
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
   -- keep tables NOT matching any include-only-table entry
   AND NOT (
       EXISTS (SELECT 1 FROM unnest(f.incl_ee_nsp, f.incl_ee_rel) AS t(nsp, rel)
               WHERE n.nspname = t.nsp AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.incl_er_nsp, f.incl_er_rel_re) AS t(nsp, rel_re)
               WHERE n.nspname = t.nsp AND c.relname ~ t.rel_re)
    OR EXISTS (SELECT 1 FROM unnest(f.incl_re_nsp_re, f.incl_re_rel) AS t(nsp_re, rel)
               WHERE n.nspname ~ t.nsp_re AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.incl_rr_nsp_re, f.incl_rr_rel_re) AS t(nsp_re, rel_re)
               WHERE n.nspname ~ t.nsp_re AND c.relname ~ t.rel_re)
   )
   -- extension guard
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
        WHERE d.classid = 'pg_class'::regclass
          AND d.objid = c.oid AND d.deptype = 'e'
   )

 ORDER BY n.nspname, c.relname;
