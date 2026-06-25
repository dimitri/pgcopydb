WITH filters AS (
    SELECT
        -- namespace OIDs from s_namespace (included schemas only)
        $1::oid[]  AS nsp_oids,
        -- include-only table: exact nsp + exact rel
        $2::text[] AS incl_ee_nsp,  $3::text[] AS incl_ee_rel,
        -- include-only table: exact nsp + regex rel
        $4::text[] AS incl_er_nsp,  $5::text[] AS incl_er_rel,
        -- include-only table: regex nsp + exact rel
        $6::text[] AS incl_re_nsp, $7::text[] AS incl_re_rel,
        -- include-only table: regex nsp + regex rel
        $8::text[] AS incl_rr_nsp, $9::text[] AS incl_rr_rel,
        -- exclude table: exact nsp + exact rel
        $10::text[] AS excl_ee_nsp, $11::text[] AS excl_ee_rel,
        -- exclude table: exact nsp + regex rel
        $12::text[] AS excl_er_nsp, $13::text[] AS excl_er_rel,
        -- exclude table: regex nsp + exact rel
        $14::text[] AS excl_re_nsp, $15::text[] AS excl_re_rel,
        -- exclude table: regex nsp + regex rel
        $16::text[] AS excl_rr_nsp, $17::text[] AS excl_rr_rel,
        -- exclude-data: exact nsp + exact rel
        $18::text[] AS xdat_ee_nsp, $19::text[] AS xdat_ee_rel,
        -- exclude-data: exact nsp + regex rel
        $20::text[] AS xdat_er_nsp, $21::text[] AS xdat_er_rel,
        -- exclude-data: regex nsp + exact rel
        $22::text[] AS xdat_re_nsp, $23::text[] AS xdat_re_rel,
        -- exclude-data: regex nsp + regex rel
        $24::text[] AS xdat_rr_nsp, $25::text[] AS xdat_rr_rel
)
SELECT c.oid,
       format('%I', n.nspname) AS nspname,
       format('%I', c.relname) AS relname,
       c.relkind AS relkind,
       pg_am.amname,
       c.relpages, c.reltuples::bigint,
       c.relpages * current_setting('block_size')::bigint AS bytesestimate,
       pg_size_pretty(c.relpages * current_setting('block_size')::bigint),
       -- exclude-data: true if table matches any exclude-data entry
       (
           EXISTS (SELECT 1 FROM unnest(f.xdat_ee_nsp, f.xdat_ee_rel) AS t(nsp, rel)
                   WHERE n.nspname = t.nsp AND c.relname = t.rel)
        OR EXISTS (SELECT 1 FROM unnest(f.xdat_er_nsp, f.xdat_er_rel) AS t(nsp, rel_re)
                   WHERE n.nspname = t.nsp AND c.relname ~ t.rel_re)
        OR EXISTS (SELECT 1 FROM unnest(f.xdat_re_nsp, f.xdat_re_rel) AS t(nsp_re, rel)
                   WHERE n.nspname ~ t.nsp_re AND c.relname = t.rel)
        OR EXISTS (SELECT 1 FROM unnest(f.xdat_rr_nsp, f.xdat_rr_rel) AS t(nsp_re, rel_re)
                   WHERE n.nspname ~ t.nsp_re AND c.relname ~ t.rel_re)
       ) AS excludedata,
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

 WHERE c.relkind IN ('r', 'm') AND c.relpersistence IN ('p', 'u')
   AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
   AND n.nspname !~ 'pgcopydb'
   -- namespace filter: restrict to included schemas
   AND n.oid = ANY(f.nsp_oids)
   -- include-only table filter: pass when no include-only constraints, or table matches one
   AND (
       f.incl_ee_nsp IS NULL AND f.incl_er_nsp IS NULL
       AND f.incl_re_nsp IS NULL AND f.incl_rr_nsp IS NULL
    OR EXISTS (SELECT 1 FROM unnest(f.incl_ee_nsp, f.incl_ee_rel) AS t(nsp, rel)
               WHERE n.nspname = t.nsp AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.incl_er_nsp, f.incl_er_rel) AS t(nsp, rel_re)
               WHERE n.nspname = t.nsp AND c.relname ~ t.rel_re)
    OR EXISTS (SELECT 1 FROM unnest(f.incl_re_nsp, f.incl_re_rel) AS t(nsp_re, rel)
               WHERE n.nspname ~ t.nsp_re AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.incl_rr_nsp, f.incl_rr_rel) AS t(nsp_re, rel_re)
               WHERE n.nspname ~ t.nsp_re AND c.relname ~ t.rel_re)
   )
   -- exclude table filter: reject tables that match any exclude entry
   AND NOT (
       EXISTS (SELECT 1 FROM unnest(f.excl_ee_nsp, f.excl_ee_rel) AS t(nsp, rel)
               WHERE n.nspname = t.nsp AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.excl_er_nsp, f.excl_er_rel) AS t(nsp, rel_re)
               WHERE n.nspname = t.nsp AND c.relname ~ t.rel_re)
    OR EXISTS (SELECT 1 FROM unnest(f.excl_re_nsp, f.excl_re_rel) AS t(nsp_re, rel)
               WHERE n.nspname ~ t.nsp_re AND c.relname = t.rel)
    OR EXISTS (SELECT 1 FROM unnest(f.excl_rr_nsp, f.excl_rr_rel) AS t(nsp_re, rel_re)
               WHERE n.nspname ~ t.nsp_re AND c.relname ~ t.rel_re)
   )
   -- extension guard
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
        WHERE d.classid = 'pg_class'::regclass
          AND d.objid = c.oid AND d.deptype = 'e'
   )

 ORDER BY n.nspname, c.relname;
