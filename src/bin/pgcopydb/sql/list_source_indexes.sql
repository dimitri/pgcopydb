-- $1::oid[]   : table OIDs from s_table (NULL means all tables)
-- $2::text[], $3::text[] : excl_index nspname/relname paired arrays
-- $4::boolean : true = filtersDB query (include excl indexes + indexes on excl tables)
--               false = sourceDB query (exclude explicitly-excluded indexes)
WITH filters AS (
    SELECT $1::oid[]  AS table_oids,
           $2::text[] AS excl_idx_nsp,
           $3::text[] AS excl_idx_rel,
           $4::boolean AS for_filter
)
SELECT i.oid,
       format('%I', n.nspname) AS inspname,
       format('%I', i.relname) AS irelname,
       r.oid,
       format('%I', rn.nspname) AS rnspname,
       format('%I', r.relname) AS rrelname,
       indisprimary,
       indisunique,
       (SELECT string_agg(format('%I', attname), ',')
          FROM pg_attribute
         WHERE attrelid = r.oid
           AND array[attnum::integer] <@ indkey::integer[]
       ) AS cols,
       pg_get_indexdef(indexrelid),
       c.oid,
       c.condeferrable,
       c.condeferred,
       CASE WHEN conname IS NOT NULL
            THEN format('%I', c.conname)
       END AS conname,
       pg_get_constraintdef(c.oid),
       format('%s %s %s',
              regexp_replace(n.nspname, '[\n\r]', ' '),
              regexp_replace(i.relname, '[\n\r]', ' '),
              regexp_replace(auth.rolname, '[\n\r]', ' '))

  FROM pg_index x
  JOIN pg_class i ON i.oid = x.indexrelid
  JOIN pg_class r ON r.oid = x.indrelid
  JOIN pg_namespace n ON n.oid = i.relnamespace
  JOIN pg_namespace rn ON rn.oid = r.relnamespace
  JOIN pg_roles auth ON auth.oid = i.relowner
  LEFT JOIN pg_depend d
         ON d.classid = 'pg_class'::regclass
        AND d.objid = i.oid
        AND d.refclassid = 'pg_constraint'::regclass
        AND d.deptype = 'i'
  LEFT JOIN pg_constraint c ON c.oid = d.refobjid,
       filters f

 WHERE r.relkind IN ('r', 'm') AND r.relpersistence IN ('p', 'u')
   AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
   AND n.nspname !~ 'pgcopydb'
   -- extension guard
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d2
        WHERE d2.classid = 'pg_class'::regclass
          AND d2.objid = r.oid AND d2.deptype = 'e'
   )
   AND (
       -- sourceDB path: index on an in-scope table AND not in excl list
       (NOT f.for_filter
        AND (f.table_oids IS NULL OR r.oid = ANY(f.table_oids))
        AND NOT EXISTS (SELECT 1 FROM unnest(f.excl_idx_nsp, f.excl_idx_rel) AS t(nsp, rel)
                        WHERE n.nspname = t.nsp AND i.relname = t.rel)
       )
    OR
       -- filtersDB path: index on excluded table OR explicitly excluded
       (f.for_filter
        AND (
            r.oid = ANY(f.table_oids)
            OR (f.excl_idx_nsp IS NOT NULL
                AND EXISTS (SELECT 1 FROM unnest(f.excl_idx_nsp, f.excl_idx_rel) AS t(nsp, rel)
                            WHERE n.nspname = t.nsp AND i.relname = t.rel))
        )
       )
   )

 ORDER BY n.nspname, r.relname, i.relname;
