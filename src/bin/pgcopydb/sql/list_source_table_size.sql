-- $1::oid[] : table OIDs from s_table (NULL means all tables)
WITH filters AS (
    SELECT $1::oid[] AS table_oids
)
SELECT c.oid,
       pg_table_size(c.oid) AS bytes,
       pg_size_pretty(pg_table_size(c.oid))

  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid,
       filters f

 WHERE c.relkind = 'r' AND c.relpersistence IN ('p', 'u')
   AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
   -- table filter: restrict to included tables (or all when NULL)
   AND (f.table_oids IS NULL OR c.oid = ANY(f.table_oids))
   -- extension guard
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
        WHERE d.classid = 'pg_class'::regclass
          AND d.objid = c.oid AND d.deptype = 'e'
   );
