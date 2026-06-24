-- $1::oid[] : table OIDs from s_table (NULL means no table filter)
-- $2::oid[] : namespace OIDs from s_namespace (NULL means no namespace filter)
WITH
 filters AS (
     SELECT $1::oid[] AS table_oids,
            $2::oid[] AS namespace_oids
 ),
 seqs AS (
    SELECT s.oid AS seqoid,
           format('%I', sn.nspname) AS nspname,
           format('%I', s.relname) AS relname,
           format('%s %s %s',
                  regexp_replace(sn.nspname, '[\n\r]', ' '),
                  regexp_replace(s.relname, '[\n\r]', ' '),
                  regexp_replace(auth.rolname, '[\n\r]', ' ')) AS restore_list_name,
           sn.oid AS relnamespace

      FROM pg_class s
      JOIN pg_namespace sn ON sn.oid = s.relnamespace
      JOIN pg_roles auth ON auth.oid = s.relowner

     WHERE s.relkind = 'S'
       AND sn.nspname !~ 'pgcopydb'
       AND NOT EXISTS (
           SELECT 1 FROM pg_depend d
            WHERE d.classid = 'pg_class'::regclass
              AND d.objid = s.oid
              AND d.deptype = 'e'
       )
 )
SELECT s.seqoid,
       s.nspname,
       s.relname,
       s.restore_list_name,
       r1.oid AS ownedby,
       r2.oid AS attrelid,
       a.oid  AS attroid

  FROM seqs AS s

  LEFT JOIN pg_depend d1
         ON d1.objid = s.seqoid
        AND d1.classid = 'pg_class'::regclass
        AND d1.refclassid = 'pg_class'::regclass
        AND d1.deptype IN ('i', 'a')

  LEFT JOIN pg_depend d2
         ON d2.refobjid = s.seqoid
        AND d2.refclassid = 'pg_class'::regclass
        AND d2.classid = 'pg_attrdef'::regclass
  LEFT JOIN pg_attrdef a ON a.oid = d2.objid
  LEFT JOIN pg_attribute at
         ON at.attrelid = a.adrelid
        AND at.attnum = a.adnum

  LEFT JOIN pg_class r1 ON r1.oid = d1.refobjid
  LEFT JOIN pg_class r2 ON r2.oid = at.attrelid

  CROSS JOIN filters f

 WHERE (
     -- no filter: include all sequences
     (f.table_oids IS NULL AND f.namespace_oids IS NULL)

     -- standalone sequence: no ownership → filter by namespace
  OR (r1.oid IS NULL AND r2.oid IS NULL
      AND (f.namespace_oids IS NULL OR s.relnamespace = ANY(f.namespace_oids)))

     -- sequence owned via OWNED BY relationship
  OR (r1.oid IS NOT NULL
      AND (f.table_oids IS NULL OR r1.oid = ANY(f.table_oids)))

     -- sequence used as column DEFAULT
  OR (r2.oid IS NOT NULL
      AND (f.table_oids IS NULL OR r2.oid = ANY(f.table_oids)))
 )

 ORDER BY nspname, relname;
