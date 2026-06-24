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
       r1.oid              AS ownedby,
       r2.oid              AS attrelid,
       d2.attrdef_oid      AS attroid

  FROM seqs AS s

  -- Use LATERAL+LIMIT 1 to avoid duplicate rows when a sequence has both
  -- deptype='a' (OWNED BY) and deptype='i' (internal) pg_depend entries,
  -- which happens for SERIAL columns after pg_restore.  Prefer 'a' over 'i'.
  LEFT JOIN LATERAL (
      SELECT refobjid
        FROM pg_depend
       WHERE objid = s.seqoid
         AND classid = 'pg_class'::regclass
         AND refclassid = 'pg_class'::regclass
         AND deptype IN ('i', 'a')
       ORDER BY CASE deptype WHEN 'a' THEN 0 ELSE 1 END
       LIMIT 1
  ) d1 ON true

  -- Use LATERAL+LIMIT 1 to avoid duplicate rows when a sequence is used as
  -- DEFAULT for multiple columns (e.g. partitioned table + each partition).
  LEFT JOIN LATERAL (
      SELECT d2.objid AS attrdef_oid, at.attrelid
        FROM pg_depend d2
        JOIN pg_attrdef a  ON a.oid = d2.objid
        JOIN pg_attribute at ON at.attrelid = a.adrelid AND at.attnum = a.adnum
       WHERE d2.refobjid = s.seqoid
         AND d2.refclassid = 'pg_class'::regclass
         AND d2.classid = 'pg_attrdef'::regclass
       ORDER BY at.attrelid
       LIMIT 1
  ) d2 ON true

  LEFT JOIN pg_class r1 ON r1.oid = d1.refobjid
  LEFT JOIN pg_class r2 ON r2.oid = d2.attrelid

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

     -- sequence used as column DEFAULT (no table filter: include if any attrdef exists)
  OR (r2.oid IS NOT NULL AND f.table_oids IS NULL)

     -- sequence used as column DEFAULT (table filter active): use EXISTS to check ALL
     -- tables using this sequence as default, not just the d2 LATERAL representative.
     -- Needed when a sequence is OWNED BY a filtered-out table but used as DEFAULT
     -- by an included table (e.g. LIKE ... INCLUDING ALL across schemas).
  OR (f.table_oids IS NOT NULL
      AND EXISTS (
          SELECT 1
            FROM pg_depend d2x
            JOIN pg_attrdef a2 ON a2.oid = d2x.objid
            JOIN pg_attribute at2 ON at2.attrelid = a2.adrelid AND at2.attnum = a2.adnum
           WHERE d2x.refobjid = s.seqoid
             AND d2x.refclassid = 'pg_class'::regclass
             AND d2x.classid = 'pg_attrdef'::regclass
             AND at2.attrelid = ANY(f.table_oids)
      ))
 )

 ORDER BY nspname, relname;
