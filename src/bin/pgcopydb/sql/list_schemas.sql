WITH filters AS (
    SELECT $1::text[] AS incl_exact,
           $2::text[] AS incl_re,
           $3::text[] AS excl_exact,
           $4::text[] AS excl_re
)
SELECT n.oid,
       n.nspname,
       format('- %s %s',
                regexp_replace(n.nspname, '[\n\r]', ' '),
                regexp_replace(auth.rolname, '[\n\r]', ' '))
  FROM pg_namespace n
  JOIN pg_roles auth ON auth.oid = n.nspowner,
       filters f
 WHERE n.nspname <> 'information_schema'
   AND n.nspname !~ '^pg_'
   AND n.nspname <> 'pgcopydb'
   AND (
       f.incl_exact IS NULL AND f.incl_re IS NULL
       OR (f.incl_exact IS NOT NULL AND n.nspname = ANY(f.incl_exact))
       OR (f.incl_re IS NOT NULL AND n.nspname ~ ANY(f.incl_re))
   )
   AND (f.excl_exact IS NULL OR n.nspname <> ALL(f.excl_exact))
   AND (f.excl_re IS NULL OR NOT (n.nspname ~ ANY(f.excl_re)))
 ORDER BY n.nspname;
