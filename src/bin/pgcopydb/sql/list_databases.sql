select d.oid, datname, pg_database_size(d.oid) as bytes,
       pg_size_pretty(pg_database_size(d.oid))
  from pg_database d
 where datname not in ('template0', 'template1')
order by datname
