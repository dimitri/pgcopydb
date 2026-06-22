with indcols as
 (
   select indexrelid, n, colloid
     from pg_index i
     join pg_class c on c.oid = i.indexrelid
     join pg_namespace n on n.oid = c.relnamespace,
          unnest(indcollation) with ordinality as t (colloid, n)
    where n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
 ),
all_colls as
 (
   select colloid, collname,
          pg_describe_object('pg_class'::regclass, indexrelid, 0)
              as description,
          format('%s %s %s',
                 regexp_replace(n.nspname, '[\n\r]', ' '),
                 regexp_replace(c.collname, '[\n\r]', ' '),
                 regexp_replace(auth.rolname, '[\n\r]', ' '))
              as restore_list_name
     from indcols
          join pg_collation c on c.oid = colloid
          join pg_roles auth ON auth.oid = c.collowner
          join pg_namespace n on n.oid = c.collnamespace
    where colloid <> 0
      and collname <> 'default'
   union
   select c.oid as colloid, c.collname,
          format('database %s', d.datname) as description,
          format('%s %s %s',
                 regexp_replace(n.nspname, '[\n\r]', ' '),
                 regexp_replace(c.collname, '[\n\r]', ' '),
                 regexp_replace(auth.rolname, '[\n\r]', ' '))
              as restore_list_name
     from pg_database d
          join pg_collation c on c.collname = d.datcollate
          join pg_roles auth ON auth.oid = c.collowner
          join pg_namespace n on n.oid = c.collnamespace
    where d.datname = current_database()
   union
   select coll.oid as colloid, coll.collname,
          pg_describe_object('pg_class'::regclass, attrelid, attnum)
              as description,
          format('%s %s %s',
                 regexp_replace(cn.nspname, '[\n\r]', ' '),
                 regexp_replace(coll.collname, '[\n\r]', ' '),
                 regexp_replace(auth.rolname, '[\n\r]', ' '))
              as restore_list_name
     from pg_attribute a
          join pg_class c on c.oid = a.attrelid
          join pg_namespace n on n.oid = c.relnamespace
          join pg_collation coll on coll.oid = attcollation
          join pg_roles auth ON auth.oid = coll.collowner
          join pg_namespace cn on cn.oid = coll.collnamespace
    where collname <> 'default'
      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
 )
select distinct on (colloid) colloid, collname, description,
       restore_list_name
  from all_colls
order by colloid, description;
