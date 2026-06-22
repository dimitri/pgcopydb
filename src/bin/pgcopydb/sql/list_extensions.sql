with recursive extconfig_paths as (
     select extconfig
     from pg_extension
     where extconfig is not null
 ), fk_constraints as (
     select fk.oid, fk.conrelid, fk.confrelid
     from pg_constraint fk
     inner join extconfig_paths
         on fk.conrelid = any(extconfig_paths.extconfig)
         or fk.confrelid = any(extconfig_paths.extconfig)
     where fk.contype = 'f' and fk.conrelid <> fk.confrelid
 ), raw_ordered_fk_constraints as (
     select
            distinct c.confrelid as relid,
            0 as depth,
            false as is_cycle,
            ARRAY[c.oid] as path
       from fk_constraints c
      where not exists (
            select 1
              from fk_constraints fc
             where fc.conrelid = c.confrelid
            )
     UNION
     select
            distinct c.conrelid as relid,
            r.depth + 1 as depth,
            c.oid = ANY(path) as is_cycle,
            path || c.oid as path
       from raw_ordered_fk_constraints r
       join fk_constraints c ON c.confrelid = r.relid
      where not is_cycle
 ), ordered_fk_constraints AS (
     select
            relid,
            max(depth) as depth
       from raw_ordered_fk_constraints group by relid
 ), extension_config_data as (
select e.oid, extname, extnamespace::regnamespace, extrelocatable,
       0 as count, null as n,
       null as extconfig, null as nspname, null as relname,
       null as extcondition,
       null as relkind
  from pg_extension e
 where extconfig is null

 UNION ALL

  select e.oid, extname, extnamespace::regnamespace, extrelocatable,
         array_length(e.extconfig, 1) as count,
         extconfig.n,
         extconfig.extconfig,
         format('%I', n.nspname) as nspname,
         format('%I', c.relname) as relname,
         extcondition[extconfig.n],
         c.relkind as relkind
    from pg_extension e,
         unnest(extconfig) with ordinality as extconfig(extconfig, n)
          left join pg_class c on c.oid = extconfig.extconfig
          join pg_namespace n on c.relnamespace = n.oid
   where extconfig.extconfig is not null
)
select oid, extname, extnamespace, extrelocatable,
       count,
       row_number() over (partition by oid order by depth) as n,
       extconfig,
       nspname,
       relname,
       extcondition,
       relkind
 from extension_config_data
      left outer join ordered_fk_constraints ofc on extconfig = ofc.relid
