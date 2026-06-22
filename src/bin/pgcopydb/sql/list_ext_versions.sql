select e.name, e.default_version, e.installed_version,
       u.versions

from pg_available_extensions e
     left join lateral
     (
       with updates as
       (
         select source,
                array_length(regexp_split_to_array(path, '--'), 1) as steps
           from pg_extension_update_paths(e.name)
          where (   target = e.default_version
                 or source = e.default_version)
           and source not in ('unpackaged', 'ANY')
           and path is not null

     union all

        select e.default_version, 0

      order by steps, source desc
       )
       select coalesce(jsonb_agg(source),
                       jsonb_build_array(e.default_version))
       from updates
     )
     as u(versions) on true

group by e.name, e.default_version, e.installed_version, u.versions
order by e.name;
