select d.datname, NULL as rolname,
       unnest(rs.setconfig) as setconfig
  from pg_db_role_setting rs
       join pg_database d on d.oid = rs.setdatabase
 where d.datname = current_database()
   and setrole = 0

union all

select d.datname, format('%I', rolname) as rolname,
       unnest(rs.setconfig)  as setconfig
  from pg_db_role_setting rs
       join pg_database d on d.oid = rs.setdatabase
       join pg_roles r on r.oid = rs.setrole
 where d.datname = current_database();
