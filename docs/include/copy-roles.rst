::

   pgcopydb copy roles: Copy the roles from the source instance to the target instance
   usage: pgcopydb copy roles  --source ... --target ... 
   
     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --no-role-passwords   Do not dump passwords for roles
   
