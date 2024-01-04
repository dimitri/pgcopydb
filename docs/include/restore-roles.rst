::

   pgcopydb restore roles: Restore database roles from SQL file to target database
   usage: pgcopydb restore roles  --dir <dir> [ --source <URI> ] --target <URI> 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --restore-jobs       Number of concurrent jobs for pg_restore
   
