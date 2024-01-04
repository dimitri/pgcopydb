::

   pgcopydb restore parse-list: Parse pg_restore --list output from custom file
   usage: pgcopydb restore parse-list  [ <pre.list> ] 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --filters <filename> Use the filters defined in <filename>
     --skip-extensions    Skip restoring extensions
     --skip-ext-comments  Skip restoring COMMENT ON EXTENSION
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database
   
