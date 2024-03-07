::

   pgcopydb restore schema: Restore a database schema from custom files to target database
   usage: pgcopydb restore schema  --dir <dir> [ --source <URI> ] --target <URI> 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --restore-jobs       Number of concurrent jobs for pg_restore
     --drop-if-exists     On the target database, clean-up from a previous run first
     --no-owner           Do not set ownership of objects to match the original database
     --no-acl             Prevent restoration of access privileges (grant/revoke commands).
     --no-comments        Do not output commands to restore comments
     --no-tablespaces     Do not output commands to select tablespaces
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database
   
