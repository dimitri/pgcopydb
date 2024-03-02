::

   pgcopydb restore post-data: Restore a database post-data schema from custom file to target database
   usage: pgcopydb restore post-data  --dir <dir> [ --source <URI> ] --target <URI> 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --restore-jobs       Number of concurrent jobs for pg_restore
     --no-owner           Do not set ownership of objects to match the original database
     --no-acl             Prevent restoration of access privileges (grant/revoke commands).
     --no-comments        Do not output commands to restore comments
     --no-tablespaces     Do not output commands to select tablespaces
     --skip-extensions    Skip restoring extensions
     --skip-ext-comments  Skip restoring COMMENT ON EXTENSION
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database
   
