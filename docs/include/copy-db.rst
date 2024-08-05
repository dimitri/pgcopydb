::

   pgcopydb copy db: Copy an entire database from source to target
   usage: pgcopydb copy db  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 
   
     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --table-jobs          Number of concurrent COPY jobs to run
     --index-jobs          Number of concurrent CREATE INDEX jobs to run
     --restore-jobs        Number of concurrent jobs for pg_restore
     --drop-if-exists      On the target database, clean-up from a previous run first
     --roles               Also copy roles found on source to target
     --no-owner            Do not set ownership of objects to match the original database
     --no-acl              Prevent restoration of access privileges (grant/revoke commands).
     --no-comments         Do not output commands to restore comments
     --no-tablespaces      Do not output commands to select tablespaces
     --skip-large-objects  Skip copying large objects (blobs)
     --filters <filename>  Use the filters defined in <filename>
     --fail-fast           Abort early in case of error
     --restart             Allow restarting when temp files exist already
     --resume              Allow resuming operations after a failure
     --not-consistent      Allow taking a new snapshot on the source database
     --snapshot            Use snapshot obtained with pg_export_snapshot
     --use-copy-binary     Use the COPY BINARY format for COPY operations
   
