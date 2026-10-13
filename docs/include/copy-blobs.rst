::

   pgcopydb copy blobs: Copy the blob data from the source database to the target
   usage: pgcopydb copy blobs  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --large-objects-jobs Number of concurrent Large Objects jobs to run
     --drop-if-exists     Drop and copy again large objects existing on the target
     --no-owner           Do not restore large objects ownership
     --no-acl             Prevent restoration of large objects access privileges
     --no-comments        Do not copy the comments on large objects
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database
     --snapshot           Use snapshot obtained with pg_export_snapshot
   
