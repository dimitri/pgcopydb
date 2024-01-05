::

   pgcopydb copy data: Copy the data section from source to target
   usage: pgcopydb copy data  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 
   
     --source              Postgres URI to the source database
     --target              Postgres URI to the target database
     --dir                 Work directory to use
     --table-jobs          Number of concurrent COPY jobs to run
     --index-jobs          Number of concurrent CREATE INDEX jobs to run
     --restore-jobs        Number of concurrent jobs for pg_restore
     --skip-large-objects  Skip copying large objects (blobs)
     --filters <filename>  Use the filters defined in <filename>
     --restart             Allow restarting when temp files exist already
     --resume              Allow resuming operations after a failure
     --not-consistent      Allow taking a new snapshot on the source database
     --snapshot            Use snapshot obtained with pg_export_snapshot
   
