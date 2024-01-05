::

   pgcopydb copy table-data: Copy the data from all tables in database from source to target
   usage: pgcopydb copy table-data  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --table-jobs         Number of concurrent COPY jobs to run
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database
     --snapshot           Use snapshot obtained with pg_export_snapshot
   
