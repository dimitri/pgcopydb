::

   pgcopydb copy sequences: Copy the current value from all sequences in database from source to target
   usage: pgcopydb copy sequences  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 
   
     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database
     --snapshot           Use snapshot obtained with pg_export_snapshot
   
