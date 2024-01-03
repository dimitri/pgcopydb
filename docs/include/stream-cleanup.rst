::

   pgcopydb stream cleanup: Cleanup source and target systems for logical decoding
   usage: pgcopydb stream cleanup 
   
     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --snapshot       Use snapshot obtained with pg_export_snapshot
     --slot-name      Stream changes recorded by this slot
     --origin         Name of the Postgres replication origin
   
