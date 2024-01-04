::

   pgcopydb stream catchup: Apply prefetched changes from SQL files to the target database
   usage: pgcopydb stream catchup 
   
     --source         Postgres URI to the source database
     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes
     --origin         Name of the Postgres replication origin
   
