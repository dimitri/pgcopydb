::

   pgcopydb stream apply: Apply changes from the source database into the target database
   usage: pgcopydb stream apply  <sql filename> 
   
     --target         Postgres URI to the target database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --origin         Name of the Postgres replication origin
   
