::

   pgcopydb stream apply: Apply changes from the replayDB to the target database, or stdout
   usage: pgcopydb stream apply 
   
     --target         Postgres URI to the target database
                      Use '-' to emit SQL to stdout without connecting
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --origin         Name of the Postgres replication origin
   
