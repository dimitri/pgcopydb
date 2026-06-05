::

   pgcopydb stream transform: Transform CDC messages from the replayDB output table into SQL
   usage: pgcopydb stream transform 
   
     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --endpos         LSN position where to stop transforming
   
