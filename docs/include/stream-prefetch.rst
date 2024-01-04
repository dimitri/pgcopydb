::

   pgcopydb stream prefetch: Stream JSON changes from the source database and transform them to SQL
   usage: pgcopydb stream prefetch 
   
     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes
