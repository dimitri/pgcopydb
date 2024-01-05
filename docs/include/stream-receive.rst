::

   pgcopydb stream receive: Stream changes from the source database
   usage: pgcopydb stream receive 
   
     --source         Postgres URI to the source database
     --dir            Work directory to use
     --to-stdout      Stream logical decoding messages to stdout
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --not-consistent Allow taking a new snapshot on the source database
     --slot-name      Stream changes recorded by this slot
     --endpos         LSN position where to stop receiving changes
