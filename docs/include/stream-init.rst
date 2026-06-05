::

   pgcopydb stream init: Initialise the pgcopydb streaming work directory and SQLite catalogs
   usage: pgcopydb stream init 
   
     --source         Postgres URI to the source database
     --dir            Work directory to use
     --restart        Allow restarting when temp files exist already
     --resume         Allow resuming operations after a failure
     --slot-name      Replication slot name
     --plugin         Logical decoding output plugin (test_decoding or wal2json)
     --endpos         LSN position where to stop receiving changes
