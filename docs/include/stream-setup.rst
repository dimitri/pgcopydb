::

   pgcopydb stream setup: Setup source and target systems for logical decoding
   usage: pgcopydb stream setup 
   
     --source                      Postgres URI to the source database
     --target                      Postgres URI to the target database
     --dir                         Work directory to use
     --restart                     Allow restarting when temp files exist already
     --resume                      Allow resuming operations after a failure
     --not-consistent              Allow taking a new snapshot on the source database
     --snapshot                    Use snapshot obtained with pg_export_snapshot
     --plugin                      Output plugin to use (pgoutput, test_decoding, wal2json)
     --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin
     --replay-no-op-updates        Replay UPDATE statements even when no column values changed
     --slot-name                   Stream changes recorded by this slot
     --origin                      Name of the Postgres replication origin
   
