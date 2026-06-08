::

   pgcopydb follow: Replay changes from the source database to the target database
   usage: pgcopydb follow  --source ... --target ...  
   
     --source                      Postgres URI to the source database
     --target                      Postgres URI to the target database
     --dir                         Work directory to use
     --filters <filename>          Use the filters defined in <filename>
     --restart                     Allow restarting when temp files exist already
     --resume                      Allow resuming operations after a failure
     --not-consistent              Allow taking a new snapshot on the source database
     --snapshot                    Use snapshot obtained with pg_export_snapshot
     --plugin                      Output plugin to use (test_decoding, wal2json)
     --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin
     --slot-name                   Use this Postgres replication slot name
     --create-slot                 Create the replication slot
     --origin                      Use this Postgres replication origin node name
     --endpos                      Stop replaying changes when reaching this LSN
   
