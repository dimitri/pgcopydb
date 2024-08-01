::

   pgcopydb clone: Clone an entire database from source to target
   usage: pgcopydb clone  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 
   
     --source                      Postgres URI to the source database
     --target                      Postgres URI to the target database
     --dir                         Work directory to use
     --table-jobs                  Number of concurrent COPY jobs to run
     --index-jobs                  Number of concurrent CREATE INDEX jobs to run
     --restore-jobs                Number of concurrent jobs for pg_restore
     --large-objects-jobs          Number of concurrent Large Objects jobs to run
     --split-tables-larger-than    Same-table concurrency size threshold
     --split-max-parts             Maximum number of jobs for Same-table concurrency 
     --estimate-table-sizes        Allow using estimates for relation sizes
     --drop-if-exists              On the target database, clean-up from a previous run first
     --roles                       Also copy roles found on source to target
     --no-role-passwords           Do not dump passwords for roles
     --no-owner                    Do not set ownership of objects to match the original database
     --no-acl                      Prevent restoration of access privileges (grant/revoke commands).
     --no-comments                 Do not output commands to restore comments
     --no-tablespaces              Do not output commands to select tablespaces
     --skip-large-objects          Skip copying large objects (blobs)
     --skip-extensions             Skip restoring extensions
     --skip-ext-comments           Skip restoring COMMENT ON EXTENSION
     --skip-collations             Skip restoring collations
     --skip-vacuum                 Skip running VACUUM ANALYZE
     --skip-analyze                Skip running vacuumdb --analyze-only
     --skip-db-properties          Skip copying ALTER DATABASE SET properties
     --skip-split-by-ctid          Skip spliting tables by ctid
     --requirements <filename>     List extensions requirements
     --filters <filename>          Use the filters defined in <filename>
     --fail-fast                   Abort early in case of error
     --restart                     Allow restarting when temp files exist already
     --resume                      Allow resuming operations after a failure
     --not-consistent              Allow taking a new snapshot on the source database
     --snapshot                    Use snapshot obtained with pg_export_snapshot
     --follow                      Implement logical decoding to replay changes
     --plugin                      Output plugin to use (test_decoding, wal2json)
     --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin
     --slot-name                   Use this Postgres replication slot name
     --create-slot                 Create the replication slot
     --origin                      Use this Postgres replication origin node name
     --endpos                      Stop replaying changes when reaching this LSN
     --use-copy-binary			 Use the COPY BINARY format for COPY operations
   
