::

     pgcopydb
       clone     Clone an entire database from source to target
       fork      Clone an entire database from source to target
       follow    Replay changes from the source database to the target database
       snapshot  Create and export a snapshot on the source database
     + compare   Compare source and target databases
     + copy      Implement the data section of the database copy
     + dump      Dump database objects from a Postgres instance
     + restore   Restore database objects into a Postgres instance
     + list      List database objects from a Postgres instance
     + stream    Stream changes from the source database
       ping      Attempt to connect to the source and target instances
       help      Print help message
       version   Print pgcopydb version
   
     pgcopydb compare
       schema  Compare source and target schema
       data    Compare source and target data
   
     pgcopydb copy
       db           Copy an entire database from source to target
       roles        Copy the roles from the source instance to the target instance
       extensions   Copy the extensions from the source instance to the target instance
       schema       Copy the database schema from source to target
       data         Copy the data section from source to target
       table-data   Copy the data from all tables in database from source to target
       blobs        Copy the blob data from the source database to the target
       sequences    Copy the current value from all sequences in database from source to target
       indexes      Create all the indexes found in the source database in the target
       constraints  Create all the constraints found in the source database in the target
   
     pgcopydb dump
       schema  Dump source database schema as custom files in work directory
       roles   Dump source database roles as custome file in work directory
   
     pgcopydb restore
       schema      Restore a database schema from custom files to target database
       pre-data    Restore a database pre-data schema from custom file to target database
       post-data   Restore a database post-data schema from custom file to target database
       roles       Restore database roles from SQL file to target database
       parse-list  Parse pg_restore --list output from custom file
   
     pgcopydb list
       databases    List databases
       extensions   List all the source extensions to copy
       collations   List all the source collations to copy
       tables       List all the source tables to copy data from
       table-parts  List a source table copy partitions
       sequences    List all the source sequences to copy data from
       indexes      List all the indexes to create again after copying the data
       depends      List all the dependencies to filter-out
       schema       List the schema to migrate, formatted in JSON
       progress     List the progress
   
     pgcopydb stream
       setup      Setup source and target systems for logical decoding
       cleanup    Cleanup source and target systems for logical decoding
       prefetch   Stream JSON changes from the source database and transform them to SQL
       catchup    Apply prefetched changes from SQL files to the target database
       replay     Replay changes from the source to the target database, live
     + sentinel   Maintain a sentinel table
       receive    Stream changes from the source database
       transform  Transform changes from the source database into SQL commands
       apply      Apply changes from the source database into the target database
   
     pgcopydb stream sentinel
       setup  Setup the sentinel table
       get    Get the sentinel table values
     + set    Set the sentinel table values
   
     pgcopydb stream sentinel set
       startpos  Set the sentinel start position LSN
       endpos    Set the sentinel end position LSN
       apply     Set the sentinel apply mode
       prefetch  Set the sentinel prefetch mode
   
