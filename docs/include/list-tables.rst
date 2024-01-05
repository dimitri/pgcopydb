::

   pgcopydb list tables: List all the source tables to copy data from
   usage: pgcopydb list tables  --source ... 
   
     --source            Postgres URI to the source database
     --filter <filename> Use the filters defined in <filename>
     --force             Force fetching catalogs again
     --cache             Cache table size in relation pgcopydb.pgcopydb_table_size
     --drop-cache        Drop relation pgcopydb.pgcopydb_table_size
     --list-skipped      List only tables that are setup to be skipped
     --without-pkey      List only tables that have no primary key
   
