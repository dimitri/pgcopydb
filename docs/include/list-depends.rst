::

   pgcopydb list depends: List all the dependencies to filter-out
   usage: pgcopydb list depends  --source ... [ --schema-name [ --table-name ] ]
   
     --source            Postgres URI to the source database
     --force             Force fetching catalogs again
     --schema-name       Name of the schema where to find the table
     --table-name        Name of the target table
     --filter <filename> Use the filters defined in <filename>
     --list-skipped      List only tables that are setup to be skipped
   
