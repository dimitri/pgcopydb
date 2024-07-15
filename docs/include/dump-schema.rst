::

   pgcopydb dump schema: Dump source database schema as custom files in work directory
   usage: pgcopydb dump schema  --source <URI> 
   
     --source             Postgres URI to the source database
     --target             Directory where to save the dump files
     --dir                Work directory to use
     --skip-extensions    Skip restoring extensions
     --filters <filename> Use the filters defined in <filename>
     --snapshot           Use snapshot obtained with pg_export_snapshot
   
