::

   pgcopydb dump pre-data: Dump source database pre-data schema as custom files in work directory
   usage: pgcopydb dump pre-data  --source <URI> 
   
     --source          Postgres URI to the source database
     --target          Directory where to save the dump files
     --dir             Work directory to use
     --skip-extensions Skip restoring extensions
     --snapshot        Use snapshot obtained with pg_export_snapshot
   
