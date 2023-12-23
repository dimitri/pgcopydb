::

   pgcopydb dump post-data: Dump source database post-data schema as custom files in work directory
   usage: pgcopydb dump post-data  --source <URI>
   
     --source          Postgres URI to the source database
     --target          Directory where to save the dump files
     --dir             Work directory to use
     --snapshot        Use snapshot obtained with pg_export_snapshot
   
