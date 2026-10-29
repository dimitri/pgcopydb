::

   pgcopydb list toc: List and filter a pg_dump archive's Table of Contents
   usage: pgcopydb list toc  [ --dump ... | --restore-list ... ] 
   
     --dump <filename>         Custom-format pg_dump archive to inspect
     --restore-list <filename> A pg_restore --list output already saved to a file
     --source                  Postgres URI to the source database (required with --filter)
     --filter <filename>       Apply the pgcopydb filters defined in <filename>
     --schema-name             Only list entries in this schema
     --name <pattern>          Only list entries whose name matches this glob pattern
     --type <typename>         Only list entries whose signature mentions this type
     --use-list <filename>     Write a pg_restore --use-list file out of the matches
     --json                    Format the output using JSON
   
