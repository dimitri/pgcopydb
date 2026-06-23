::

   pgcopydb stream prune: Remove already-applied CDC files from disk to reclaim disk space
   usage: pgcopydb stream prune  [ --dir ] [ --dry-run ] [ --host ] [ --port ]
   
     --dir            Work directory to use
     --dry-run        List files that would be removed without deleting them
     --host           Host of the running pgcopydb follow process to connect to
     --port           Port of the running pgcopydb follow process to connect to
   
