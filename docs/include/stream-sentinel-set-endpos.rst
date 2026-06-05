::

   pgcopydb stream sentinel set endpos: Set the sentinel end position LSN
   usage: pgcopydb stream sentinel set endpos [ --source ... ] [ <end lsn> | --current ]
   
     --source      Postgres URI to the source database
     --current     Use pg_current_wal_flush_lsn() as the endpos
   
