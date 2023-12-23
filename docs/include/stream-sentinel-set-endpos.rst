::

   pgcopydb stream sentinel set endpos: Set the sentinel end position LSN on the source database
   usage: pgcopydb stream sentinel set endpos  --source ... <end LSN>
   
     --source      Postgres URI to the source database
     --current     Use pg_current_wal_flush_lsn() as the endpos
   
