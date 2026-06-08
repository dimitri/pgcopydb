::

   pgcopydb stream sentinel set endpos: Set the sentinel end position LSN
   usage: pgcopydb stream sentinel set endpos [ --source ... ] [ <end lsn> | --current ]
   
     --source      Postgres URI to the source database
     --current     Use pg_current_wal_flush_lsn() as the endpos
     --host        Reach the follow coordinator over TCP at this host
     --port        Follow coordinator TCP port (default 5442)
   
