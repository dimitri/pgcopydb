::

   pgcopydb stream: Stream changes from the source database
   
   Available commands:
     pgcopydb stream
       setup     Setup source and target systems for logical decoding
       cleanup   Cleanup source and target systems for logical decoding
       prune     Remove already-applied CDC files from disk to reclaim disk space
       prefetch  Stream changes from the source database into the SQLite CDC store
       catchup   Transform and apply prefetched changes from the SQLite CDC store to the target
       replay    Replay changes from the source to the target database, live
     + sentinel  Maintain a sentinel table
       receive   Stream changes from the source database
       apply     Apply changes from the replayDB to the target database, or stdout
   
