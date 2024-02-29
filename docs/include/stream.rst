::

   pgcopydb stream: Stream changes from the source database
   
   Available commands:
     pgcopydb stream
       setup      Setup source and target systems for logical decoding
       cleanup    Cleanup source and target systems for logical decoding
       prefetch   Stream JSON changes from the source database and transform them to SQL
       catchup    Apply prefetched changes from SQL files to the target database
       replay     Replay changes from the source to the target database, live
     + sentinel   Maintain a sentinel table on the source database
       receive    Stream changes from the source database
       transform  Transform changes from the source database into SQL commands
       apply      Apply changes from the source database into the target database
   
