::

   pgcopydb stream: Stream changes from the source database
   
   Available commands:
     pgcopydb stream
       init       Initialise the pgcopydb streaming work directory and SQLite catalogs
       setup      Setup source and target systems for logical decoding
       cleanup    Cleanup source and target systems for logical decoding
       prefetch   Stream JSON changes from the source database and transform them to SQL
       catchup    Apply prefetched changes from SQL files to the target database
       replay     Replay changes from the source to the target database, live
     + sentinel   Maintain a sentinel table
       receive    Stream changes from the source database
       transform  Transform CDC messages from the replayDB output table into SQL
       apply      Apply changes from the replayDB to the target database, or stdout
   
