::

   pgcopydb: pgcopydb tool
   usage: pgcopydb [ --verbose --quiet ]
   
   
   Available commands:
     pgcopydb
       clone     Clone an entire database from source to target
       fork      Clone an entire database from source to target
       follow    Replay changes from the source database to the target database
       snapshot  Create and export a snapshot on the source database
     + compare   Compare source and target databases
     + copy      Implement the data section of the database copy
     + dump      Dump database objects from a Postgres instance
     + restore   Restore database objects into a Postgres instance
     + list      List database objects from a Postgres instance
     + stream    Stream changes from the source database
       ping      Attempt to connect to the source and target instances
       help      Print help message
       version   Print pgcopydb version
   
