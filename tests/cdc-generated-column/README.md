Change Data Capture
===================

pgcopydb implements logical decoding through using the wal2json plugin:

  https://github.com/eulerto/wal2json
  
This means that changes made to the source database during the copying of
the data can be replayed to the target database.

This directory implements testing for the change data capture capabilities
of pgcopydb. Tests are using the pagila database, and a set of SQL scripts
that run some DML trafic.
