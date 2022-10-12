Change Data Capture
===================

pgcopydb implements logical decoding through using the wal2json plugin:

  https://github.com/eulerto/wal2json

This means that changes made to the source database during the copying of
the data can be replayed to the target database.

This directory implements testing for the change data capture capabilities
of pgcopydb. Tests are using the pagila database, and a set of SQL scripts
that run some DML trafic.

Follow 9.6 test
===============

This test is meant to test pgcopydb Logical Decoding support compatibility
with Postgres 9.6 as a source server. It has also be made compatible with
Postgres 9.5 and Postgres 10, as all of those docker images are using the
same debian stretch distribution.

```
$ make -C tests/follow-9.6 PGVERSION=9.5 up
$ make -C tests/follow-9.6 PGVERSION=9.6 up
$ make -C tests/follow-9.6 PGVERSION=10 up
```
