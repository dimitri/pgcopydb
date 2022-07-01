.. _pgcopydb:

pgcopydb
=========

pgcopydb - copy an entire Postgres database from source to target

Synopsis
--------

pgcopydb provides the following commands::

  pgcopydb
    clone    Clone an entire database from source to target
    fork     Clone an entire database from source to target
    follow   Replay changes from the source database to the target database
  + copy     Implement the data section of the database copy
  + create   Create resources needed for pgcopydb
  + dump     Dump database objects from a Postgres instance
  + restore  Restore database objects into a Postgres instance
  + list     List database objects from a Postgres instance
  + stream   Stream changes from the source database
    help     print help message
    version  print pgcopydb version

Description
-----------

The pgcopydb command implements a full migration of an entire Postgres
database from a source instance to a target instance. Both the Postgres
instances must be available for the entire duration of the command.

Help
----

To get the full recursive list of supported commands, use::

  pgcopydb help

Version
-------

To grab the version of pgcopydb that you're using, use::

   pgcopydb --version
   pgcopydb version
