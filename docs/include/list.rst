::

   pgcopydb list: List database objects from a Postgres instance
   
   Available commands:
     pgcopydb list
       databases    List databases
       extensions   List all the source extensions to copy
       collations   List all the source collations to copy
       tables       List all the source tables to copy data from
       table-parts  List a source table copy partitions
       sequences    List all the source sequences to copy data from
       indexes      List all the indexes to create again after copying the data
       depends      List all the dependencies to filter-out
       schema       List the schema to migrate, formatted in JSON
       progress     List the progress
   
