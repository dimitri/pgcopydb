# Unit/Regression Testing for pgcopydb

In addition to the pagila and large object testing, when a specific issue is
opened on pgcopydb we might want to add testing that covers just the failing
bits.

This testing directory is meant to allow for covering those extra regression
testing / unit testing. At the moment there are three modes of operations:

  1. the pgcopydb command is expected to return a zero return code (unix
     command success)

  2. the pgcopydb is expected to have done something specific on the target
     database and we want to check that.

  3. the pgcopydb command should generate the expected output without performing
     the full migration.

## Regression testing

In the spirit of pg_regress, the regression testing is done in the following
three steps:

  1. run the setup/*.sql file with psql

     This creates the testing environment with tables, constraints, data, etc

  2. for each file in the sql and scripts directory, run it with psql (against
     the target database) or bash, respectively, and capture the output

  3. compare the previous step output to the expected/${test}.out file
