# Unit/Regression Testing for pgcopydb

In addition to the pagila and large object testing, when a specific issue is
opened on pgcopydb we might want to add testing that covers just the failing
bits.

This testing directory is meant to allow for covering those extra regression
testing / unit testing. At the moment there are two modes of operations:

  1. the pgcopydb command is expected to return a zero return code (unix
     command success)

  2. the pgcopydb is expected to have done something specific on the target
     database and we want to check that.


At the moment only the first case is handled, because that's all we need.
