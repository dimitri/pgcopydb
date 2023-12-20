---
--- pgcopydb test/cdc/dml.sql
---
--- This file implements DML changes in the pagila database and covers edge
--  cases of postgres objects like identifiers length, identifiers that requires
--  double quotes
begin;

insert into "Sp1eCial .Char"."source1testing"("s1")
select
    x
from
    generate_series(1, 5) as t(x);

insert into "sp4ecial$char"."source4testing"("s1")
select
    x
from
    generate_series(1, 5) as t(x);

commit;

begin;

insert into "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456"("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456")
select
    x
from
    generate_series(1, 5) as t(x);

commit;

