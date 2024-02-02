---
--- pgcopydb test/cdc/dml.sql
---
--- This file implements DML changes in the pagila database and covers edge
--  cases of postgres objects like identifiers length, identifiers that requires
--  double quotes
truncate "Sp1eCial .Char"."source1testing";

insert into "Sp1eCial .Char"."source1testing"("s""1")
select
    x
from
    generate_series(1, 5) as t(x);

update
    "Sp1eCial .Char"."source1testing"
set
    "s""1" = "s""1" * 2;

delete from "Sp1eCial .Char"."source1testing"
where ("s""1" % 3) = 0;

insert into "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456"("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456")
select
    x
from
    generate_series(1, 5) as t(x);

update
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456"
set
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" * 2;

