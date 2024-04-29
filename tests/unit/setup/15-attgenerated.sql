---
--- See https://github.com/dimitri/pgcopydb/issues/757
---

create table gen
 (
   nb int generated always as (1) stored
 );

insert into gen values(default);
