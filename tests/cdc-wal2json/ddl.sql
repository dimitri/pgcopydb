---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;
alter table payment_p2022_01 replica identity full;
alter table payment_p2022_02 replica identity full;
alter table payment_p2022_03 replica identity full;
alter table payment_p2022_04 replica identity full;
alter table payment_p2022_05 replica identity full;
alter table payment_p2022_06 replica identity full;
alter table payment_p2022_07 replica identity full;
commit;

begin;
alter table address replica identity full;
commit;

begin;
create table if not exists generated_column_test
(
    id bigint primary key,
    name text,
    greet_hello text generated always as ('Hello ' || name) stored,
    greet_hi  text generated always as ('Hi ' || name) stored,
    -- tests identifier escaping
    time text generated always as ('identifier 1' || 'now') stored,
    email text not null,
    -- tests identifier escaping
    "table" text generated always as ('identifier 2' || name) stored,
    """table""" text generated always as ('identifier 3' || name) stored,
    """hel""lo""" text generated always as ('identifier 4' || name) stored
);
commit;
