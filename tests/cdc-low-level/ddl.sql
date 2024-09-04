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

create table metrics(id int, time timestamptz, name text, value numeric);

-- Create a trigger function that artificially raises notice on insert
-- to send large message which fills the libpq connection's read buffer
-- which is 16KB by default.
-- https://github.com/postgres/postgres/blob/master/src/interfaces/libpq/fe-connect.c#L4616
CREATE OR REPLACE FUNCTION notify_on_insert() RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'A new record has been inserted into the metrics table! %', repeat('a', 32 * 1024);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_notice_trigger
AFTER INSERT ON metrics
FOR EACH ROW
EXECUTE FUNCTION notify_on_insert();

-- Apply executes on session_replication_role = 'replica'
-- Lets enable the trigger for the replica mode
ALTER TABLE metrics ENABLE REPLICA TRIGGER insert_notice_trigger;

commit;
