--
-- pgcopydb test/cdc-file-rotation/ddl.sql
--
-- Simple schema for the file-rotation test: a single table with
-- a text payload column so we can generate large transactions easily.

begin;
create table if not exists rotation_test (
    id   bigserial primary key,
    data text not null
);
commit;
