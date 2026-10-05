---
--- pgcopydb test/cdc/dml.sql
---
--- This file implements DML changes in the pagila database.

\set customerid1 291
\set customerid2 292

\set staffid1 1
\set staffid2 2

\set inventoryid1 371
\set inventoryid2 1097

begin;

with r as
 (
   insert into rental(rental_date, inventory_id, customer_id, staff_id, last_update)
        select '2022-06-01', :inventoryid1, :customerid1, :staffid1, '2022-06-01'
     returning rental_id, customer_id, staff_id
 )
 insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
      select customer_id, staff_id, rental_id, 5.99, '2022-06-01'
        from r;

commit;

-- update 10 rows in a single UPDATE command
update public.payment set amount = 11.95 where amount = 11.99;

begin;

delete from payment
      using rental
      where rental.rental_id = payment.rental_id
        and rental.last_update = '2022-06-01';

delete from rental where rental.last_update = '2022-06-01';

commit;

--
-- update the payments back to their original values
--
begin;

update public.payment set amount = 11.99 where amount = 11.95;

commit;

--
-- Logical decoding messages (pg_logical_emit_message, wal2json action "M")
-- carry no DML and must be skipped: transactional within a DML transaction,
-- transactional on its own (like PeerDB / ClickPipes heartbeats), and
-- non-transactional. The actor row is a net change that copydb.sh checks on
-- the target.
--
begin;

insert into actor(first_name, last_name) values ('LOGICAL', 'MESSAGE');
select pg_logical_emit_message(true, 'pgcopydb_test', 'within a transaction');

commit;

select pg_logical_emit_message(true, 'pgcopydb_test', 'transactional');
select pg_logical_emit_message(false, 'pgcopydb_test', 'non-transactional');
