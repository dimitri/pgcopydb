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
-- Test "is null" transformation in change data capture.
--
begin;

-- Disable triggers to prevent automatic refresh of 'last_updated' attribute
-- when modifying rows in the address table.
set session_replication_role = replica;

delete from address where city_id = 300 and address2 is null;

update address set postal_code = '751007' where phone = '6172235589' and address2 is null;

commit;

--
-- Test generated columns insert, update, and delete
--
begin;
insert into generated_column_test(id, name, email) values
(1, 'Tiger', 'tiger@wild.com'),
(2, 'Elephant', 'elephant@wild.com'),
(3, 'Cat', 'cat@home.net');
commit;

begin;
update generated_column_test set name = 'Lion'
where id = 1;
update generated_column_test set email='lion@wild.com'
where email = 'tiger@wild.com';
commit;

begin;
update generated_column_test set name = 'Kitten', email='kitten@home.com'
where id = 3;
commit;

begin;
delete from generated_column_test where id = 2;
commit;

--
-- Test update is not failing when value is not changed.
--
begin;
insert into single_column_table(id) values (1), (2);
insert into multi_column_table(id, name, email) values
(1, 'Alice', 'alice@hello.com'),
(2, 'Bob', 'bob@hello.com')
;
commit;

begin;
update single_column_table set id = id;
update multi_column_table set id = id, name = name, email = email;
commit;

--
-- Test json columns with REPLICA IDENTITY FULL.
-- json has no equality operator, so WHERE clauses need ::text casts.
--
-- json stores the exact input text (whitespace, key order, duplicates),
-- so json::text is deterministic — it returns the verbatim stored string.
-- These test cases exercise multi-key objects, varied whitespace, nested
-- structures, and duplicate keys to prove the ::text comparison is stable.
--
begin;
insert into json_column_table(id, data) values
(1, '{"b": 2, "a": 1}'),
(2, '{"key":    "spaces",  "num": 42}'),
(3, '{"nested": {"inner": [1,2,3]}, "top": true}'),
(4, '{"dup": "first", "dup": "second"}');
commit;

begin;
update json_column_table set data = '{"b": 2, "a": "updated"}' where id = 1;
update json_column_table set data = '{"key": "no-spaces", "num": 42}' where id = 2;
commit;

begin;
delete from json_column_table where id = 3;
delete from json_column_table where id = 4;
commit;

--
-- Test float8 precision in change data capture. These values need more than
-- 6 fractional digits (or are too small for fixed notation), which the old
-- "%f" formatting truncated to "-216237.000000" / "0.000000". Under REPLICA
-- IDENTITY FULL the value is also a WHERE-clause key on UPDATE/DELETE, so it
-- must round-trip exactly or the row would not match.
--
begin;
insert into float_precision_table(id, val) values
(1, -216237.00000035969),
(2, 0.1234567890123),
(3, 1e-20);
commit;

begin;
update float_precision_table set val = 999.0000004 where id = 1;
delete from float_precision_table where id = 2;
commit;
