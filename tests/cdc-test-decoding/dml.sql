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
-- run an update statement that doesn't output old-key: and new-key: when
-- using test_decoding
--
begin;

update public.staff set store_id = store_id where staff_id = 1;

commit;

--
-- insert a new line in our table with double-quote in its name
--
begin;

insert into public."""dqname""" default values;

commit;

--
-- insert, update, delete our table which has identifer as column name
--
begin;

insert into "Foo"".Bar".":Identifer As ""Column"".$1:" ("time", "[column name]") values (1, 'foo');

update "Foo"".Bar".":Identifer As ""Column"".$1:" set "time" = 2, "[column name]" = '[bar]' where "time" = 1;

delete from "Foo"".Bar".":Identifer As ""Column"".$1:" where "time" = 2;

insert into "Unicode""Test".U&"\0441\043B\043E\043D" (id, U&"!0441!043B!043E!043D" UESCAPE '!', U&"!043A!043E!043B!043E!043D!043A!0430" UESCAPE '!') values (1, 'open', 'foo');

update "Unicode""Test".U&"\0441\043B\043E\043D" set id = 2, U&"!0441!043B!043E!043D" UESCAPE '!' = 'closed', U&"\043A\043E\043B\043E\043D\043A\0430" = '[bar]' where id = 1;

delete from "Unicode""Test".U&"\0441\043B\043E\043D" where id = 2;
commit;

--
-- See https://github.com/dimitri/pgcopydb/issues/736
--
begin;

insert into t_bit_types (a,b) values (B'10'::bit(3), B'101');

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
