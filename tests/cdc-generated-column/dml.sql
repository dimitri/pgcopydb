---
--- pgcopydb test/cdc/dml.sql
---
--- This file implements DML changes in the pagila database.

\set customerid1 291
\set customerid2 293

\set staffid1 1
\set staffid2 2

\set inventoryid1 371
\set inventoryid2 373

begin;

insert into rental(rental_date, inventory_id, customer_id, staff_id, last_update)
    values
    ('2022-06-01', :inventoryid1, :customerid1, :staffid1, '2022-06-01'),
    ('2022-06-01', :inventoryid2, :customerid2, :staffid2, '2022-06-01');

insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
  select customer_id, staff_id, rental_id, 5.99, rental_date
    from rental where rental_date='2022-06-01';

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
