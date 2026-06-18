---
--- follow-filtering/dml.sql
---
--- DML changes injected while pgcopydb clone --follow is running.
--- Only touches tables that are included by the filter (not staff).

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

update public.payment set amount = 11.95 where amount = 11.99;

begin;

delete from payment
      using rental
      where rental.rental_id = payment.rental_id
        and rental.last_update = '2022-06-01';

delete from rental where rental.last_update = '2022-06-01';

commit;

begin;
update public.payment set amount = 11.99 where amount = 11.95;
commit;

-- Also insert a new actor to provide a simple inclusion check
begin;
insert into public.actor (actor_id, first_name, last_name, last_update)
values (999, 'Follow', 'Filter', now());
commit;
