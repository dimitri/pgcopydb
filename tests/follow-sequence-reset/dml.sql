---
--- pgcopydb test/follow-sequence-reset/dml.sql
---
--- Advance rental_rental_id_seq on the source by inserting rows without an
--- explicit rental_id, so nextval() fires. rental_date is varied to satisfy
--- the (rental_date, inventory_id, customer_id) unique constraint.
---
insert into rental(rental_date, inventory_id, customer_id, staff_id, last_update)
select '2022-06-01'::timestamp + (x || ' seconds')::interval,
       371, 291, 1, '2022-06-01'
from generate_series(1, 10) as t(x);
