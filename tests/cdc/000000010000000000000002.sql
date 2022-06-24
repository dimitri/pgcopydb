BEGIN; -- {"xid":488,"lsn":"0/236D948"}
INSERT INTO public.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) VALUES (16050, '2022-06-01 00:00:00+00', 371, 291, NULL, 1, '2022-06-01 00:00:00+00');
INSERT INTO public.payment_p2020_06 (payment_id, customer_id, staff_id, rental_id, amount, payment_date) VALUES (32099, 291, 1, 16050, 5.99, '2020-06-01 00:00:00+00');
COMMIT; -- {"xid": 488,"lsn":"0/236D948"}
