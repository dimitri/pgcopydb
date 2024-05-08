BEGIN; -- {"xid":489,"lsn":"0/24D2208","timestamp":"2024-05-08 11:10:09.242465+0000","commit_lsn":"0/24D4588"}
PREPARE 58013803 AS INSERT INTO public.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14);
EXECUTE 58013803["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00","16051","2022-06-01 00:00:00+00","373","293",null,"2","2022-06-01 00:00:00+00"];
PREPARE dd147129 AS INSERT INTO public.payment_p2022_06 (payment_id, customer_id, staff_id, rental_id, amount, payment_date) overriding system value VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12);
EXECUTE dd147129["32099","291","1","16050","5.99","2022-06-01 00:00:00+00","32100","293","2","16051","5.99","2022-06-01 00:00:00+00"];
COMMIT; -- {"xid":489,"lsn":"0/24D4588","timestamp":"2024-05-08 11:10:09.242465+0000"}
BEGIN; -- {"xid":490,"lsn":"0/24D4588","timestamp":"2024-05-08 11:10:09.242871+0000","commit_lsn":"0/24D5658"}
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.95","23757","116","2","14763","11.99","2022-02-11 03:52:25.634006+00"];
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.95","24866","237","2","11479","11.99","2022-02-07 18:37:34.579143+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.95","17055","196","2","106","11.99","2022-03-18 18:50:39.243747+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.95","28799","591","2","4383","11.99","2022-03-08 16:41:23.911522+00"];
PREPARE a368a817 AS UPDATE public.payment_p2022_04 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a368a817["11.95","20403","362","1","14759","11.99","2022-04-16 04:35:36.904758+00"];
PREPARE f53be34c AS UPDATE public.payment_p2022_05 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE f53be34c["11.95","17354","305","1","2166","11.99","2022-05-12 11:28:17.949049+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.95","22650","204","2","15415","11.99","2022-06-11 11:17:22.428079+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.95","24553","195","2","16040","11.99","2022-06-15 02:21:00.279776+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.95","28814","592","1","3973","11.99","2022-07-06 12:15:38.928947+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.95","29136","13","2","8831","11.99","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":490,"lsn":"0/24D5658","timestamp":"2024-05-08 11:10:09.242871+0000"}
BEGIN; -- {"xid":491,"lsn":"0/24D5818","timestamp":"2024-05-08 11:10:09.242917+0000","commit_lsn":"0/24D59C8"}
PREPARE e1d51ac7 AS DELETE FROM public.payment_p2022_06 WHERE payment_id = $1 and customer_id = $2 and staff_id = $3 and rental_id = $4 and amount = $5 and payment_date = $6;
EXECUTE e1d51ac7["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
PREPARE e1d51ac7 AS DELETE FROM public.payment_p2022_06 WHERE payment_id = $1 and customer_id = $2 and staff_id = $3 and rental_id = $4 and amount = $5 and payment_date = $6;
EXECUTE e1d51ac7["32100","293","2","16051","5.99","2022-06-01 00:00:00+00"];
PREPARE 3f2797d9 AS DELETE FROM public.rental WHERE rental_id = $1;
EXECUTE 3f2797d9["16050"];
PREPARE 3f2797d9 AS DELETE FROM public.rental WHERE rental_id = $1;
EXECUTE 3f2797d9["16051"];
COMMIT; -- {"xid":491,"lsn":"0/24D59C8","timestamp":"2024-05-08 11:10:09.242917+0000"}
BEGIN; -- {"xid":492,"lsn":"0/24D59C8","timestamp":"2024-05-08 11:10:09.243006+0000","commit_lsn":"0/24D5F48"}
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.99","23757","116","2","14763","11.95","2022-02-11 03:52:25.634006+00"];
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.99","24866","237","2","11479","11.95","2022-02-07 18:37:34.579143+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.99","17055","196","2","106","11.95","2022-03-18 18:50:39.243747+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.99","28799","591","2","4383","11.95","2022-03-08 16:41:23.911522+00"];
PREPARE a368a817 AS UPDATE public.payment_p2022_04 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a368a817["11.99","20403","362","1","14759","11.95","2022-04-16 04:35:36.904758+00"];
PREPARE f53be34c AS UPDATE public.payment_p2022_05 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE f53be34c["11.99","17354","305","1","2166","11.95","2022-05-12 11:28:17.949049+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.99","22650","204","2","15415","11.95","2022-06-11 11:17:22.428079+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.99","24553","195","2","16040","11.95","2022-06-15 02:21:00.279776+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.99","28814","592","1","3973","11.95","2022-07-06 12:15:38.928947+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.99","29136","13","2","8831","11.95","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":492,"lsn":"0/24D5F48","timestamp":"2024-05-08 11:10:09.243006+0000"}
-- KEEPALIVE {"lsn":"0/24D5F48","timestamp":"2024-05-08 11:10:09.243014+0000"}
-- ENDPOS {"lsn":"0/24D5F48"}
