-- KEEPALIVE {"lsn":"0/244BAE0","timestamp":"2023-09-04 13:24:59.807759+0000"}
BEGIN; -- {"xid":492,"lsn":"0/244F6D0","timestamp":"2023-09-04 13:24:59.808525+0000","commit_lsn":"0/244FB38"}
PREPARE 8ffad89d AS INSERT INTO public.rental ("rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7);
EXECUTE 8ffad89d["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00"];
PREPARE 1825441d AS INSERT INTO public.payment_p2022_06 ("payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date") overriding system value VALUES ($1, $2, $3, $4, $5, $6);
EXECUTE 1825441d["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
COMMIT; -- {"xid":492,"lsn":"0/244FB38","timestamp":"2023-09-04 13:24:59.808525+0000"}
BEGIN; -- {"xid":493,"lsn":"0/244FB38","timestamp":"2023-09-04 13:24:59.809267+0000","commit_lsn":"0/2450C20"}
PREPARE 32de52b9 AS UPDATE public.payment_p2022_02 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 32de52b9["11.95","23757","116","2","14763","11.99","2022-02-11 03:52:25.634006+00"];
PREPARE 32de52b9 AS UPDATE public.payment_p2022_02 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 32de52b9["11.95","24866","237","2","11479","11.99","2022-02-07 18:37:34.579143+00"];
PREPARE a5d9c563 AS UPDATE public.payment_p2022_03 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE a5d9c563["11.95","17055","196","2","106","11.99","2022-03-18 18:50:39.243747+00"];
PREPARE a5d9c563 AS UPDATE public.payment_p2022_03 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE a5d9c563["11.95","28799","591","2","4383","11.99","2022-03-08 16:41:23.911522+00"];
PREPARE 1d7c9a4f AS UPDATE public.payment_p2022_04 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 1d7c9a4f["11.95","20403","362","1","14759","11.99","2022-04-16 04:35:36.904758+00"];
PREPARE 7978edcc AS UPDATE public.payment_p2022_05 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 7978edcc["11.95","17354","305","1","2166","11.99","2022-05-12 11:28:17.949049+00"];
PREPARE 72ebfeff AS UPDATE public.payment_p2022_06 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 72ebfeff["11.95","22650","204","2","15415","11.99","2022-06-11 11:17:22.428079+00"];
PREPARE 72ebfeff AS UPDATE public.payment_p2022_06 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 72ebfeff["11.95","24553","195","2","16040","11.99","2022-06-15 02:21:00.279776+00"];
PREPARE 3b977bd8 AS UPDATE public.payment_p2022_07 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 3b977bd8["11.95","28814","592","1","3973","11.99","2022-07-06 12:15:38.928947+00"];
PREPARE 3b977bd8 AS UPDATE public.payment_p2022_07 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 3b977bd8["11.95","29136","13","2","8831","11.99","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":493,"lsn":"0/2450C20","timestamp":"2023-09-04 13:24:59.809267+0000"}
BEGIN; -- {"xid":494,"lsn":"0/2450DE0","timestamp":"2023-09-04 13:24:59.809367+0000","commit_lsn":"0/2450EF0"}
PREPARE 2fa3c9c9 AS DELETE FROM public.payment_p2022_06 WHERE "payment_id" = $1 and "customer_id" = $2 and "staff_id" = $3 and "rental_id" = $4 and "amount" = $5 and "payment_date" = $6;
EXECUTE 2fa3c9c9["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
PREPARE 4f0082a0 AS DELETE FROM public.rental WHERE "rental_id" = $1;
EXECUTE 4f0082a0["16050"];
COMMIT; -- {"xid":494,"lsn":"0/2450EF0","timestamp":"2023-09-04 13:24:59.809367+0000"}
BEGIN; -- {"xid":495,"lsn":"0/2450EF0","timestamp":"2023-09-04 13:24:59.809700+0000","commit_lsn":"0/2451470"}
PREPARE 32de52b9 AS UPDATE public.payment_p2022_02 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 32de52b9["11.99","23757","116","2","14763","11.95","2022-02-11 03:52:25.634006+00"];
PREPARE 32de52b9 AS UPDATE public.payment_p2022_02 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 32de52b9["11.99","24866","237","2","11479","11.95","2022-02-07 18:37:34.579143+00"];
PREPARE a5d9c563 AS UPDATE public.payment_p2022_03 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE a5d9c563["11.99","17055","196","2","106","11.95","2022-03-18 18:50:39.243747+00"];
PREPARE a5d9c563 AS UPDATE public.payment_p2022_03 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE a5d9c563["11.99","28799","591","2","4383","11.95","2022-03-08 16:41:23.911522+00"];
PREPARE 1d7c9a4f AS UPDATE public.payment_p2022_04 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 1d7c9a4f["11.99","20403","362","1","14759","11.95","2022-04-16 04:35:36.904758+00"];
PREPARE 7978edcc AS UPDATE public.payment_p2022_05 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 7978edcc["11.99","17354","305","1","2166","11.95","2022-05-12 11:28:17.949049+00"];
PREPARE 72ebfeff AS UPDATE public.payment_p2022_06 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 72ebfeff["11.99","22650","204","2","15415","11.95","2022-06-11 11:17:22.428079+00"];
PREPARE 72ebfeff AS UPDATE public.payment_p2022_06 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 72ebfeff["11.99","24553","195","2","16040","11.95","2022-06-15 02:21:00.279776+00"];
PREPARE 3b977bd8 AS UPDATE public.payment_p2022_07 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 3b977bd8["11.99","28814","592","1","3973","11.95","2022-07-06 12:15:38.928947+00"];
PREPARE 3b977bd8 AS UPDATE public.payment_p2022_07 SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 3b977bd8["11.99","29136","13","2","8831","11.95","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":495,"lsn":"0/2451470","timestamp":"2023-09-04 13:24:59.809700+0000"}
BEGIN; -- {"xid":496,"lsn":"0/2451470","timestamp":"2023-09-04 13:24:59.809833+0000","commit_lsn":"0/24515A0"}
PREPARE 87f8bc56 AS UPDATE public.staff SET "first_name" = $1, "last_name" = $2, "address_id" = $3, "email" = $4, "store_id" = $5, "active" = $6, "username" = $7, "password" = $8, "last_update" = $9, "picture" = $10 WHERE "staff_id" = $11;
EXECUTE 87f8bc56["Mike","Hillyer","3","Mike.Hillyer@sakilastaff.com","1","true","Mike","8cb2237d0679ca88db6464eac60da96345513964","2023-09-04 13:24:59.621395+00","\\x89504e470d0a5a0a","1"];
COMMIT; -- {"xid":496,"lsn":"0/24515A0","timestamp":"2023-09-04 13:24:59.809833+0000"}
BEGIN; -- {"xid":497,"lsn":"0/24515A0","timestamp":"2023-09-04 13:24:59.809880+0000","commit_lsn":"0/2451678"}
PREPARE 5eff0dcd AS INSERT INTO public."""dqname""" ("id") overriding system value VALUES ($1);
EXECUTE 5eff0dcd["1"];
COMMIT; -- {"xid":497,"lsn":"0/2451678","timestamp":"2023-09-04 13:24:59.809880+0000"}
-- KEEPALIVE {"lsn":"0/2451678","timestamp":"2023-09-04 13:24:59.809931+0000"}
-- ENDPOS {"lsn":"0/2451678"}
