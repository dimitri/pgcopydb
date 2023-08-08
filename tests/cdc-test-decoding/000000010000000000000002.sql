-- KEEPALIVE {"lsn":"0/2448E08","timestamp":"2023-06-20 15:40:11.982016+0000"}
BEGIN; -- {"xid":491,"lsn":"0/244BAB8","timestamp":"2023-06-20 15:40:11.982679+0000","commit_lsn":"0/244BF20"}
PREPARE ec9f2790 AS INSERT INTO "public"."rental" ("rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7);
EXECUTE ec9f2790["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00"];
PREPARE 4afa901a AS INSERT INTO "public"."payment_p2022_06" ("payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date") overriding system value VALUES ($1, $2, $3, $4, $5, $6);
EXECUTE 4afa901a["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
COMMIT; -- {"xid":491,"lsn":"0/244BF20","timestamp":"2023-06-20 15:40:11.982679+0000"}
BEGIN; -- {"xid":492,"lsn":"0/244BF20","timestamp":"2023-06-20 15:40:11.983092+0000","commit_lsn":"0/244D008"}
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.95","23757","116","2","14763","11.99","2022-02-11 03:52:25.634006+00"];
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.95","24866","237","2","11479","11.99","2022-02-07 18:37:34.579143+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.95","17055","196","2","106","11.99","2022-03-18 18:50:39.243747+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.95","28799","591","2","4383","11.99","2022-03-08 16:41:23.911522+00"];
PREPARE 6e01df31 AS UPDATE "public"."payment_p2022_04" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6e01df31["11.95","20403","362","1","14759","11.99","2022-04-16 04:35:36.904758+00"];
PREPARE b44f83e2 AS UPDATE "public"."payment_p2022_05" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE b44f83e2["11.95","17354","305","1","2166","11.99","2022-05-12 11:28:17.949049+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.95","22650","204","2","15415","11.99","2022-06-11 11:17:22.428079+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.95","24553","195","2","16040","11.99","2022-06-15 02:21:00.279776+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.95","28814","592","1","3973","11.99","2022-07-06 12:15:38.928947+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.95","29136","13","2","8831","11.99","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":492,"lsn":"0/244D008","timestamp":"2023-06-20 15:40:11.983092+0000"}
BEGIN; -- {"xid":493,"lsn":"0/244D1C8","timestamp":"2023-06-20 15:40:11.983134+0000","commit_lsn":"0/244D2D8"}
PREPARE 9b3560f5 AS DELETE FROM "public"."payment_p2022_06" WHERE "payment_id" = $1 and "customer_id" = $2 and "staff_id" = $3 and "rental_id" = $4 and "amount" = $5 and "payment_date" = $6;
EXECUTE 9b3560f5["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
PREPARE 2ca9993d AS DELETE FROM "public"."rental" WHERE "rental_id" = $1;
EXECUTE 2ca9993d["16050"];
COMMIT; -- {"xid":493,"lsn":"0/244D2D8","timestamp":"2023-06-20 15:40:11.983134+0000"}
BEGIN; -- {"xid":494,"lsn":"0/244D2D8","timestamp":"2023-06-20 15:40:11.983253+0000","commit_lsn":"0/244D858"}
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.99","23757","116","2","14763","11.95","2022-02-11 03:52:25.634006+00"];
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.99","24866","237","2","11479","11.95","2022-02-07 18:37:34.579143+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.99","17055","196","2","106","11.95","2022-03-18 18:50:39.243747+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.99","28799","591","2","4383","11.95","2022-03-08 16:41:23.911522+00"];
PREPARE 6e01df31 AS UPDATE "public"."payment_p2022_04" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6e01df31["11.99","20403","362","1","14759","11.95","2022-04-16 04:35:36.904758+00"];
PREPARE b44f83e2 AS UPDATE "public"."payment_p2022_05" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE b44f83e2["11.99","17354","305","1","2166","11.95","2022-05-12 11:28:17.949049+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.99","22650","204","2","15415","11.95","2022-06-11 11:17:22.428079+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.99","24553","195","2","16040","11.95","2022-06-15 02:21:00.279776+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.99","28814","592","1","3973","11.95","2022-07-06 12:15:38.928947+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.99","29136","13","2","8831","11.95","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":494,"lsn":"0/244D858","timestamp":"2023-06-20 15:40:11.983253+0000"}
BEGIN; -- {"xid":495,"lsn":"0/244D858","timestamp":"2023-06-20 15:40:11.983352+0000","commit_lsn":"0/244D988"}
PREPARE 73b7f705 AS UPDATE "public"."staff" SET "first_name" = $1, "last_name" = $2, "address_id" = $3, "email" = $4, "store_id" = $5, "active" = $6, "username" = $7, "password" = $8, "last_update" = $9, "picture" = $10 WHERE "staff_id" = $11;
EXECUTE 73b7f705["Mike","Hillyer","3","Mike.Hillyer@sakilastaff.com","1","true","Mike","8cb2237d0679ca88db6464eac60da96345513964","2023-06-20 15:40:11.823094+00","\\x89504e470d0a5a0a","1"];
COMMIT; -- {"xid":495,"lsn":"0/244D988","timestamp":"2023-06-20 15:40:11.983352+0000"}
-- KEEPALIVE {"lsn":"0/244D988","timestamp":"2023-06-20 15:40:11.983399+0000"}
-- ENDPOS {"lsn":"0/244D988"}
