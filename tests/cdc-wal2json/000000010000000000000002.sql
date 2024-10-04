-- KEEPALIVE {"lsn":"0/24EDFE0","timestamp":"2024-09-18 15:02:33.330366+0000"}
BEGIN; -- {"xid":501,"lsn":"0/24EDFE0","timestamp":"2024-09-18 15:02:33.331532+0000","commit_lsn":"0/24EE460"}
PREPARE ec9f2790 AS INSERT INTO "public"."rental" ("rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7);
EXECUTE ec9f2790["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00"];
PREPARE 4afa901a AS INSERT INTO "public"."payment_p2022_06" ("payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date") overriding system value VALUES ($1, $2, $3, $4, $5, $6);
EXECUTE 4afa901a["32099","291","1","16050","5.990000","2022-06-01 00:00:00+00"];
COMMIT; -- {"xid":501,"lsn":"0/24EE460","timestamp":"2024-09-18 15:02:33.331532+0000"}
BEGIN; -- {"xid":502,"lsn":"0/24EE460","timestamp":"2024-09-18 15:02:33.333161+0000","commit_lsn":"0/24EF530"}
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.950000","23757","116","2","14763","11.990000","2022-02-11 03:52:25.634006+00"];
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.950000","24866","237","2","11479","11.990000","2022-02-07 18:37:34.579143+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.950000","17055","196","2","106","11.990000","2022-03-18 18:50:39.243747+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.950000","28799","591","2","4383","11.990000","2022-03-08 16:41:23.911522+00"];
PREPARE 6e01df31 AS UPDATE "public"."payment_p2022_04" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6e01df31["11.950000","20403","362","1","14759","11.990000","2022-04-16 04:35:36.904758+00"];
PREPARE b44f83e2 AS UPDATE "public"."payment_p2022_05" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE b44f83e2["11.950000","17354","305","1","2166","11.990000","2022-05-12 11:28:17.949049+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.950000","22650","204","2","15415","11.990000","2022-06-11 11:17:22.428079+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.950000","24553","195","2","16040","11.990000","2022-06-15 02:21:00.279776+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.950000","28814","592","1","3973","11.990000","2022-07-06 12:15:38.928947+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.950000","29136","13","2","8831","11.990000","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":502,"lsn":"0/24EF530","timestamp":"2024-09-18 15:02:33.333161+0000"}
BEGIN; -- {"xid":503,"lsn":"0/24EF6F0","timestamp":"2024-09-18 15:02:33.333359+0000","commit_lsn":"0/24EF800"}
PREPARE 9b3560f5 AS DELETE FROM "public"."payment_p2022_06" WHERE "payment_id" = $1 and "customer_id" = $2 and "staff_id" = $3 and "rental_id" = $4 and "amount" = $5 and "payment_date" = $6;
EXECUTE 9b3560f5["32099","291","1","16050","5.990000","2022-06-01 00:00:00+00"];
PREPARE 2ca9993d AS DELETE FROM "public"."rental" WHERE "rental_id" = $1;
EXECUTE 2ca9993d["16050"];
COMMIT; -- {"xid":503,"lsn":"0/24EF800","timestamp":"2024-09-18 15:02:33.333359+0000"}
BEGIN; -- {"xid":504,"lsn":"0/24EF800","timestamp":"2024-09-18 15:02:33.333619+0000","commit_lsn":"0/24EFD80"}
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.990000","23757","116","2","14763","11.950000","2022-02-11 03:52:25.634006+00"];
PREPARE 6ee4a968 AS UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6ee4a968["11.990000","24866","237","2","11479","11.950000","2022-02-07 18:37:34.579143+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.990000","17055","196","2","106","11.950000","2022-03-18 18:50:39.243747+00"];
PREPARE 61566f27 AS UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 61566f27["11.990000","28799","591","2","4383","11.950000","2022-03-08 16:41:23.911522+00"];
PREPARE 6e01df31 AS UPDATE "public"."payment_p2022_04" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 6e01df31["11.990000","20403","362","1","14759","11.950000","2022-04-16 04:35:36.904758+00"];
PREPARE b44f83e2 AS UPDATE "public"."payment_p2022_05" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE b44f83e2["11.990000","17354","305","1","2166","11.950000","2022-05-12 11:28:17.949049+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.990000","22650","204","2","15415","11.950000","2022-06-11 11:17:22.428079+00"];
PREPARE 547dee5b AS UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE 547dee5b["11.990000","24553","195","2","16040","11.950000","2022-06-15 02:21:00.279776+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.990000","28814","592","1","3973","11.950000","2022-07-06 12:15:38.928947+00"];
PREPARE dc973d3c AS UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "customer_id" = $3 and "staff_id" = $4 and "rental_id" = $5 and "amount" = $6 and "payment_date" = $7;
EXECUTE dc973d3c["11.990000","29136","13","2","8831","11.950000","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":504,"lsn":"0/24EFD80","timestamp":"2024-09-18 15:02:33.333619+0000"}
BEGIN; -- {"xid":505,"lsn":"0/24EFD80","timestamp":"2024-09-18 15:02:33.334859+0000","commit_lsn":"0/24F0070"}
PREPARE 21a8a4dc AS DELETE FROM "public"."address" WHERE "address_id" = $1 and "address" = $2 and "address2" IS NULL and "district" = $3 and "city_id" = $4 and "postal_code" = $5 and "phone" = $6 and "last_update" = $7;
EXECUTE 21a8a4dc["1","47 MySakila Drive","Alberta","300","","","2022-02-15 09:45:30+00"];
PREPARE 21a8a4dc AS DELETE FROM "public"."address" WHERE "address_id" = $1 and "address" = $2 and "address2" IS NULL and "district" = $3 and "city_id" = $4 and "postal_code" = $5 and "phone" = $6 and "last_update" = $7;
EXECUTE 21a8a4dc["3","23 Workhaven Lane","Alberta","300","","14033335568","2022-02-15 09:45:30+00"];
PREPARE 3f3ad11 AS UPDATE "public"."address" SET "postal_code" = $1 WHERE "address_id" = $2 and "address" = $3 and "address2" IS NULL and "district" = $4 and "city_id" = $5 and "postal_code" = $6 and "phone" = $7 and "last_update" = $8;
EXECUTE 3f3ad11["751007","4","1411 Lillydale Drive","QLD","576","","6172235589","2022-02-15 09:45:30+00"];
COMMIT; -- {"xid":505,"lsn":"0/24F0070","timestamp":"2024-09-18 15:02:33.334859+0000"}
BEGIN; -- {"xid":506,"lsn":"0/24F0070","timestamp":"2024-09-18 15:02:33.335686+0000","commit_lsn":"0/24F03E8"}
PREPARE a5a12d9d AS INSERT INTO "public"."generated_column_test" ("id", "name", "email") overriding system value VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9);
EXECUTE a5a12d9d["1","Tiger","tiger@wild.com","2","Elephant","elephant@wild.com","3","Cat","cat@home.net"];
COMMIT; -- {"xid":506,"lsn":"0/24F03E8","timestamp":"2024-09-18 15:02:33.335686+0000"}
BEGIN; -- {"xid":507,"lsn":"0/24F03E8","timestamp":"2024-09-18 15:02:33.336310+0000","commit_lsn":"0/24F0588"}
PREPARE acf3f3fd AS UPDATE "public"."generated_column_test" SET "name" = $1, "greet_hello" = DEFAULT, "greet_hi" = DEFAULT, "time" = DEFAULT, "email" = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE "id" = $3;
EXECUTE acf3f3fd["Lion","tiger@wild.com","1"];
PREPARE acf3f3fd AS UPDATE "public"."generated_column_test" SET "name" = $1, "greet_hello" = DEFAULT, "greet_hi" = DEFAULT, "time" = DEFAULT, "email" = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE "id" = $3;
EXECUTE acf3f3fd["Lion","lion@wild.com","1"];
COMMIT; -- {"xid":507,"lsn":"0/24F0588","timestamp":"2024-09-18 15:02:33.336310+0000"}
BEGIN; -- {"xid":508,"lsn":"0/24F0588","timestamp":"2024-09-18 15:02:33.336565+0000","commit_lsn":"0/24F0680"}
PREPARE acf3f3fd AS UPDATE "public"."generated_column_test" SET "name" = $1, "greet_hello" = DEFAULT, "greet_hi" = DEFAULT, "time" = DEFAULT, "email" = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE "id" = $3;
EXECUTE acf3f3fd["Kitten","kitten@home.com","3"];
COMMIT; -- {"xid":508,"lsn":"0/24F0680","timestamp":"2024-09-18 15:02:33.336565+0000"}
BEGIN; -- {"xid":509,"lsn":"0/24F0680","timestamp":"2024-09-18 15:02:33.336794+0000","commit_lsn":"0/24F0700"}
PREPARE 586d3754 AS DELETE FROM "public"."generated_column_test" WHERE "id" = $1;
EXECUTE 586d3754["2"];
COMMIT; -- {"xid":509,"lsn":"0/24F0700","timestamp":"2024-09-18 15:02:33.336794+0000"}
BEGIN; -- {"xid":510,"lsn":"0/24F0700","timestamp":"2024-09-18 15:02:33.338455+0000","commit_lsn":"0/24F0860"}
PREPARE 14a82b14 AS INSERT INTO "public"."single_column_table" ("id") overriding system value VALUES ($1), ($2);
EXECUTE 14a82b14["1","2"];
PREPARE 45a47ea2 AS INSERT INTO "public"."multi_column_table" ("id", "name", "email") overriding system value VALUES ($1, $2, $3), ($4, $5, $6);
EXECUTE 45a47ea2["1","Alice","alice@hello.com","2","Bob","bob@hello.com"];
COMMIT; -- {"xid":510,"lsn":"0/24F0860","timestamp":"2024-09-18 15:02:33.338455+0000"}
BEGIN; -- {"xid":511,"lsn":"0/24F0860","timestamp":"2024-09-18 15:02:33.338882+0000","commit_lsn":"0/24F0A48"}
COMMIT; -- {"xid":511,"lsn":"0/24F0A48","timestamp":"2024-09-18 15:02:33.338882+0000"}
BEGIN; -- {"xid":512,"lsn":"0/24F0A48","timestamp":"2024-09-18 15:02:33.340091+0000","commit_lsn":"0/24F10F0"}
TRUNCATE ONLY "Sp1eCial .Char"."source1testing"
COMMIT; -- {"xid":512,"lsn":"0/24F10F0","timestamp":"2024-09-18 15:02:33.340091+0000"}
BEGIN; -- {"xid":513,"lsn":"0/24F10F0","timestamp":"2024-09-18 15:02:33.341283+0000","commit_lsn":"0/24F1408"}
PREPARE 5fb4b087 AS INSERT INTO "Sp1eCial .Char"."source1testing" ("s0", "s""1") overriding system value VALUES ($1, $2), ($3, $4), ($5, $6), ($7, $8), ($9, $10);
EXECUTE 5fb4b087["6","1","7","2","8","3","9","4","10","5"];
COMMIT; -- {"xid":513,"lsn":"0/24F1408","timestamp":"2024-09-18 15:02:33.341283+0000"}
BEGIN; -- {"xid":514,"lsn":"0/24F1408","timestamp":"2024-09-18 15:02:33.342246+0000","commit_lsn":"0/24F15C8"}
PREPARE 67577134 AS UPDATE "Sp1eCial .Char"."source1testing" SET "s""1" = $1 WHERE "s0" = $2;
EXECUTE 67577134["2","6"];
PREPARE 67577134 AS UPDATE "Sp1eCial .Char"."source1testing" SET "s""1" = $1 WHERE "s0" = $2;
EXECUTE 67577134["4","7"];
PREPARE 67577134 AS UPDATE "Sp1eCial .Char"."source1testing" SET "s""1" = $1 WHERE "s0" = $2;
EXECUTE 67577134["6","8"];
PREPARE 67577134 AS UPDATE "Sp1eCial .Char"."source1testing" SET "s""1" = $1 WHERE "s0" = $2;
EXECUTE 67577134["8","9"];
PREPARE 67577134 AS UPDATE "Sp1eCial .Char"."source1testing" SET "s""1" = $1 WHERE "s0" = $2;
EXECUTE 67577134["10","10"];
COMMIT; -- {"xid":514,"lsn":"0/24F15C8","timestamp":"2024-09-18 15:02:33.342246+0000"}
BEGIN; -- {"xid":515,"lsn":"0/24F15C8","timestamp":"2024-09-18 15:02:33.342650+0000","commit_lsn":"0/24F1638"}
PREPARE fddd6a1b AS DELETE FROM "Sp1eCial .Char"."source1testing" WHERE "s0" = $1;
EXECUTE fddd6a1b["8"];
COMMIT; -- {"xid":515,"lsn":"0/24F1638","timestamp":"2024-09-18 15:02:33.342650+0000"}
BEGIN; -- {"xid":516,"lsn":"0/24F1638","timestamp":"2024-09-18 15:02:33.343718+0000","commit_lsn":"0/24F18E8"}
PREPARE 477f61f7 AS INSERT INTO "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" ("abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456", "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456") overriding system value VALUES ($1, $2), ($3, $4), ($5, $6), ($7, $8), ($9, $10);
EXECUTE 477f61f7["6","1","7","2","8","3","9","4","10","5"];
COMMIT; -- {"xid":516,"lsn":"0/24F18E8","timestamp":"2024-09-18 15:02:33.343718+0000"}
BEGIN; -- {"xid":517,"lsn":"0/24F18E8","timestamp":"2024-09-18 15:02:33.345559+0000","commit_lsn":"0/24F1C38"}
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["4","1"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["8","2"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["12","3"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["16","4"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["20","5"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["2","6"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["4","7"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["6","8"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["8","9"];
PREPARE c2fc8166 AS UPDATE "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" SET "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" = $1 WHERE "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" = $2;
EXECUTE c2fc8166["10","10"];
COMMIT; -- {"xid":517,"lsn":"0/24F1C38","timestamp":"2024-09-18 15:02:33.345559+0000"}
-- KEEPALIVE {"lsn":"0/24F1C38","timestamp":"2024-09-18 15:02:33.345697+0000"}
-- ENDPOS {"lsn":"0/24F1C38"}
