INSERT INTO "public"."rental" ("rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14)
INSERT INTO "public"."payment_p2022_06" ("payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date") overriding system value VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)
UPDATE "public"."payment_p2022_02" SET "amount" = $1 WHERE "payment_id" = $2 and "payment_date" = $3
UPDATE "public"."payment_p2022_03" SET "amount" = $1 WHERE "payment_id" = $2 and "payment_date" = $3
UPDATE "public"."payment_p2022_04" SET "amount" = $1 WHERE "payment_id" = $2 and "payment_date" = $3
UPDATE "public"."payment_p2022_05" SET "amount" = $1 WHERE "payment_id" = $2 and "payment_date" = $3
UPDATE "public"."payment_p2022_06" SET "amount" = $1 WHERE "payment_id" = $2 and "payment_date" = $3
UPDATE "public"."payment_p2022_07" SET "amount" = $1 WHERE "payment_id" = $2 and "payment_date" = $3
DELETE FROM "public"."payment_p2022_06" WHERE ("payment_id", "payment_date") IN (($1, $2), ($3, $4))
DELETE FROM "public"."rental" WHERE "rental_id" IN ($1, $2)
