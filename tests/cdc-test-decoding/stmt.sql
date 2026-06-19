INSERT INTO public.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7)
INSERT INTO public.payment_p2022_06 (payment_id, customer_id, staff_id, rental_id, amount, payment_date) overriding system value VALUES ($1, $2, $3, $4, $5, $6)
UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7
UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7
UPDATE public.payment_p2022_04 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7
UPDATE public.payment_p2022_05 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7
UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7
UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7
DELETE FROM public.payment_p2022_06 WHERE payment_id = $1 and customer_id = $2 and staff_id = $3 and rental_id = $4 and amount = $5 and payment_date = $6
DELETE FROM public.rental WHERE rental_id = $1
UPDATE public.staff SET first_name = $1, last_name = $2, address_id = $3, email = $4, store_id = $5, active = $6, username = $7, password = $8, last_update = $9, picture = $10 WHERE staff_id = $11
INSERT INTO public."""dqname""" (id) overriding system value VALUES ($1)
INSERT INTO "Foo"".Bar".":Identifer As ""Column"".$1:" ("time", "[column name]") overriding system value VALUES ($1, $2)
UPDATE "Foo"".Bar".":Identifer As ""Column"".$1:" SET "time" = $1, "[column name]" = $2 WHERE "time" = $3 and "[column name]" = $4
DELETE FROM "Foo"".Bar".":Identifer As ""Column"".$1:" WHERE "time" = $1 and "[column name]" = $2
INSERT INTO "Unicode""Test"."слон" (id, "слон", "колонка") overriding system value VALUES ($1, $2, $3)
UPDATE "Unicode""Test"."слон" SET id = $1, "слон" = $2, "колонка" = $3 WHERE id = $4 and "слон" = $5
DELETE FROM "Unicode""Test"."слон" WHERE id = $1 and "слон" = $2
INSERT INTO public.t_bit_types (id, a, b) overriding system value VALUES ($1, $2, $3)
INSERT INTO public.generated_column_test (id, name, email) overriding system value VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)
UPDATE public.generated_column_test SET name = $1, greet_hello = DEFAULT, greet_hi = DEFAULT, "time" = DEFAULT, email = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE id = $3
DELETE FROM public.generated_column_test WHERE id = $1
INSERT INTO public.xpto (id, toasted_col1, rand1, toasted_col2, rand2) overriding system value VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)
UPDATE public.xpto SET toasted_col1 = $1, rand1 = $2, rand2 = $3 WHERE id = $4
UPDATE public.xpto SET rand1 = $1, rand2 = $2 WHERE id = $3
INSERT INTO public.xpto2 (toasted_col1, toasted_col2) overriding system value VALUES ($1, $2)
INSERT INTO public.identity_column_test (pk_col, id_col, name) overriding system value VALUES ($1, $2, $3), ($4, $5, $6)
UPDATE public.identity_column_test SET name = $1 WHERE pk_col = $2
DELETE FROM public.identity_column_test WHERE pk_col = $1
INSERT INTO public.quote_escaping_test (id, varchar_col, text_col) overriding system value VALUES ($1, $2, $3), ($4, $5, $6)
UPDATE public.quote_escaping_test SET varchar_col = $1, text_col = $2 WHERE id = $3
DELETE FROM public.quote_escaping_test WHERE id = $1
