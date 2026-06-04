BEGIN
PREPARE e491e18b AS INSERT INTO public.items (id, name) overriding system value VALUES ($1, $2), ($3, $4);
EXECUTE e491e18b ('4', 'cdc-new-1', '5', 'cdc-new-2')
PREPARE b025120d AS UPDATE public.items SET name = $1 WHERE id = $2;
EXECUTE b025120d ('seed-updated-1', '1')
PREPARE 76e9cd55 AS DELETE FROM public.items WHERE id = $1;
EXECUTE 76e9cd55 ('2')
COMMIT
