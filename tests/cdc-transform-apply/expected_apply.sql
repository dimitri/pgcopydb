BEGIN
PREPARE XXXXXXXX AS INSERT INTO public.items (id, name) overriding system value VALUES ($1, $2), ($3, $4);
EXECUTE XXXXXXXX ('4', 'cdc-new-1', '5', 'cdc-new-2')
PREPARE YYYYYYYY AS UPDATE public.items SET name = $1 WHERE id = $2;
EXECUTE YYYYYYYY ('seed-updated-1', '1')
PREPARE ZZZZZZZZ AS DELETE FROM public.items WHERE id = $1;
EXECUTE ZZZZZZZZ ('2')
COMMIT
