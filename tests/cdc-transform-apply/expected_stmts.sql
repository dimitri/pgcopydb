INSERT INTO public.items (id, name) overriding system value VALUES ($1, $2), ($3, $4)
UPDATE public.items SET name = $1 WHERE id = $2
DELETE FROM public.items WHERE id = $1
