-- public.film_category should not have a record where film_id = 1
select
    film_id
from
    public.film_category
where
    film_id = 1;
