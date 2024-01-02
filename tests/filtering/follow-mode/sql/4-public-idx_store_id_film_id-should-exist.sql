-- public.idx_store_id_film_id should exist
select
    indexname
from
    pg_indexes
where
    indexname = 'idx_store_id_film_id';
