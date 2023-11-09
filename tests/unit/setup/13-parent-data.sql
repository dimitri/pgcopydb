DROP TABLE IF EXISTS public.parent_model CASCADE;

CREATE TABLE IF NOT EXISTS public.parent_model
 (
    id integer NOT NULL,
     a integer NOT NULL,
     b boolean NOT NULL,
    CONSTRAINT pk_parent_model PRIMARY KEY (id)
 );

CREATE TABLE IF NOT EXISTS public.child_model_1
 (
    -- Inherited from table public.parent_model: id integer NOT NULL,
    -- Inherited from table public.parent_model: a integer NOT NULL,
    -- Inherited from table public.parent_model: b boolean NOT NULL,
    c jsonb NOT NULL,
    CONSTRAINT pk_child_model_1 PRIMARY KEY (id)
 )
INHERITS (public.parent_model);

INSERT INTO child_model_1 (id, a, b, c)
SELECT
    10000 + s,
    1,
    false,
    '[1, 2, 3, 4]'
FROM
    generate_series(1, 1000) s;