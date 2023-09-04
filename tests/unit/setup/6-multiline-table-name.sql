CREATE TABLE public."with MyTableName

     AS (SELECT row_id

  " (
    row_id uuid NOT NULL
);

--
-- See https://github.com/dimitri/pgcopydb/issues/430
--
-- Migration fails when table has double quotes in them
--

CREATE TABLE IF NOT EXISTS public."""dqname"""
(
    id integer
);
