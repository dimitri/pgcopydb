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


--
-- See https://github.com/dimitri/pgcopydb/issues/483
--
-- Migrations fails when the column name of a table is very long
--
CREATE TABLE """long"""
(
    """aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""" INT
);

INSERT INTO """long""" VALUES (1);
