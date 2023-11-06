CREATE TABLE public.employees (
    name character varying NOT NULL,
    emp_id uuid
);

--
-- See https://github.com/dimitri/pgcopydb/issues/498
--
-- Migration fails when handling CHECK CONSTRAINT
--

ALTER TABLE public.employees
    ADD CONSTRAINT chk_employees_name_not_abc CHECK (((name)::text !~ 'abc'::text)) NOT VALID;
