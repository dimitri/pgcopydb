CREATE FUNCTION fixture_transform_from_sql(internal) RETURNS internal
    AS 'fixture_transform', 'fixture_transform_from_sql' LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION fixture_transform_to_sql(internal) RETURNS int4
    AS 'fixture_transform', 'fixture_transform_to_sql' LANGUAGE C STRICT IMMUTABLE;
