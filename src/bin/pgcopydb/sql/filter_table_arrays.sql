-- SQLite query used by catalog_filter_table_arrays().
-- $1 : section name ('excl_table', 'incl_table', 'xdat_table', 'excl_index')
--
-- Returns 8 PostgreSQL array literals '{v1,...}' (or NULL when the class has
-- no entries) for the four EE/ER/RE/RR match classes in one scan of f_table.
-- One CTE scan replaces the eight correlated subqueries of the original query.
--
-- group_concat() returns NULL for zero matching rows, so the expression
-- '{' || NULL || '}' evaluates to NULL — the same sentinel the C caller uses
-- to detect an absent category (outputs[col] == NULL check).
WITH rows AS (
    SELECT nspname, nspname_re, relname, relname_re
      FROM f_table
     WHERE section = $1
)
SELECT
  -- EE: exact nspname + exact relname
  '{' || group_concat(nspname,    ',') FILTER (WHERE nspname    IS NOT NULL AND nspname_re IS NULL
                                                 AND relname    IS NOT NULL AND relname_re IS NULL) || '}',
  '{' || group_concat(relname,    ',') FILTER (WHERE nspname    IS NOT NULL AND nspname_re IS NULL
                                                 AND relname    IS NOT NULL AND relname_re IS NULL) || '}',
  -- ER: exact nspname + regex relname
  '{' || group_concat(nspname,    ',') FILTER (WHERE nspname    IS NOT NULL AND nspname_re IS NULL
                                                 AND relname    IS NULL     AND relname_re IS NOT NULL) || '}',
  '{' || group_concat(relname_re, ',') FILTER (WHERE nspname    IS NOT NULL AND nspname_re IS NULL
                                                 AND relname    IS NULL     AND relname_re IS NOT NULL) || '}',
  -- RE: regex nspname + exact relname
  '{' || group_concat(nspname_re, ',') FILTER (WHERE nspname    IS NULL     AND nspname_re IS NOT NULL
                                                 AND relname    IS NOT NULL AND relname_re IS NULL) || '}',
  '{' || group_concat(relname,    ',') FILTER (WHERE nspname    IS NULL     AND nspname_re IS NOT NULL
                                                 AND relname    IS NOT NULL AND relname_re IS NULL) || '}',
  -- RR: regex nspname + regex relname
  '{' || group_concat(nspname_re, ',') FILTER (WHERE nspname    IS NULL     AND nspname_re IS NOT NULL
                                                 AND relname    IS NULL     AND relname_re IS NOT NULL) || '}',
  '{' || group_concat(relname_re, ',') FILTER (WHERE nspname    IS NULL     AND nspname_re IS NOT NULL
                                                 AND relname    IS NULL     AND relname_re IS NOT NULL) || '}'
  FROM rows;
