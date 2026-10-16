#! /bin/bash

set -x
set -e

# Verify a column-level ACL and a column COMMENT survive the copy on the target.

psql -At -d "${PGCOPYDB_TARGET_PGURI}" <<'EOF'
SELECT CASE
    WHEN attacl IS NOT NULL
    THEN 'column ACL preserved'
    ELSE 'column ACL missing'
END
FROM pg_attribute
WHERE attrelid = 'public.column_acl_test'::regclass
  AND attname = 'id';

SELECT CASE
    WHEN col_description('public.column_acl_test'::regclass, attnum) IS NOT NULL
    THEN 'column comment preserved'
    ELSE 'column comment missing'
END
FROM pg_attribute
WHERE attrelid = 'public.column_acl_test'::regclass
  AND attname = 'name';

SELECT CASE
    WHEN attacl IS NOT NULL
    THEN 'quoted column ACL preserved'
    ELSE 'quoted column ACL missing'
END
FROM pg_attribute
WHERE attrelid = 'public."column.acl ""quoted"" test"'::regclass
  AND attname = 'id.col';

SELECT CASE
    WHEN col_description('public."column.acl ""quoted"" test"'::regclass, attnum) IS NOT NULL
    THEN 'quoted column comment preserved'
    ELSE 'quoted column comment missing'
END
FROM pg_attribute
WHERE attrelid = 'public."column.acl ""quoted"" test"'::regclass
  AND attname = 'id.col';
EOF
