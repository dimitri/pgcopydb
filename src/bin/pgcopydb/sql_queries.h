/*
 * GENERATED FILE - do not edit.
 * Source: src/bin/pgcopydb/sql/
 * Run 'make gen-sql' after modifying .sql files.
 */

#ifndef PGCOPYDB_SQL_QUERIES_H
#define PGCOPYDB_SQL_QUERIES_H

#include <stdbool.h>

////////////////////////////////////////////////////////////////
/* list_source_tables                                           */
////////////////////////////////////////////////////////////////

typedef enum SqlListSourceTablesFilter
{
    SQL_LIST_SOURCE_TABLES_NONE = 0,
    SQL_LIST_SOURCE_TABLES_INCL = 1,
    SQL_LIST_SOURCE_TABLES_EXCL = 2,
    SQL_LIST_SOURCE_TABLES_LIST_NOT_INCL = 3,
    SQL_LIST_SOURCE_TABLES_LIST_EXCL = 4,
} SqlListSourceTablesFilter;

bool pgcopydb_sql_list_source_tables(int pg_version,
                                     SqlListSourceTablesFilter filter,
                                     const char **sql);

#endif  /* PGCOPYDB_SQL_QUERIES_H */
