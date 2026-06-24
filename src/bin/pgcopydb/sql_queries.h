/*
 * sql_queries.h — public API for SQL query string selection.
 *
 * Each function fills *sql with a pointer to a static C string containing the
 * appropriate SQL for the given filter type and/or PostgreSQL server version.
 * The strings are defined in sql_queries_data.inc (generated) and the dispatch
 * logic lives in sql_queries.c (hand-written).
 */

#ifndef PGCOPYDB_SQL_QUERIES_H
#define PGCOPYDB_SQL_QUERIES_H

#include <stdbool.h>

bool pgcopydb_sql_expand_filter_schema(const char **sql);
bool pgcopydb_sql_expand_filter_tables(const char **sql);

bool pgcopydb_sql_list_collations(const char **sql);
bool pgcopydb_sql_list_database_properties(const char **sql);
bool pgcopydb_sql_list_databases(const char **sql);
bool pgcopydb_sql_list_ext_schemas(const char **sql);
bool pgcopydb_sql_list_ext_versions(const char **sql);
bool pgcopydb_sql_list_extensions(const char **sql);
bool pgcopydb_sql_list_schemas(const char **sql);

bool pgcopydb_sql_list_source_depend(int filter, const char **sql);
bool pgcopydb_sql_list_source_indexes(int filter, const char **sql);
bool pgcopydb_sql_list_source_sequences(int filter, const char **sql);
bool pgcopydb_sql_list_source_table_size(int filter, const char **sql);
bool pgcopydb_sql_list_source_tables(int filter, const char **sql);
bool pgcopydb_sql_list_table_attributes(int pg_version, const char **sql);

#endif  /* PGCOPYDB_SQL_QUERIES_H */
