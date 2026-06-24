/*
 * sql_queries.c — SQL query string selection for pgcopydb.
 *
 * sql_queries_data.inc is generated from src/bin/pgcopydb/sql/ by
 * 'make gen-sql'.  It contains one static const char array per SQL file.
 * The dispatch functions below expose the right array for each query.
 *
 * When the .sql file tree changes, regenerate the data file with:
 *   make -C src/bin/pgcopydb gen-sql
 */

#include "parson.h"
#include "filtering.h"
#include "sql_queries.h"

/* Generated SQL string data — included here so the static arrays share this TU */
#include "sql/sql_queries_data.inc"


bool
pgcopydb_sql_list_collations(const char **sql)
{
	*sql = sql_list_collations;
	return true;
}


bool
pgcopydb_sql_list_database_properties(const char **sql)
{
	*sql = sql_list_database_properties;
	return true;
}


bool
pgcopydb_sql_list_databases(const char **sql)
{
	*sql = sql_list_databases;
	return true;
}


bool
pgcopydb_sql_list_ext_schemas(const char **sql)
{
	*sql = sql_list_ext_schemas;
	return true;
}


bool
pgcopydb_sql_list_ext_versions(const char **sql)
{
	*sql = sql_list_ext_versions;
	return true;
}


bool
pgcopydb_sql_list_extensions(const char **sql)
{
	*sql = sql_list_extensions;
	return true;
}


bool
pgcopydb_sql_list_schemas(const char **sql)
{
	*sql = sql_list_schemas;
	return true;
}


bool
pgcopydb_sql_list_source_tables(const char **sql)
{
	*sql = sql_list_source_tables;
	return true;
}


bool
pgcopydb_sql_list_filtered_not_incl_tables(const char **sql)
{
	*sql = sql_list_filtered_not_incl_tables;
	return true;
}


bool
pgcopydb_sql_list_filtered_excl_tables(const char **sql)
{
	*sql = sql_list_filtered_excl_tables;
	return true;
}


bool
pgcopydb_sql_list_source_indexes(const char **sql)
{
	*sql = sql_list_source_indexes;
	return true;
}


bool
pgcopydb_sql_list_source_sequences(const char **sql)
{
	*sql = sql_list_source_sequences;
	return true;
}


bool
pgcopydb_sql_list_source_depend(const char **sql)
{
	*sql = sql_list_source_depend;
	return true;
}


bool
pgcopydb_sql_list_source_table_size(const char **sql)
{
	*sql = sql_list_source_table_size;
	return true;
}


bool
pgcopydb_sql_list_table_attributes(int pg_version, const char **sql)
{
	*sql = (pg_version < 100000)
		   ? sql_list_table_attributes_pg96
		   : sql_list_table_attributes;
	return true;
}
