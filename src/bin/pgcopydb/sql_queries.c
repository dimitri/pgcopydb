/*
 * sql_queries.c — SQL query string selection for pgcopydb.
 *
 * sql_queries_data.inc is generated from src/bin/pgcopydb/sql/ by
 * 'make gen-sql'.  It contains one static const char array per SQL variant.
 * The dispatch functions below select the right array for a given filter type
 * and/or PostgreSQL server version.
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


/*
 * list_source_depend is filtered but has no no-filter variant — it is always
 * called with a specific filter type (incl, excl, list-not-incl, list-excl).
 */
bool
pgcopydb_sql_list_source_depend(int filter, const char **sql)
{
	switch (filter)
	{
		case SOURCE_FILTER_TYPE_INCL:
		{
			*sql = sql_list_source_depend_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			*sql = sql_list_source_depend_excl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			*sql = sql_list_source_depend_list_not_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			*sql = sql_list_source_depend_list_excl;
			return true;
		}

		default:
		{
			break;
		}
	}
	return false;
}


bool
pgcopydb_sql_list_source_indexes(int filter, const char **sql)
{
	switch (filter)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			*sql = sql_list_source_indexes_no_filter;
			return true;
		}

		case SOURCE_FILTER_TYPE_INCL:
		{
			*sql = sql_list_source_indexes_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			*sql = sql_list_source_indexes_excl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			*sql = sql_list_source_indexes_list_not_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			*sql = sql_list_source_indexes_list_excl;
			return true;
		}

		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			*sql = sql_list_source_indexes_excl_index;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			*sql = sql_list_source_indexes_list_excl_index;
			return true;
		}

		default:
		{
			break;
		}
	}
	return false;
}


bool
pgcopydb_sql_list_source_sequences(int filter, const char **sql)
{
	switch (filter)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			*sql = sql_list_source_sequences_no_filter;
			return true;
		}

		case SOURCE_FILTER_TYPE_INCL:
		{
			*sql = sql_list_source_sequences_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			*sql = sql_list_source_sequences_excl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			*sql = sql_list_source_sequences_list_not_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			*sql = sql_list_source_sequences_list_excl;
			return true;
		}

		default:
		{
			break;
		}
	}
	return false;
}


bool
pgcopydb_sql_list_source_table_size(int filter, const char **sql)
{
	switch (filter)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			*sql = sql_list_source_table_size_no_filter;
			return true;
		}

		case SOURCE_FILTER_TYPE_INCL:
		{
			*sql = sql_list_source_table_size_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			*sql = sql_list_source_table_size_excl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			*sql = sql_list_source_table_size_list_not_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			*sql = sql_list_source_table_size_list_excl;
			return true;
		}

		default:
		{
			break;
		}
	}
	return false;
}


/*
 * list_source_tables has both filter and pg_version dispatch.
 * The pg-96 variants cover pg_version < 100000 (before PG 10).
 */
bool
pgcopydb_sql_list_source_tables(int pg_version, int filter, const char **sql)
{
	switch (filter)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			*sql = (pg_version < 100000)
				   ? sql_list_source_tables_no_filter_pg96
				   : sql_list_source_tables_no_filter;
			return true;
		}

		case SOURCE_FILTER_TYPE_INCL:
		{
			*sql = (pg_version < 100000)
				   ? sql_list_source_tables_incl_pg96
				   : sql_list_source_tables_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			*sql = (pg_version < 100000)
				   ? sql_list_source_tables_excl_pg96
				   : sql_list_source_tables_excl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			*sql = (pg_version < 100000)
				   ? sql_list_source_tables_list_not_incl_pg96
				   : sql_list_source_tables_list_not_incl;
			return true;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			*sql = (pg_version < 100000)
				   ? sql_list_source_tables_list_excl_pg96
				   : sql_list_source_tables_list_excl;
			return true;
		}

		default:
		{
			break;
		}
	}
	return false;
}
