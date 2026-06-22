/*
 * src/bin/pgcopydb/schema.c
 *	 SQL queries to discover the source database schema
 */

#include <inttypes.h>
#include <limits.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#include "parson.h"

#include "postgres_fe.h"
#include "pqexpbuffer.h"

#include "catalog.h"
#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "sql_queries.h"
#include "string_utils.h"
#include <math.h>

static bool prepareFilters(PGSQL *pgsql, SourceFilters *filters);

static bool prepareFilterCopyIncludeOnlySchema(PGSQL *pgsql,
											   SourceFilters *filters);

static bool prepareFilterCopyExcludeSchema(PGSQL *pgsql,
										   SourceFilters *filters);

static bool prepareFilterCopyTableList(PGSQL *pgsql,
									   SourceFilterTableList *tableList,
									   const char *temp_table_name);


/* Context used when fetching database definitions */
typedef struct SourceDatabaseArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceDatabaseArrayContext;

/* Context used when fetching schema definitions */
typedef struct SourceSchemaArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceSchemaArrayContext;

/* Context used when fetching role definitions */
typedef struct SourceRoleArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceRoleArrayContext;

/* Context used when fetching database properties */
typedef struct SourcePropertiesArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourcePropertiesArrayContext;

/* Context used when fetching all the extension definitions */
typedef struct SourceExtensionArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceExtensionArrayContext;

/* Context used when fetching extension versions as a json array */
typedef struct ExtensionsVersionsArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	ExtensionsVersionsArray *evArray;
	bool parsedOk;
} ExtensionsVersionsArrayContext;

/* Context used when fetching collation definitions */
typedef struct SourceCollationArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceCollationArrayContext;

/* Context used when fetching all the table definitions */
typedef struct SourceTableArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool estimateTableSizes;
	bool parsedOk;
	char datname[PG_NAMEDATALEN]; /* database name, filled by schema_list_ordinary_tables */
} SourceTableArrayContext;


/* Context used when fetching all the table size definitions */
typedef struct SourceTableSizeArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceTableSizeArrayContext;

/* Context used when fetching candidate partition key range for a table */
typedef struct SourceTablePartKeyMinMaxValueContext
{
	char sqlstate[SQLSTATE_LENGTH];
	int64_t min;
	int64_t max;
	bool parsedOk;
} SourceTablePartKeyMinMaxValueContext;

/* Context used when fetching all the sequence definitions */
typedef struct SourceSequenceArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
	char datname[PG_NAMEDATALEN];
} SourceSequenceArrayContext;

/* Context used when fetching all the indexes definitions */
typedef struct SourceIndexArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceIndexArrayContext;

/* Context used when fetching all the table dependencies */
typedef struct SourceDependArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceDependArrayContext;

/* Context used when fetching a table's rowcount and checksum */
typedef struct ChecksumContext
{
	char sqlstate[SQLSTATE_LENGTH];
	TableChecksum *sum;
	bool parsedOk;
} ChecksumContext;


static void getSchemaList(void *ctx, PGresult *result);

static void getRoleList(void *ctx, PGresult *result);

static void getDatabaseList(void *ctx, PGresult *result);

static bool parseCurrentDatabase(PGresult *result,
								 int rowNumber,
								 SourceDatabase *database);

static void getDatabaseProperties(void *ctx, PGresult *result);

static bool parseDatabaseProperty(PGresult *result,
								  int rowNumber,
								  SourceProperty *property);

static void getExtensionList(void *ctx, PGresult *result);

static bool parseCurrentExtension(PGresult *result,
								  int rowNumber,
								  SourceExtension *extension,
								  int *confIndex);

static bool parseCurrentExtensionConfig(PGresult *result,
										int rowNumber,
										SourceExtensionConfig *extConfig);

static void getExtensionsVersions(void *ctx, PGresult *result);

static void getCollationList(void *ctx, PGresult *result);

static void getTableArray(void *ctx, PGresult *result);

static bool parseCurrentSourceTable(PGresult *result,
									int rowNumber,
									SourceTable *table);

static void getTableSizeArray(void *ctx, PGresult *result);

static bool parseCurrentSourceTableSize(PGresult *result,
										int rowNumber,
										SourceTableSize *tableSize);
static void parsePartKeyMinMaxValue(void *ctx, PGresult *result);

static bool getPartKeyMinMaxValue(PGSQL *pgsql, SourceTable *table);

static bool schema_list_table_attributes(PGSQL *pgsql, DatabaseCatalog *catalog);

static void getSequenceArray(void *ctx, PGresult *result);

static bool parseCurrentSourceSequence(PGresult *result,
									   int rowNumber,
									   SourceSequence *seq);

static void getIndexArray(void *ctx, PGresult *result);

static bool parseCurrentSourceIndex(PGresult *result,
									int rowNumber,
									SourceIndex *index);

static void getDependArray(void *ctx, PGresult *result);

static bool parseCurrentSourceDepend(PGresult *result,
									 int rowNumber,
									 SourceDepend *depend);

static void getTableChecksum(void *ctx, PGresult *result);


/*
 * schema_query_privileges queries the given database connection to figure out
 * if we can create a schema, and if we can create temporary objects.
 */
bool
schema_query_privileges(PGSQL *pgsql,
						bool *hasDBCreatePrivilage,
						bool *hasDBTempPrivilege)
{
	if (!pgsql_has_database_privilege(pgsql, "create", hasDBCreatePrivilage))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_has_database_privilege(pgsql, "temp", hasDBTempPrivilege))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * schema_list_databases grabs the list of databases from the given source
 * Postgres instance and allocates a SourceDatabase array with the result of
 * the query.
 */
bool
schema_list_databases(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourceDatabaseArrayContext parseContext = { { 0 }, catalog, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_databases(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getDatabaseList))
	{
		log_error("Failed to list databases");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list databases");
		return false;
	}

	return true;
}


/*
 * schema_list_database_properties grabs the list of GUC settings attached to a
 * given database with either ALTER DATABASE SET or ALTER ROLE IN DATABASE SET
 * commands.
 */
bool
schema_list_database_properties(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourcePropertiesArrayContext parseContext = { { 0 }, catalog, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_database_properties(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}
	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &parseContext, &getDatabaseProperties))
	{
		log_error("Failed to list databases properties");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list databases properties");
		return false;
	}

	return true;
}


/*
 * schema_list_schemas grabs the list of schema from the given Postgres
 * instance and allocates a SourceSchemaArray array with the result of the
 * query.
 */
bool
schema_list_schemas(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourceSchemaArrayContext parseContext = { { 0 }, catalog, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_schemas(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}
	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getSchemaList))
	{
		log_error("Failed to list schemas that extensions depend on");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list schemas that extensions depend on");
		return false;
	}

	return true;
}


/*
 * schema_list_roles grabs the list of role from the given Postgres
 * instance and allocates a SourceRoleArray array with the result of the
 * query.
 */
bool
schema_list_roles(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourceRoleArrayContext parseContext = { { 0 }, catalog, false };

	char *sql = "select oid, format('%I', rolname) as rolname from pg_roles";

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getRoleList))
	{
		log_error("Failed to list roles");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list roles");
		return false;
	}

	return true;
}


/*
 * schema_list_extensions grabs the list of extensions from the given source
 * Postgres instance and allocates a SourceExtension array with the result of
 * the query.
 */
bool
schema_list_extensions(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourceExtensionArrayContext parseContext = { { 0 }, catalog, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_extensions(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getExtensionList))
	{
		log_error("Failed to list extensions");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list extensions");
		return false;
	}

	return true;
}


/*
 * schema_list_ext_schemas grabs the list of schema that extensions depend on
 * from the given source Postgres instance and allocates a SourceSchemaArray
 * array with the result of the query.
 */
bool
schema_list_ext_schemas(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourceSchemaArrayContext parseContext = { { 0 }, catalog, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_ext_schemas(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}
	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getSchemaList))
	{
		log_error("Failed to list schemas that extensions depend on");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list schemas that extensions depend on");
		return false;
	}

	return true;
}


/*
 * schema_list_ext_versions lists available extensions versions.
 */
bool
schema_list_ext_versions(PGSQL *pgsql, ExtensionsVersionsArray *array)
{
	ExtensionsVersionsArrayContext parseContext = { { 0 }, array, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_ext_versions(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}
	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getExtensionsVersions))
	{
		log_error("Failed to list available extensions versions");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list available extensions versions");
		return false;
	}

	return true;
}


/*
 * schema_list_collations grabs the list of collations used in the given
 * database connection. Collations listed may be used in the database
 * definition itself, in a column in any table in that database, or in an index
 * definition.
 */
bool
schema_list_collations(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	SourceCollationArrayContext parseContext = { { 0 }, catalog, false };

	/*
	 * Each arm of the UNION may return the same colloid multiple times (once
	 * per column or index that references it).  UNION deduplicates only fully
	 * identical rows, so two rows that differ only in their description column
	 * (e.g. two views that both use collation "C") both survive.  Inserting
	 * them into s_coll, which has oid as PRIMARY KEY, then fails with a
	 * SQLite constraint error.
	 *
	 * The all_colls CTE collects every usage row, and the outer SELECT uses
	 * DISTINCT ON (colloid) to return exactly one row per collation OID.  The
	 * description column is informational only (shown by pgcopydb list
	 * collations) so any single value among the multiple usages is fine.
	 */
	const char *sql = NULL;

	if (!pgcopydb_sql_list_collations(&sql))
	{
		/* can't happen — always returns true */
		return false;
	}
	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &parseContext, &getCollationList))
	{
		log_error("Failed to list non-default collations in use in database");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to list non-default collations in use in database");
		return false;
	}

	return true;
}


/*
 * schema_prepare_pgcopydb_table_size creates an internal catalog table named
 * s_table_size.
 */
bool
schema_prepare_pgcopydb_table_size(PGSQL *pgsql,
								   SourceFilters *filters,
								   DatabaseCatalog *catalog)
{
	log_trace("schema_prepare_pgcopydb_table_size");

	SourceFilterType filterType = SOURCE_FILTER_TYPE_NONE;

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			/* skip filters preparing (temp tables) */
			break;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}

			filterType = filters->type;

			break;
		}

		/* ignore "exclude-index" here */
		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return true;
		}

		default:
		{
			log_error("BUG: schema_prepare_pgcopydb_table_size called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	SourceTableSizeArrayContext context = { { 0 }, catalog, false };

	const char *sql = NULL;

	if (!pgcopydb_sql_list_source_table_size(filterType, &sql))
	{
		log_error("BUG: no SQL for list_source_table_size filter %d", filterType);
		return false;
	}

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &context, &getTableSizeArray))
	{
		log_error("Failed to compute table size, see above for details");
		return false;
	}

	return true;
}


/*
 * schema_list_ordinary_tables grabs the list of tables from the given source
 * Postgres instance and allocates a SourceTable array with the result of the
 * query.
 */
bool
schema_list_ordinary_tables(PGSQL *pgsql,
							SourceFilters *filters,
							bool estimateTableSizes,
							DatabaseCatalog *catalog)
{
	SourceTableArrayContext context = { { 0 }, catalog, estimateTableSizes, false };

	log_trace("schema_list_ordinary_tables");

	if (pgsql->safeURI.uriParams.dbname != NULL)
	{
		strlcpy(context.datname, pgsql->safeURI.uriParams.dbname,
				sizeof(context.datname));
	}

	SourceFilterType filterType = SOURCE_FILTER_TYPE_NONE;

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			/* skip filters preparing (temp tables) */
			break;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}

			filterType = filters->type;

			break;
		}

		/* ignore "exclude-index" listing of filtered-out tables */
		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return true;
		}

		default:
		{
			log_error("BUG: schema_list_ordinary_tables called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("pgcopydb_sql_list_source_tables[%s]",
			  filterTypeToString(filterType));

	const char *sql = NULL;

	if (!pgcopydb_sql_list_source_tables(filterType, &sql))
	{
		log_error("BUG: no SQL for list_source_tables filter %d", filterType);
		return false;
	}

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getTableArray))
	{
		log_error("Failed to list tables");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list tables");
		return false;
	}

	if (!schema_list_table_attributes(pgsql, catalog))
	{
		log_error("Failed to list table attributes");
		return false;
	}

	return true;
}


/*
 * schema_list_sequences grabs the list of sequences from the given source
 * Postgres instance and allocates a SourceSequence array with the result of
 * the query.
 */
bool
schema_list_sequences(PGSQL *pgsql,
					  SourceFilters *filters,
					  DatabaseCatalog *catalog)
{
	SourceSequenceArrayContext context = { { 0 }, catalog, false };

	log_trace("schema_list_sequences");

	if (pgsql->safeURI.uriParams.dbname != NULL)
	{
		strlcpy(context.datname, pgsql->safeURI.uriParams.dbname,
				sizeof(context.datname));
	}

	SourceFilterType filterType = SOURCE_FILTER_TYPE_NONE;

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			/* skip filters preparing (temp tables) */
			break;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}

			filterType = filters->type;

			break;
		}

		/* ignore "exclude-index" listing of filtered-out tables */
		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return true;
		}

		default:
		{
			log_error("BUG: schema_list_sequences called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("listSourceSequencesSQL[%s]", filterTypeToString(filterType));

	const char *sql = NULL;

	if (!pgcopydb_sql_list_source_sequences(filterType, &sql))
	{
		log_error("BUG: no SQL for list_source_sequences filter %d", filterType);
		return false;
	}

	/*
	 * A single sequence can be attached to more than one table, and it could
	 * be that some of the tables are excluded and some of the tables are
	 * included in our filtering. In that case we want to remove from the
	 * SOURCE_FILTER_TYPE_LIST_EXCL list of sequences the sequences from the
	 * SOURCE_FILTER_TYPE_EXCL list.
	 */
	PQExpBuffer buffer = NULL;

	if (filters->type == SOURCE_FILTER_TYPE_LIST_EXCL)
	{
		buffer = createPQExpBuffer();

		const char *exclude = sql;
		const char *keep = NULL;

		if (!pgcopydb_sql_list_source_sequences(SOURCE_FILTER_TYPE_EXCL, &keep))
		{
			log_error("BUG: no SQL for list_source_sequences filter EXCL");
			(void) destroyPQExpBuffer(buffer);
			return false;
		}

		char *sqlTmpl =
			"select seqoid, "
			"       format('%%I', nspname) as nspname, "
			"       format('%%I', relname) as relname, "
			"       restore_list_name, "
			"       ownedby, attrelid, attroid "
			"  from (%s) as exclude "
			" where not exists "
			" ( "
			"   select 1 "
			"     from (%s) as keep "
			"    where keep.seqoid = exclude.seqoid "
			"      and keep.ownedby is not distinct from exclude.ownedby "
			"      and keep.attrelid is not distinct from exclude.attrelid "
			"      and keep.attroid is not distinct from exclude.attroid "
			" ) ";

		appendPQExpBuffer(buffer, sqlTmpl, exclude, keep);

		if (PQExpBufferBroken(buffer))
		{
			log_error("Failed to create SQL query: out of memory");
			(void) destroyPQExpBuffer(buffer);
			return false;
		}

		sql = buffer->data;
	}

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getSequenceArray))
	{
		log_error("Failed to list sequences");
		(void) destroyPQExpBuffer(buffer);
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list sequences");
		(void) destroyPQExpBuffer(buffer);
		return false;
	}

	(void) destroyPQExpBuffer(buffer);

	return true;
}


/*
 * schema_get_sequence_value fetches sequence metadata last_value and
 * is_called for the given sequence.
 */
bool
schema_get_sequence_value(PGSQL *pgsql, SourceSequence *seq)
{
	return pgsql_get_sequence(pgsql,
							  seq->qname,
							  &(seq->lastValue),
							  &(seq->isCalled));
}


/*
 * schema_list_relpages fetches the number of pages for the given table
 * and updates our internal catalog with that information.
 */
bool
schema_list_relpages(PGSQL *pgsql, SourceTable *table, DatabaseCatalog *catalog)
{
	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_INT, false };

	char *sql = "select relpages from pg_class where oid = $1::regclass";

	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { table->qname };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to get number of pages for table %s", table->qname);
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to get number of pages for table %s", table->qname);
		return false;
	}

	table->relpages = parseContext.intVal;

	if (catalog != NULL && catalog->db != NULL)
	{
		if (!catalog_update_s_table_relpages(catalog, table))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * schema_set_sequence_value calls pg_catalog.setval() on the given sequence.
 */
bool
schema_set_sequence_value(PGSQL *pgsql, SourceSequence *seq)
{
	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_BIGINT, false };
	char *sql = "select pg_catalog.setval($1::regclass, $2, $3)";

	int paramCount = 3;
	Oid paramTypes[3] = { TEXTOID, INT8OID, BOOLOID };
	const char *paramValues[3];

	IntString lastValueStr = intToString(seq->lastValue);
	paramValues[0] = seq->qname;
	paramValues[1] = lastValueStr.strValue;
	paramValues[2] = seq->isCalled ? "true" : "false";

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to set sequence %s last value to %lld",
				  seq->qname, (long long) seq->lastValue);
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to set sequence %s last value to %lld",
				  seq->qname, (long long) seq->lastValue);
		return false;
	}

	return true;
}


/*
 * schema_list_all_indexes grabs the list of indexes from the given source
 * Postgres instance and allocates a SourceIndex array with the result of the
 * query.
 */
bool
schema_list_all_indexes(PGSQL *pgsql,
						SourceFilters *filters,
						DatabaseCatalog *catalog)
{
	SourceIndexArrayContext context = { { 0 }, catalog, false };

	log_trace("schema_list_all_indexes");

	if (filters->type != SOURCE_FILTER_TYPE_NONE)
	{
		if (!prepareFilters(pgsql, filters))
		{
			log_error("Failed to prepare pgcopydb filters, "
					  "see above for details");
			return false;
		}
	}

	log_debug("listSourceIndexesSQL[%s]", filterTypeToString(filters->type));

	const char *sql = NULL;

	if (!pgcopydb_sql_list_source_indexes(filters->type, &sql))
	{
		log_error("BUG: no SQL for list_source_indexes filter %d",
				  filters->type);
		return false;
	}

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getIndexArray))
	{
		log_error("Failed to list all indexes");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list all indexes");
		return false;
	}

	return true;
}


/*
 * schema_list_pg_depend recursively walks the pg_catalog.pg_depend view and
 * builds the list of objects that depend on tables that are filtered-out from
 * our operations.
 */
bool
schema_list_pg_depend(PGSQL *pgsql,
					  SourceFilters *filters,
					  DatabaseCatalog *catalog)
{
	SourceDependArrayContext context = { { 0 }, catalog, false };

	log_trace("schema_list_pg_depend");

	switch (filters->type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			/* skip pg_depend computing entirely */
			return true;
		}

		case SOURCE_FILTER_TYPE_INCL:
		case SOURCE_FILTER_TYPE_EXCL:
		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			if (!prepareFilters(pgsql, filters))
			{
				log_error("Failed to prepare pgcopydb filters, "
						  "see above for details");
				return false;
			}
			break;
		}

		/* ignore "exclude-index" listing of filtered-out tables */
		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return true;
		}

		default:
		{
			log_error("BUG: schema_list_pg_depend called with "
					  "filtering type %d",
					  filters->type);
			return false;
		}
	}

	log_debug("listSourceDependSQL[%s]", filterTypeToString(filters->type));

	const char *sql = NULL;

	if (!pgcopydb_sql_list_source_depend(filters->type, &sql))
	{
		log_error("BUG: no SQL for list_source_depend filter %d",
				  filters->type);
		return false;
	}

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getDependArray))
	{
		log_error("Failed to list table dependencies");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to list table dependencies");
		return false;
	}

	return true;
}


/*
 * schema_list_partitions prepares the list of partitions that we can drive from
 * our parameters: table size, --split-tables-larger-than, and
 * --split-max-parts.
 */
bool
schema_list_partitions(PGSQL *pgsql,
					   DatabaseCatalog *catalog,
					   SourceTable *table,
					   uint64_t partSize,
					   int splitMaxParts)
{
	/* no partKey, no partitions, done. */
	if (IS_EMPTY_STRING_BUFFER(table->partKey))
	{
		table->partition.partCount = 0;
		return true;
	}

	/* when partSize is zero, just don't partition the COPY */
	if (partSize == 0)
	{
		table->partition.partCount = 0;
		return true;
	}

	/* if we have a partKey and it's not "ctid", calculate key bounds  */
	if (!IS_EMPTY_STRING_BUFFER(table->partKey) && !streq(table->partKey, "ctid"))
	{
		if (!getPartKeyMinMaxValue(pgsql, table))
		{
			/* errors have already been logged */
			return false;
		}
	}

	int64_t min = table->partmin;
	int64_t max = table->partmax;

	int64_t partsCount = 1;
	int64_t partsSize = max - min + 1;

	/*
	 * When the partition key is set to "ctid", it means that the table will be
	 * partitioned based on the physical location of the rows in the table.
	 *
	 * The relpages value represents the total number of pages in the table,
	 * which can be used as the maximum value for the partition range. By
	 * setting min to 0 and max to table->relpages, we ensure that each
	 * partition covers the entire range of pages in the table.
	 */
	bool splitByCTID = streq(table->partKey, "ctid");

	if (splitByCTID)
	{
		min = 0;
		max = table->relpages;

		/*
		 * Get the block size from the origin in the first attempt
		 * and then memoize it.
		 */
		static int blockSize = 0;
		bool isBlockSizeCached = blockSize != 0;
		if (!isBlockSizeCached && !pgsql_get_block_size(pgsql, &blockSize))
		{
			/* errors have already been logged */
			return false;
		}
		uint64_t pagesPerPart = ceil((double) partSize / blockSize);

		partsCount = ceil((double) table->relpages / (double) pagesPerPart);

		if (splitMaxParts > 0 && partsCount > splitMaxParts)
		{
			partsCount = splitMaxParts;
		}

		partsSize = ceil((double) table->relpages / partsCount);
	}

	/*
	 * Below code block calculates the number of parts needed and assigns the
	 * minimum and maximum values for each part. It also logs information about
	 * each partition and adds the table part to the catalog if provided.
	 *
	 * Example:
	 * int64_t tableSize (table->bytes) = 100;
	 * int64_t partSize = 10;
	 * int64_t min = 1;
	 * int64_t max = 100;
	 * int64_t result = partitionTable(&table, partSize, min, max, &catalog);
	 *
	 * Output:
	 *  Partition table#1: 1 - 10 (10)
	 *  Partition table#2: 11 - 20 (10)
	 *  Partition table#3: 21 - 30 (10)
	 *  ...
	 *  Partition table#10: 91 - 100 (10)
	 */
	else
	{
		/* add a partition for IS NULL (first) */
		partsCount = ceil((double) table->bytes / (double) partSize) + 1;

		if (splitMaxParts > 0 && partsCount > splitMaxParts)
		{
			partsCount = splitMaxParts;
		}

		partsSize = ceil((double) (max - min + 1) / partsCount);
	}

	/*
	 * Now add an s_table_part row per partition.
	 */
	for (int64_t i = 0; i < partsCount; i++)
	{
		int64_t partNumber = i + 1;
		SourceTableParts *parts = &(table->partition);

		bzero(parts, sizeof(SourceTableParts));

		parts->partNumber = partNumber;
		parts->partCount = partsCount;

		/* take care of NULL values (we accept partkey with unique indexes) */
		if (i == 0 && !splitByCTID)
		{
			parts->min = -1;
			parts->max = -1;
			parts->count = -1;
		}
		else if (splitByCTID)
		{
			parts->min = min + (i * partsSize);
			parts->max = min + ((i + 1) * partsSize) - 1;
			parts->count = parts->max - parts->min + 1;
		}
		else
		{
			/*
			 * partNumber == 0 is for NULL values
			 * partNumber == 1 is for range [ 0 .. a ], etc
			 */
			parts->min = min + ((i - 1) * partsSize);
			parts->max = min + (i * partsSize) - 1;
			parts->count = parts->max - parts->min + 1;
		}

		/* the last partition has no upper bound */
		if (partNumber == partsCount)
		{
			parts->max = -1;
			parts->count = -1;
		}

		log_debug("Partition %s #%d/%d: [%lld .. %lld] (%lld)",
				  table->qname,
				  parts->partNumber,
				  parts->partCount,
				  (long long) parts->min,
				  (long long) parts->max,
				  (long long) parts->count);

		if (catalog != NULL && catalog->db != NULL)
		{
			if (!catalog_add_s_table_part(catalog, table))
			{
				/* errors have already been logged */
			}
		}
	}

	return true;
}


/*
 * schema_checksum_table runs a SQL query that computes the number of rows of a
 * table and also a checksum for all the rows contents.
 */
bool
schema_send_table_checksum(PGSQL *pgsql, SourceTable *table)
{
	if (table->attributes.count == 0)
	{
		char sql[BUFSIZE] = { 0 };

		sformat(sql, sizeof(sql),
				"select count(1) as cnt, 0 as chksum from only %s",
				table->qname);

		if (!pgsql_send_with_params(pgsql, sql, 0, NULL, NULL))
		{
			log_error("Failed to compute checksum for table %s", table->qname);
			return false;
		}

		return true;
	}

	/* first prepare the column list */
	PQExpBuffer attrList = createPQExpBuffer();

	appendPQExpBuffer(attrList, "(");

	for (int c = 0; c < table->attributes.count; c++)
	{
		char *srcAttName = table->attributes.array[c].attname;

		appendPQExpBuffer(attrList, "%s%s",
						  c > 0 ? ", " : "",
						  srcAttName);
	}

	appendPQExpBuffer(attrList, ")");

	if (PQExpBufferBroken(attrList))
	{
		(void) destroyPQExpBuffer(attrList);
		log_error("Failed to build attribute list: Out of Memory");
		return false;
	}

	/* now prepare the actual query */
	PQExpBuffer sql = createPQExpBuffer();

	/*
	 * Compute the hashtext of every single row in the table, and aggregate the
	 * results as a sum of bigint numbers. Because the sum of bigint could
	 * overflow to numeric, the aggregated sum is then hashed into an MD5
	 * value: bigint is 64 bits, MD5 is 128 bits.
	 *
	 * Also, to lower the chances of a collision, include the row count in the
	 * computation of the MD5 by appending it to the input string of the MD5
	 * function.
	 */
	appendPQExpBuffer(sql,
					  "select count(1) as cnt, "
					  "md5(format('%%s-%%s', "
					  "      sum(hashtext(%s::text)::bigint),"
					  "      count(1))"
					  ")::uuid as chksum "
					  "from only %s",
					  attrList->data,
					  table->qname);

	(void) destroyPQExpBuffer(attrList);

	if (PQExpBufferBroken(sql))
	{
		(void) destroyPQExpBuffer(sql);
		log_error("Failed to build attribute list: Out of Memory");
		return false;
	}

	if (!pgsql_send_with_params(pgsql, sql->data, 0, NULL, NULL))
	{
		log_error("Failed to compute checksum for table %s", table->qname);
		(void) destroyPQExpBuffer(sql);
		return false;
	}

	(void) destroyPQExpBuffer(sql);

	return true;
}


/*
 * schema_fetch_table_checksum fetches the results from the
 * schema_send_table_checksum async query.
 */
bool
schema_fetch_table_checksum(PGSQL *pgsql, TableChecksum *sum, bool *done)
{
	ChecksumContext parseContext = { { 0 }, sum, false };

	if (!pgsql_fetch_results(pgsql, done, &parseContext, &getTableChecksum))
	{
		log_error("Failed to fetch table checksum results");
		return false;
	}

	return true;
}


/*
 * prepareFilters prepares the temporary tables that are needed on the Postgres
 * session where we want to implement a catalog query with filtering. The
 * filtering rules are then uploaded in those temp tables, and the filtering is
 * implemented with SQL joins.
 */
static bool
prepareFilters(PGSQL *pgsql, SourceFilters *filters)
{
	/*
	 * Temporary tables only are available within a session, so we need a
	 * multi-statement connection here.
	 */
	if (pgsql->connection == NULL)
	{
		/* open a multi-statements connection then */
		pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;
	}
	else if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT)
	{
		log_error("BUG: calling prepareFilters with a "
				  "non PGSQL_CONNECTION_MULTI_STATEMENT connection");
		pgsql_finish(pgsql);
		return false;
	}

	/*
	 * Resolve and normalise every filter name against the live source
	 * catalogs before using them in temp-table uploads or pg_dump args.
	 * The function is idempotent: it sets filters->normalized = true on
	 * first success and returns immediately on subsequent calls.
	 */
	if (!filters->normalized)
	{
		if (!filters_validate_and_normalize(pgsql, filters))
		{
			log_error("Failed to validate and normalize filter names");
			return false;
		}
	}

	/* if the filters have already been prepared, we're good */
	if (filters->prepared)
	{
		return true;
	}

	/*
	 * First, create the temp tables.
	 */
	char *tempTables[] = {
		"create temp table filter_exclude_schema(nspname name)",
		"create temp table filter_include_only_schema(nspname name)",
		"create temp table filter_include_only_table(nspname name, relname name)",
		"create temp table filter_exclude_table(nspname name, relname name)",
		"create temp table filter_exclude_table_data(nspname name, relname name)",
		"create temp table filter_exclude_index(nspname name, relname name)",
		NULL
	};

	for (int i = 0; tempTables[i] != NULL; i++)
	{
		if (!pgsql_execute(pgsql, tempTables[i]))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Now, fill-in the temp tables with the data that we have.
	 */
	if (!prepareFilterCopyIncludeOnlySchema(pgsql, filters))
	{
		/* errors have already been logged */
		return false;
	}

	if (!prepareFilterCopyExcludeSchema(pgsql, filters))
	{
		/* errors have already been logged */
		return false;
	}

	struct name_list_pair
	{
		char *name;
		SourceFilterTableList *list;
	}
	nameListPair[] =
	{
		{ "filter_include_only_table", &(filters->includeOnlyTableList) },
		{ "filter_exclude_table", &(filters->excludeTableList) },
		{ "filter_exclude_table_data", &(filters->excludeTableDataList) },
		{ "filter_exclude_index", &(filters->excludeIndexList) },
		{ NULL, NULL },
	};

	for (int i = 0; nameListPair[i].name != NULL; i++)
	{
		if (!prepareFilterCopyTableList(pgsql,
										nameListPair[i].list,
										nameListPair[i].name))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* mark the filters as prepared already */
	filters->prepared = true;

	return true;
}


/*
 * prepareFilterCopyExcludeSchema sends a COPY from STDIN query and then
 * uploads the local filters that we have in the pg_temp.filter_exclude_schema
 * table.
 */
static bool
prepareFilterCopyExcludeSchema(PGSQL *pgsql, SourceFilters *filters)
{
	if (filters->excludeSchemaList.count == 0)
	{
		return true;
	}

	char *qname = "\"pg_temp\".\"filter_exclude_schema\"";

	if (!pg_copy_from_stdin(pgsql, qname))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < filters->excludeSchemaList.count; i++)
	{
		char *nspname = filters->excludeSchemaList.array[i].nspname;

		if (!pg_copy_row_from_stdin(pgsql, "s", nspname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pg_copy_end(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * prepareFilterCopyIncludeOnlySchema sends a COPY from STDIN query and then
 * uploads the local filters that we have in the
 * pg_temp.filter_include_only_schema table.
 *
 * Then it prepares the pg_temp.filter_exclude_schema table with all the schema
 * names found in pg_namespace that are not in the include-only-schema list.
 */
static bool
prepareFilterCopyIncludeOnlySchema(PGSQL *pgsql, SourceFilters *filters)
{
	if (filters->includeOnlySchemaList.count == 0)
	{
		return true;
	}

	char *qname = "\"pg_temp\".\"filter_include_only_schema\"";

	if (!pg_copy_from_stdin(pgsql, qname))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < filters->includeOnlySchemaList.count; i++)
	{
		char *nspname = filters->includeOnlySchemaList.array[i].nspname;

		log_trace("prepareFilterCopyIncludeOnlySchema: \"%s\"", nspname);

		if (!pg_copy_row_from_stdin(pgsql, "s", nspname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pg_copy_end(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	char *sql =
		"insert into \"pg_temp\".\"filter_exclude_schema\" "
		"     select n.nspname "
		"       from pg_namespace n "
		"  left join \"pg_temp\".\"filter_include_only_schema\" inc "
		"         on n.nspname = inc.nspname "
		"      where inc.nspname is null ";

	if (!pgsql_execute(pgsql, sql))
	{
		log_error("Failed to prepare include-only-schema filters, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * prepareFilterCopyTableList sends a COPY from STDIN query and then uploads
 * the local filters that we have in the given target table.
 */
static bool
prepareFilterCopyTableList(PGSQL *pgsql,
						   SourceFilterTableList *tableList,
						   const char *temp_table_name)
{
	char qname[BUFSIZE] = { 0 };

	sformat(qname, sizeof(qname), "\"pg_temp\".\"%s\"", temp_table_name);

	if (!pg_copy_from_stdin(pgsql, qname))
	{
		/* errors have already been logged */
		return false;
	}

	for (int i = 0; i < tableList->count; i++)
	{
		char *nspname = tableList->array[i].nspname;
		char *relname = tableList->array[i].relname;

		log_trace("\"%s\"\t\"%s\"", nspname, relname);

		if (!pg_copy_row_from_stdin(pgsql, "ss", nspname, relname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pg_copy_end(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * getSchemaList loops over the SQL result for the schema array query and
 * allocates an array of schemas then populates it with the query result.
 */
static void
getSchemaList(void *ctx, PGresult *result)
{
	SourceSchemaArrayContext *context = (SourceSchemaArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 3)
	{
		log_error("Query returned %d columns, expected 3", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	int errors = 0;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceSchema *schema = (SourceSchema *) calloc(1, sizeof(SourceSchema));

		if (schema == NULL)
		{
			++errors;
			log_error(ALLOCATION_FAILED_ERROR);
			break;
		}

		/* 1. oid */
		char *value = PQgetvalue(result, rowNumber, 0);

		if (!stringToUInt32(value, &(schema->oid)) || schema->oid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}

		/* 2. nspname */
		value = PQgetvalue(result, rowNumber, 1);
		int length = strlcpy(schema->nspname, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Schema name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}

		/* 3. restoreListName */
		value = PQgetvalue(result, rowNumber, 2);
		length = strlcpy(schema->restoreListName, value, RESTORE_LIST_NAMEDATALEN);

		if (length >= RESTORE_LIST_NAMEDATALEN)
		{
			log_error("Schema restore list name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
					  value, length, RESTORE_LIST_NAMEDATALEN - 1);
			++errors;
		}

		log_trace("getSchemaList: %u \"%s\" %s",
				  schema->oid,
				  schema->nspname,
				  schema->restoreListName);

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_namespace(context->catalog, schema))
			{
				/* errors have already been logged */
				++errors;
				break;
			}
		}
	}

	context->parsedOk = errors == 0;
}


/*
 * getRoleList loops over the SQL result for the role array query and
 * allocates an array of roles then populates it with the query result.
 */
static void
getRoleList(void *ctx, PGresult *result)
{
	SourceRoleArrayContext *context = (SourceRoleArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 2)
	{
		log_error("Query returned %d columns, expected 2", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	int errors = 0;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceRole *role = (SourceRole *) calloc(1, sizeof(SourceRole));

		if (role == NULL)
		{
			++errors;
			log_error(ALLOCATION_FAILED_ERROR);
			break;
		}

		/* 1. oid */
		char *value = PQgetvalue(result, rowNumber, 0);

		if (!stringToUInt32(value, &(role->oid)) || role->oid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}

		/* 2. rolname */
		value = PQgetvalue(result, rowNumber, 1);
		int length = strlcpy(role->rolname, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Role name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}

		log_trace("getRoleList: %u %s", role->oid, role->rolname);

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_role(context->catalog, role))
			{
				/* errors have already been logged */
				++errors;
				break;
			}
		}
	}

	context->parsedOk = errors == 0;
}


/*
 * getDatabaseList loops over the SQL result for the database array query and
 * allocates an array of databases then populates it with the query result.
 */
static void
getDatabaseList(void *ctx, PGresult *result)
{
	SourceDatabaseArrayContext *context = (SourceDatabaseArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 4)
	{
		log_error("Query returned %d columns, expected 4", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceDatabase *database =
			(SourceDatabase *) calloc(1, sizeof(SourceDatabase));

		if (!parseCurrentDatabase(result, rowNumber, database))
		{
			parsedOk = false;
			break;
		}

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_database(context->catalog, database))
			{
				/* errors have already been logged */
				parsedOk = false;
				break;
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentDatabase parses a single row of the database listing query
 * result.
 */
static bool
parseCurrentDatabase(PGresult *result, int rowNumber, SourceDatabase *database)
{
	int errors = 0;

	/* 1. oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(database->oid)) || database->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. datname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(database->datname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Database name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 3. bytes */
	value = PQgetvalue(result, rowNumber, 2);
	if (PQgetisnull(result, rowNumber, 2))
	{
		/*
		 * It may happen that pg_table_size() returns NULL (when failing to
		 * open the given relation).
		 */
		database->bytes = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 2);

		if (!stringToInt64(value, &(database->bytes)))
		{
			log_error("Invalid pg_database_size: \"%s\"", value);
			++errors;
		}
	}

	/* 4. pg_size_pretty */
	value = PQgetvalue(result, rowNumber, 3);
	length = strlcpy(database->bytesPretty, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Pretty printed byte size \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getDatabaseList loops over the SQL result for the database properties array
 * query and allocates an array of GUC settings then populates it with the
 * query result.
 */
static void
getDatabaseProperties(void *ctx, PGresult *result)
{
	SourcePropertiesArrayContext *context = (SourcePropertiesArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 3)
	{
		log_error("Query returned %d columns, expected 3", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceProperty *property =
			(SourceProperty *) calloc(1, sizeof(SourceProperty));

		if (!parseDatabaseProperty(result, rowNumber, property))
		{
			parsedOk = false;
			break;
		}

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_database_properties(context->catalog, property))
			{
				/* errors have already been logged */
				parsedOk = false;
				break;
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentProperty parses a single row of the database properties listing
 * query result.
 */
static bool
parseDatabaseProperty(PGresult *result, int rowNumber, SourceProperty *property)
{
	int errors = 0;

	/* 1. datname */
	if (PQgetisnull(result, rowNumber, 0))
	{
		log_error("BUG: parseDatabaseProperty: datname is NULL");
		++errors;
	}
	else
	{
		char *value = PQgetvalue(result, rowNumber, 0);
		int length = strlcpy(property->datname, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Properties role name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}
	}

	/* 2. rolname */
	if (PQgetisnull(result, rowNumber, 1))
	{
		property->roleInDatabase = false;
	}
	else
	{
		property->roleInDatabase = true;

		char *value = PQgetvalue(result, rowNumber, 1);
		int length = strlcpy(property->rolname, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Properties role name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}
	}

	/* 3. setconfig */
	if (PQgetisnull(result, rowNumber, 2))
	{
		log_error("BUG: parseDatabaseProperty: setconfig is NULL");
		++errors;
	}
	else
	{
		char *value = PQgetvalue(result, rowNumber, 2);
		int len = strlen(value);
		int bytes = len + 1;

		property->setconfig = (char *) calloc(bytes, sizeof(char));

		if (property->setconfig == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(property->setconfig, value, bytes);
	}

	return errors == 0;
}


/*
 * getExtensionList loops over the SQL result for the extension array query and
 * allocates an array of extensions then populates it with the query result.
 */
static void
getExtensionList(void *ctx, PGresult *result)
{
	SourceExtensionArrayContext *context = (SourceExtensionArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 11)
	{
		log_error("Query returned %d columns, expected 11", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	SourceExtension *extension =
		(SourceExtension *) calloc(1, sizeof(SourceExtension));

	if (extension == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		parsedOk = false;
		return;
	}

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		int confIndex = 0;

		(void) bzero(extension, sizeof(SourceExtension));

		if (!parseCurrentExtension(result, rowNumber, extension, &confIndex))
		{
			parsedOk = false;
			break;
		}

		log_trace("getExtensionList: %s [%d/%d]",
				  extension->extname,
				  confIndex,
				  extension->config.count);

		/*
		 * Only the first extension of a series gets into the extension list.
		 *
		 * Each extension has an array of extconfig (pg_class oids) and an
		 * array of extcondition (WHERE clauses, as text) of the same
		 * dimensions.
		 *
		 * The arrays may be empty, in which case confIndex == 0, and we can
		 * skip the extension configuration parts.
		 *
		 * The arrays may contain a single entry, in which case parsing the
		 * current row is self-contained.
		 *
		 * The arrays may contain 2 or more values, in which case the first row
		 * we read in the loop is where we build the SourceExtension structure
		 * instance, and then the next rows of the SQL query have the same
		 * first columns values and vary only in their extconfig/extcondition
		 * columns. The arrays have been UNNESTed, so each row contains the
		 * next value from the array.
		 */
		if (confIndex == 0 || confIndex == 1)
		{
			if (context->catalog != NULL && context->catalog->db != NULL)
			{
				if (!catalog_add_s_extension(context->catalog, extension))
				{
					/* errors have already been logged */
					parsedOk = false;
					break;
				}
			}
		}

		/* now loop over extension configuration, if any */
		if (extension->config.count > 0)
		{
			SourceExtensionConfig *config =
				(SourceExtensionConfig *) calloc(1, sizeof(SourceExtensionConfig));

			if (config == NULL)
			{
				log_fatal(ALLOCATION_FAILED_ERROR);
				parsedOk = false;
				return;
			}

			if (!parseCurrentExtensionConfig(result, rowNumber, config))
			{
				parsedOk = false;
				break;
			}

			if (context->catalog != NULL && context->catalog->db != NULL)
			{
				config->extoid = extension->oid;

				if (!catalog_add_s_extension_config(context->catalog, config))
				{
					/* errors have already been logged */
					parsedOk = false;
					break;
				}
			}
		}
	}


	context->parsedOk = parsedOk;
}


/*
 * parseCurrentExtension parses a single row of the extension listing query
 * result.
 */
static bool
parseCurrentExtension(PGresult *result,
					  int rowNumber,
					  SourceExtension *extension,
					  int *confIndex)
{
	int errors = 0;

	/* 1. oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(extension->oid)) || extension->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. extname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(extension->extname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Extension name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 3. extnamespace */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(extension->extnamespace, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Extension extnamespace \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 4. extrelocatable */
	value = PQgetvalue(result, rowNumber, 3);
	extension->extrelocatable = (*value) == 't';

	/* 5. array_length(extconfig), or NULL */
	if (PQgetisnull(result, rowNumber, 4))
	{
		extension->config.count = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 4);

		if (!stringToInt(value, &(extension->config.count)))
		{
			log_error("Invalid extension configuration count \"%s\"", value);
			++errors;
		}
	}

	/* 6. n (position over count), or NULL */
	if (PQgetisnull(result, rowNumber, 5))
	{
		*confIndex = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 5);

		if (!stringToInt(value, confIndex))
		{
			log_error("Invalid extension configuration index \"%s\"", value);
			++errors;
		}
	}

	return errors == 0;
}


/*
 * parseCurrentExtensionConfig parses a single row of the extension listing
 * query and adds the extconfig and extcondition columns to the given array
 * entry of SourceExtensionConfig.
 */
static bool
parseCurrentExtensionConfig(PGresult *result,
							int rowNumber,
							SourceExtensionConfig *extConfig)
{
	int errors = 0;

	/* 7. extconfig (pg_class oid) */
	char *value = PQgetvalue(result, rowNumber, 6);

	if (!stringToUInt32(value, &(extConfig->reloid)))
	{
		log_error("Invalid extension configuration OID \"%s\"", value);
		++errors;
	}

	/* 8. n.nspname */
	value = PQgetvalue(result, rowNumber, 7);
	int length = strlcpy(extConfig->nspname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 9. c.relname */
	value = PQgetvalue(result, rowNumber, 8);
	length = strlcpy(extConfig->relname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Extension configuration table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 10. extcondition */
	value = PQgetvalue(result, rowNumber, 9);
	extConfig->condition = strdup(value);

	if (extConfig->condition == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		++errors;
	}

	/* 11. relkind */
	value = PQgetvalue(result, rowNumber, 10);
	extConfig->relkind = value[0];

	if (extConfig->relkind == 0)
	{
		log_error("Extension configuration relkind is empty");
		++errors;
	}

	return errors == 0;
}


/*
 * getExtensionsVersions loops over the SQL result for the available extension
 * versions list.
 */
static void
getExtensionsVersions(void *ctx, PGresult *result)
{
	ExtensionsVersionsArrayContext *context =
		(ExtensionsVersionsArrayContext *) ctx;

	int nTuples = PQntuples(result);

	if (PQnfields(result) != 4)
	{
		log_error("Query returned %d columns, expected 4", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	if (nTuples == 0)
	{
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->evArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getExtensionsVersions");

		context->evArray->array = NULL;
	}

	context->evArray->count = nTuples;
	context->evArray->array =
		(ExtensionsVersions *) calloc(nTuples, sizeof(ExtensionsVersions));

	if (context->evArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	int errors = 0;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		ExtensionsVersions *ev = &(context->evArray->array[rowNumber]);

		/* 1. name */
		char *value = PQgetvalue(result, rowNumber, 0);
		int length = strlcpy(ev->name, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Extension name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}

		/* 2. defaultVersion */
		value = PQgetvalue(result, rowNumber, 1);
		length = strlcpy(ev->defaultVersion, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Extension version \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}

		/* 3. installedVersion */
		value = PQgetvalue(result, rowNumber, 2);
		length = strlcpy(ev->installedVersion, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Extension version \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}

		/* 4. versions JSON array */
		if (!PQgetisnull(result, rowNumber, 3))
		{
			value = PQgetvalue(result, rowNumber, 3);
			ev->json = json_parse_string(value);

			if (ev->json == NULL || json_type(ev->json) != JSONArray)
			{
				log_error("Failed to parse extension \"%s\" available versions "
						  "JSON array: %s",
						  ev->name, value);
				++errors;
			}
		}
	}

	context->parsedOk = errors == 0;
}


/*
 * getCollationList loops over the SQL result for the collation array query and
 * allocates an array of schemas then populates it with the query result.
 */
static void
getCollationList(void *ctx, PGresult *result)
{
	SourceCollationArrayContext *context = (SourceCollationArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 4)
	{
		log_error("Query returned %d columns, expected 4", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	int errors = 0;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceCollation *collation =
			(SourceCollation *) calloc(1, sizeof(SourceCollation));

		if (collation == NULL)
		{
			++errors;
			log_error(ALLOCATION_FAILED_ERROR);
			break;
		}

		/* 1. oid */
		char *value = PQgetvalue(result, rowNumber, 0);

		if (!stringToUInt32(value, &(collation->oid)) || collation->oid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}

		/* 2. collname */
		value = PQgetvalue(result, rowNumber, 1);
		int length = strlcpy(collation->collname, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Collation name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}

		/* 3. desc */
		value = PQgetvalue(result, rowNumber, 2);
		length = strlen(value) + 1;
		collation->desc = (char *) calloc(length, sizeof(char));

		if (collation->desc == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return;
		}

		strlcpy(collation->desc, value, length);

		/* 4. restoreListName */
		value = PQgetvalue(result, rowNumber, 3);
		length =
			strlcpy(collation->restoreListName,
					value,
					RESTORE_LIST_NAMEDATALEN);

		if (length >= RESTORE_LIST_NAMEDATALEN)
		{
			log_error("Collation restore list name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
					  value, length, RESTORE_LIST_NAMEDATALEN - 1);
			++errors;
		}

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_coll(context->catalog, collation))
			{
				/* errors have already been logged */
				++errors;
				break;
			}
		}
	}

	context->parsedOk = errors == 0;
}


/*
 * getTableSizeArray retrieves the table size array from the PostgreSQL result and populates the context.
 */
static void
getTableSizeArray(void *ctx, PGresult *result)
{
	SourceTableSizeArrayContext *context = (SourceTableSizeArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 3)
	{
		log_error("Query returned %d columns, expected 3", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceTableSize *tableSize =
			(SourceTableSize *) calloc(1, sizeof(SourceTableSize));

		if (!parseCurrentSourceTableSize(result, rowNumber, tableSize))
		{
			parsedOk = false;
			break;
		}

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_table_size(context->catalog, tableSize))
			{
				/* errors have already been logged */
				parsedOk = false;
				break;
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceTableSize parses the current source table size from the given PGresult object.
 */
static bool
parseCurrentSourceTableSize(PGresult *result, int rowNumber, SourceTableSize *tableSize)
{
	int errors = 0;

	int fnoid = PQfnumber(result, "oid");
	int fnbytes = PQfnumber(result, "bytes");
	int fnbytespretty = PQfnumber(result, "pg_size_pretty");

	/* 1. oid */
	char *value = PQgetvalue(result, rowNumber, fnoid);

	if (!stringToUInt32(value, &(tableSize->oid)) || tableSize->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. bytes */
	value = PQgetvalue(result, rowNumber, fnbytes);

	if (!stringToInt64(value, &(tableSize->bytes)))
	{
		log_error("Invalid pg_table_size: \"%s\"", value);
		++errors;
	}

	/* 3. pg_size_pretty */
	value = PQgetvalue(result, rowNumber, fnbytespretty);
	int length = strlcpy(tableSize->bytesPretty, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Pretty printed byte size \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getTableArray loops over the SQL result for the tables array query and saves
 * each source table in our internal catalog.
 *
 * If we enabled estimating table sizes, we also store an estimage for the table
 * sizes in the internal catalog.
 */
static void
getTableArray(void *ctx, PGresult *result)
{
	SourceTableArrayContext *context = (SourceTableArrayContext *) ctx;

	int nTuples = PQntuples(result);

	if (PQnfields(result) != 12)
	{
		log_error("Query returned %d columns, expected 12", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples && parsedOk; rowNumber++)
	{
		SourceTable *table = (SourceTable *) calloc(1, sizeof(SourceTable));

		if (!parseCurrentSourceTable(result, rowNumber, table))
		{
			parsedOk = false;
			break;
		}

		strlcpy(table->datname, context->datname, sizeof(table->datname));

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			switch (table->relkind)
			{
				/* regular or partitioned table */
				case 'r':
				case 'p':
				{
					parsedOk = catalog_add_s_table(context->catalog, table);

					if (parsedOk && context->estimateTableSizes)
					{
						SourceTableSize sourceTableSize = {
							.oid = table->oid,
							.bytes = table->bytesEstimate
						};

						strlcpy(sourceTableSize.bytesPretty,
								table->bytesEstimatePretty,
								sizeof(sourceTableSize.bytesPretty));

						parsedOk = catalog_add_s_table_size(context->catalog,
															&sourceTableSize);
					}

					break;
				}

				/* materialized view */
				case 'm':
				{
					parsedOk = catalog_add_s_matview(context->catalog, table);
					break;
				}

				default:
				{
					log_error("Unknown relkind \"%c\" for relation \"%s\"",
							  table->relkind, table->qname);
					parsedOk = false;
					break;
				}
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * getPartKeyMinMaxValue retrieves the min and max values for the
 * candidate partition key of the given table.
 */
static bool
getPartKeyMinMaxValue(PGSQL *pgsql, SourceTable *table)
{
	PQExpBuffer sql = createPQExpBuffer();
	char *sqlTemplate = "select min(%s), max(%s) "
						"  from %s ";

	appendPQExpBuffer(sql, sqlTemplate, table->partKey, table->partKey, table->qname);

	if (PQExpBufferBroken(sql))
	{
		(void) destroyPQExpBuffer(sql);
		log_error(
			"Failed to allocate memory for SQL query string to get partition key range");
		return false;
	}

	SourceTablePartKeyMinMaxValueContext partKeyMinMaxValueContext = { 0 };
	if (!pgsql_execute_with_params(pgsql, sql->data, 0, NULL, NULL,
								   &partKeyMinMaxValueContext, &parsePartKeyMinMaxValue))
	{
		(void) destroyPQExpBuffer(sql);
		log_error("Failed to execute SQL query to get partition key range");
		return false;
	}

	if (!partKeyMinMaxValueContext.parsedOk)
	{
		(void) destroyPQExpBuffer(sql);
		log_error("Failed to parse SQL query to get partition key range");
		return false;
	}

	(void) destroyPQExpBuffer(sql);

	table->partmax = (partKeyMinMaxValueContext.max);
	table->partmin = (partKeyMinMaxValueContext.min);

	return true;
}


/*
 * Parses the minimum and maximum values of a partition key from a PostgreSQL result.
 */
static void
parsePartKeyMinMaxValue(void *ctx, PGresult *result)
{
	SourceTablePartKeyMinMaxValueContext *context =
		(SourceTablePartKeyMinMaxValueContext *) ctx;

	int nTuples = PQntuples(result);
	int errors = 0;

	if (nTuples != 1)
	{
		log_error("Query returned %d tuples, expected 1", nTuples);
		context->parsedOk = false;
		return;
	}

	if (PQnfields(result) != 2)
	{
		log_error("Query returned %d columns, expected 2", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* min and max are both null on empty tables */
	if (PQgetisnull(result, 0, 0) &&
		PQgetisnull(result, 0, 1))
	{
		context->min = 0;
		context->max = 0;
	}
	else
	{
		/* 1. min */
		if (PQgetisnull(result, 0, 0))
		{
			log_error("Invalid min value: NULL");
			++errors;
		}
		else
		{
			char *value = PQgetvalue(result, 0, 0);
			if (!stringToInt64(value, &(context->min)))
			{
				log_error("Invalid min value: \"%s\"", value);
				++errors;
			}
		}

		/* 2. max */
		if (PQgetisnull(result, 0, 1))
		{
			log_error("Invalid max value: NULL");
			++errors;
		}
		else
		{
			char *value = PQgetvalue(result, 0, 1);
			if (!stringToInt64(value, &(context->max)))
			{
				log_error("Invalid max value: \"%s\"", value);
				++errors;
			}
		}
	}

	context->parsedOk = errors == 0;
}


/*
 * parseCurrentSourceTable parses a single row of the table listing query
 * result.
 */
static bool
parseCurrentSourceTable(PGresult *result, int rowNumber, SourceTable *table)
{
	int errors = 0;

	int fnoid = PQfnumber(result, "oid");
	int fnnspname = PQfnumber(result, "nspname");
	int fnrelname = PQfnumber(result, "relname");
	int fnamname = PQfnumber(result, "amname");
	int fnrelkind = PQfnumber(result, "relkind");
	int fnrelpages = PQfnumber(result, "relpages");
	int fnreltuples = PQfnumber(result, "reltuples");
	int fnbytesestimate = PQfnumber(result, "bytesestimate");
	int fnbytesestimatepp = PQfnumber(result, "pg_size_pretty");
	int fnexcldata = PQfnumber(result, "excludedata");
	int fnrestorelistname = PQfnumber(result, "format");
	int fnpartkey = PQfnumber(result, "partkey");

	/* c.oid */
	char *value = PQgetvalue(result, rowNumber, fnoid);

	if (!stringToUInt32(value, &(table->oid)) || table->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* n.nspname */
	value = PQgetvalue(result, rowNumber, fnnspname);
	int length = strlcpy(table->nspname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* c.relname */
	value = PQgetvalue(result, rowNumber, fnrelname);
	length = strlcpy(table->relname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* compute the qualified name from the nspname and relname */
	length = sformat(table->qname, sizeof(table->qname), "%s.%s",
					 table->nspname,
					 table->relname);

	if (length >= sizeof(table->qname))
	{
		log_error("Qualified table name \"%s\".\"%s\" is %d bytes long, "
				  "the maximum expected is %lld",
				  table->nspname,
				  table->relname,
				  length,
				  (long long) sizeof(table->qname) - 1);
		++errors;
	}

	/* pgam_amname */
	if (PQgetisnull(result, rowNumber, fnamname))
	{
		/* table started having an amname in Postgres 12 */
		strlcpy(table->amname, "heap", PG_NAMEDATALEN);
	}
	else
	{
		value = PQgetvalue(result, rowNumber, fnamname);
		length = strlcpy(table->amname, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Access Method name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}
	}

	/* c.relkind */
	value = PQgetvalue(result, rowNumber, fnrelkind);
	table->relkind = value[0];

	/* c.relpages */
	if (PQgetisnull(result, rowNumber, fnrelpages))
	{
		/*
		 * reltuples is NULL when table has never been ANALYZEd, just count
		 * zero then.
		 */
		table->relpages = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, fnrelpages);

		if (!stringToInt64(value, &(table->relpages)))
		{
			log_error("Invalid relpages \"%s\"", value);
			++errors;
		}
	}

	/* c.reltuples::bigint */
	if (PQgetisnull(result, rowNumber, fnreltuples))
	{
		/*
		 * reltuples is NULL when table has never been ANALYZEd, just count
		 * zero then.
		 */
		table->reltuples = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, fnreltuples);

		if (!stringToInt64(value, &(table->reltuples)))
		{
			log_error("Invalid reltuples::bigint \"%s\"", value);
			++errors;
		}
	}

	/* fnbytesestimate */
	if (PQgetisnull(result, rowNumber, fnbytesestimate))
	{
		/* the query didn't care to add the estimates, skip parsing them */
		table->bytesEstimate = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, fnbytesestimate);

		if (!stringToInt64(value, &(table->bytesEstimate)))
		{
			log_error("Invalid bytesestimate::bigint \"%s\"", value);
			++errors;
		}
	}


	/* fnbytesestimatepp */
	if (PQgetisnull(result, rowNumber, fnbytesestimatepp))
	{
		/* the query didn't care to add the estimates, skip parsing them */
		table->bytesEstimatePretty[0] = '\0';
	}
	else
	{
		value = PQgetvalue(result, rowNumber, fnbytesestimatepp);
		length = strlcpy(table->bytesEstimatePretty, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Pretty printed table size estimate \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}
	}


	/* excludeData */
	value = PQgetvalue(result, rowNumber, fnexcldata);
	table->excludeData = (*value) == 't';

	/* restoreListName */
	value = PQgetvalue(result, rowNumber, fnrestorelistname);
	length = strlcpy(table->restoreListName, value, RESTORE_LIST_NAMEDATALEN);

	if (length >= RESTORE_LIST_NAMEDATALEN)
	{
		log_error("Table restore list name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
				  value, length, RESTORE_LIST_NAMEDATALEN - 1);
		++errors;
	}

	/* partkey */
	if (PQgetisnull(result, rowNumber, fnpartkey))
	{
		log_debug("Table %s with oid %u has not part key column",
				  table->qname,
				  table->oid);
	}
	else
	{
		value = PQgetvalue(result, rowNumber, fnpartkey);
		length = strlcpy(table->partKey, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Partition key column name %s is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}
	}

	log_trace("parseCurrentSourceTable: %s.%s", table->nspname, table->relname);

	return errors == 0;
}


/*
 * SourceAttributeContext is used by getTableAttributeArray to stream
 * attribute rows directly into the SQLite catalog.
 */
typedef struct SourceAttributeContext
{
	DatabaseCatalog *catalog;
	bool parsedOk;
} SourceAttributeContext;

static void getTableAttributeArray(void *ctx, PGresult *result);


/*
 * schema_list_table_attributes issues the list_table_attributes query for all
 * tables already recorded in the catalog and inserts each row into s_attr.
 */
static bool
schema_list_table_attributes(PGSQL *pgsql, DatabaseCatalog *catalog)
{
	if (catalog == NULL || catalog->db == NULL)
	{
		return true;
	}

	char *oidArray = NULL;
	int count = 0;

	if (!catalog_s_table_oid_array(catalog, &oidArray, &count))
	{
		log_error("Failed to collect table OIDs from catalog");
		return false;
	}

	if (count == 0)
	{
		free(oidArray);
		return true;
	}

	if (pgsql->pgversion_num == 0)
	{
		(void) pgsql_server_version(pgsql);
	}

	const char *sql = NULL;

	if (!pgcopydb_sql_list_table_attributes(pgsql->pgversion_num, &sql))
	{
		free(oidArray);
		return false;
	}

	SourceAttributeContext context = { catalog, true };

	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { oidArray };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &getTableAttributeArray))
	{
		log_error("Failed to list table attributes");
		free(oidArray);
		return false;
	}

	free(oidArray);

	if (!context.parsedOk)
	{
		log_error("Failed to list table attributes");
		return false;
	}

	return true;
}


/*
 * send functions whose binary output is known to cause alignment faults on some
 * platforms (e.g. tsvector on ARM64).  Any column whose resolved type chain
 * includes one of these functions is not safe for COPY BINARY.
 */
static const char *binaryCopyBlocklist[] = {
	"tsvectorsend",
	NULL
};


/*
 * sendFuncBlocked returns true when any comma-separated name in funcList
 * matches an entry in binaryCopyBlocklist.
 */
static bool
sendFuncBlocked(const char *funcList)
{
	for (int i = 0; binaryCopyBlocklist[i] != NULL; i++)
	{
		const char *name = binaryCopyBlocklist[i];
		size_t nameLen = strlen(name);
		const char *p = funcList;

		while ((p = strstr(p, name)) != NULL)
		{
			bool startOk = (p == funcList) || (*(p - 1) == ',');
			bool endOk = (*(p + nameLen) == '\0') || (*(p + nameLen) == ',');

			if (startOk && endOk)
			{
				return true;
			}
			p += nameLen;
		}
	}
	return false;
}


/*
 * getTableAttributeArray is the PGresult callback for the list_table_attributes
 * query.  Each row is one (table, column) pair; we insert it directly into
 * s_attr via catalog_add_s_attr.
 */
static void
getTableAttributeArray(void *ctx, PGresult *result)
{
	SourceAttributeContext *context = (SourceAttributeContext *) ctx;

	int nTuples = PQntuples(result);

	if (PQnfields(result) != 10)
	{
		log_error("Query returned %d columns, expected 10", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	int fnreloid = PQfnumber(result, "attrelid");
	int fnattnum = PQfnumber(result, "attnum");
	int fnatttypid = PQfnumber(result, "atttypid");
	int fnattname = PQfnumber(result, "attname");
	int fnattisprimary = PQfnumber(result, "attisprimary");
	int fnattisreplident = PQfnumber(result, "attisreplident");
	int fnattisgenerated = PQfnumber(result, "attisgenerated");
	int fnattidentity = PQfnumber(result, "attidentity");
	int fnhasbinaryio = PQfnumber(result, "hasbinaryio");
	int fntypsendfunc = PQfnumber(result, "typsendfunc");

	for (int rowNumber = 0; rowNumber < nTuples && context->parsedOk; rowNumber++)
	{
		uint32_t tableoid = 0;
		SourceTableAttribute attr = { 0 };

		char *value = PQgetvalue(result, rowNumber, fnreloid);

		if (!stringToUInt32(value, &tableoid) || tableoid == 0)
		{
			log_error("Invalid attrelid OID \"%s\"", value);
			context->parsedOk = false;
			return;
		}

		value = PQgetvalue(result, rowNumber, fnattnum);
		if (!stringToInt(value, &attr.attnum))
		{
			log_error("Invalid attnum \"%s\"", value);
			context->parsedOk = false;
			return;
		}

		value = PQgetvalue(result, rowNumber, fnatttypid);
		if (!stringToUInt32(value, &attr.atttypid))
		{
			log_error("Invalid atttypid \"%s\"", value);
			context->parsedOk = false;
			return;
		}

		value = PQgetvalue(result, rowNumber, fnattname);
		strlcpy(attr.attname, value, sizeof(attr.attname));

		value = PQgetvalue(result, rowNumber, fnattisprimary);
		attr.attisprimary = (*value == 't');

		value = PQgetvalue(result, rowNumber, fnattisreplident);
		attr.attisreplident = (*value == 't');

		value = PQgetvalue(result, rowNumber, fnattisgenerated);
		attr.attisgenerated = (*value == 't');

		value = PQgetvalue(result, rowNumber, fnattidentity);
		if (value != NULL && value[0] != '\0')
		{
			attr.attidentity = value[0];
		}

		value = PQgetvalue(result, rowNumber, fnhasbinaryio);
		bool hasBinaryIO = (*value == 't');

		if (!PQgetisnull(result, rowNumber, fntypsendfunc))
		{
			value = PQgetvalue(result, rowNumber, fntypsendfunc);
			strlcpy(attr.atttypsend, value, sizeof(attr.atttypsend));
			attr.attisbinarycompatible = hasBinaryIO && !sendFuncBlocked(attr.atttypsend);
		}
		else
		{
			attr.attisbinarycompatible = hasBinaryIO;
		}

		if (!catalog_add_s_attr(context->catalog, tableoid, &attr))
		{
			context->parsedOk = false;
			return;
		}
	}
}


/*
 * getSequenceArray loops over the SQL result for the sequence array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getSequenceArray(void *ctx, PGresult *result)
{
	SourceSequenceArrayContext *context = (SourceSequenceArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 7)
	{
		log_error("Query returned %d columns, expected 7", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceSequence *seq =
			(SourceSequence *) calloc(1, sizeof(SourceSequence));

		if (!parseCurrentSourceSequence(result, rowNumber, seq))
		{
			parsedOk = false;
			break;
		}

		strlcpy(seq->datname, context->datname, sizeof(seq->datname));

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_seq(context->catalog, seq))
			{
				/* errors have already been logged */
				parsedOk = false;
				break;
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceSequence parses a single row of the sequence listing query
 * result.
 */
static bool
parseCurrentSourceSequence(PGresult *result, int rowNumber, SourceSequence *seq)
{
	int errors = 0;

	/* 1. c.oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(seq->oid)) || seq->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. n.nspname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(seq->nspname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 3. c.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(seq->relname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Sequence name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* compute the qualified name from the nspname and relname */
	length = sformat(seq->qname, sizeof(seq->qname), "%s.%s",
					 seq->nspname,
					 seq->relname);

	if (length >= sizeof(seq->qname))
	{
		log_error("Qualified seq name \"%s\".\"%s\" is %d bytes long, "
				  "the maximum expected is %lld",
				  seq->nspname,
				  seq->relname,
				  length,
				  (long long) sizeof(seq->qname) - 1);
		++errors;
	}

	/* 4. restoreListName */
	value = PQgetvalue(result, rowNumber, 3);
	length = strlcpy(seq->restoreListName, value, RESTORE_LIST_NAMEDATALEN);

	if (length >= RESTORE_LIST_NAMEDATALEN)
	{
		log_error("Table restore list name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
				  value, length, RESTORE_LIST_NAMEDATALEN - 1);
		++errors;
	}

	/* 5. ownedby */
	if (PQgetisnull(result, rowNumber, 4))
	{
		seq->ownedby = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 4);

		if (!stringToUInt32(value, &(seq->ownedby)) || seq->ownedby == 0)
		{
			log_error("Invalid pg_class OID for ownedby: \"%s\"", value);
			++errors;
		}
	}

	/* 6. attrelid */
	if (PQgetisnull(result, rowNumber, 5))
	{
		seq->attrelid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 5);

		if (!stringToUInt32(value, &(seq->attrelid)) || seq->attrelid == 0)
		{
			log_error("Invalid pg_class OID for attrelid: \"%s\"", value);
			++errors;
		}
	}

	/* 6. attroid */
	if (PQgetisnull(result, rowNumber, 6))
	{
		seq->attroid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 6);

		if (!stringToUInt32(value, &(seq->attroid)) || seq->attroid == 0)
		{
			log_error("Invalid pg_attribute OID \"%s\"", value);
			++errors;
		}
	}

	return errors == 0;
}


/*
 * getIndexArray loops over the SQL result for the index array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getIndexArray(void *ctx, PGresult *result)
{
	SourceIndexArrayContext *context = (SourceIndexArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 16)
	{
		log_error("Query returned %d columns, expected 16", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceIndex *index = (SourceIndex *) calloc(1, sizeof(SourceIndex));

		if (!parseCurrentSourceIndex(result, rowNumber, index))
		{
			parsedOk = false;
			break;
		}

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_index(context->catalog, index))
			{
				/* errors have already been logged */
				parsedOk = false;
				break;
			}

			/* not all indexes are supporting a constraint, of course */
			if (index->constraintOid > 0)
			{
				if (!catalog_add_s_constraint(context->catalog, index))
				{
					/* errors have already been logged */
					parsedOk = false;
					break;
				}
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceIndex parses a single row of the index listing query
 * result.
 */
static bool
parseCurrentSourceIndex(PGresult *result, int rowNumber, SourceIndex *index)
{
	int errors = 0;

	/* 1. i.oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(index->indexOid)) || index->indexOid == 0)
	{
		log_error("Invalid index OID \"%s\"", value);
		++errors;
	}

	/* 2. n.nspname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(index->indexNamespace, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 3. i.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(index->indexRelname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* compute the qualified name from the nspname and relname */
	length = sformat(index->indexQname, sizeof(index->indexQname), "%s.%s",
					 index->indexNamespace,
					 index->indexRelname);

	if (length >= sizeof(index->tableQname))
	{
		log_error("Qualified index name \"%s\".\"%s\" is %d bytes long, "
				  "the maximum expected is %lld",
				  index->indexNamespace,
				  index->indexRelname,
				  length,
				  (long long) sizeof(index->tableQname) - 1);
		++errors;
	}

	/* 4. r.oid */
	value = PQgetvalue(result, rowNumber, 3);

	if (!stringToUInt32(value, &(index->tableOid)) || index->tableOid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 5. rn.nspname */
	value = PQgetvalue(result, rowNumber, 4);
	length = strlcpy(index->tableNamespace, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 6. r.relname */
	value = PQgetvalue(result, rowNumber, 5);
	length = strlcpy(index->tableRelname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* compute the qualified name from the nspname and relname */
	length = sformat(index->tableQname, sizeof(index->tableQname), "%s.%s",
					 index->tableNamespace,
					 index->tableRelname);

	if (length >= sizeof(index->tableQname))
	{
		log_error("Qualified table name \"%s\".\"%s\" is %d bytes long, "
				  "the maximum expected is %lld",
				  index->tableNamespace,
				  index->tableRelname,
				  length,
				  (long long) sizeof(index->tableQname) - 1);
		++errors;
	}

	/* 7. indisprimary */
	value = PQgetvalue(result, rowNumber, 6);
	if (value == NULL || ((*value != 't') && (*value != 'f')))
	{
		log_error("Invalid indisprimary value \"%s\"", value);
		++errors;
	}
	else
	{
		index->isPrimary = (*value) == 't';
	}

	/* 8. indisunique */
	value = PQgetvalue(result, rowNumber, 7);
	if (value == NULL || ((*value != 't') && (*value != 'f')))
	{
		log_error("Invalid indisunique value \"%s\"", value);
		++errors;
	}
	else
	{
		index->isUnique = (*value) == 't';
	}

	/* 9. cols */
	value = PQgetvalue(result, rowNumber, 8);
	length = strlen(value) + 1;
	index->indexColumns = (char *) calloc(length, sizeof(char));

	if (index->indexColumns == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	strlcpy(index->indexColumns, value, length);

	/* 10. pg_get_indexdef() */
	value = PQgetvalue(result, rowNumber, 9);
	length = strlen(value) + 1;
	index->indexDef = (char *) calloc(length, sizeof(char));

	if (index->indexDef == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	strlcpy(index->indexDef, value, length);

	/* 11. c.oid */
	if (PQgetisnull(result, rowNumber, 10))
	{
		index->constraintOid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 10);

		if (!stringToUInt32(value, &(index->constraintOid)) ||
			index->constraintOid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 12. c.condeferrable */
	if (!PQgetisnull(result, rowNumber, 11))
	{
		value = PQgetvalue(result, rowNumber, 11);
		if (value == NULL || ((*value != 't') && (*value != 'f')))
		{
			log_error("Invalid condeferrable value \"%s\"", value);
			++errors;
		}
		else
		{
			index->condeferrable = (*value) == 't';
		}
	}

	/* 13. c.condeferred */
	if (!PQgetisnull(result, rowNumber, 12))
	{
		value = PQgetvalue(result, rowNumber, 12);
		if (value == NULL || ((*value != 't') && (*value != 'f')))
		{
			log_error("Invalid condeferred value \"%s\"", value);
			++errors;
		}
		else
		{
			index->condeferred = (*value) == 't';
		}
	}

	/* 14. conname */
	if (!PQgetisnull(result, rowNumber, 13))
	{
		value = PQgetvalue(result, rowNumber, 13);
		length = strlcpy(index->constraintName, value, PG_NAMEDATALEN);

		if (length >= PG_NAMEDATALEN)
		{
			log_error("Index name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
					  value, length, PG_NAMEDATALEN - 1);
			++errors;
		}
	}

	/* 15. pg_get_constraintdef */
	if (!PQgetisnull(result, rowNumber, 14))
	{
		value = PQgetvalue(result, rowNumber, 14);
		length = strlen(value) + 1;
		index->constraintDef = (char *) calloc(length, sizeof(char));

		if (index->constraintDef == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(index->constraintDef, value, length);
	}

	/* 16. indexRestoreListName */
	value = PQgetvalue(result, rowNumber, 15);
	length =
		strlcpy(index->indexRestoreListName, value, RESTORE_LIST_NAMEDATALEN);

	if (length >= RESTORE_LIST_NAMEDATALEN)
	{
		log_error("Index restore list name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (RESTORE_LIST_NAMEDATALEN - 1)",
				  value, length, RESTORE_LIST_NAMEDATALEN - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getDependArray loops over the SQL result for the table dependencies array
 * query and allocates an array of tables then populates it with the query
 * result.
 */
static void
getDependArray(void *ctx, PGresult *result)
{
	SourceDependArrayContext *context = (SourceDependArrayContext *) ctx;
	int nTuples = PQntuples(result);

	if (PQnfields(result) != 9)
	{
		log_error("Query returned %d columns, expected 9", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceDepend *depend = (SourceDepend *) calloc(1, sizeof(SourceDepend));

		if (!parseCurrentSourceDepend(result, rowNumber, depend))
		{
			parsedOk = false;
			break;
		}

		if (context->catalog != NULL && context->catalog->db != NULL)
		{
			if (!catalog_add_s_depend(context->catalog, depend))
			{
				/* errors have already been logged */
				parsedOk = false;
				break;
			}
		}
	}

	context->parsedOk = parsedOk;
}


/*
 * parseCurrentSourceDepend parses a single row of the dependency listing query
 * result.
 */
static bool
parseCurrentSourceDepend(PGresult *result, int rowNumber, SourceDepend *depend)
{
	int errors = 0;

	/* 1. n.nspname */
	char *value = PQgetvalue(result, rowNumber, 0);
	int length = strlcpy(depend->nspname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 2. c.relname */
	value = PQgetvalue(result, rowNumber, 1);
	length = strlcpy(depend->relname, value, PG_NAMEDATALEN);

	if (length >= PG_NAMEDATALEN)
	{
		log_error("Table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (PG_NAMEDATALEN - 1)",
				  value, length, PG_NAMEDATALEN - 1);
		++errors;
	}

	/* 3. refclassid */
	if (PQgetisnull(result, rowNumber, 2))
	{
		depend->refclassid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 2);

		if (!stringToUInt32(value, &(depend->refclassid)) || depend->refclassid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 4. refobjid */
	if (PQgetisnull(result, rowNumber, 3))
	{
		depend->refobjid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 3);

		if (!stringToUInt32(value, &(depend->refobjid)) || depend->refobjid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 5. classid */
	if (PQgetisnull(result, rowNumber, 4))
	{
		depend->classid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 4);

		if (!stringToUInt32(value, &(depend->classid)) || depend->classid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 6. objid */
	if (PQgetisnull(result, rowNumber, 5))
	{
		depend->objid = 0;
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 5);

		if (!stringToUInt32(value, &(depend->objid)) || depend->objid == 0)
		{
			log_error("Invalid OID \"%s\"", value);
			++errors;
		}
	}

	/* 7. deptype */
	if (PQgetisnull(result, rowNumber, 6))
	{
		depend->deptype = 's';  /* invent something for schemas */
	}
	else
	{
		value = PQgetvalue(result, rowNumber, 6);
		depend->deptype = value[0];
	}

	/* 8. type */
	value = PQgetvalue(result, rowNumber, 7);
	length = strlcpy(depend->type, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Table dependency type \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	/* 9. identity */
	value = PQgetvalue(result, rowNumber, 8);
	length = strlcpy(depend->identity, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Table dependency identity \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	return errors == 0;
}


/*
 * getTableChecksum assigns the rowcount and checksum fields of a table from
 * the result of an SQL query.
 */
static void
getTableChecksum(void *ctx, PGresult *result)
{
	ChecksumContext *context = (ChecksumContext *) ctx;

	int nTuples = PQntuples(result);
	int errors = 0;

	if (nTuples != 1)
	{
		log_error("Query returned %d columns, expected 1", nTuples);
		context->parsedOk = false;
		return;
	}

	if (PQnfields(result) != 2)
	{
		log_error("Query returned %d columns, expected 2", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	TableChecksum *sum = context->sum;

	/* 1. count */
	char *value = PQgetvalue(result, 0, 0);

	if (!stringToUInt64(value, &(sum->rowcount)))
	{
		log_error("Invalid row count value: \"%s\"", value);
		++errors;
	}

	value = PQgetvalue(result, 0, 1);
	strlcpy(sum->checksum, value, CHECKSUMLEN);

	context->parsedOk = errors == 0;
}
