/*
 * src/bin/pgcopydb/schema.c
 *	 SQL queries to discover the source database schema
 */

#include <inttypes.h>
#include <limits.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#include "defaults.h"
#include "env_utils.h"
#include "log.h"
#include "parsing.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"


/* Context used when fetching all the table definitions */
typedef struct SourceTableArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceTableArray *tableArray;
	bool parsedOk;
} SourceTableArrayContext;

/* Context used when fetching all the indexes definitions */
typedef struct SourceIndexArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceIndexArray *indexArray;
	bool parsedOk;
} SourceIndexArrayContext;

static void getTableArray(void *ctx, PGresult *result);

static bool parseCurrentSourceTable(PGresult *result,
									int rowNumber,
									SourceTable *table);

static void getIndexArray(void *ctx, PGresult *result);

static bool parseCurrentSourceIndex(PGresult *result,
									int rowNumber,
									SourceIndex *index);


/*
 * schema_list_ordinary_tables grabs the list of tables from the given source
 * Postgres instance and allocates a SourceTable array with the result of the
 * query.
 */
bool
schema_list_ordinary_tables(PGSQL *pgsql, SourceTableArray *tableArray)
{
	SourceTableArrayContext context = { { 0 }, tableArray, false };

	char *sql =
		"  select c.oid, n.nspname, c.relname, c.reltuples::bigint "
		"    from pg_catalog.pg_class c join pg_catalog.pg_namespace n "
		"      on c.relnamespace = n.oid "
		"   where c.relkind = 'r' and c.relpersistence = 'p' "
		"     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"order by c.reltuples::bigint desc, n.nspname, c.relname";

	log_trace("schema_list_ordinary_tables");

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getTableArray))
	{
		log_error("Failed to retrieve current state from the monitor");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to parse current state from the monitor");
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
schema_list_all_indexes(PGSQL *pgsql, SourceIndexArray *indexArray)
{
	SourceIndexArrayContext context = { { 0 }, indexArray, false };

	char *sql =
		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid)"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"
		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		" order by n.nspname, r.relname";

	log_trace("schema_list_all_indexes");

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getIndexArray))
	{
		log_error("Failed to retrieve current state from the monitor");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to parse current state from the monitor");
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
schema_list_table_indexes(PGSQL *pgsql,
						  const char *schemaName,
						  const char *tableName,
						  SourceIndexArray *indexArray)
{
	SourceIndexArrayContext context = { { 0 }, indexArray, false };

	char *sql =
		"   select i.oid, n.nspname, i.relname,"
		"          r.oid, rn.nspname, r.relname,"
		"          indisprimary,"
		"          indisunique,"
		"          (select string_agg(attname, ',')"
		"             from pg_attribute"
		"            where attrelid = r.oid"
		"              and array[attnum::integer] <@ indkey::integer[]"
		"          ) as cols,"
		"          pg_get_indexdef(indexrelid),"
		"          c.oid,"
		"          c.conname,"
		"          pg_get_constraintdef(c.oid)"
		"     from pg_index x"
		"          join pg_class i ON i.oid = x.indexrelid"
		"          join pg_class r ON r.oid = x.indrelid"
		"          join pg_namespace n ON n.oid = i.relnamespace"
		"          join pg_namespace rn ON rn.oid = r.relnamespace"
		"          left join pg_depend d "
		"                 on d.classid = 'pg_class'::regclass"
		"                and d.objid = i.oid"
		"                and d.refclassid = 'pg_constraint'::regclass"
		"                and d.deptype = 'i'"
		"          left join pg_constraint c ON c.oid = d.refobjid"
		"    where r.relkind = 'r' and r.relpersistence = 'p' "
		"      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'"
		"      and rn.nspname = $1 and r.relname = $2"
		" order by n.nspname, r.relname";

	int paramCount = 2;
	Oid paramTypes[2] = { TEXTOID, TEXTOID };
	const char *paramValues[2] = { schemaName, tableName };

	log_trace("schema_list_table_indexes");

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &getIndexArray))
	{
		log_error("Failed to retrieve current state from the monitor");
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to parse current state from the monitor");
		return false;
	}

	return true;
}


/*
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getTableArray(void *ctx, PGresult *result)
{
	SourceTableArrayContext *context = (SourceTableArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_trace("getTableArray: %d", nTuples);

	if (PQnfields(result) != 4)
	{
		log_error("Query returned %d columns, expected 4", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->tableArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getTableArray");

		free(context->tableArray->array);
		context->tableArray->array = NULL;
	}

	context->tableArray->count = nTuples;
	context->tableArray->array =
		(SourceTable *) malloc(nTuples * sizeof(SourceTable));

	if (context->tableArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceTable *table = &(context->tableArray->array[rowNumber]);

		parsedOk = parsedOk &&
			parseCurrentSourceTable(result, rowNumber, table);
	}

	if (!parsedOk)
	{
		free(context->tableArray->array);
		context->tableArray->array = NULL;
	}

	context->parsedOk = parsedOk;

	return;
}


/*
 * parseCurrentSourceTable parses a single row of the table listing query
 * result.
 */
static bool
parseCurrentSourceTable(PGresult *result, int rowNumber, SourceTable *table)
{
	int errors = 0;

	/* 1. c.oid */
	char *value = PQgetvalue(result, rowNumber, 0);

	if (!stringToUInt32(value, &(table->oid)) || table->oid == 0)
	{
		log_error("Invalid OID \"%s\"", value);
		++errors;
	}

	/* 2. n.nspname */
	value = PQgetvalue(result, rowNumber, 1);
	int length = strlcpy(table->nspname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. c.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(table->relname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Table name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 4. c.reltuples::bigint */
	value = PQgetvalue(result, rowNumber, 3);

	if (!stringToInt64(value, &(table->reltuples)))
	{
		log_error("Invalid reltuples::bigint \"%s\"", value);
		++errors;
	}

	return errors == 0;
}


/*
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
 */
static void
getIndexArray(void *ctx, PGresult *result)
{
	SourceIndexArrayContext *context = (SourceIndexArrayContext *) ctx;
	int nTuples = PQntuples(result);

	log_trace("getIndexArray: %d", nTuples);

	if (PQnfields(result) != 13)
	{
		log_error("Query returned %d columns, expected 13", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* we're not supposed to re-cycle arrays here */
	if (context->indexArray->array != NULL)
	{
		/* issue a warning but let's try anyway */
		log_warn("BUG? context's array is not null in getIndexArray");

		free(context->indexArray->array);
		context->indexArray->array = NULL;
	}

	context->indexArray->count = nTuples;
	context->indexArray->array =
		(SourceIndex *) malloc(nTuples * sizeof(SourceIndex));

	if (context->indexArray->array == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return;
	}

	bool parsedOk = true;

	for (int rowNumber = 0; rowNumber < nTuples; rowNumber++)
	{
		SourceIndex *index = &(context->indexArray->array[rowNumber]);

		parsedOk = parsedOk &&
			parseCurrentSourceIndex(result, rowNumber, index);
	}

	if (!parsedOk)
	{
		free(context->indexArray->array);
		context->indexArray->array = NULL;
	}

	context->parsedOk = parsedOk;

	return;
}


/*
 * getTableArray loops over the SQL result for the tables array query and
 * allocates an array of tables then populates it with the query result.
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
	int length = strlcpy(index->indexNamespace, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 3. i.relname */
	value = PQgetvalue(result, rowNumber, 2);
	length = strlcpy(index->indexRelname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
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
	length = strlcpy(index->tableNamespace, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Schema name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
		++errors;
	}

	/* 6. r.relname */
	value = PQgetvalue(result, rowNumber, 5);
	length = strlcpy(index->tableRelname, value, NAMEDATALEN);

	if (length >= NAMEDATALEN)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (NAMEDATALEN - 1)",
				  value, length, NAMEDATALEN - 1);
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
	length = strlcpy(index->indexColumns, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

	/* 10. pg_get_indexdef() */
	value = PQgetvalue(result, rowNumber, 9);
	length = strlcpy(index->indexDef, value, BUFSIZE);

	if (length >= BUFSIZE)
	{
		log_error("Index name \"%s\" is %d bytes long, "
				  "the maximum expected is %d (BUFSIZE - 1)",
				  value, length, BUFSIZE - 1);
		++errors;
	}

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

	/* 12. conname */
	if (!PQgetisnull(result, rowNumber, 11))
	{
		value = PQgetvalue(result, rowNumber, 11);
		length = strlcpy(index->constraintName, value, NAMEDATALEN);

		if (length >= NAMEDATALEN)
		{
			log_error("Index name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (NAMEDATALEN - 1)",
					  value, length, NAMEDATALEN - 1);
			++errors;
		}
	}

	/* 13. pg_get_constraintdef */
	if (!PQgetisnull(result, rowNumber, 12))
	{
		value = PQgetvalue(result, rowNumber, 12);
		length = strlcpy(index->constraintDef, value, BUFSIZE);

		if (length >= BUFSIZE)
		{
			log_error("Index name \"%s\" is %d bytes long, "
					  "the maximum expected is %d (BUFSIZE - 1)",
					  value, length, BUFSIZE - 1);
			++errors;
		}
	}

	return errors == 0;
}
