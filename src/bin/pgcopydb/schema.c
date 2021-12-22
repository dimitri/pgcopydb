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


typedef struct SourceTableArrayContext
{
	char sqlstate[SQLSTATE_LENGTH];
	SourceTableArray *tableArray;
	bool parsedOk;
} SourceTableArrayContext;

static void getTableArray(void *ctx, PGresult *result);

static bool parseCurrentSourceTable(PGresult *result,
									int rowNumber,
									SourceTable *table);


/*
 * schema_list_ordinary_tables grabs the list of tables from the Postgres
 * instance reachable from the given pguri and allocates a SourceTable array
 * with the result of the query.
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
		"     and n.nspname not in ('pg_catalog', 'information_schema') "
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
		log_error("Invalid port number \"%s\" returned by monitor", value);
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
