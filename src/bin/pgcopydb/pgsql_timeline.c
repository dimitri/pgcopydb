/*
 * src/bin/pgcopydb/pgsql_timeline.c
 *	 API for sending SQL commands about timelines to a PostgreSQL server
 */

#include "catalog.h"
#include "file_utils.h"
#include "log.h"
#include "pg_utils.h"
#include "pgsql_timeline.h"
#include "pgsql_utils.h"
#include "pgsql.h"


typedef struct IdentifySystemResult
{
	char sqlstate[6];
	bool parsedOk;
	IdentifySystem *system;
} IdentifySystemResult;


typedef struct TimelineHistoryResult
{
	char sqlstate[6];
	bool parsedOk;
	char filename[MAXPGPATH];
} TimelineHistoryResult;

static void parseIdentifySystemResult(IdentifySystemResult *ctx, PGresult *result);
static void parseTimelineHistoryResult(TimelineHistoryResult *context, PGresult *result,
									   char *cdcPathDir);
static bool writeTimelineHistoryFile(char *filename, char *content);
static bool register_timeline_hook(void *ctx, char *line);


/*
 * pgsql_identify_system connects to the given pgsql client and issue the
 * replication command IDENTIFY_SYSTEM. The pgsql connection string should
 * contain the 'replication=1' parameter.
 */
bool
pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system, char *cdcPathDir)
{
	bool connIsOurs = pgsql->connection == NULL;

	PGconn *connection = pgsql_open_connection(pgsql);
	if (connection == NULL)
	{
		/* error message was logged in pgsql_open_connection */
		return false;
	}

	/* extended query protocol not supported in a replication connection */
	PGresult *result = PQexec(connection, "IDENTIFY_SYSTEM");

	if (!is_response_ok(result))
	{
		log_error("Failed to IDENTIFY_SYSTEM: %s", PQerrorMessage(connection));
		PQclear(result);
		clear_results(pgsql);

		PQfinish(connection);

		return false;
	}

	IdentifySystemResult isContext = { { 0 }, false, system };

	(void) parseIdentifySystemResult((void *) &isContext, result);

	PQclear(result);
	clear_results(pgsql);

	log_sql("IDENTIFY_SYSTEM: timeline %d, xlogpos %s, systemid %" PRIu64,
			system->timeline,
			system->xlogpos,
			system->identifier);

	if (!isContext.parsedOk)
	{
		log_error("Failed to get result from IDENTIFY_SYSTEM");
		PQfinish(connection);
		return false;
	}

	/* while at it, we also run the TIMELINE_HISTORY command */
	if (system->timeline > 1)
	{
		TimelineHistoryResult hContext = { 0 };

		char sql[BUFSIZE] = { 0 };
		sformat(sql, sizeof(sql), "TIMELINE_HISTORY %d", system->timeline);

		result = PQexec(connection, sql);

		if (!is_response_ok(result))
		{
			log_error("Failed to request TIMELINE_HISTORY: %s",
					  PQerrorMessage(connection));
			PQclear(result);
			clear_results(pgsql);

			PQfinish(connection);

			return false;
		}

		(void) parseTimelineHistoryResult((void *) &hContext, result, cdcPathDir);

		PQclear(result);
		clear_results(pgsql);

		if (!hContext.parsedOk)
		{
			log_error("Failed to get result from TIMELINE_HISTORY");
			PQfinish(connection);
			return false;
		}

		/* store the filename for the timeline history file */
		strlcpy(system->timelineHistoryFilename, hContext.filename, MAXPGPATH);
	}

	if (connIsOurs)
	{
		(void) pgsql_finish(pgsql);
	}

	return true;
}


/*
 * writeTimelineHistoryFile writes the content of a timeline history file to disk.
 * The filename is determined by the PostgreSQL TIMELINE_HISTORY command.
 */
static bool
writeTimelineHistoryFile(char *filename, char *content)
{
	log_debug("Writing timeline history file \"%s\"", filename);

	size_t size = strlen(content);
	return write_file(content, size, filename);
}


/*
 * parseIdentifySystemResult parses the result from a replication query
 * IDENTIFY_SYSTEM, and fills the given IdentifySystem structure.
 */
static void
parseIdentifySystemResult(IdentifySystemResult *context, PGresult *result)
{
	if (PQnfields(result) != 4)
	{
		log_error("Query returned %d columns, expected 4", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	if (PQntuples(result) == 0)
	{
		log_sql("parseIdentifySystem: query returned no rows");
		context->parsedOk = false;
		return;
	}
	if (PQntuples(result) != 1)
	{
		log_error("Query returned %d rows, expected 1", PQntuples(result));
		context->parsedOk = false;
		return;
	}

	/* systemid (text) */
	char *value = PQgetvalue(result, 0, 0);
	if (!stringToUInt64(value, &(context->system->identifier)))
	{
		log_error("Failed to parse system_identifier \"%s\"", value);
		context->parsedOk = false;
		return;
	}

	/* timeline (int4) */
	value = PQgetvalue(result, 0, 1);
	if (!stringToUInt32(value, &(context->system->timeline)))
	{
		log_error("Failed to parse timeline \"%s\"", value);
		context->parsedOk = false;
		return;
	}

	/* xlogpos (text) */
	value = PQgetvalue(result, 0, 2);
	strlcpy(context->system->xlogpos, value, PG_LSN_MAXLENGTH);

	/* dbname (text) Database connected to or null */
	if (!PQgetisnull(result, 0, 3))
	{
		value = PQgetvalue(result, 0, 3);
		strlcpy(context->system->dbname, value, NAMEDATALEN);
	}

	context->parsedOk = true;
}


/*
 * parseTimelineHistoryResult parses the result of the TIMELINE_HISTORY
 * replication command. The content is written to disk, and the filename
 * is stored in the TimelineHistoryResult structure.
 */
static void
parseTimelineHistoryResult(TimelineHistoryResult *context, PGresult *result,
						   char *cdcPathDir)
{
	if (PQnfields(result) != 2)
	{
		log_error("Query returned %d columns, expected 2", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	if (PQntuples(result) == 0)
	{
		log_sql("parseTimelineHistoryResult: query returned no rows");
		context->parsedOk = false;
		return;
	}

	if (PQntuples(result) != 1)
	{
		log_error("Query returned %d rows, expected 1", PQntuples(result));
		context->parsedOk = false;
		return;
	}

	/* filename (text) */
	char *value = PQgetvalue(result, 0, 0);
	sformat(context->filename, sizeof(context->filename), "%s/%s", cdcPathDir, value);

	/*
	 * content (bytea)
	 *
	 * We do not want to store this value in memory. Instead we write it to disk
	 * as it is.
	 */
	value = PQgetvalue(result, 0, 1);
	if (!writeTimelineHistoryFile(context->filename, value))
	{
		log_error("Failed to write timeline history file \"%s\"", context->filename);
		context->parsedOk = false;
		return;
	}

	context->parsedOk = true;
}


typedef struct TimeLineHistoryContext
{
	DatabaseCatalog *catalog;
	uint32_t prevtli;
	uint32_t prevend;
} TimelineHistoryContext;


/*
 * parse_timeline_history_file is a wrapper for the iterator that prepares the
 * context etc.
 */
bool
parse_timeline_history_file(char *filename,
							DatabaseCatalog *catalog,
							uint32_t currentTimeline)
{
	/* step 1: prepare the context */
	TimelineHistoryContext context =
	{
		.catalog = catalog,
		.prevtli = 0,
		.prevend = InvalidXLogRecPtr
	};

	/* step 2: iterate over the file */
	if (!file_iter_lines(filename, BUFSIZE, &context, register_timeline_hook))
	{
		/* errors have already been logged */
		return false;
	}

	/* step 3: add the current timeline to catalog */
	if (currentTimeline != context.prevtli + 1)
	{
		log_warn("parse_timeline_history_file: Expected timeline %d, got %d",
				 context.prevtli + 1, currentTimeline);
	}

	TimelineHistoryEntry entry = {
		.tli = currentTimeline,
		.begin = context.prevend,
		.end = InvalidXLogRecPtr
	};

	if (!catalog_add_timeline_history(catalog, &entry))
	{
		log_error("Failed to add timeline history entry to catalog");
		return false;
	}

	return true;
}


/* register_timeline_hook is the callback that is called for each line of a */
/* timeline history file. */
static bool
register_timeline_hook(void *ctx, char *line)
{
	TimelineHistoryContext *context = (TimelineHistoryContext *) ctx;

	char *ptr = line;

	/* skip leading whitespace */
	for (; *ptr; ptr++)
	{
		if (!isspace((unsigned char) *ptr))
		{
			break;
		}
	}

	if (*ptr == '\0')
	{
		/* skip empty lines */
		return true;
	}

	log_trace("register_timeline_hook: line is \"%s\"", line);

	char *tabptr = strchr(ptr, '\t');

	if (tabptr == NULL)
	{
		log_error("Failed to parse history file line \"%s\"", line);
		return false;
	}

	*tabptr = '\0';

	uint32_t current_tli = 0;
	uint64_t current_lsn = InvalidXLogRecPtr;

	if (!stringToUInt(ptr, &current_tli))
	{
		log_error("Failed to parse history timeline \"%s\"", line);
		return false;
	}

	char *lsn = tabptr + 1;

	for (char *lsnend = lsn; *lsnend; lsnend++)
	{
		if (!(isxdigit((unsigned char) *lsnend) || *lsnend == '/'))
		{
			*lsnend = '\0';
			break;
		}
	}

	if (!parseLSN(lsn, &current_lsn))
	{
		log_error("Failed to parse history timeline %d LSN \"%s\"",
				  current_tli, lsn);
		return false;
	}

	TimelineHistoryEntry entry = {
		.tli = current_tli,
		.begin = context->prevend,
		.end = current_lsn
	};

	if (!catalog_add_timeline_history(context->catalog, &entry))
	{
		log_error("Failed to add timeline history entry to catalog");
		return false;
	}

	context->prevtli = current_tli;
	context->prevend = current_lsn;

	return true;
}
