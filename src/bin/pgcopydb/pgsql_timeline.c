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
	char *content;
} TimelineHistoryResult;

static void parseIdentifySystemResult(IdentifySystemResult *ctx, PGresult *result);
static void parseTimelineHistoryResult(TimelineHistoryResult *context, PGresult *result);
static bool writeTimelineHistoryFile(char *filename, char *content, char *cdcPathDir);


/*
 * pgsql_identify_system connects to the given pgsql client and issue the
 * replication command IDENTIFY_SYSTEM. The pgsql connection string should
 * contain the 'replication=1' parameter.
 */
bool
pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system, DatabaseCatalog *catalog,
					  char *cdcPathDir)
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

		(void) parseTimelineHistoryResult((void *) &hContext, result);

		PQclear(result);
		clear_results(pgsql);

		if (!hContext.parsedOk)
		{
			log_error("Failed to get result from TIMELINE_HISTORY");
			PQfinish(connection);
			return false;
		}

		if (!writeTimelineHistoryFile(hContext.filename, hContext.content, cdcPathDir))
		{
			log_error("Failed to write contents of TIMELINE_HISTORY command to disk");
			PQfinish(connection);
			return false;
		}

		if (!parseTimelineHistory(hContext.content, system, catalog))
		{
			/* errors have already been logged */
			PQfinish(connection);
			return false;
		}

		TimelineHistoryEntry *current = &(system->currentTimeline);

		log_sql("TIMELINE_HISTORY: \"%s\", timeline %d started at %X/%X",
				hContext.filename,
				current->tli,
				LSN_FORMAT_ARGS(current->begin));
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
writeTimelineHistoryFile(char *filename, char *content, char *cdcPathDir)
{
	char path[MAXPGPATH] = { 0 };
	sformat(path, MAXPGPATH, "%s/%s", cdcPathDir, filename);

	log_debug("Writing timeline history file \"%s\"", path);

	size_t size = strlen(content);
	return write_file(content, size, path);
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
 * replication command.
 */
static void
parseTimelineHistoryResult(TimelineHistoryResult *context, PGresult *result)
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
	strlcpy(context->filename, value, sizeof(context->filename));

	/* content (bytea) */
	value = PQgetvalue(result, 0, 1);
	context->content = strdup(value);

	if (context->content == NULL)
	{
		log_error("parseTimelineHistoryResult: Failed to allocate memory for "
				  "timeline history file of %zu bytes",
				  strlen(value));
		context->parsedOk = false;
		return;
	}

	context->parsedOk = true;
}


/*
 * parseTimelineHistory parses the content of a timeline history file.
 */
bool
parseTimelineHistory(const char *content, IdentifySystem *system,
					 DatabaseCatalog *catalog)
{
	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, (char *) content))
	{
		/* errors have already been logged */
		return false;
	}

	uint64_t prevend = InvalidXLogRecPtr;

	TimelineHistoryEntry *entry = &system->currentTimeline;
	int timelineCount = 0;

	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		char *ptr = lbuf.lines[lineNumber];

		/* skip leading whitespace and check for # comment */
		for (; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
			{
				break;
			}
		}

		if (*ptr == '\0' || *ptr == '#')
		{
			continue;
		}

		log_trace("parseTimelineHistory line %lld is \"%s\"",
				  (long long) lineNumber,
				  lbuf.lines[lineNumber]);

		char *tabptr = strchr(lbuf.lines[lineNumber], '\t');

		if (tabptr == NULL)
		{
			log_error("Failed to parse history file line %lld: \"%s\"",
					  (long long) lineNumber, ptr);
			return false;
		}

		*tabptr = '\0';

		if (!stringToUInt(lbuf.lines[lineNumber], &entry->tli))
		{
			log_error("Failed to parse history timeline \"%s\"", tabptr);
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

		if (!parseLSN(lsn, &entry->end))
		{
			log_error("Failed to parse history timeline %d LSN \"%s\"",
					  entry->tli, lsn);
			return false;
		}

		entry->begin = prevend;
		prevend = entry->end;

		log_trace("parseTimelineHistory[%d]: tli %d [%X/%X %X/%X]",
				  timelineCount,
				  entry->tli,
				  LSN_FORMAT_ARGS(entry->begin),
				  LSN_FORMAT_ARGS(entry->end));

		if (!catalog_add_timeline_history(catalog, entry))
		{
			log_error("Failed to add timeline history entry, see above for details");
			return false;
		}
		timelineCount++;
	}

	/*
	 * Create one more entry for the "tip" of the timeline, which has no entry
	 * in the history file.
	 */
	entry->tli = system->timeline;
	entry->begin = prevend;
	entry->end = InvalidXLogRecPtr;

	log_trace("parseTimelineHistory[%d]: tli %d [%X/%X %X/%X]",
			  timelineCount,
			  entry->tli,
			  LSN_FORMAT_ARGS(entry->begin),
			  LSN_FORMAT_ARGS(entry->end));

	if (!catalog_add_timeline_history(catalog, entry))
	{
		log_error("Failed to add timeline history entry, see above for details");
		return false;
	}

	return true;
}
