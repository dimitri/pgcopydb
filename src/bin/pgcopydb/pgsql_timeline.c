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
static bool writeTimelineHistoryFile(char *filename, char *content, char *cdcPathDir);


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

		sformat(system->timelineHistoryFilename, sizeof(system->timelineHistoryFilename),
				"%s", hContext.filename);

		PQclear(result);
		clear_results(pgsql);

		if (!hContext.parsedOk)
		{
			log_error("Failed to get result from TIMELINE_HISTORY");
			PQfinish(connection);
			return false;
		}
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
	strlcpy(context->filename, value, sizeof(context->filename));

	/*
	 * content (bytea)
	 *
	 * We do not want to store this value in memory. Instead we write it to disk
	 * as it is.
	 */
	value = PQgetvalue(result, 0, 1);
	if (!writeTimelineHistoryFile(context->filename, value, cdcPathDir))
	{
		log_error("Failed to write timeline history file \"%s\"", context->filename);
		context->parsedOk = false;
		return;
	}

	context->parsedOk = true;
}


/* timeline_iter_history is used to iterate over the content of a timeline history file. */
bool
timeline_iter_history(char *filename,
					  DatabaseCatalog *catalog,
					  uint32_t timeline,
					  TimelineHistoryFun *callback)
{
	TimelineHistoryIterator *iter =
		(TimelineHistoryIterator *) malloc(sizeof(TimelineHistoryIterator));

	if (iter == NULL)
	{
		log_error("Failed to allocate memory for timeline history iterator");
		return false;
	}

	iter->filename = filename;
	iter->currentTimeline = timeline;
	iter->prevend = InvalidXLogRecPtr;

	if (!timeline_iter_history_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!timeline_iter_history_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		TimelineHistoryEntry *entry = iter->entry;

		if (entry == NULL)
		{
			if (!timeline_iter_history_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		if (!callback(catalog, entry))
		{
			log_error("Failed to parse timeline history entry in file \"%s\"",
					  filename);
			return false;
		}
	}

	/* use the callback for the final entry that holds the details of the current timeline */
	if (!callback(catalog, iter->entry))
	{
		log_error("Failed to parse timeline history entry in file \"%s\"",
				  filename);
		return false;
	}


	return true;
}


/*
 * timeline_iter_history_init initializes a TimelineHistoryIterator that will be
 * used to iterate over the content of a timeline history file.
 */
bool
timeline_iter_history_init(TimelineHistoryIterator *iter)
{
	iter->prevend = InvalidXLogRecPtr;

	iter->fileIterator = (FileLinesIterator *) malloc(sizeof(FileLinesIterator));

	if (iter->fileIterator == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * A timeline history file has a fixed format that looks like the following:
	 *
	 * 1	0/5000148	no recovery target specified
	 * 2	0/7000148	no recovery target specified
	 * 3	0/C0109B8	no recovery target specified
	 *
	 * We are only interested in the LSN values in the second column.
	 */
	iter->fileIterator->bufsize = BUFSIZE;
	iter->fileIterator->filename = iter->filename;

	if (!file_iter_lines_init(iter->fileIterator))
	{
		/* errors have already been logged */
		return false;
	}

	iter->entry = (TimelineHistoryEntry *) malloc(sizeof(TimelineHistoryEntry));

	if (iter->entry == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	return true;
}


/* timeline_iter_history_next reads the next line of the timeline history file */
bool
timeline_iter_history_next(TimelineHistoryIterator *iter)
{
	/* read next line using the file iterator low-level API */
	bool skippingEmptyLinesAndComments = true;

	while (skippingEmptyLinesAndComments)
	{
		if (!file_iter_lines_next(iter->fileIterator))
		{
			/* errors have already been logged */
			return false;
		}

		if (iter->fileIterator->line == NULL)
		{
			iter->entry = NULL;
			return true;
		}

		char *line = iter->fileIterator->line;

		/* skip leading whitespace */
		for (; *line; line++)
		{
			if (!isspace((unsigned char) *line))
			{
				break;
			}
		}

		skippingEmptyLinesAndComments = (*line == '\0' || *line == '#');
	}

	char *line = iter->fileIterator->line;

	log_trace("timeline_iter_history_next: line is \"%s\"", line);

	char *tabptr = strchr(line, '\t');

	if (tabptr == NULL)
	{
		log_error("Failed to parse history file line \"%s\"", line);
		return false;
	}

	*tabptr = '\0';

	TimelineHistoryEntry *entry = iter->entry;

	if (!stringToUInt(line, &entry->tli))
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

	if (!parseLSN(lsn, &entry->end))
	{
		log_error("Failed to parse history timeline %d LSN \"%s\"",
				  entry->tli, lsn);
		return false;
	}

	entry->begin = iter->prevend;
	iter->prevend = entry->end;

	log_trace("timeline_iter_history_next: tli %d [%X/%X %X/%X]",
			  entry->tli,
			  LSN_FORMAT_ARGS(entry->begin),
			  LSN_FORMAT_ARGS(entry->end));

	return true;
}


/*
 * timeline_iter_history_finish closes the file iterator and creates a final entry
 * for the "tip" of the timeline, which has no entry in the history file.
 */
bool
timeline_iter_history_finish(TimelineHistoryIterator *iter)
{
	/*
	 * Create one more entry for the "tip" of the timeline, which has no entry
	 * in the history file.
	 */
	TimelineHistoryEntry *entry = iter->entry;
	entry->tli = iter->currentTimeline;
	entry->begin = iter->prevend;
	entry->end = InvalidXLogRecPtr;

	if (!file_iter_lines_finish(iter->fileIterator))
	{
		/* errors have already been logged */
		return false;
	}

	log_trace("timeline_iter_history_next: tli %d [%X/%X %X/%X]",
			  entry->tli,
			  LSN_FORMAT_ARGS(entry->begin),
			  LSN_FORMAT_ARGS(entry->end));


	return true;
}
