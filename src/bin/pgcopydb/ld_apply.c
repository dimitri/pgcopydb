/*
 * src/bin/pgcopydb/ld_apply.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#include "parson.h"

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "stream.h"
#include "string_utils.h"
#include "summary.h"


/*
 * stream_apply_file connects to the target database system and applies the
 * given SQL file as prepared by the stream_transform_file function.
 */
bool
stream_apply_file(StreamApplyContext *context, char *sqlfilename)
{
	StreamContent content = { 0 };
	long size = 0L;

	strlcpy(content.filename, sqlfilename, sizeof(content.filename));

	if (!read_file(content.filename, &(content.buffer), &size))
	{
		/* errors have already been logged */
		return false;
	}

	content.count =
		splitLines(content.buffer, content.lines, MAX_STREAM_CONTENT_COUNT);

	PGSQL *pgsql = &(context->pgsql);
	bool reachedStartingPosition = false;

	/* replay the SQL commands from the SQL file */
	for (int i = 0; i < content.count; i++)
	{
		const char *sql = content.lines[i];
		LogicalMessageMetadata metadata = { 0 };

		StreamAction action = parseSQLAction(sql, &metadata);

		switch (action)
		{
			case STREAM_ACTION_BEGIN:
			{
				bool skipping = metadata.lsn <= context->previousLSN;

				log_debug("BEGIN %lld LSN %X/%X @%s, Previous LSN %X/%X %s",
						  (long long) metadata.xid,
						  LSN_FORMAT_ARGS(metadata.lsn),
						  metadata.timestamp,
						  LSN_FORMAT_ARGS(context->previousLSN),
						  skipping ? "[skipping]" : "");

				context->nextlsn = metadata.nextlsn;

				if (!reachedStartingPosition)
				{
					if (context->previousLSN < metadata.lsn)
					{
						reachedStartingPosition = true;
					}
					else
					{
						continue;
					}
				}

				if (!pgsql_begin(pgsql))
				{
					/* errors have already been logged */
					return false;
				}

				char lsn[PG_LSN_MAXLENGTH] = { 0 };

				sformat(lsn, sizeof(lsn), "%X/%X",
						LSN_FORMAT_ARGS(metadata.lsn));

				if (!pgsql_replication_origin_xact_setup(pgsql,
														 lsn,
														 metadata.timestamp))
				{
					/* errors have already been logged */
					return false;
				}

				break;
			}

			case STREAM_ACTION_COMMIT:
			{
				if (!reachedStartingPosition)
				{
					continue;
				}

				log_debug("COMMIT %lld LSN %X/%X next LSN %X/%X",
						  (long long) metadata.xid,
						  LSN_FORMAT_ARGS(metadata.lsn),
						  LSN_FORMAT_ARGS(metadata.nextlsn));

				context->nextlsn = metadata.nextlsn;

				/* calling pgsql_commit() would finish the connection, avoid */
				if (!pgsql_execute(pgsql, "COMMIT"))
				{
					/* errors have already been logged */
					return false;
				}

				break;
			}

			case STREAM_ACTION_INSERT:
			case STREAM_ACTION_UPDATE:
			case STREAM_ACTION_DELETE:
			case STREAM_ACTION_TRUNCATE:
			{
				if (!reachedStartingPosition)
				{
					continue;
				}

				/* chomp the final semi-colon that we added */
				int len = strlen(sql);

				if (sql[len - 1] == ';')
				{
					char *ptr = (char *) sql + len - 1;
					*ptr = '\0';
				}

				if (!pgsql_execute(pgsql, sql))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			default:
			{
				log_error("Failed to parse SQL query \"%s\"", sql);
				return false;
			}
		}
	}

	free(content.buffer);

	return true;
}


/*
 * setupReplicationOrigin ensures that a replication origin has been created on
 * the target database, and if it has been created previously then fetches the
 * previous LSN position it was at.
 *
 * Also setupReplicationOrigin calls pg_replication_origin_setup() in the
 * current connection, that is set to be
 */
bool
setupReplicationOrigin(StreamApplyContext *context,
					   CDCPaths *paths,
					   char *target_pguri,
					   char *origin)
{
	PGSQL *pgsql = &(context->pgsql);
	char *nodeName = context->origin;

	context->paths = *paths;
	strlcpy(context->target_pguri, target_pguri, sizeof(context->target_pguri));
	strlcpy(context->origin, origin, sizeof(context->origin));

	if (!pgsql_init(pgsql, context->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* we're going to send several replication origin commands */
	pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	uint32_t oid = 0;

	if (!pgsql_replication_origin_oid(pgsql, nodeName, &oid))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("setupReplicationOrigin: oid == %u", oid);

	if (oid == 0)
	{
		log_error("Failed to fetch progress for replication origin \"%s\": "
				  "replication origin not found on target database",
				  nodeName);
		(void) pgsql_finish(pgsql);
		return false;
	}

	if (!pgsql_replication_origin_progress(pgsql,
										   nodeName,
										   true,
										   &(context->previousLSN)))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * At init time, we trick nextlsn to open the sql filename that matches
	 * with our previousLSN from progress tracking.
	 */
	context->nextlsn = context->previousLSN;

	if (!computeSQLFileName(context))
	{
		/* errors have already been logged */
		return false;
	}

	/* compute the WAL filename that would host the previous LSN */
	log_debug("setupReplicationOrigin: replication origin \"%s\" "
			  "found at %X/%X, expected in file \"%s\"",
			  nodeName,
			  LSN_FORMAT_ARGS(context->previousLSN),
			  context->sqlFileName);

	if (!pgsql_replication_origin_session_setup(pgsql, nodeName))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * computeSQLFileName updates the StreamApplyContext structure with the current
 * LSN applied to the target system, and computed
 */
bool
computeSQLFileName(StreamApplyContext *context)
{
	XLogSegNo segno;

	XLByteToSeg(context->nextlsn, segno, context->WalSegSz);
	XLogFileName(context->wal, context->system.timeline, segno, context->WalSegSz);

	sformat(context->sqlFileName, sizeof(context->sqlFileName),
			"%s/%s.sql",
			context->paths.dir,
			context->wal);

	return true;
}


/*
 * parseSQLAction returns the action that is implemented in the given SQL
 * query.
 */
StreamAction
parseSQLAction(const char *query, LogicalMessageMetadata *metadata)
{
	StreamAction action = STREAM_ACTION_UNKNOWN;

	if (strcmp(query, "") == 0)
	{
		return action;
	}

	char *message = NULL;
	char *begin = strstr(query, OUTPUT_BEGIN);
	char *commit = strstr(query, OUTPUT_COMMIT);

	/* do we have a BEGIN or a COMMIT message to parse metadata of? */
	if (begin != NULL)
	{
		action = STREAM_ACTION_BEGIN;
		message = begin + strlen(OUTPUT_BEGIN);
	}
	else if (commit != NULL)
	{
		action = STREAM_ACTION_COMMIT;
		message = commit + strlen(OUTPUT_BEGIN);
	}

	if (message != NULL)
	{
		JSON_Value *json = json_parse_string(message);

		metadata->action = action;

		if (!parseMessageMetadata(metadata, message, json, true))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		json_value_free(json);
	}

	if (strstr(query, "INSERT INTO") != NULL)
	{
		action = STREAM_ACTION_INSERT;
	}
	else if (strstr(query, "UPDATE ") != NULL)
	{
		action = STREAM_ACTION_UPDATE;
	}
	else if (strstr(query, "DELETE FROM ") != NULL)
	{
		action = STREAM_ACTION_DELETE;
	}
	else if (strstr(query, "TRUNCATE ") != NULL)
	{
		action = STREAM_ACTION_TRUNCATE;
	}

	return action;
}
