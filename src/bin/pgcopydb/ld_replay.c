/*
 * src/bin/pgcopydb/ld_replay.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parson.h"

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "ld_stream.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"


typedef struct ReplayStreamCtx
{
	StreamApplyContext applyContext;
} ReplayStreamCtx;


/*
 * stream_apply_replay implements "live replay" of the changes from the source
 * database directly to the target database.
 */
bool
stream_apply_replay(StreamSpecs *specs)
{
	ReplayStreamCtx ctx = { 0 };
	StreamApplyContext *context = &(ctx.applyContext);

	if (!specs->stdIn)
	{
		log_error("BUG: stream_apply_replay requires specs->stdIn");
		return false;
	}

	if (!stream_apply_setup(specs, context))
	{
		log_error("Failed to setup for replay, see above for details");
		return false;
	}

	if (!context->apply)
	{
		/* errors have already been logged */
		return true;
	}

	/*
	 * The stream_replay_line read_from_stream callback is going to send async
	 * queries to the source server to maintain the sentinel tables. Initialize
	 * our connection info now.
	 */
	PGSQL *src = &(context->src);

	if (!pgsql_init(src, context->connStrings->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	/* check for having reached endpos in a previous run already */
	(void) stream_replay_reached_endpos(specs, context, false);

	if (context->reachedEndPos)
	{
		/* reaching endpos has already been logged */
		return true;
	}

	/*
	 * Setup our PIPE reading callback function and read from the PIPE.
	 */
	ReadFromStreamContext readerContext = {
		.callback = stream_replay_line,
		.ctx = &ctx
	};

	if (!read_from_stream(specs->in, &readerContext))
	{
		log_error("Failed to read SQL lines from input stream, "
				  "see above for details");
		return false;
	}

	/*
	 * When we are done reading our input stream and applying changes, we might
	 * still have a sentinel query in flight. Make sure to terminate it now.
	 */
	while (context->sentinelQueryInProgress)
	{
		if (!stream_apply_fetch_sync_sentinel(context))
		{
			/* errors have already been logged */
			return false;
		}

		/* sleep 100ms between retries */
		pg_usleep(100 * 1000);
	}

	/* make sure to send a last round of sentinel update before exit */
	bool findDurableLSN = true;

	if (!stream_apply_sync_sentinel(context, findDurableLSN))
	{
		log_error("Failed to update pgcopydb.sentinel replay_lsn to %X/%X",
				  LSN_FORMAT_ARGS(context->replay_lsn));
		return false;
	}

	/* we might still have to disconnect now */
	(void) pgsql_finish(&(context->pgsql));

	/* check for reaching endpos */
	(void) stream_replay_reached_endpos(specs, context, true);

	return true;
}


/*
 * stream_replay_reached_endpos checks current replay_lsn with sentinel endpos.
 */
bool
stream_replay_reached_endpos(StreamSpecs *specs,
							 StreamApplyContext *context,
							 bool stop)
{
	if (context->endpos != InvalidXLogRecPtr &&
		context->endpos <= context->replay_lsn)
	{
		context->reachedEndPos = true;

		log_info("Replayed reached endpos %X/%X at replay_lsn %X/%X, stopping",
				 LSN_FORMAT_ARGS(context->endpos),
				 LSN_FORMAT_ARGS(context->replay_lsn));
	}
	else if (stop && context->replay_lsn != InvalidXLogRecPtr)
	{
		log_info("Replayed up to replay_lsn %X/%X, stopping",
				 LSN_FORMAT_ARGS(context->replay_lsn));
	}
	else if (stop)
	{
		log_notice("Replay process stopping");
	}

	return true;
}


/*
 * stream_replay_line is a callback function for the ReadFromStreamContext and
 * read_from_stream infrastructure. It's called on each line read from a stream
 * such as a unix pipe.
 */
bool
stream_replay_line(void *ctx, const char *line, bool *stop)
{
	ReplayStreamCtx *replayCtx = (ReplayStreamCtx *) ctx;
	StreamApplyContext *context = &(replayCtx->applyContext);

	LogicalMessageMetadata metadata = { 0 };

	if (!parseSQLAction((char *) line, &metadata))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_apply_sql(context, &metadata, line))
	{
		/* errors have already been logged */
		return false;
	}

	/* update progres on source database when needed */
	switch (metadata.action)
	{
		/* these actions are good points when to report progress */
		case STREAM_ACTION_COMMIT:
		case STREAM_ACTION_KEEPALIVE:
		{
			uint64_t now = time(NULL);

			if (context->sentinelQueryInProgress)
			{
				if (!stream_apply_fetch_sync_sentinel(context))
				{
					/* errors have already been logged */
					return false;
				}
			}

			/* rate limit to 1 update per second */
			else if (1 < (now - context->sentinelSyncTime))
			{
				if (!stream_apply_send_sync_sentinel(context))
				{
					/* errors have already been logged */
					return false;
				}
			}
			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			PGSQL src = { 0 };
			char *dsn = context->connStrings->source_pguri;

			if (!pgsql_init(&src, dsn, PGSQL_CONN_SOURCE))
			{
				/* errors have already been logged */
				return false;
			}

			CopyDBSentinel sentinel = { 0 };

			if (!pgsql_get_sentinel(&src, &sentinel))
			{
				/* errors have already been logged */
				return false;
			}

			if (sentinel.endpos != InvalidXLogRecPtr &&
				sentinel.endpos <= metadata.lsn)
			{
				*stop = true;
				context->reachedEndPos = true;

				log_info("Replay reached ENDPOS %X/%X",
						 LSN_FORMAT_ARGS(metadata.lsn));
			}
			break;
		}

		/* skip reporting progress in other cases */
		case STREAM_ACTION_BEGIN:
		case STREAM_ACTION_INSERT:
		case STREAM_ACTION_UPDATE:
		case STREAM_ACTION_DELETE:
		case STREAM_ACTION_TRUNCATE:
		case STREAM_ACTION_MESSAGE:
		case STREAM_ACTION_SWITCH:
		default:
		{
			break;
		}
	}

	/*
	 * When syncing with the pgcopydb sentinel we might receive a
	 * new endpos, and it might mean we're done already.
	 */
	if (context->reachedEndPos ||
		(context->endpos != InvalidXLogRecPtr &&
		 context->endpos <= context->previousLSN))
	{
		*stop = true;
		context->reachedEndPos = true;

		log_info("Replay reached end position %X/%X at %X/%X",
				 LSN_FORMAT_ARGS(context->endpos),
				 LSN_FORMAT_ARGS(context->previousLSN));
	}

	return true;
}
