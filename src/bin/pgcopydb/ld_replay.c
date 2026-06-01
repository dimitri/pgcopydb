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

	/* reconnect state */
	bool connectionFailed;      /* target connection died, need to reconnect */
	bool drainPipe;             /* skip execution until next txn boundary */
	bool drainComplete;         /* drain finished (saw COMMIT or ROLLBACK) */
	int skippedStmtCount;       /* statements skipped during drain */
} ReplayStreamCtx;


/*
 * apply_connection_failed returns true when either apply connection is gone.
 * Called after stream_apply_sql returns false to distinguish connection
 * failures (retriable) from real SQL errors (not retriable).
 */
static bool
apply_connection_failed(StreamApplyContext *context)
{
	bool applyBad =
		context->applyPgConn.connection == NULL ||
		PQstatus(context->applyPgConn.connection) == CONNECTION_BAD;

	bool controlBad =
		context->controlPgConn.connection == NULL ||
		PQstatus(context->controlPgConn.connection) == CONNECTION_BAD;

	return applyBad || controlBad;
}


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

	int reconnectAttempt = 0;
	time_t reconnectWindowStart = 0;

	while (true)
	{
		if (!read_from_stream(specs->in, &readerContext))
		{
			log_error("Failed to read SQL lines from input stream, "
					  "see above for details");
			return false;
		}

		if (!ctx.connectionFailed)
		{
			/* normal pipe EOF: done */
			break;
		}

		/*
		 * Target connection was lost. If we were mid-transaction, drain the
		 * pipe to the next COMMIT/ROLLBACK before attempting to reconnect.
		 * The partial transaction was already rolled back on the target.
		 */
		if (ctx.drainPipe)
		{
			log_debug("Draining pipe to next transaction boundary "
					  "before reconnecting to target");

			if (!read_from_stream(specs->in, &readerContext))
			{
				/*
				 * Pipe closed before we found the boundary — that's fine,
				 * the transaction was rolled back on the target already.
				 */
				log_warn("Pipe closed mid-drain; partial transaction "
						 "was rolled back on target");
				ctx.drainPipe = false;
				ctx.drainComplete = true;
			}
		}

		/* record the start of this reconnect window */
		if (reconnectWindowStart == 0)
		{
			reconnectWindowStart = time(NULL);
		}

		/* inner loop: attempt reconnect with exponential backoff */
		bool reconnected = false;

		while (!reconnected)
		{
			if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
			{
				return false;
			}

			int elapsed = (int) (time(NULL) - reconnectWindowStart);

			if (elapsed >= STREAM_RECONNECT_MAX_TOTAL_SECS)
			{
				log_error("Target connection lost and could not reconnect "
						  "within %d minutes; manual intervention required",
						  STREAM_RECONNECT_MAX_TOTAL_SECS / 60);
				return false;
			}

			pgsql_finish(&context->applyPgConn);
			pgsql_finish(&context->controlPgConn);

			int sleepSecs =
				STREAM_RECONNECT_BASE_SLEEP_SECS * (1 << reconnectAttempt);

			if (sleepSecs > STREAM_RECONNECT_MAX_SLEEP_SECS)
			{
				sleepSecs = STREAM_RECONNECT_MAX_SLEEP_SECS;
			}

			log_warn("Target connection lost, reconnecting in %ds "
					 "(attempt %d, %ds/%ds elapsed)",
					 sleepSecs,
					 reconnectAttempt + 1,
					 elapsed,
					 STREAM_RECONNECT_MAX_TOTAL_SECS);

			for (int s = 0; s < sleepSecs; s++)
			{
				if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
				{
					return false;
				}
				pg_usleep(1000 * 1000);
			}

			++reconnectAttempt;

			if (!setupReplicationOrigin(context))
			{
				if (pgsql_is_permissions_error(&context->applyPgConn) ||
					pgsql_is_permissions_error(&context->controlPgConn))
				{
					log_error("Target connection failed with authorization "
							  "error; manual intervention required");
					return false;
				}

				/* connection failure: keep retrying */
				continue;
			}

			reconnected = true;
		}

		log_info("Reconnected to target database after %ds",
				 (int) (time(NULL) - reconnectWindowStart));

		/* reset reconnect state for next potential failure */
		reconnectAttempt = 0;
		reconnectWindowStart = 0;
		ctx.connectionFailed = false;
		ctx.drainPipe = false;
		ctx.drainComplete = false;
		ctx.skippedStmtCount = 0;
	}

	/* make sure to send a last round of sentinel update before exit */
	bool findDurableLSN = true;

	if (!stream_apply_sync_sentinel(context, findDurableLSN))
	{
		log_error("Failed to update pgcopydb.sentinel replay_lsn to %X/%X",
				  LSN_FORMAT_ARGS(context->replay_lsn));
		return false;
	}

	(void) stream_apply_cleanup(context);

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

	/*
	 * Drain mode: the connection died mid-transaction. Skip execution of all
	 * SQL until we reach the COMMIT or ROLLBACK that ends the transaction.
	 * The partial transaction was already rolled back on the target.
	 */
	if (replayCtx->drainPipe)
	{
		if (!parseSQLAction((char *) line, &metadata, context->filters))
		{
			++replayCtx->skippedStmtCount;
			return true;
		}

		if (metadata.action == STREAM_ACTION_COMMIT ||
			metadata.action == STREAM_ACTION_ROLLBACK)
		{
			log_warn("Skipped partial transaction ending at LSN %X/%X after "
					 "target connection failure: %d statement%s discarded, "
					 "transaction was rolled back on target and will be "
					 "replayed on reconnect from replication origin position",
					 LSN_FORMAT_ARGS(metadata.lsn),
					 replayCtx->skippedStmtCount,
					 replayCtx->skippedStmtCount == 1 ? "" : "s");

			replayCtx->drainPipe = false;
			replayCtx->drainComplete = true;
			*stop = true;
		}
		else
		{
			log_debug("Drain: skipping %s at LSN %X/%X",
					  StreamActionToString(metadata.action),
					  LSN_FORMAT_ARGS(metadata.lsn));
			++replayCtx->skippedStmtCount;
		}

		return true;
	}

	if (!parseSQLAction((char *) line, &metadata, context->filters))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_apply_sql(context, &metadata, line))
	{
		/*
		 * Distinguish connection failures (retriable) from real SQL errors.
		 * On connection failure, signal the outer loop to reconnect rather
		 * than propagating the error up through read_from_stream.
		 */
		if (apply_connection_failed(context))
		{
			log_warn("Target connection lost at LSN %X/%X during %s; "
					 "will attempt reconnect",
					 LSN_FORMAT_ARGS(metadata.lsn),
					 StreamActionToString(metadata.action));

			replayCtx->connectionFailed = true;

			if (context->transactionInProgress)
			{
				log_warn("Connection lost mid-transaction at LSN %X/%X; "
						 "draining pipe to next transaction boundary "
						 "before reconnecting",
						 LSN_FORMAT_ARGS(metadata.lsn));
				replayCtx->drainPipe = true;
				replayCtx->skippedStmtCount = 0;
			}

			*stop = true;
			return true;
		}

		/* errors have already been logged */
		return false;
	}

	/* update progress on source database when needed */
	switch (metadata.action)
	{
		/* these actions are good points when to report progress */
		case STREAM_ACTION_COMMIT:
		case STREAM_ACTION_KEEPALIVE:
		{
			uint64_t now = time(NULL);

			/* rate limit to 1 update per second */
			if (1 < (now - context->sentinelSyncTime))
			{
				bool findDurableLSN = true;

				if (!stream_apply_sync_sentinel(context, findDurableLSN))
				{
					/* errors have already been logged */
					return false;
				}
			}

			/* rate limit to 1 pipeline sync per seconds */
			if (1 < (now - context->applyPgConn.pipelineSyncTime))
			{
				if (!pgsql_sync_pipeline(&(context->applyPgConn)))
				{
					if (apply_connection_failed(context))
					{
						log_warn("Target connection lost during pipeline sync; "
								 "will attempt reconnect");

						replayCtx->connectionFailed = true;

						if (context->transactionInProgress)
						{
							log_warn("Connection lost mid-transaction during "
									 "pipeline sync; draining pipe to next "
									 "transaction boundary before reconnecting");
							replayCtx->drainPipe = true;
							replayCtx->skippedStmtCount = 0;
						}

						*stop = true;
						return true;
					}

					log_error("Failed to sync the pipeline, see previous "
							  "error for details");
					return false;
				}
			}
			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			CopyDBSentinel sentinel = { 0 };

			if (!sentinel_get(context->sourceDB, &sentinel))
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

	if (*stop)
	{
		if (!pgsql_sync_pipeline(&(context->applyPgConn)))
		{
			log_error("Failed to sync the pipeline, see previous error for "
					  "details");
			return false;
		}
	}

	return true;
}
