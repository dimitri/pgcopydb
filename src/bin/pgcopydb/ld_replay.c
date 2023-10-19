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

	bool isTxnBuffering;
	bool skipTxnBuffering;

	FILE *txnBuffer;

	char currentTxnFileName[MAXPGPATH];
} ReplayStreamCtx;

static bool streamReplayLineIntoBuffer(void *ctx, const char *line, bool *stop);
static bool streamBufferedXID(ReplayStreamCtx *replayCtx);

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
		.callback = streamReplayLineIntoBuffer,
		.ctx = &ctx
	};

	if (!read_from_stream(specs->in, &readerContext))
	{
		log_error("Failed to transform JSON messages from input stream, "
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


/*
 * streamBufferedXID reads a transaction buffer file and streams it into the
 * target database.
 */
static bool
streamBufferedXID(ReplayStreamCtx *replayCtx)
{
	/* Open replaybuffer in read-only mode to use it as input stream */
	FILE *txnBufferForRead = fopen_read_only(replayCtx->currentTxnFileName);

	if (txnBufferForRead == NULL)
	{
		log_error("Failed to open transaction buffer file \"%s\" in "
				  "readonly mode: %m",
				  replayCtx->currentTxnFileName);
		return false;
	}

	ReadFromStreamContext readerContext = {
		.callback = stream_replay_line,
		.ctx = replayCtx
	};

	if (!read_from_stream(txnBufferForRead, &readerContext))
	{
		log_error("Failed to read from replay buffer, "
				  "see above for details");
		return false;
	}

	fclose(txnBufferForRead);

	return true;
}


/* streamReplayLineIntoBuffer is a callback function for the ReadFromStreamContext
 * and read_from_stream infrastructure. It's called on each line read from a
 * stream such as a unix pipe and buffers the line into a file when we encounter
 * a COMMIT without a valid txnCommitLSN.
 *
 * This ensures that the apply process doesn't block the generation of the
 * transaction metadata file created by the transform process on arrival of
 * a COMMIT message.
 */
static bool
streamReplayLineIntoBuffer(void *ctx, const char *line, bool *stop)
{
	ReplayStreamCtx *replayCtx = (ReplayStreamCtx *) ctx;
	StreamApplyContext *context = &(replayCtx->applyContext);

	LogicalMessageMetadata metadata = { 0 };

	if (!parseSQLAction((char *) line, &metadata))
	{
		/* errors have already been logged */
		return false;
	}

	/* We don't use this metadata in this function */
	free(metadata.jsonBuffer);


	if (metadata.action == STREAM_ACTION_BEGIN)
	{
		/*
		 * Enable transaction buffering only when the BEGIN doesn't have
		 * valid commit LSN, otherwise we can skip buffering and stream
		 * the transaction directly.
		 */
		replayCtx->skipTxnBuffering = metadata.txnCommitLSN != InvalidXLogRecPtr;
		if (replayCtx->skipTxnBuffering)
		{
			return stream_replay_line(ctx, line, stop);
		}

		if (replayCtx->isTxnBuffering)
		{
			/*
			 * When the follow switches from prefetch to replay mode, we
			 * call stream_transform_stream which might stream the partially
			 * written transaction created during the prefetch mode.
			 *
			 * For example, lets consider the partially written C.sql file,
			 *
			 * BEGIN -- {"xid": 1000, "commitLSN": "0/1234"};
			 * INSERT INTO C VALUES (1);
			 * INSERT INTO C VALUES (2);
			 * KEEPALIVE;
			 *
			 * After switching to the replay mode, logical decoding will resume
			 * from consistent point which again starts with a valid transcation
			 * block.
			 *
			 * Lets assume the following contents streamed from the transform
			 * to catchup process in UNIX PIPE after switching to replay mode,
			 *
			 * BEGIN -- {"xid": 999, "commitLSN": "0/1230"};
			 * INSERT INTO A VALUES (1);
			 * INSERT INTO A VALUES (2);
			 * COMMIT -- {"xid": 999, "lsn": "0/1230"};
			 * BEGIN -- {"xid": 1000, "commitLSN": "0/1234"};
			 * INSERT INTO C VALUES (1);
			 * INSERT INTO C VALUES (2);
			 * COMMIT -- {"xid": 1000, "lsn": "0/1234"};
			 *
			 * Contents of C.sql will be streamed by stream_transform_stream
			 * and followed by the contents from UNIX PIPE. Since both of the
			 * contents are streamed one after the other, the second block
			 * contains the full content of previously written
			 * transaction. So, we can skip previous transaction buffer and
			 * start buffering from the current transaction.
			 */
			log_debug("Received %s when transaction is already "
					  "in buffering mode, ignoring.", line);
			fclose(replayCtx->txnBuffer);

			/* Remove symlink to the replay buffer */
			(void) unlink_file(context->paths.txnlatestfile);
		}

		sformat(replayCtx->currentTxnFileName, MAXPGPATH, "%s/%d.sql",
				context->paths.dir, metadata.xid);

		/*
		 * Open current transaction file in w+ mode to truncate if the file
		 * already exists.
		 */
		replayCtx->txnBuffer = fopen_with_umask(replayCtx->currentTxnFileName,
												"w+",
												O_RDWR | O_TRUNC | O_CREAT,
												0644);
		if (replayCtx->txnBuffer == NULL)
		{
			log_error("Failed to open transaction buffer file \"%s\": %m",
					  replayCtx->currentTxnFileName);
			return false;
		}

		/* Create as a symlink to the current transcation file */
		if (!create_symbolic_link(replayCtx->currentTxnFileName,
								  context->paths.txnlatestfile))
		{
			/* errors have already been logged */
			return false;
		}

		fformat(replayCtx->txnBuffer, "%s\n", line);

		replayCtx->isTxnBuffering = true;
	}
	else if (metadata.action == STREAM_ACTION_COMMIT)
	{
		if (replayCtx->skipTxnBuffering)
		{
			return stream_replay_line(ctx, line, stop);
		}

		if (!replayCtx->isTxnBuffering)
		{
			/*
			 * When the follow switches from prefetch to replay mode, we
			 * call stream_transform_stream which might stream the partially
			 * written transaction created during the prefetch mode.
			 *
			 * For example, lets consider the partially written C.sql file,
			 *
			 * COMMIT -- {"xid": 999, "lsn": "0/1230"};
			 * BEGIN -- {"xid": 1000, "commitLSN": "0/1234"};
			 * INSERT INTO C VALUES (1);
			 * INSERT INTO C VALUES (2);
			 * KEEPALIVE;
			 *
			 * After switching to the replay mode, logical decoding will resume
			 * from consistent point which again starts with a valid transcation
			 * block.
			 *
			 * Lets assume the following contents streamed from the transform
			 * to catchup process in UNIX PIPE after switching to replay mode,
			 *
			 * BEGIN -- {"xid": 999, "commitLSN": "0/1230"};
			 * INSERT INTO A VALUES (1);
			 * INSERT INTO A VALUES (2);
			 * COMMIT -- {"xid": 999, "lsn": "0/1230"};
			 * BEGIN -- {"xid": 1000, "commitLSN": "0/1234"};
			 * INSERT INTO C VALUES (1);
			 * INSERT INTO C VALUES (2);
			 * COMMIT -- {"xid": 1000, "lsn": "0/1234"};
			 *
			 * Contents of C.sql will be streamed by stream_transform_stream
			 * and followed by the contents from UNIX PIPE. Since both of the
			 * contents are streamed one after the other, the second block
			 * contains the full content of previously written
			 * transaction. So, we can skip previous transaction buffer and
			 * start buffering from the current transaction.
			 */
			log_debug("Received %s when transaction is not "
					  "in buffering mode, ignoring.", line);
			return true;
		}

		fformat(replayCtx->txnBuffer, "%s\n", line);

		/* Close replaybuffer to mark it as complete */
		fclose(replayCtx->txnBuffer);

		/* Stream the transaction buffer into the target database */
		if (!streamBufferedXID(replayCtx))
		{
			/* errors have already been logged */
			return false;
		}

		replayCtx->isTxnBuffering = false;

		/* Remove the symlink to the replay buffer */
		(void) unlink_file(context->paths.txnlatestfile);

		/* Early exit if we reached the end position */
		*stop = context->reachedEndPos;

		replayCtx->txnBuffer = NULL;
	}
	else
	{
		if (replayCtx->skipTxnBuffering)
		{
			return stream_replay_line(ctx, line, stop);
		}

		if (replayCtx->isTxnBuffering)
		{
			/*
			 * We are in a transaction block, buffer all the messages
			 * including KEEPALIVE, SWITCHWAL and ENPOS.
			 */
			fformat(replayCtx->txnBuffer, "%s\n", line);
		}
		else if (metadata.action == STREAM_ACTION_KEEPALIVE ||
				 metadata.action == STREAM_ACTION_SWITCH ||
				 metadata.action == STREAM_ACTION_ENDPOS)
		{
			/*
			 * We allow KEEPALIVE, SWITCHWAL and ENPOS messages in a
			 * non-transactional context. In that case, call stream_replay_line
			 * directly without buffering the message.
			 */
			return stream_replay_line(ctx, line, stop);
		}
		else
		{
			/*
			 * When the follow switches from prefetch to replay mode, we
			 * call stream_transform_stream which might stream the partially
			 * written transaction created during the prefetch mode.
			 *
			 * For example, lets consider the partially written C.sql file,
			 *
			 * INSERT INTO A VALUES (2);
			 * COMMIT -- {"xid": 999, "lsn": "0/1230"};
			 * BEGIN -- {"xid": 1000, "commitLSN": "0/1234"};
			 * INSERT INTO C VALUES (1);
			 * INSERT INTO C VALUES (2);
			 * KEEPALIVE;
			 *
			 * After switching to the replay mode, logical decoding will resume
			 * from consistent point which again starts with a valid transcation
			 * block.
			 *
			 * Lets assume the following contents streamed from the transform
			 * to catchup process in UNIX PIPE after switching to replay mode,
			 *
			 * BEGIN -- {"xid": 999, "commitLSN": "0/1230"};
			 * INSERT INTO A VALUES (1);
			 * INSERT INTO A VALUES (2);
			 * COMMIT -- {"xid": 999, "lsn": "0/1230"};
			 * BEGIN -- {"xid": 1000, "commitLSN": "0/1234"};
			 * INSERT INTO C VALUES (1);
			 * INSERT INTO C VALUES (2);
			 * COMMIT -- {"xid": 1000, "lsn": "0/1234"};
			 *
			 * Contents of C.sql will be streamed by stream_transform_stream
			 * and followed by the contents from UNIX PIPE. Since both of the
			 * contents are streamed one after the other, the second block
			 * contains the full content of previously written
			 * transaction. So, we can skip previous transaction buffer and
			 * start buffering from the current transaction.
			 */
			log_debug("Received %s when transaction is not "
					  "in buffering mode", line);
		}
	}

	return true;
}
