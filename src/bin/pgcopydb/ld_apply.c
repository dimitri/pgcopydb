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
#include "ld_stream.h"
#include "lsn_tracking.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

GUC applySettingsSync[] = {
	COMMON_GUC_SETTINGS,
	{ "synchronous_commit", "on" },
	{ "session_replication_role", "'replica'" },
	{ NULL, NULL },
};

GUC applySettings[] = {
	COMMON_GUC_SETTINGS,
	{ "synchronous_commit", "off" },
	{ "session_replication_role", "'replica'" },
	{ NULL, NULL },
};

static bool readTxnCommitLSN(LogicalMessageMetadata *metadata, const char *dir,
							 bool *txnCommitLSNFound);
static bool parseTxnMetadataFile(const char *filename, LogicalMessageMetadata *metadata);

static bool computeTxnMetadataFilename(uint32_t xid, const char *dir, char *filename);

static bool writeTxnCommitMetadata(LogicalMessageMetadata *mesg, const char *dir);

/*
 * stream_apply_catchup catches up with SQL files that have been prepared by
 * either the `pgcopydb stream prefetch` command.
 */
bool
stream_apply_catchup(StreamSpecs *specs)
{
	StreamApplyContext context = { 0 };

	if (!stream_apply_setup(specs, &context))
	{
		log_error("Failed to setup for catchup, see above for details");
		return false;
	}

	if (!context.apply)
	{
		/* errors have already been logged */
		return true;
	}

	/*
	 * Our main loop reads the current SQL file, applying all the queries from
	 * there and tracking progress, and then goes on to the next file, until no
	 * such file exists.
	 */
	char currentSQLFileName[MAXPGPATH] = { 0 };

	for (;;)
	{
		strlcpy(currentSQLFileName, context.sqlFileName, MAXPGPATH);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			break;
		}

		/*
		 * It might be the expected file doesn't exist already, in that case
		 * exit successfully so that the main process may switch from catchup
		 * mode to replay mode.
		 */
		if (!file_exists(context.sqlFileName))
		{
			log_info("File \"%s\" does not exists yet, exit",
					 context.sqlFileName);

			(void) pgsql_finish(&(context.pgsql));
			return true;
		}

		/*
		 * The SQL file exists already, apply it now.
		 */
		if (!stream_apply_file(&context))
		{
			/* errors have already been logged */
			(void) pgsql_finish(&(context.pgsql));
			return false;
		}

		/*
		 * When syncing with the pgcopydb sentinel we might receive a new
		 * endpos, and it might mean we're done already.
		 */
		if (!context.reachedEndPos &&
			context.endpos != InvalidXLogRecPtr &&
			context.endpos <= context.previousLSN)
		{
			context.reachedEndPos = true;

			log_info("Apply reached end position %X/%X at %X/%X",
					 LSN_FORMAT_ARGS(context.endpos),
					 LSN_FORMAT_ARGS(context.previousLSN));
		}

		if (context.reachedEndPos)
		{
			/* information has already been logged */
			break;
		}

		log_info("Apply reached %X/%X in \"%s\"",
				 LSN_FORMAT_ARGS(context.previousLSN),
				 currentSQLFileName);

		if (!computeSQLFileName(&context))
		{
			/* errors have already been logged */
			(void) pgsql_finish(&(context.pgsql));
			return false;
		}

		/*
		 * If we reached the end of the file and the current LSN still belongs
		 * to the same file (a SWITCH did not occur), then we exit so that the
		 * calling process may switch from catchup mode to live replay mode.
		 */
		if (streq(context.sqlFileName, currentSQLFileName))
		{
			log_info("Reached end of file \"%s\" at %X/%X.",
					 currentSQLFileName,
					 LSN_FORMAT_ARGS(context.previousLSN));

			/* make sure we close the connection on the way out */
			(void) pgsql_finish(&(context.pgsql));
			return true;
		}

		log_notice("Apply new filename: \"%s\"", context.sqlFileName);
	}

	/* make sure we close the connection on the way out */
	(void) pgsql_finish(&(context.pgsql));
	return true;
}


/*
 * stream_apply_setup does the required setup for then starting to catchup or
 * to replay changes from the SQL input (files or Unix PIPE) to the target
 * database.
 */
bool
stream_apply_setup(StreamSpecs *specs, StreamApplyContext *context)
{
	/* init our context */
	if (!stream_apply_init_context(context,
								   specs->sourceDB,
								   &(specs->paths),
								   specs->connStrings,
								   specs->origin,
								   specs->endpos))
	{
		/* errors have already been logged */
		return false;
	}

	/* read-in the previous lsn tracking file, if it exists */
	if (!lsn_tracking_read(context))
	{
		log_error("Failed to read LSN tracking file");
		return false;
	}

	/* wait until the sentinel enables the apply process */
	if (!stream_apply_wait_for_sentinel(specs, context))
	{
		/* errors have already been logged */
		return false;
	}

	if (!context->apply)
	{
		log_notice("Apply mode is still disabled, quitting now");
		return true;
	}

	if (specs->system.timeline == 0)
	{
		if (!stream_read_context(&(specs->paths),
								 &(specs->system),
								 &(specs->WalSegSz)))
		{
			log_error("Failed to read the streaming context information "
					  "from the source database, see above for details");
			return false;
		}
	}

	context->system = specs->system;
	context->WalSegSz = specs->WalSegSz;

	log_debug("Source database wal_segment_size is %u", context->WalSegSz);
	log_debug("Source database timeline is %d", context->system.timeline);

	/*
	 * Use the replication origin for our setup (context->previousLSN).
	 */
	if (!setupReplicationOrigin(context, specs->logSQL))
	{
		log_error("Failed to setup replication origin on the target database");
		return false;
	}

	char *process =
		specs->mode == STREAM_MODE_CATCHUP ? "Catchup-up with" : "Replaying";

	if (context->endpos != InvalidXLogRecPtr)
	{
		if (context->endpos <= context->previousLSN)
		{
			log_info("Current endpos %X/%X was previously reached at %X/%X",
					 LSN_FORMAT_ARGS(context->endpos),
					 LSN_FORMAT_ARGS(context->previousLSN));

			return true;
		}

		log_info("%s changes from LSN %X/%X up to endpos LSN %X/%X",
				 process,
				 LSN_FORMAT_ARGS(context->previousLSN),
				 LSN_FORMAT_ARGS(context->endpos));
	}
	else
	{
		log_info("%s changes from LSN %X/%X",
				 process,
				 LSN_FORMAT_ARGS(context->previousLSN));
	}

	return true;
}


/*
 * stream_apply_wait_for_sentinel fetches the current pgcopydb sentinel values
 * on the source database: the catchup processing only gets to start when the
 * sentinel "apply" column has been set to true.
 */
bool
stream_apply_wait_for_sentinel(StreamSpecs *specs, StreamApplyContext *context)
{
	bool firstLoop = true;
	CopyDBSentinel sentinel = { 0 };

	/* make sure context->apply is false before entering the loop */
	context->apply = false;

	while (!context->apply)
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_info("Apply process received a shutdown signal "
					 "while waiting for apply mode, "
					 "quitting now");
			return true;
		}

		/* this reconnects on each loop iteration, every 10s by default */
		if (!sentinel_get(specs->sourceDB, &sentinel))
		{
			log_warn("Retrying to fetch pgcopydb sentinel values in %ds",
					 CATCHINGUP_SLEEP_MS / 10);
			pg_usleep(CATCHINGUP_SLEEP_MS * 1000);

			continue;
		}

		/*
		 * Now grab the current sentinel values.
		 *
		 * The pgcopydb sentinel table contains an endpos. The --endpos command
		 * line option (found in specs->endpos) prevails, but when it's not
		 * been used, we have a look at the sentinel value.
		 */
		context->startpos = sentinel.startpos;
		context->apply = sentinel.apply;

		if (specs->endpos == InvalidXLogRecPtr)
		{
			context->endpos = sentinel.endpos;
		}
		else if (context->endpos != sentinel.endpos)
		{
			log_warn("Sentinel endpos is %X/%X, overriden by --endpos %X/%X",
					 LSN_FORMAT_ARGS(sentinel.endpos),
					 LSN_FORMAT_ARGS(specs->endpos));
		}

		/* TODO: find more about this */
		if (context->previousLSN == InvalidXLogRecPtr)
		{
			context->previousLSN = sentinel.replay_lsn;
		}
		else
		{
			log_warn("stream_apply_wait_for_sentinel: "
					 "previous lsn %X/%X, replay_lsn %X/%X",
					 LSN_FORMAT_ARGS(context->previousLSN),
					 LSN_FORMAT_ARGS(sentinel.replay_lsn));
		}

		log_debug("startpos %X/%X endpos %X/%X apply %s",
				  LSN_FORMAT_ARGS(context->startpos),
				  LSN_FORMAT_ARGS(context->endpos),
				  context->apply ? "enabled" : "disabled");

		if (context->apply)
		{
			break;
		}

		if (firstLoop)
		{
			firstLoop = false;

			log_info("Waiting until the pgcopydb sentinel apply is enabled");
		}

		/* avoid buzy looping and avoid hammering the source database */
		pg_usleep(CATCHINGUP_SLEEP_MS * 1000);
	}

	/* when apply was already set on first loop, don't even mention it */
	if (!firstLoop)
	{
		log_info("The pgcopydb sentinel has enabled applying changes");
	}

	return true;
}


/*
 * stream_apply_sync_sentinel sync with the pgcopydb sentinel table, sending
 * the current replay LSN position and fetching the maybe new endpos and apply
 * values.
 */
bool
stream_apply_sync_sentinel(StreamApplyContext *context, bool findDurableLSN)
{
	/* now is a good time to write the LSN tracking to disk */
	if (!lsn_tracking_write(context->sourceDB, context->lsnTrackingList))
	{
		/* errors have already been logged */
		return false;
	}

	uint64_t durableLSN = InvalidXLogRecPtr;

	/*
	 * If we know we reached endpos, then publish that as the replay_lsn.
	 */
	if (context->reachedEndPos || !findDurableLSN)
	{
		durableLSN = context->previousLSN;
	}
	else
	{
		if (!stream_apply_find_durable_lsn(context, &durableLSN))
		{
			log_warn("Skipping sentinel replay_lsn update: "
					 "failed to find a durable LSN matching current flushLSN");
			return true;
		}
	}

	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_sync_apply(context->sourceDB, durableLSN, &sentinel))
	{
		log_warn("Failed to sync progress with the pgcopydb sentinel");
		return true;
	}

	context->apply = sentinel.apply;
	context->endpos = sentinel.endpos;
	context->startpos = sentinel.startpos;

	log_debug("stream_apply_sync_sentinel: "
			  "write_lsn %X/%X flush_lsn %X/%X replay_lsn %X/%X "
			  "startpos %X/%X endpos %X/%X apply %s",
			  LSN_FORMAT_ARGS(sentinel.write_lsn),
			  LSN_FORMAT_ARGS(sentinel.flush_lsn),
			  LSN_FORMAT_ARGS(sentinel.replay_lsn),
			  LSN_FORMAT_ARGS(context->startpos),
			  LSN_FORMAT_ARGS(context->endpos),
			  context->apply ? "enabled" : "disabled");

	return true;
}


/*
 * stream_apply_file connects to the target database system and applies the
 * given SQL file as prepared by the stream_transform_file function.
 */
bool
stream_apply_file(StreamApplyContext *context)
{
	StreamContent content = { 0 };
	long size = 0L;

	strlcpy(content.filename, context->sqlFileName, sizeof(content.filename));

	if (!read_file(content.filename, &(content.buffer), &size))
	{
		/* errors have already been logged */
		return false;
	}

	content.count = countLines(content.buffer);
	content.lines = (char **) calloc(content.count, sizeof(char *));
	content.count = splitLines(content.buffer, content.lines, content.count);

	if (content.lines == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	log_info("Replaying changes from file \"%s\"", context->sqlFileName);

	log_debug("Read %d lines in file \"%s\"",
			  content.count,
			  content.filename);

	LogicalMessageMetadata *mArray =
		(LogicalMessageMetadata *) calloc(content.count,
										  sizeof(LogicalMessageMetadata));

	LogicalMessageMetadata *lastCommit = NULL;

	/* parse the SQL commands metadata from the SQL file */
	for (int i = 0; i < content.count && !context->reachedEndPos; i++)
	{
		const char *sql = content.lines[i];
		LogicalMessageMetadata *metadata = &(mArray[i]);

		if (!parseSQLAction(sql, metadata))
		{
			/* errors have already been logged */
			free(content.buffer);
			free(content.lines);

			return false;
		}

		/*
		 * The SWITCH WAL command should always be the last line of the file.
		 */
		if (metadata->action == STREAM_ACTION_SWITCH &&
			i != (content.count - 1))
		{
			log_error("SWITCH command for LSN %X/%X found in \"%s\" line %d, "
					  "before last line %d",
					  LSN_FORMAT_ARGS(metadata->lsn),
					  content.filename,
					  i + 1,
					  content.count);

			free(content.buffer);
			free(content.lines);

			return false;
		}

		if (metadata->action == STREAM_ACTION_COMMIT)
		{
			lastCommit = metadata;
		}
	}

	/* replay the SQL commands from the SQL file */
	for (int i = 0; i < content.count && !context->reachedEndPos; i++)
	{
		const char *sql = content.lines[i];
		LogicalMessageMetadata *metadata = &(mArray[i]);

		/* last commit of a file requires synchronous_commit on */
		context->reachedEOF = metadata == lastCommit;

		if (!stream_apply_sql(context, metadata, sql))
		{
			log_error("Failed to apply SQL from file \"%s\", "
					  "see above for details",
					  content.filename);

			free(content.buffer);
			free(content.lines);

			return false;
		}
	}

	/* free dynamic memory that's not needed anymore */
	free(content.buffer);
	free(content.lines);

	/*
	 * Each time we are done applying a file, we update our progress and
	 * fetch new values from the pgcopydb sentinel. Errors are warning
	 * here, we'll update next time.
	 */
	bool findDurableLSN = false;

	if (!stream_apply_sync_sentinel(context, findDurableLSN))
	{
		log_error("Failed to sync replay_lsn %X/%X",
				  LSN_FORMAT_ARGS(context->previousLSN));
		return false;
	}

	return true;
}


/*
 * stream_apply_sql connects to the target database system and applies the
 * given SQL command as prepared by the stream_transform_file or
 * stream_transform_stream function.
 */
bool
stream_apply_sql(StreamApplyContext *context,
				 LogicalMessageMetadata *metadata,
				 const char *sql)
{
	PGSQL *pgsql = &(context->pgsql);

	switch (metadata->action)
	{
		case STREAM_ACTION_SWITCH:
		{
			log_debug("SWITCH from %X/%X to %X/%X",
					  LSN_FORMAT_ARGS(context->switchLSN),
					  LSN_FORMAT_ARGS(metadata->lsn));

			/*
			 * Track the SWITCH LSN, it helps to determine the next
			 * .sql file to apply.
			 */
			context->switchLSN = metadata->lsn;

			break;
		}

		case STREAM_ACTION_BEGIN:
		{
			if (metadata->lsn == InvalidXLogRecPtr ||
				IS_EMPTY_STRING_BUFFER(metadata->timestamp))
			{
				log_fatal("Failed to parse BEGIN message: %s", sql);
				return false;
			}

			bool txnCommitLSNFound = false;

			if (!readTxnCommitLSN(metadata,
								  context->paths.dir,
								  &txnCommitLSNFound))
			{
				log_error("Failed to read transaction metadata file");
				return false;
			}

			/*
			 * Few a time, BEGIN won't have a txnCommitLSN for the txn which
			 * spread across multiple WAL segments. We call that txn as
			 * a continuedTxn and allow it to be replayed until we encounter
			 * a COMMIT message.
			 *
			 * The lsn of a COMMIT message determines whether to keep txn or
			 * abort.
			 */
			context->continuedTxn = !txnCommitLSNFound;

			/* did we reach the starting LSN positions now? */
			if (!context->reachedStartPos)
			{
				/*
				 * compare previousLSN with COMMIT LSN to safely include
				 * complete transactions while skipping already applied
				 * changes.
				 *
				 * this is particularly useful at the beginnig where
				 * BEGIN LSN of some transactions could be less than
				 * `consistent_point`, but COMMIT LSN of those transactions
				 * is guaranteed to be greater.
				 *
				 * in case of interruption and this is the first
				 * transaction to be applied, previousLSN should be equal
				 * to the last transaction's COMMIT LSN or the LSN of
				 * non-transaction action. Therefore, this condition will
				 * still hold true.
				 */
				context->reachedStartPos =
					context->previousLSN < metadata->txnCommitLSN;
			}

			bool skip = !context->reachedStartPos && !context->continuedTxn;

			log_debug("BEGIN %lld LSN %X/%X @%s, previous LSN %X/%X, COMMIT LSN %X/%X %s",
					  (long long) metadata->xid,
					  LSN_FORMAT_ARGS(metadata->lsn),
					  metadata->timestamp,
					  LSN_FORMAT_ARGS(context->previousLSN),
					  LSN_FORMAT_ARGS(metadata->txnCommitLSN),
					  skip ? "[skipping]" : "");

			/*
			 * Check if we reached the endpos LSN already.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= metadata->lsn)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at BEGIN %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(metadata->lsn));

				return true;
			}

			/* actually skip this one if we didn't reach start pos yet */
			if (skip)
			{
				return true;
			}

			/*
			 * We're all good to replay that transaction, let's BEGIN and
			 * register our origin tracking on the target database.
			 */
			if (!pgsql_begin(pgsql))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * If this transaction is going to reach the endpos, then we're
			 * happy to wait until it's been sync'ed on-disk by Postgres on the
			 * target.
			 *
			 * In other words, use synchronous_commit = on.
			 */
			bool commitLSNreachesEndPos =
				context->endpos != InvalidXLogRecPtr &&
				!context->continuedTxn &&
				context->endpos <= metadata->txnCommitLSN;

			GUC *settings =
				commitLSNreachesEndPos || context->reachedEOF
				? applySettingsSync
				: applySettings;

			if (commitLSNreachesEndPos)
			{
				log_notice("BEGIN transaction with COMMIT LSN %X/%X which is "
						   "reaching endpos %X/%X, synchronous_commit is on",
						   LSN_FORMAT_ARGS(metadata->txnCommitLSN),
						   LSN_FORMAT_ARGS(context->endpos));
			}

			if (!pgsql_set_gucs(pgsql, settings))
			{
				log_error("Failed to set the apply GUC settings, "
						  "see above for details");
				return false;
			}

			context->transactionInProgress = true;

			break;
		}

		case STREAM_ACTION_ROLLBACK:
		{
			/* Rollback the transaction */
			if (!pgsql_execute(pgsql, "ROLLBACK"))
			{
				/* errors have already been logged */
				return false;
			}

			/* Reset the transactionInProgress after abort */
			context->transactionInProgress = false;

			/* Reevaluate reachedStartPos after rollback */
			context->reachedStartPos = false;

			break;
		}

		case STREAM_ACTION_COMMIT:
		{
			context->reachedStartPos = context->previousLSN < metadata->lsn;

			if (context->continuedTxn)
			{
				/*
				 * Write the transaction metadata file for continuedTxn.
				 * This file will be used for the resumed transaction
				 * to determine whether allow the transaction to be
				 * replayed or not.
				 * Without this, executing the same continuedTxn twice
				 * will result in duplicate key errors if the table has
				 * unique constraints.
				 */
				if (!writeTxnCommitMetadata(metadata, context->paths.dir))
				{
					log_error("Failed to write transaction metadata file, "
							  "see above for details");
					return false;
				}
			}

			if (!context->reachedStartPos)
			{
				/*
				 * Abort if we are not yet reachedStartPos and txn is a
				 * continuedTxn.
				 */
				if (context->continuedTxn)
				{
					log_notice("Skip(abort) applied transaction %lld LSN %X/%X "
							   "@%s, previous LSN %X/%X",
							   (long long) metadata->xid,
							   LSN_FORMAT_ARGS(metadata->lsn),
							   metadata->timestamp,
							   LSN_FORMAT_ARGS(context->previousLSN));

					/* Rollback the transaction */
					if (!pgsql_execute(pgsql, "ROLLBACK"))
					{
						/* errors have already been logged */
						return false;
					}

					/* Reset the transactionInProgress after abort */
					context->transactionInProgress = false;
					context->continuedTxn = false;
				}

				return true;
			}

			/*
			 * update replication progress with metadata->lsn, that is,
			 * transaction COMMIT LSN
			 */
			char lsn[PG_LSN_MAXLENGTH] = { 0 };

			sformat(lsn, sizeof(lsn), "%X/%X",
					LSN_FORMAT_ARGS(metadata->lsn));

			if (!pgsql_replication_origin_xact_setup(pgsql,
													 lsn,
													 metadata->timestamp))
			{
				log_error("Failed to setup apply transaction, "
						  "see above for details");
				return false;
			}

			log_trace("COMMIT %lld LSN %X/%X",
					  (long long) metadata->xid,
					  LSN_FORMAT_ARGS(metadata->lsn));


			/* calling pgsql_commit() would finish the connection, avoid */
			if (!pgsql_execute(pgsql, "COMMIT"))
			{
				/* errors have already been logged */
				return false;
			}

			context->transactionInProgress = false;
			context->previousLSN = metadata->lsn;

			/*
			 * At COMMIT time we might have reached the endpos: we know
			 * that already when endpos <= lsn. It's important to check
			 * that at COMMIT record time, because that record might be the
			 * last entry of the file we're applying.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= context->previousLSN)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at COMMIT %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));
				return true;
			}

			/*
			 * An idle source producing only KEEPALIVE should move the
			 * replay_lsn forward.
			 */
			if (!stream_apply_track_insert_lsn(context, metadata->lsn))
			{
				log_error("Failed to track target LSN position, "
						  "see above for details");
				return false;
			}

			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			log_debug("ENDPOS %X/%X found at %X/%X",
					  LSN_FORMAT_ARGS(metadata->lsn),
					  LSN_FORMAT_ARGS(context->previousLSN));

			/*
			 * It could be the current endpos, or the endpos of a previous
			 * run.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= metadata->lsn)
			{
				context->previousLSN = metadata->lsn;
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at ENDPOS %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));

				if (context->transactionInProgress)
				{
					if (!pgsql_execute(pgsql, "ROLLBACK"))
					{
						/* errors have already been logged */
						return false;
					}

					context->transactionInProgress = false;
				}

				return true;
			}

			break;
		}

		/*
		 * A KEEPALIVE message is replayed as its own transaction where the
		 * only thgin we do is call into the replication origin tracking
		 * API to advance our position on the target database.
		 */
		case STREAM_ACTION_KEEPALIVE:
		{
			/* did we reach the starting LSN positions now? */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				context->reachedStartPos =
					context->previousLSN < metadata->lsn;
			}

			/* in a transaction only the COMMIT LSN is tracked */
			if (context->transactionInProgress)
			{
				return true;
			}

			log_trace("KEEPALIVE LSN %X/%X @%s, previous LSN %X/%X %s",
					  LSN_FORMAT_ARGS(metadata->lsn),
					  metadata->timestamp,
					  LSN_FORMAT_ARGS(context->previousLSN),
					  context->reachedStartPos ? "" : "[skipping]");

			if (metadata->lsn == InvalidXLogRecPtr ||
				IS_EMPTY_STRING_BUFFER(metadata->timestamp))
			{
				log_fatal("Failed to parse KEEPALIVE message: %s", sql);
				return false;
			}

			/*
			 * Check if we reached the endpos LSN already. If the keepalive
			 * message is the endpos, still apply it: its only purpose is
			 * to maintain our replication origin tracking on the target
			 * database.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos < metadata->lsn)
			{
				context->reachedEndPos = true;
				context->previousLSN = metadata->lsn;

				log_notice("Apply reached end position %X/%X at KEEPALIVE %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));

				return true;
			}

			/* actually skip this one if we didn't reach start pos yet */
			if (!context->reachedStartPos)
			{
				return true;
			}

			/* skip KEEPALIVE message that won't make progress */
			if (metadata->lsn == context->previousLSN)
			{
				return true;
			}

			if (!pgsql_begin(pgsql))
			{
				/* errors have already been logged */
				return false;
			}

			char lsn[PG_LSN_MAXLENGTH] = { 0 };

			sformat(lsn, sizeof(lsn), "%X/%X",
					LSN_FORMAT_ARGS(metadata->lsn));

			if (!pgsql_replication_origin_xact_setup(pgsql,
													 lsn,
													 metadata->timestamp))
			{
				/* errors have already been logged */
				return false;
			}

			/* calling pgsql_commit() would finish the connection, avoid */
			if (!pgsql_execute(pgsql, "COMMIT"))
			{
				/* errors have already been logged */
				return false;
			}

			context->previousLSN = metadata->lsn;

			/*
			 * At COMMIT time we might have reached the endpos: we know
			 * that already when endpos <= lsn. It's important to check
			 * that at COMMIT record time, because that record might be the
			 * last entry of the file we're applying.
			 */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= context->previousLSN)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at KEEPALIVE %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));
				break;
			}

			if (!stream_apply_track_insert_lsn(context, metadata->lsn))
			{
				log_error("Failed to track target LSN position, "
						  "see above for details");
				return false;
			}

			break;
		}

		case STREAM_ACTION_INSERT:
		case STREAM_ACTION_UPDATE:
		case STREAM_ACTION_DELETE:
		{
			/*
			 * We still allow continuedTxn, COMMIT message determines whether
			 * to keep the transaction or abort it.
			 */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			uint32_t hash = metadata->hash;
			PreparedStmt *stmtHashTable = context->preparedStmt;
			PreparedStmt *stmt = NULL;

			HASH_FIND(hh, stmtHashTable, &hash, sizeof(hash), stmt);

			if (stmt == NULL)
			{
				char name[NAMEDATALEN] = { 0 };
				sformat(name, sizeof(name), "%x", metadata->hash);

				if (!pgsql_prepare(pgsql, name, metadata->stmt, 0, NULL))
				{
					/* errors have already been logged */
					return false;
				}

				PreparedStmt *stmt =
					(PreparedStmt *) calloc(1, sizeof(PreparedStmt));

				stmt->hash = hash;
				stmt->prepared = true;

				HASH_ADD(hh, stmtHashTable, hash, sizeof(hash), stmt);

				/* HASH_ADD can change the pointer in place, update */
				context->preparedStmt = stmtHashTable;
			}

			break;
		}

		case STREAM_ACTION_EXECUTE:
		{
			/*
			 * We still allow continuedTxn, COMMIT message determines whether
			 * to keep the transaction or abort it.
			 */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
			}

			uint32_t hash = metadata->hash;
			PreparedStmt *stmtHashTable = context->preparedStmt;
			PreparedStmt *stmt = NULL;

			HASH_FIND(hh, stmtHashTable, &hash, sizeof(hash), stmt);

			if (stmt == NULL)
			{
				log_warn("BUG: Failed to find statement %x in stmtHashTable",
						 hash);
			}

			char name[NAMEDATALEN] = { 0 };
			sformat(name, sizeof(name), "%x", metadata->hash);

			JSON_Value *js = json_parse_string(metadata->jsonBuffer);

			if (json_value_get_type(js) != JSONArray)
			{
				log_error("Failed to parse EXECUTE array: %s",
						  metadata->jsonBuffer);
				return false;
			}

			JSON_Array *jsArray = json_value_get_array(js);

			int count = json_array_get_count(jsArray);
			const char **paramValues =
				(const char **) calloc(count, sizeof(char *));

			if (paramValues == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			for (int i = 0; i < count; i++)
			{
				const char *value = json_array_get_string(jsArray, i);
				paramValues[i] = value;
			}

			if (!pgsql_execute_prepared(pgsql, name,
										count, paramValues,
										NULL, NULL))
			{
				/* errors have already been logged */
				return false;
			}

			free(paramValues);
			free(metadata->jsonBuffer);
			json_value_free(js);

			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			/*
			 * We still allow continuedTxn, COMMIT message determines whether
			 * to keep the transaction or abort it.
			 */
			if (!context->reachedStartPos && !context->continuedTxn)
			{
				return true;
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
			log_error("Failed to parse action %c for SQL query: %s",
					  metadata->action,
					  sql);

			return false;
		}
	}

	return true;
}


/*
 * setupReplicationOrigin ensures that a replication origin has been created on
 * the target database, and if it has been created previously then fetches the
 * previous LSN position it was at.
 *
 * Also setupReplicationOrigin calls pg_replication_origin_setup() in the
 * current connection.
 */
bool
setupReplicationOrigin(StreamApplyContext *context, bool logSQL)
{
	PGSQL *pgsql = &(context->pgsql);
	char *nodeName = context->origin;

	if (!pgsql_init(pgsql, context->connStrings->target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* we're going to send several replication origin commands */
	pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	/* we also might want to skip logging any SQL query that we apply */
	pgsql->logSQL = logSQL;

	/*
	 * Grab the Postgres server version on the target, we need to know that for
	 * being able to call pgsql_current_wal_insert_lsn using the right Postgres
	 * function name.
	 */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

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

	/*
	 * Fetch the replication origin LSN tracking, which is maintained in a
	 * transactional fashion with the SQL that's been replayed. It's the
	 * authoritative value for progress at reconnect, given that we use
	 * synchronous_commit off.
	 */
	uint64_t originLSN = InvalidXLogRecPtr;

	if (!pgsql_replication_origin_progress(pgsql, nodeName, true, &originLSN))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * The context->previousLSN may have been initialized already from the
	 * sentinel, when restarting a follow operation. For more details see
	 * function stream_apply_wait_for_sentinel().
	 */
	if (context->previousLSN == InvalidXLogRecPtr)
	{
		log_info("Setting up previous LSN from "
				 "replication origin \"%s\" progress at %X/%X",
				 nodeName,
				 LSN_FORMAT_ARGS(originLSN));

		context->previousLSN = originLSN;
	}
	else if (context->previousLSN != originLSN)
	{
		log_info("Setting up previous LSN from "
				 "replication origin \"%s\" progress at %X/%X, "
				 "overriding previous value %X/%X",
				 nodeName,
				 LSN_FORMAT_ARGS(originLSN),
				 LSN_FORMAT_ARGS(context->previousLSN));

		context->previousLSN = originLSN;
	}

	if (IS_EMPTY_STRING_BUFFER(context->sqlFileName))
	{
		if (!computeSQLFileName(context))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* compute the WAL filename that would host the previous LSN */
	log_debug("setupReplicationOrigin: replication origin \"%s\" "
			  "found at %X/%X, expected at \"%s\"",
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
 * stream_apply_init_context initializes our context from pieces.
 */
bool
stream_apply_init_context(StreamApplyContext *context,
						  DatabaseCatalog *sourceDB,
						  CDCPaths *paths,
						  ConnStrings *connStrings,
						  char *origin,
						  uint64_t endpos)
{
	context->sourceDB = sourceDB;
	context->paths = *paths;

	/*
	 * We have to consider both the --endpos command line option and the
	 * pgcopydb sentinel endpos value. Typically the sentinel is updated after
	 * the fact, but we still give precedence to --endpos.
	 *
	 * The endpos parameter here comes from the --endpos command line option,
	 * the context->endpos might have been set by calling
	 * stream_apply_wait_for_sentinel() earlier (when in STREAM_MODE_PREFETCH).
	 */
	if (endpos != InvalidXLogRecPtr)
	{
		if (context->endpos != InvalidXLogRecPtr && context->endpos != endpos)
		{
			log_warn("Option --endpos %X/%X is used, "
					 "even when the pgcopydb sentinel endpos was set to %X/%X",
					 LSN_FORMAT_ARGS(endpos),
					 LSN_FORMAT_ARGS(context->endpos));
		}
		context->endpos = endpos;
	}

	context->reachedStartPos = false;
	context->continuedTxn = false;
	context->reachedEOF = false;

	context->connStrings = connStrings;

	strlcpy(context->origin, origin, sizeof(context->origin));

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

	uint64_t switchLSN = context->switchLSN;

	/*
	 * If we haven't switched WAL yet, then we're still at the previousLSN
	 * position.
	 */
	if (switchLSN == InvalidXLogRecPtr)
	{
		switchLSN = context->previousLSN;
	}

	if (context->WalSegSz == 0)
	{
		log_error("Failed to compute the SQL filename for LSN %X/%X "
				  "without context->wal_segment_size",
				  LSN_FORMAT_ARGS(switchLSN));
		return false;
	}

	XLByteToSeg(switchLSN, segno, context->WalSegSz);
	XLogFileName(context->wal, context->system.timeline, segno, context->WalSegSz);

	sformat(context->sqlFileName, sizeof(context->sqlFileName),
			"%s/%s.sql",
			context->paths.dir,
			context->wal);

	log_debug("computeSQLFileName: %X/%X \"%s\"",
			  LSN_FORMAT_ARGS(switchLSN),
			  context->sqlFileName);

	return true;
}


/*
 * parseSQLAction returns the action that is implemented in the given SQL
 * query.
 */
bool
parseSQLAction(const char *query, LogicalMessageMetadata *metadata)
{
	metadata->action = STREAM_ACTION_UNKNOWN;

	if (strcmp(query, "") == 0)
	{
		return true;
	}

	char *message = NULL;
	char *begin = strstr(query, OUTPUT_BEGIN);
	char *commit = strstr(query, OUTPUT_COMMIT);
	char *rollback = strstr(query, OUTPUT_ROLLBACK);
	char *switchwal = strstr(query, OUTPUT_SWITCHWAL);
	char *keepalive = strstr(query, OUTPUT_KEEPALIVE);
	char *endpos = strstr(query, OUTPUT_ENDPOS);

	/* do we have a BEGIN or a COMMIT message to parse metadata of? */
	if (query == begin)
	{
		metadata->action = STREAM_ACTION_BEGIN;
		message = begin + strlen(OUTPUT_BEGIN);
	}
	else if (query == commit)
	{
		metadata->action = STREAM_ACTION_COMMIT;
		message = commit + strlen(OUTPUT_COMMIT);
	}
	else if (query == rollback)
	{
		metadata->action = STREAM_ACTION_ROLLBACK;
		message = rollback + strlen(OUTPUT_ROLLBACK);
	}
	else if (query == switchwal)
	{
		metadata->action = STREAM_ACTION_SWITCH;
		message = switchwal + strlen(OUTPUT_SWITCHWAL);
	}
	else if (query == keepalive)
	{
		metadata->action = STREAM_ACTION_KEEPALIVE;
		message = keepalive + strlen(OUTPUT_KEEPALIVE);
	}
	else if (query == endpos)
	{
		metadata->action = STREAM_ACTION_ENDPOS;
		message = endpos + strlen(OUTPUT_ENDPOS);
	}

	if (message != NULL)
	{
		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, true))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		json_value_free(json);

		return true;
	}

	/*
	 * So the SQL Action is a DML (or a TRUNCATE).
	 */
	size_t tLen = sizeof(TRUNCATE) - 1;
	size_t pLen = sizeof(PREPARE) - 1;
	size_t eLen = sizeof(EXECUTE) - 1;

	if (strncmp(query, TRUNCATE, tLen) == 0)
	{
		metadata->action = STREAM_ACTION_TRUNCATE;
	}
	else if (strncmp(query, PREPARE, pLen) == 0)
	{
		char *spc = strchr(query + pLen, ' ');

		if (spc == NULL)
		{
			log_error("Failed to parse PREPARE statement: %s", query);
			return false;
		}

		/* make a copy of just the hexadecimal string */
		int len = spc - (query + pLen);
		char str[BUFSIZE] = { 0 };

		sformat(str, sizeof(str), "%.*s", len, query + pLen);

		uint32_t hash = 0;

		if (!hexStringToUInt32(str, &hash))
		{
			log_error("Failed to parse PREPARE statement name: %s", query);
			return false;
		}

		metadata->hash = hash;

		size_t iLen = sizeof(INSERT) - 1;
		size_t uLen = sizeof(UPDATE) - 1;
		size_t dLen = sizeof(DELETE) - 1;

		if (strncmp(spc + 1, INSERT, iLen) == 0)
		{
			/* skip ' AS ' and point to INSERT */
			metadata->stmt = spc + 1 + 3;
			metadata->action = STREAM_ACTION_INSERT;
		}
		else if (strncmp(spc + 1, UPDATE, uLen) == 0)
		{
			/* skip ' AS ' and point to UPDATE */
			metadata->stmt = spc + 1 + 3;
			metadata->action = STREAM_ACTION_UPDATE;
		}
		else if (strncmp(spc + 1, DELETE, dLen) == 0)
		{
			/* skip ' AS ' and point to DELETE */
			metadata->stmt = spc + 1 + 3;
			metadata->action = STREAM_ACTION_DELETE;
		}
	}
	else if (strncmp(query, EXECUTE, eLen) == 0)
	{
		metadata->action = STREAM_ACTION_EXECUTE;

		char *json = strchr(query + eLen, '[');

		if (json == NULL)
		{
			log_error("Failed to parse EXECUTE statement: %s", query);
			return false;
		}

		/* make a copy of just the hexadecimal string */
		int len = json - (query + eLen);
		char str[BUFSIZE] = { 0 };

		sformat(str, sizeof(str), "%.*s", len, query + pLen);

		uint32_t hash = 0;

		if (!hexStringToUInt32(str, &hash))
		{
			log_error("Failed to parse EXECUTE statement name: %s", query);
			return false;
		}

		metadata->hash = hash;

		/* chomp ; at the end of the query string */
		len = strlen(json) - 1;
		size_t bytes = len + 1;

		metadata->jsonBuffer = (char *) calloc(bytes, sizeof(char));

		if (metadata->jsonBuffer == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		sformat(metadata->jsonBuffer, bytes, "%.*s", len, json);
	}

	if (metadata->action == STREAM_ACTION_UNKNOWN)
	{
		log_error("Failed to parse action from query: %s", query);
		return false;
	}

	return true;
}


/*
 * stream_apply_track_insert_lsn tracks the current pg_current_wal_insert_lsn()
 * location on the target system right after a COMMIT; of a transaction that
 * was assigned sourceLSN on the source system.
 */
bool
stream_apply_track_insert_lsn(StreamApplyContext *context, uint64_t sourceLSN)
{
	LSNTracking *lsn_tracking = (LSNTracking *) calloc(1, sizeof(LSNTracking));

	if (lsn_tracking == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	lsn_tracking->sourceLSN = sourceLSN;

	if (!pgsql_current_wal_insert_lsn(&(context->pgsql),
									  &(lsn_tracking->insertLSN)))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("stream_apply_track_insert_lsn: %X/%X :: %X/%X",
			  LSN_FORMAT_ARGS(sourceLSN),
			  LSN_FORMAT_ARGS(lsn_tracking->insertLSN));

	/* update the linked list */
	lsn_tracking->previous = context->lsnTrackingList;
	context->lsnTrackingList = lsn_tracking;

	return true;
}


/*
 * stream_apply_find_durable_lsn fetches the LSN for the current durable
 * location on the target system, and finds the greatest sourceLSN with an
 * associated insertLSN that's before the current (durable) write location.
 */
bool
stream_apply_find_durable_lsn(StreamApplyContext *context, uint64_t *durableLSN)
{
	uint64_t flushLSN = InvalidXLogRecPtr;

	if (!stream_fetch_current_lsn(&flushLSN,
								  context->connStrings->target_pguri,
								  PGSQL_CONN_SOURCE))
	{
		log_error("Failed to retrieve current WAL positions, "
				  "see above for details");
		return false;
	}

	bool found = false;
	LSNTracking *current = context->lsnTrackingList;

	for (; current != NULL; current = current->previous)
	{
		if (current->insertLSN <= flushLSN)
		{
			found = true;
			*durableLSN = current->sourceLSN;
			break;
		}
	}

	if (!found)
	{
		*durableLSN = InvalidXLogRecPtr;

		log_debug("Failed to find a durable source LSN for target LSN %X/%X",
				  LSN_FORMAT_ARGS(flushLSN));

		return false;
	}

	log_debug("stream_apply_find_durable_lsn(%X/%X): %X/%X :: %X/%X",
			  LSN_FORMAT_ARGS(flushLSN),
			  LSN_FORMAT_ARGS(current->sourceLSN),
			  LSN_FORMAT_ARGS(current->insertLSN));

	/* clean-up the lsn tracking list */
	LSNTracking *tail = current->previous;
	current->previous = NULL;

	while (tail != NULL)
	{
		LSNTracking *previous = tail->previous;
		free(tail);
		tail = previous;
	}

	return true;
}


/*
 * readTxnCommitLSN ensures metadata has transaction COMMIT LSN by fetching it
 * from metadata file if it is not present
 */
static bool
readTxnCommitLSN(LogicalMessageMetadata *metadata,
				 const char *dir,
				 bool *txnCommitLSNFound)
{
	/* if txnCommitLSN is invalid, then fetch it from txn metadata file */
	if (metadata->txnCommitLSN != InvalidXLogRecPtr)
	{
		*txnCommitLSNFound = true;
		return true;
	}

	char txnfilename[MAXPGPATH] = { 0 };

	if (!computeTxnMetadataFilename(metadata->xid,
									dir,
									txnfilename))
	{
		/* errors have already been logged */
		return false;
	}

	if (!file_exists(txnfilename))
	{
		*txnCommitLSNFound = false;
		return true;
	}

	log_debug("stream_apply_sql: BEGIN message without a commit LSN, "
			  "fetching commit LSN from transaction metadata file \"%s\"",
			  txnfilename);

	LogicalMessageMetadata txnMetadata = { .xid = metadata->xid };

	if (!parseTxnMetadataFile(txnfilename, &txnMetadata))
	{
		/* errors have already been logged */
		return false;
	}

	*txnCommitLSNFound = true;
	metadata->txnCommitLSN = txnMetadata.txnCommitLSN;

	return true;
}


/*
 * parseTxnMetadataFile returns the transaction metadata content for the given
 * metadata filename.
 */
static bool
parseTxnMetadataFile(const char *filename, LogicalMessageMetadata *metadata)
{
	/* store xid as it will be overwritten while parsing metadata */
	uint32_t xid = metadata->xid;

	if (xid == 0)
	{
		log_error("BUG: parseTxnMetadataFile is called with "
				  "transaction xid: %lld", (long long) xid);
		return false;
	}

	char *txnMetadataContent = NULL;
	long size = 0L;

	if (!read_file(filename, &txnMetadataContent, &size))
	{
		/* errors have already been logged */
		return false;
	}

	JSON_Value *json = json_parse_string(txnMetadataContent);

	if (!parseMessageMetadata(metadata, txnMetadataContent, json, true))
	{
		/* errors have already been logged */
		json_value_free(json);
		return false;
	}

	json_value_free(json);
	free(txnMetadataContent);

	if (metadata->txnCommitLSN == InvalidXLogRecPtr ||
		metadata->xid != xid ||
		IS_EMPTY_STRING_BUFFER(metadata->timestamp))
	{
		log_error("Failed to parse metadata for transaction metadata file "
				  "\"%s\": %s", filename, txnMetadataContent);
		return false;
	}

	return true;
}


/*
 *  computeTxnMetadataFilename computes the file path for transaction metadata
 *  based on its transaction id
 */
static bool
computeTxnMetadataFilename(uint32_t xid, const char *dir, char *filename)
{
	if (dir == NULL)
	{
		log_error("BUG: computeTxnMetadataFilename is called with "
				  "directory: NULL");
		return false;
	}

	if (xid == 0)
	{
		log_error("BUG: computeTxnMetadataFilename is called with "
				  "transaction xid: %lld", (long long) xid);
		return false;
	}

	sformat(filename, MAXPGPATH, "%s/%lld.json", dir, (long long) xid);

	return true;
}


/*
 * writeTxnCommitMetadata writes the transaction metadata to a file in the given
 * directory
 */
static bool
writeTxnCommitMetadata(LogicalMessageMetadata *mesg, const char *dir)
{
	char txnfilename[MAXPGPATH] = { 0 };

	if (mesg->action != STREAM_ACTION_COMMIT)
	{
		log_error("BUG: writeTxnCommitMetadata is called with "
				  "action: %s", StreamActionToString(mesg->action));
		return false;
	}

	if (!computeTxnMetadataFilename(mesg->xid, dir, txnfilename))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("stream_write_commit_metadata_file: writing transaction "
			  "metadata file \"%s\" with commit lsn %X/%X",
			  txnfilename,
			  LSN_FORMAT_ARGS(mesg->lsn));

	char contents[BUFSIZE] = { 0 };

	sformat(contents, BUFSIZE,
			"{\"xid\":%lld,\"commit_lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			(long long) mesg->xid,
			LSN_FORMAT_ARGS(mesg->lsn),
			mesg->timestamp);

	/* write the metadata to txnfilename */
	if (!write_file(contents, strlen(contents), txnfilename))
	{
		log_error("Failed to write file \"%s\"", txnfilename);
		return false;
	}

	return true;
}
