/*
 * src/bin/pgcopydb/ld_apply.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/select.h>
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
#include "ld_store.h"
#include "ld_stream.h"
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

static bool setupConnection(PGSQL *pgsql, StreamApplyContext *context);

static bool stream_apply_dml(StreamApplyContext *context, ReplayDBStmt *s);
static bool stream_apply_transaction(StreamApplyContext *context,
									 uint32_t xid,
									 uint64_t begin_id,
									 uint64_t commitLSN);

bool stream_apply_replaydb(StreamSpecs *specs, StreamApplyContext *context);

/*
 * stream_apply_catchup applies CDC changes stored in the SQLite replayDB to
 * the target database, catching up from the last-known replay LSN position.
 *
 * It replaces the old file-based approach (read .sql files written by
 * pgcopydb stream prefetch/transform) with a direct read from the replay
 * and stmt tables of the replayDB SQLite file.
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
		log_notice("Apply mode is still disabled, quitting now");
		(void) stream_apply_cleanup(&context);
		return true;
	}

	/*
	 * In catchup-only mode (no live pipe from receive), read the receive
	 * process's pipeline_state to learn where it stopped.  Use that LSN as
	 * our endpos if the caller didn't supply one — this is how apply knows
	 * when to stop without an ENDPOS marker in the replay table.
	 */
	if (specs->pipe_rt[0] < 0 && context.endpos == InvalidXLogRecPtr)
	{
		PipelineStateEntry rs = { 0 };

		if (pipeline_state_get(specs->sourceDB, "receive", &rs) &&
			rs.process_name[0] != '\0' &&
			strcmp(rs.run_state, "done") == 0 &&
			rs.run_end_lsn != InvalidXLogRecPtr)
		{
			context.endpos = rs.run_end_lsn;
			log_info("Apply catchup: using receive run_end_lsn %X/%X as endpos",
					 LSN_FORMAT_ARGS(context.endpos));
		}
	}

	/*
	 * Initialise the transform context once (opens target connection for
	 * identifier quoting and prepares the generated-columns cache).
	 * Then do an initial pass to populate replayDB from whatever outputDB
	 * already contains (in CATCHUP mode this covers everything; in FOLLOW
	 * mode the main loop will call stream_transform_from_outputdb() again
	 * as more rows arrive in outputDB).
	 */
	if (!stream_transform_context_init(specs))
	{
		log_error("Failed to initialize inline transform context");
		return false;
	}

	/*
	 * The single apply driver loop (stream_apply_replaydb) performs the inline
	 * transform stage itself, one dispatch per iteration, so there is no
	 * separate pre-loop transform pass: the first iteration primes replayDB
	 * from whatever outputDB already contains, and a mid-transaction endpos is
	 * handled inside the loop like any other terminal condition.
	 */
	(void) pipeline_state_start(specs->sourceDB, "apply", context.previousLSN);

	bool success = stream_apply_replaydb(specs, &context);

	(void) pipeline_state_end(specs->sourceDB, "apply",
							  context.previousLSN, success);
	(void) stream_apply_cleanup(&context);

	return success;
}


/* how often (in driver iterations) to checkpoint the in-memory apply state */
#define APPLY_STATE_SYNC_EVERY 64

/*
 * stream_apply_state_progressed returns true when the in-memory apply pipeline
 * state advanced between two snapshots — i.e. the transform stage wrote a new
 * transaction to replayDB, or the apply stage committed one to the target.
 * The driver uses this to avoid declaring "done" in the same iteration in
 * which work was actually produced.
 */
static bool
stream_apply_state_progressed(const PipelineStateEntry *before,
							  const PipelineStateEntry *after)
{
	return before->last_xid != after->last_xid ||
		   before->last_txn_begin_lsn != after->last_txn_begin_lsn ||
		   before->last_txn_end_lsn != after->last_txn_end_lsn ||
		   before->last_txn_complete != after->last_txn_complete ||
		   before->last_txn_processed != after->last_txn_processed ||
		   before->run_end_lsn != after->run_end_lsn;
}


/*
 * stream_apply_replaydb is the main catchup loop for the SQLite-based CDC
 * pipeline.  It drives a transaction-oriented outer loop:
 *
 *   1. Fetch the next event (BEGIN or non-txn) from the replayDB.
 *   2. For BEGIN rows, apply the full transaction via stream_apply_transaction().
 *   3. For non-txn events (KEEPALIVE/SWITCH/ENDPOS), call stream_apply_sql().
 *
 * The outer query only returns BEGIN rows whose COMMIT has already been stored
 * (endlsn > previousLSN), so the loop never sees a half-written transaction.
 *
 * Three explicit guards before each transaction:
 *   Guard 1 – commitLSN <= previousLSN:  already applied, skip.
 *   Guard 2 – endpos < beginLSN:         endpos before this txn, stop.
 *   Guard 3 – beginLSN < endpos < commitLSN: endpos is mid-transaction;
 *             we still apply the full transaction and stop afterwards.
 *
 * The loop exits when no more rows, endpos is reached, or a signal arrives.
 */
bool
stream_apply_replaydb(StreamSpecs *specs, StreamApplyContext *context)
{
	DatabaseCatalog *replayDB = specs->replayDB;

	if (replayDB == NULL || replayDB->db == NULL)
	{
		log_error("BUG: stream_apply_replaydb: replayDB is NULL");
		return false;
	}

	log_info("Applying CDC changes from replayDB \"%s\" after LSN %X/%X",
			 replayDB->dbfile,
			 LSN_FORMAT_ARGS(context->previousLSN));

	context->replayDB = replayDB;

	bool success = true;
	ReplayDBStmt s = { 0 };
	bool lock_held = false;

	/*
	 * pipe_rt[0] carries the out-of-band "receive done" lifecycle signal.
	 * When receive finishes it writes the final LSN and closes the write end.
	 * We select() on this fd while waiting for more replay rows, so we wake
	 * up immediately rather than sleeping 500 ms.
	 *
	 * In catchup-only mode (no live pipe) pipe_rd == -1 and we fall back to
	 * the sentinel.endpos / previousLSN comparison.
	 */
	int pipe_rd = (specs->pipe_rt[0] > 0) ? specs->pipe_rt[0] : -1;

	/*
	 * In-memory apply pipeline state.  Both data-processing stages update it as
	 * they work: the inline-transform hook (via specs->private.applyState) when
	 * it writes a complete transaction to replayDB, and the apply stage below
	 * when it commits one to the target.  Each iteration snapshots it before
	 * dispatching work and compares afterwards to decide progress.  It is
	 * checkpointed to sourceDB every APPLY_STATE_SYNC_EVERY iterations and once
	 * more at end of processing.
	 */
	PipelineStateEntry current = { 0 };
	strlcpy(current.process_name, "apply", sizeof(current.process_name));
	current.run_start_lsn = context->previousLSN;
	current.last_txn_end_lsn = context->previousLSN;
	current.run_end_lsn = context->previousLSN;

	specs->private.applyState = &current;

	int syncCounter = 0;

	for (;;)
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_info("Apply process received a shutdown signal, stopping");
			break;
		}

		/* ── check for upstream-done signal ──────────────────────────── */
		if (!specs->upstream_done && pipe_rd >= 0)
		{
			fd_set rfds;
			FD_ZERO(&rfds);
			FD_SET(pipe_rd, &rfds);
			struct timeval tv = { .tv_sec = 0, .tv_usec = 100 * 1000 };

			int nready = select(pipe_rd + 1, &rfds, NULL, NULL, &tv);
			if (nready < 0 && errno != EINTR)
			{
				log_error("apply select() on pipe: %m");
				return false;
			}
			if (nready > 0 && FD_ISSET(pipe_rd, &rfds))
			{
				(void) stream_recv_upstream_done(specs, pipe_rd);
				pipe_rd = -1;

				/* use signal LSN as our endpos if not already set */
				if (context->endpos == InvalidXLogRecPtr &&
					specs->upstream_done_lsn != InvalidXLogRecPtr)
				{
					context->endpos = specs->upstream_done_lsn;
					log_info("Apply endpos set from receive signal: %X/%X",
							 LSN_FORMAT_ARGS(context->endpos));
				}
			}
		}

		/*
		 * Lock the replayDB only while querying for the next event.
		 * This allows the inline transform (ld_store_iter_output in the
		 * "no rows" path above) to write new rows without contention,
		 * avoiding deadlock while apply sleeps waiting for more data.
		 */
		if (!semaphore_lock(&(replayDB->sema)))
		{
			/* errors have already been logged */
			return false;
		}
		lock_held = true;

		if (!ld_store_replay_next_event(replayDB, context->previousLSN, &s))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		if (s.action == STREAM_ACTION_UNKNOWN)
		{
			/*
			 * No more rows available in replayDB.  Release the lock so that
			 * the inline transform can write new rows, then attempt to
			 * transform more data from outputDB into replayDB.
			 */
			semaphore_unlock(&(replayDB->sema));
			lock_held = false;

			/*
			 * Replay is drained.  Snapshot the in-memory apply state, then
			 * dispatch the transform stage (outputDB -> replayDB).  The
			 * inline-transform hook updates `current` for every complete
			 * transaction it writes, so comparing against this snapshot tells
			 * us whether the transform stage made progress this pass.
			 *
			 * The call is safe to repeat: ld_store_iter_output uses
			 * specs->sentinel.replay_lsn as a cursor and only processes rows
			 * newer than the last committed apply position.  Afterwards
			 * specs->private.midTxnEndpos may be set, meaning receive finished
			 * with an uncommitted transaction open.
			 */
			PipelineStateEntry before = current;

			if (!stream_transform_from_outputdb(specs, context->previousLSN))
			{
				log_warn("Failed to transform from outputDB, will retry");
			}

			if (specs->private.midTxnEndpos)
			{
				log_notice("Apply: inline transform detected mid-transaction "
						   "endpos — endpos %X/%X fell inside an uncommitted "
						   "transaction; stopping at last commit %X/%X "
						   "(replay_lsn stays at last commit boundary)",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));

				/*
				 * Do NOT advance previousLSN to endpos: endpos is inside
				 * an uncommitted transaction and has no backing Postgres
				 * COMMIT.  Leave replay_lsn at the last real commit.
				 * follow_reached_endpos() detects completion via
				 * pipeline_state["apply"].run_state = 'done'.
				 */
				context->reachedEndPos = true;
				(void) stream_apply_sync_sentinel(context, false);
				break;
			}

			if (!stream_apply_sync_sentinel(context, false))
			{
				log_warn("Failed to sync sentinel when caught up with replayDB");
			}

			/* Exit if endpos has been reached via previousLSN */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= context->previousLSN)
			{
				context->reachedEndPos = true;
				log_notice("Apply reached end position %X/%X at LSN %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(context->previousLSN));
				break;
			}

			/*
			 * Driver invariant: if the transform stage produced new work this
			 * pass, loop back and apply it before deciding we are done.  This
			 * is precisely what prevents declaring "done" in the same
			 * iteration in which transform wrote a fully-committed transaction
			 * to replayDB (the bug that dropped the last transaction(s) when
			 * endpos landed on a commit boundary).
			 */
			if (stream_apply_state_progressed(&before, &current))
			{
				/* checkpoint progress to sourceDB once in a while */
				if ((++syncCounter % APPLY_STATE_SYNC_EVERY) == 0)
				{
					(void) pipeline_state_sync(specs->sourceDB, &current);
				}

				continue;
			}

			/*
			 * No new work from the transform stage.  Before checking whether
			 * receive has finished, see if the receive process rotated to a new
			 * output.db file.  If the current output.db has been closed
			 * (done_time_epoch IS NOT NULL in cdc_files) and a successor file
			 * exists, advance to it immediately rather than spinning.
			 */
			{
				bool advanced = false;

				if (!ld_store_advance_cdc_files(specs, &advanced))
				{
					log_warn("Failed to check/advance CDC file rotation, "
							 "will retry");

					/* non-fatal — try again next iteration */
				}
				else if (advanced)
				{
					/*
					 * Successfully switched to the next output.db/replay.db
					 * pair.  Update local aliases so this loop iteration
					 * processes the new file.
					 */
					replayDB = specs->replayDB;
					context->replayDB = replayDB;
					continue;
				}
			}

			/*
			 * No new work from the transform stage.  If receive has finished
			 * there is nothing left to do: everything up to the last committed
			 * transaction boundary at or before endpos has been applied.
			 *
			 * In FOLLOW mode the upstream_done flag signals receive completion;
			 * in CATCHUP mode (no pipe) consult receive's pipeline_state row.
			 */
			if (context->endpos != InvalidXLogRecPtr)
			{
				bool receive_done = specs->upstream_done;

				if (!receive_done && pipe_rd < 0)
				{
					/* Catchup mode: consult the durable pipeline_state record */
					PipelineStateEntry ts = { 0 };

					if (pipeline_state_get(specs->sourceDB,
										   "receive", &ts) &&
						ts.process_name[0] != '\0' &&
						strcmp(ts.run_state, "done") == 0)
					{
						receive_done = true;
					}
				}

				if (receive_done)
				{
					log_notice("Apply: receive done and no more complete "
							   "transactions to transform — stopping at last "
							   "commit %X/%X",
							   LSN_FORMAT_ARGS(context->previousLSN));

					context->reachedEndPos = true;
					(void) stream_apply_sync_sentinel(context, false);
					break;
				}
			}

			/*
			 * Not caught up yet.  In pipe mode the select() ceiling gives
			 * ≤100 ms latency; in catchup-only mode sleep briefly.
			 */
			if (pipe_rd < 0 && !specs->upstream_done)
			{
				pg_usleep(100 * 1000);  /* 100 ms */
			}
			continue;
		}

		/*
		 * We have a row to process. Release the lock now since we've already
		 * fetched the row data. This allows transform to keep writing while
		 * we apply the row.
		 */
		semaphore_unlock(&(replayDB->sema));
		lock_held = false;

		if (s.action == STREAM_ACTION_BEGIN)
		{
			uint64_t beginLSN = s.lsn;
			uint64_t commitLSN = s.endlsn;
			uint32_t xid = s.xid;
			uint64_t begin_id = s.id;

			/* Guard 1: already applied — should not happen given the query,
			 * but be safe on restart edge cases. */
			if (commitLSN <= context->previousLSN)
			{
				log_debug("Skip already-applied txn %u commitLSN %X/%X "
						  "<= previousLSN %X/%X",
						  xid,
						  LSN_FORMAT_ARGS(commitLSN),
						  LSN_FORMAT_ARGS(context->previousLSN));
				continue;
			}

			/* Guard 2: endpos lies entirely before this transaction. */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos < beginLSN)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X before "
						   "xid %u beginLSN %X/%X — stopping at "
						   "last commit %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   xid,
						   LSN_FORMAT_ARGS(beginLSN),
						   LSN_FORMAT_ARGS(context->previousLSN));

				/*
				 * Do NOT advance previousLSN to endpos.  endpos is between
				 * two transactions and has no backing Postgres COMMIT.
				 * Leave replay_lsn at the last real commit so the Postgres
				 * origin position remains accurate.
				 *
				 * follow_reached_endpos() detects this case via its secondary
				 * pipeline_state check (both transform and apply 'done').
				 */
				(void) stream_apply_sync_sentinel(context, false);
				break;
			}

			/*
			 * Guard 3: endpos falls mid-transaction.
			 *
			 * beginLSN < endpos < commitLSN means the endpos checkpoint was
			 * set to a WAL position inside this uncommitted transaction.  We
			 * must apply the full transaction (cannot partially apply it), so
			 * we do not stop here.  After the commit we will set reachedEndPos.
			 *
			 * Note: when beginLSN == commitLSN (single-DML autocommit) the
			 * strict inequalities make this condition false — correct, since
			 * there is no "inside" the transaction to straddle.
			 */
			bool midTxnEndpos =
				context->endpos != InvalidXLogRecPtr &&
				beginLSN < context->endpos &&
				context->endpos < commitLSN;

			if (midTxnEndpos)
			{
				log_notice("Apply endpos %X/%X falls mid-transaction "
						   "(xid %u begin %X/%X commit %X/%X); "
						   "applying full transaction then stopping",
						   LSN_FORMAT_ARGS(context->endpos),
						   xid,
						   LSN_FORMAT_ARGS(beginLSN),
						   LSN_FORMAT_ARGS(commitLSN));
			}

			/* Record (in-memory) that a transaction is about to be applied. */
			current.last_xid = xid;
			current.last_txn_begin_lsn = beginLSN;
			current.last_txn_end_lsn = InvalidXLogRecPtr;
			current.last_txn_complete = false;
			current.last_txn_processed = false;

			if (!stream_apply_transaction(context, xid, begin_id, commitLSN))
			{
				/* errors have already been logged */
				success = false;
				break;
			}

			/* Transaction applied to target: record completion in-memory. */
			current.last_txn_end_lsn = commitLSN;
			current.last_txn_complete = true;
			current.last_txn_processed = true;
			current.run_end_lsn = context->previousLSN;

			/* After commit: check if endpos has been reached. */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= context->previousLSN)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X after "
						   "committing xid %u at %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   xid,
						   LSN_FORMAT_ARGS(context->previousLSN));

				/* ensure replay_lsn advances to endpos for follow_reached_endpos */
				if (context->previousLSN < context->endpos)
				{
					context->previousLSN = context->endpos;
				}
				(void) stream_apply_sync_sentinel(context, false);
				break;
			}
		}
		else
		{
			/*
			 * Non-transactional event: KEEPALIVE only.
			 * ENDPOS rows no longer appear in the replay table.
			 * SWITCH is no longer emitted by the pipeline.
			 */
			LogicalMessageMetadata metadata = { 0 };

			metadata.action = s.action;
			metadata.xid = s.xid;
			metadata.lsn = s.lsn;

			if (!IS_EMPTY_STRING_BUFFER(s.timestamp))
			{
				strlcpy(metadata.timestamp, s.timestamp,
						sizeof(metadata.timestamp));
			}

			if (!stream_apply_sql(context, &metadata, ""))
			{
				log_error("Failed to apply replayDB event (action %c, LSN %X/%X)",
						  s.action,
						  LSN_FORMAT_ARGS(s.lsn));
				success = false;
				break;
			}

			if (s.action == STREAM_ACTION_KEEPALIVE)
			{
				bool findDurableLSN = false;

				if (!stream_apply_sync_sentinel(context, findDurableLSN))
				{
					log_warn("Failed to sync sentinel at LSN %X/%X, "
							 "will retry on next iteration",
							 LSN_FORMAT_ARGS(context->previousLSN));
				}

				/* record progress in the in-memory apply state */
				current.run_end_lsn = context->previousLSN;
			}

			if (context->reachedEndPos)
			{
				log_info("Apply reached endpos %X/%X",
						 LSN_FORMAT_ARGS(context->endpos));

				/* ensure replay_lsn advances to endpos for follow_reached_endpos */
				if (context->previousLSN < context->endpos)
				{
					context->previousLSN = context->endpos;
				}
				(void) stream_apply_sync_sentinel(context, false);
				break;
			}
		}
	}

	if (lock_held)
	{
		(void) semaphore_unlock(&(replayDB->sema));
	}

	/* enforce a final durable checkpoint of the in-memory apply state */
	(void) pipeline_state_sync(specs->sourceDB, &current);
	specs->private.applyState = NULL;

	return success;
}


/*
 * stream_apply_transaction applies a single committed transaction to the
 * target database.  It opens a Postgres transaction, iterates all DML rows
 * for xid in replay order (starting from begin_id), and commits with the
 * replication origin set to commitLSN.
 *
 * When endpos <= commitLSN, synchronous_commit = on is enabled so the origin
 * position is durably flushed before the caller signals endpos reached.
 *
 * Returns false on error; on success context->previousLSN = commitLSN.
 */
static bool
stream_apply_transaction(StreamApplyContext *context,
						 uint32_t xid,
						 uint64_t begin_id,
						 uint64_t commitLSN)
{
	PGSQL *applyPgConn = &(context->applyPgConn);
	DatabaseCatalog *replayDB = context->replayDB;

	bool sync = (context->endpos != InvalidXLogRecPtr &&
				 context->endpos <= commitLSN);

	if (!pgsql_begin(applyPgConn))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_set_gucs(applyPgConn,
						sync ? applySettingsSync : applySettings))
	{
		/* errors have already been logged */
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		return false;
	}

	context->transactionInProgress = true;
	context->reachedStartPos = true;
	context->continuedTxn = false;

	ReplayDBReplayTxnIterator iter = { 0 };

	iter.catalog = replayDB;
	iter.xid = xid;
	iter.begin_id = begin_id;

	if (!ld_store_iter_replay_txn_init(&iter))
	{
		/* errors have already been logged */
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		context->transactionInProgress = false;
		return false;
	}

	bool success = true;
	char commitTimestamp[PG_MAX_TIMESTAMP] = { 0 };

	for (;;)
	{
		if (!ld_store_iter_replay_txn_next(&iter))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		ReplayDBStmt *s = iter.current;

		if (s == NULL)
		{
			log_error("BUG: stream_apply_transaction: no COMMIT/ROLLBACK row "
					  "found for xid %u (begin_id %lld commitLSN %X/%X)",
					  xid, (long long) begin_id,
					  LSN_FORMAT_ARGS(commitLSN));
			success = false;
			break;
		}

		if (s->action == STREAM_ACTION_BEGIN)
		{
			/* skip: the outer loop already processed this */
			continue;
		}

		if (s->action == STREAM_ACTION_COMMIT)
		{
			strlcpy(commitTimestamp, s->timestamp, sizeof(commitTimestamp));
			break;
		}

		if (s->action == STREAM_ACTION_ROLLBACK)
		{
			/*
			 * Rolled-back transactions are normally not emitted by logical
			 * decoding, but handle defensively.  We abort the target
			 * transaction and leave previousLSN unchanged.
			 */
			log_notice("Rolling back transaction %u (rollback LSN %X/%X)",
					   xid, LSN_FORMAT_ARGS(s->lsn));

			(void) ld_store_iter_replay_txn_finish(&iter);

			if (!pgsql_execute(applyPgConn, "ROLLBACK"))
			{
				/* errors have already been logged */
				return false;
			}

			context->transactionInProgress = false;
			return true;
		}

		/* DML row (INSERT / UPDATE / DELETE / TRUNCATE) */
		if (!stream_apply_dml(context, s))
		{
			/* errors have already been logged */
			success = false;
			break;
		}
	}

	(void) ld_store_iter_replay_txn_finish(&iter);

	if (!success)
	{
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		context->transactionInProgress = false;
		return false;
	}

	/* set replication origin to commitLSN before committing */
	char lsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(commitLSN));

	if (IS_EMPTY_STRING_BUFFER(commitTimestamp))
	{
		TimestampTz now = feGetCurrentTimestamp();

		(void) pgsql_timestamptz_to_string(now,
										   commitTimestamp,
										   sizeof(commitTimestamp));
	}

	if (!pgsql_replication_origin_xact_setup(applyPgConn, lsn, commitTimestamp))
	{
		log_error("Failed to setup replication origin for xid %u at %X/%X",
				  xid, LSN_FORMAT_ARGS(commitLSN));
		(void) pgsql_execute(applyPgConn, "ROLLBACK");
		context->transactionInProgress = false;
		return false;
	}

	log_debug("COMMIT xid %u LSN %X/%X", xid, LSN_FORMAT_ARGS(commitLSN));

	if (!pgsql_execute(applyPgConn, "COMMIT"))
	{
		/* errors have already been logged */
		context->transactionInProgress = false;
		return false;
	}

	context->transactionInProgress = false;
	context->previousLSN = commitLSN;

	bool findDurableLSN = false;

	if (!stream_apply_sync_sentinel(context, findDurableLSN))
	{
		log_warn("Failed to sync sentinel after xid %u at %X/%X, "
				 "will retry on next iteration",
				 xid, LSN_FORMAT_ARGS(commitLSN));
	}

	/*
	 * The driver loop (stream_apply_replaydb) records completion of this
	 * transaction in its in-memory pipeline state once we return, and
	 * checkpoints it to sourceDB periodically and at end of processing.
	 */
	return true;
}


/*
 * stream_apply_dml applies a single DML row (INSERT/UPDATE/DELETE/TRUNCATE)
 * to the target database.  The transaction must already be open.
 *
 * INSERT/UPDATE/DELETE use the PREPARE-once-per-hash + EXECUTE pattern.
 * TRUNCATE is executed directly (with TRUNCATE ONLY → TRUNCATE rewrite for
 * partitioned target tables).
 *
 * Rows with a NULL stmt and zero hash (generated-column degenerate rows) are
 * silently skipped.
 */
static bool
stream_apply_dml(StreamApplyContext *context, ReplayDBStmt *s)
{
	PGSQL *applyPgConn = &(context->applyPgConn);

	if (s->action == STREAM_ACTION_TRUNCATE)
	{
		const char *sql = (s->stmt != NULL) ? s->stmt : "";
		int len = strlen(sql);
		char truncateSQL[BUFSIZE] = { 0 };

		strlcpy(truncateSQL, sql, sizeof(truncateSQL));

		/* chomp trailing semicolon */
		if (len > 0 && truncateSQL[len - 1] == ';')
		{
			truncateSQL[len - 1] = '\0';
		}

		const char *execSQL = truncateSQL;
		char rewritten[BUFSIZE] = { 0 };
		const char onlyPrefix[] = "TRUNCATE ONLY ";
		size_t prefixLen = sizeof(onlyPrefix) - 1;

		if (strncmp(truncateSQL, onlyPrefix, prefixLen) == 0)
		{
			char relkind = '\0';
			const char *qname = truncateSQL + prefixLen;

			if (pgsql_get_table_relkind(&(context->controlPgConn),
										qname, &relkind))
			{
				if (relkind == 'p')
				{
					sformat(rewritten, sizeof(rewritten), "TRUNCATE %s", qname);
					execSQL = rewritten;
				}
			}
			else
			{
				log_warn("Could not resolve relkind for replicated "
						 "TRUNCATE on \"%s\"; replaying as TRUNCATE ONLY.",
						 qname);
			}
		}

		if (!pgsql_execute(applyPgConn, execSQL))
		{
			/* errors have already been logged */
			return false;
		}

		return true;
	}

	/* INSERT / UPDATE / DELETE */

	if (s->stmt == NULL && s->hash == 0)
	{
		/* degenerate row (e.g. UPDATE on a generated column) — skip */
		return true;
	}

	uint32_t hash = s->hash;
	PreparedStmt *stmtHashTable = context->preparedStmt;
	PreparedStmt *stmt = NULL;

	HASH_FIND(hh, stmtHashTable, &hash, sizeof(hash), stmt);

	if (stmt == NULL)
	{
		char name[NAMEDATALEN] = { 0 };
		sformat(name, sizeof(name), "%x", hash);

		if (!pgsql_prepare(applyPgConn, name, s->stmt, 0, NULL))
		{
			/* errors have already been logged */
			return false;
		}

		stmt = (PreparedStmt *) calloc(1, sizeof(PreparedStmt));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->hash = hash;
		stmt->prepared = true;

		HASH_ADD(hh, stmtHashTable, hash, sizeof(hash), stmt);

		/* HASH_ADD can change the pointer in place, update */
		context->preparedStmt = stmtHashTable;
	}

	if (s->data != NULL)
	{
		char name[NAMEDATALEN] = { 0 };
		sformat(name, sizeof(name), "%x", hash);

		JSON_Value *js = json_parse_string(s->data);

		if (json_value_get_type(js) != JSONArray)
		{
			log_error("Failed to parse DML args array: %s", s->data);
			json_value_free(js);
			return false;
		}

		JSON_Array *jsArray = json_value_get_array(js);
		int count = json_array_get_count(jsArray);

		if (count > 0)
		{
			const char **paramValues =
				(const char **) calloc(count, sizeof(char *));

			if (paramValues == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				json_value_free(js);
				return false;
			}

			for (int i = 0; i < count; i++)
			{
				paramValues[i] = json_array_get_string(jsArray, i);
			}

			if (!pgsql_execute_prepared(applyPgConn, name,
										count, paramValues,
										NULL, NULL))
			{
				/* errors have already been logged */
				free(paramValues);
				json_value_free(js);
				return false;
			}

			free(paramValues);
		}

		json_value_free(js);
	}

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
								   specs->replayDB,
								   &(specs->paths),
								   specs->connStrings,
								   specs->origin,
								   specs->endpos))
	{
		/* errors have already been logged */
		return false;
	}

	context->logSQL = specs->logSQL;

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

	if (!ld_store_open_outputdb(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!ld_store_open_replaydb(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Use the replication origin for our setup (context->previousLSN).
	 */
	if (!setupReplicationOrigin(context))
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
 * stream_apply_cleanup cleans up the resources used by the apply process.
 */
bool
stream_apply_cleanup(StreamApplyContext *context)
{
	/* make sure we close the connection on the way out */
	(void) pgsql_finish(&(context->controlPgConn));

	(void) pgsql_finish(&(context->applyPgConn));

	return true;
}


/*
 * stream_apply_wait_for_sentinel fetches the current pgcopydb sentinel values:
 * the catchup processing only gets to start when the sentinel "apply" column
 * has been set to true.
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

		if (context->previousLSN == InvalidXLogRecPtr)
		{
			context->previousLSN = sentinel.replay_lsn;
		}
		else
		{
			log_debug("stream_apply_wait_for_sentinel: "
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
	context->sentinelSyncTime = time(NULL);

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
 * stream_apply_sql connects to the target database system and applies the
 * given SQL command as prepared by the stream_transform_file or
 * stream_transform_stream function.
 */
bool
stream_apply_sql(StreamApplyContext *context,
				 LogicalMessageMetadata *metadata,
				 const char *sql)
{
	PGSQL *applyPgConn = &(context->applyPgConn);

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

			/*
			 * Advance previousLSN and sync sentinel.replay_lsn to the SWITCH
			 * position.  Without this, follow_reached_endpos() compares
			 * sentinel.replay_lsn (last COMMIT) against sentinel.endpos and
			 * finds them unequal, causing follow_main_loop() to restart the
			 * CATCHUP→REPLAY cycle forever even though all WAL has been applied.
			 *
			 * A SWITCH message marks a WAL-segment boundary and is always the
			 * last record in a replayDB segment; its LSN is the endpos that the
			 * inject/sentinel process sets.  Syncing here allows
			 * follow_reached_endpos() to detect completion correctly.
			 */
			context->previousLSN = metadata->lsn;

			bool findDurableLSN =
				context->reachedEOF ||
				(context->endpos != InvalidXLogRecPtr &&
				 context->endpos <= metadata->lsn);

			if (!stream_apply_sync_sentinel(context, findDurableLSN))
			{
				log_warn("Failed to sync sentinel at SWITCH LSN %X/%X, "
						 "will retry on next iteration",
						 LSN_FORMAT_ARGS(metadata->lsn));
			}

			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= metadata->lsn)
			{
				context->reachedEndPos = true;

				log_notice("Apply reached end position %X/%X at SWITCH %X/%X",
						   LSN_FORMAT_ARGS(context->endpos),
						   LSN_FORMAT_ARGS(metadata->lsn));
			}

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
			if (!pgsql_begin(applyPgConn))
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

			if (!pgsql_set_gucs(applyPgConn, settings))
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
			if (!pgsql_execute(applyPgConn, "ROLLBACK"))
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
					if (!pgsql_execute(applyPgConn, "ROLLBACK"))
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

			if (!pgsql_replication_origin_xact_setup(applyPgConn,
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
			if (!pgsql_execute(applyPgConn, "COMMIT"))
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
					if (!pgsql_execute(applyPgConn, "ROLLBACK"))
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

			if (metadata->lsn == InvalidXLogRecPtr)
			{
				log_fatal("Failed to parse KEEPALIVE message: %s", sql);
				return false;
			}

			/*
			 * When the timestamp is empty (e.g. from a stale SQL file
			 * written before the empty-timestamp fix), use the current
			 * time as a fallback. The timestamp is only used for
			 * replication origin tracking, so a local timestamp is safe.
			 */
			if (IS_EMPTY_STRING_BUFFER(metadata->timestamp))
			{
				TimestampTz now = feGetCurrentTimestamp();

				if (!pgsql_timestamptz_to_string(now,
												 metadata->timestamp,
												 sizeof(metadata->timestamp)))
				{
					log_fatal("Failed to generate fallback timestamp "
							  "for KEEPALIVE message: %s", sql);
					return false;
				}

				log_debug("KEEPALIVE at LSN %X/%X has empty timestamp, "
						  "using current time \"%s\" as fallback",
						  LSN_FORMAT_ARGS(metadata->lsn),
						  metadata->timestamp);
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

			if (!pgsql_begin(applyPgConn))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * Replication origin is handled differently by the postgres
			 * backend to avoid database bloat and runtime overhead[1].
			 * This optimization leads to persist origin progress only when
			 * the txn modifies the state of the database. So, an empty txn
			 * created to update KEEPALIVE LSN effectively ignored by the
			 * backend leading to not updating the origin progress.
			 *
			 * To workaround this, we execute `SELECT txid_current()` query to
			 * force the backend to update the origin progress.
			 *
			 * [1] https://www.postgresql.org/docs/current/replication-origins.html
			 */
			char *sql = "SELECT txid_current()";

			if (!pgsql_execute(applyPgConn, sql))
			{
				/* errors have already been logged */
				return false;
			}

			/*
			 * Use context->previousLSN (the last committed data-transaction
			 * LSN) rather than metadata->lsn (the KEEPALIVE position) for the
			 * replication origin advancement.
			 *
			 * A KEEPALIVE fires at cur_record_lsn which may be *inside* an
			 * uncommitted transaction (the mid-transaction endpos scenario).
			 * If we advance the origin to that position, the next apply
			 * session starts its replay cursor at the KEEPALIVE lsn, which
			 * skips the straddling transaction's BEGIN and DML rows (their
			 * lsn < KEEPALIVE lsn).
			 *
			 * By recording only the last durably committed transaction's LSN,
			 * the replay cursor on the next session will start from
			 * previousLSN and naturally include any straddling transaction.
			 *
			 * Fall back to metadata->lsn when previousLSN is zero (origin
			 * has never been advanced in this session).
			 */
			uint64_t originLSN =
				(context->previousLSN != InvalidXLogRecPtr)
				? context->previousLSN
				: metadata->lsn;

			char lsn[PG_LSN_MAXLENGTH] = { 0 };

			sformat(lsn, sizeof(lsn), "%X/%X",
					LSN_FORMAT_ARGS(originLSN));

			if (!pgsql_replication_origin_xact_setup(applyPgConn,
													 lsn,
													 metadata->timestamp))
			{
				/* errors have already been logged */
				return false;
			}

			/* calling pgsql_commit() would finish the connection, avoid */
			if (!pgsql_execute(applyPgConn, "COMMIT"))
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

				if (!pgsql_prepare(applyPgConn, name, metadata->stmt, 0, NULL))
				{
					/* errors have already been logged */
					return false;
				}

				stmt = (PreparedStmt *) calloc(1, sizeof(PreparedStmt));
				stmt->hash = hash;
				stmt->prepared = true;

				HASH_ADD(hh, stmtHashTable, hash, sizeof(hash), stmt);

				/* HASH_ADD can change the pointer in place, update */
				context->preparedStmt = stmtHashTable;
			}

			/*
			 * In the SQLite pipeline the replay row carries the parameter
			 * array in the same row (passed here via metadata->jsonBuffer).
			 * Execute the statement immediately after preparing it.
			 */
			if (metadata->jsonBuffer != NULL)
			{
				char name[NAMEDATALEN] = { 0 };
				sformat(name, sizeof(name), "%x", metadata->hash);

				JSON_Value *js = json_parse_string(metadata->jsonBuffer);

				if (json_value_get_type(js) != JSONArray)
				{
					log_error("Failed to parse DML args array: %s",
							  metadata->jsonBuffer);
					return false;
				}

				JSON_Array *jsArray = json_value_get_array(js);
				int count = json_array_get_count(jsArray);

				if (0 < count)
				{
					const char **paramValues =
						(const char **) calloc(count, sizeof(char *));

					if (paramValues == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}

					for (int i = 0; i < count; i++)
					{
						paramValues[i] = json_array_get_string(jsArray, i);
					}

					if (!pgsql_execute_prepared(applyPgConn, name,
												count, paramValues,
												NULL, NULL))
					{
						/* errors have already been logged */
						return false;
					}

					free(paramValues);
				}

				json_value_free(js);
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

			if (0 < count)
			{
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

				if (!pgsql_execute_prepared(applyPgConn, name,
											count, paramValues,
											NULL, NULL))
				{
					/* errors have already been logged */
					return false;
				}
			}


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

			/*
			 * Postgres rejects TRUNCATE ONLY on partitioned tables. Mirror
			 * the runtime fix in pgsql_truncate: when the target relation is
			 * partitioned, drop the ONLY keyword. The lookup runs on the
			 * non-pipeline controlPgConn because applyPgConn is in pipeline
			 * mode (which forbids parseFun callbacks; see pgsql_execute_with_params).
			 *
			 * Invariant: stream_write_truncate emits exactly one relation per
			 * statement (LogicalMessageTruncate.table is singular), so the
			 * single-relation regclass lookup below is sufficient. If a future
			 * change ever makes this multi-relation, the regclass cast will
			 * fail, the rewrite will be skipped, and Postgres will surface a
			 * loud error from the original TRUNCATE ONLY.
			 */
			char rewritten[BUFSIZE] = { 0 };
			const char *truncateSQL = sql;
			const char onlyPrefix[] = "TRUNCATE ONLY ";
			size_t prefixLen = sizeof(onlyPrefix) - 1;

			if (strncmp(sql, onlyPrefix, prefixLen) == 0)
			{
				char relkind = '\0';
				const char *qname = sql + prefixLen;

				if (pgsql_get_table_relkind(&(context->controlPgConn),
											qname, &relkind))
				{
					if (relkind == 'p')
					{
						sformat(rewritten, sizeof(rewritten),
								"TRUNCATE %s", qname);
						truncateSQL = rewritten;
					}
				}
				else
				{
					log_warn("Could not resolve relkind for replicated "
							 "TRUNCATE on \"%s\"; replaying as TRUNCATE ONLY. "
							 "If the target is partitioned, Postgres will "
							 "reject this statement.",
							 qname);
				}
			}

			if (!pgsql_execute(applyPgConn, truncateSQL))
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
 * setupConnection sets up a connection to the target database.
 */
static bool
setupConnection(PGSQL *pgsql, StreamApplyContext *context)
{
	if (!pgsql_init(pgsql,
					context->connStrings->target_pguri,
					PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	/* we're going to send several replication origin commands */
	pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	/* we also might want to skip logging any SQL query that we apply */
	pgsql->logSQL = context->logSQL;

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
setupReplicationOrigin(StreamApplyContext *context)
{
	char *nodeName = context->origin;

	/*
	 * A dedicated connection to apply logical messages into the target.
	 * This will be converted to pipeline mode after we have setup the
	 * replication origin.
	 */
	PGSQL *applyPgConn = &(context->applyPgConn);
	if (!setupConnection(applyPgConn, context))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Establish a regular connection for operations requiring immediate
	 * responses, such as finding the WAL insert LSN.
	 */
	if (!setupConnection(&context->controlPgConn, context))
	{
		log_error("Failed to setup pipeline mode on target connection");
		return false;
	}

	uint32_t oid = 0;

	if (!pgsql_replication_origin_oid(applyPgConn, nodeName, &oid))
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
		(void) pgsql_finish(applyPgConn);
		(void) pgsql_finish(&context->controlPgConn);
		return false;
	}

	/*
	 * Fetch the replication origin LSN tracking, which is maintained in a
	 * transactional fashion with the SQL that's been replayed. It's the
	 * authoritative value for progress at reconnect, given that we use
	 * synchronous_commit off.
	 */
	uint64_t originLSN = InvalidXLogRecPtr;

	if (!pgsql_replication_origin_progress(applyPgConn, nodeName, true, &originLSN))
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

	log_debug("setupReplicationOrigin: replication origin \"%s\" "
			  "found at %X/%X",
			  nodeName,
			  LSN_FORMAT_ARGS(context->previousLSN));

	if (!pgsql_replication_origin_session_setup(applyPgConn, nodeName))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Enter into pipeline mode, SQL statements which expects sync responses
	 * are not allowed in this connection anymore.
	 */
	if (!pgsql_enable_pipeline_mode(applyPgConn))
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
						  DatabaseCatalog *replayDB,
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
			return false;
		}


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
 * stream_apply_find_durable_lsn fetches the LSN for the current durable
 * location on the target system using pg_replication_origin_progress.
 */
bool
stream_apply_find_durable_lsn(StreamApplyContext *context, uint64_t *durableLSN)
{
	uint64_t flushLSN = InvalidXLogRecPtr;

	bool flush = true;

	if (!pgsql_replication_origin_progress(&(context->controlPgConn),
										   context->origin,
										   flush,
										   &flushLSN))
	{
		/* errors have already been logged */
		log_error("Failed to retrieve origin progress, "
				  "see above for details");
		return false;
	}

	*durableLSN = flushLSN;

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
		return false;
	}


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


/*
 * stream_apply_to_stdout reads the replayDB stmt + replay tables and writes
 * the SQL that would normally be sent to the target Postgres to the given
 * FILE * (typically stdout).
 *
 * This mirrors pg_restore --no-target behaviour: useful for debugging and for
 * unit-testing the apply read path without an actual database connection.
 *
 * Output format per transaction:
 *
 *   BEGIN
 *   PREPARE <stmt_name> AS <sql_template>;
 *   EXECUTE <stmt_name> (arg1, arg2, ...);
 *   ...
 *   COMMIT
 */
bool
stream_apply_to_stdout(StreamSpecs *specs, FILE *out)
{
	DatabaseCatalog *replayDB = specs->replayDB;

	if (replayDB == NULL || replayDB->db == NULL)
	{
		log_error("BUG: stream_apply_to_stdout: replayDB is NULL");
		return false;
	}

	if (!semaphore_lock(&(replayDB->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	ReplayDBReplayIterator iter = { 0 };

	iter.catalog = replayDB;
	iter.previousLSN = InvalidXLogRecPtr;   /* start from the beginning */
	iter.endpos = specs->endpos;

	if (!ld_store_iter_replay_init(&iter))
	{
		(void) semaphore_unlock(&(replayDB->sema));
		return false;
	}

	bool inTxn = false;
	char prevHash[16] = { 0 };   /* track which stmts have been PREPAREd */

	/*
	 * We use a simple in-memory hash set to avoid printing the same PREPARE
	 * more than once.  For the expected number of distinct statements per
	 * unit-test (< 10) a linear scan is fast enough.
	 */
#define MAX_PREPARED 64
	uint32_t preparedHashes[MAX_PREPARED];
	int preparedCount = 0;
	memset(preparedHashes, 0, sizeof(preparedHashes));

	(void) prevHash;   /* suppress unused-variable warning */

	for (;;)
	{
		if (!ld_store_iter_replay_next(&iter))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(replayDB->sema));
			return false;
		}

		ReplayDBStmt *s = iter.current;

		if (s == NULL)
		{
			break;  /* no more rows */
		}

		switch (s->action)
		{
			case STREAM_ACTION_BEGIN:
			{
				fformat(out, "BEGIN\n");
				inTxn = true;
				break;
			}

			case STREAM_ACTION_COMMIT:
			{
				fformat(out, "COMMIT\n");
				inTxn = false;

				/* stop at endpos */
				if (specs->endpos != InvalidXLogRecPtr &&
					specs->endpos <= s->lsn)
				{
					goto done;
				}

				break;
			}

			case STREAM_ACTION_ROLLBACK:
			{
				fformat(out, "ROLLBACK\n");
				inTxn = false;
				break;
			}

			case STREAM_ACTION_INSERT:
			case STREAM_ACTION_UPDATE:
			case STREAM_ACTION_DELETE:
			case STREAM_ACTION_TRUNCATE:
			{
				if (s->stmt == NULL || s->data == NULL)
				{
					/* no SQL template or args — skip */
					break;
				}

				/* PREPARE once per distinct hash */
				bool alreadyPrepared = false;

				for (int i = 0; i < preparedCount; i++)
				{
					if (preparedHashes[i] == s->hash)
					{
						alreadyPrepared = true;
						break;
					}
				}

				if (!alreadyPrepared)
				{
					char name[NAMEDATALEN] = { 0 };
					sformat(name, sizeof(name), "%x", s->hash);

					fformat(out, "PREPARE %s AS %s;\n", name, s->stmt);

					if (preparedCount < MAX_PREPARED)
					{
						preparedHashes[preparedCount++] = s->hash;
					}
				}

				/* EXECUTE with args */
				char name[NAMEDATALEN] = { 0 };
				sformat(name, sizeof(name), "%x", s->hash);

				/*
				 * Parse the JSON array of argument values and format them as
				 * a comma-separated list: EXECUTE name ('v1', 'v2', NULL, ...)
				 */
				JSON_Value *js = json_parse_string(s->data);

				if (js == NULL ||
					json_value_get_type(js) != JSONArray)
				{
					log_error("stream_apply_to_stdout: failed to parse "
							  "args JSON for replay id %lld: %s",
							  (long long) s->id, s->data);
					json_value_free(js);
					(void) semaphore_unlock(&(replayDB->sema));
					return false;
				}

				JSON_Array *jsArray = json_value_get_array(js);
				int count = json_array_get_count(jsArray);

				fformat(out, "EXECUTE %s (", name);

				for (int i = 0; i < count; i++)
				{
					const char *val = json_array_get_string(jsArray, i);

					if (i > 0)
					{
						fformat(out, ", ");
					}

					if (val == NULL)
					{
						fformat(out, "NULL");
					}
					else
					{
						fformat(out, "'%s'", val);
					}
				}

				fformat(out, ")\n");

				json_value_free(js);
				break;
			}

			default:
			{
				/* KEEPALIVE, SWITCH, ENDPOS: skip */
				break;
			}
		}
	}

done:
	if (inTxn)
	{
		/* unterminated transaction at end of file — emit ROLLBACK */
		fformat(out, "ROLLBACK\n");
	}

	(void) semaphore_unlock(&(replayDB->sema));

	if (fflush(out) != 0)
	{
		log_error("stream_apply_to_stdout: fflush failed: %m");
		return false;
	}

	return true;
}


/*
 * stream_apply_stdin reads SQL text from stdin (as produced by
 * `pgcopydb stream transform - -`) and applies each statement to the target
 * database.  This is the legacy Unix-pipe path:
 *
 *   pgcopydb stream receive --to-stdout  \
 *     | pgcopydb stream transform - -    \
 *     | pgcopydb stream apply -
 *
 * Each line is parsed with parseSQLAction to determine the action, then
 * handed to stream_apply_sql.  The transform step always includes
 * "commit_lsn" in BEGIN lines, so readTxnCommitLSN never needs to read
 * a .txnmeta file.
 */
bool
stream_apply_stdin(StreamSpecs *specs, StreamApplyContext *context)
{
	char line[BUFSIZE * 16];

	while (fgets(line, sizeof(line), stdin) != NULL)
	{
		/* strip trailing newline */
		int len = strlen(line);

		if (len > 0 && line[len - 1] == '\n')
		{
			line[--len] = '\0';
		}

		if (len == 0)
		{
			continue;
		}

		LogicalMessageMetadata metadata = { 0 };

		if (!parseSQLAction(line, &metadata))
		{
			log_error("stream_apply_stdin: failed to parse line: %s", line);
			return false;
		}

		if (metadata.action == STREAM_ACTION_UNKNOWN)
		{
			/* not a recognised control line — skip silently */
			continue;
		}

		if (!stream_apply_sql(context, &metadata, line))
		{
			/* errors have already been logged */
			return false;
		}

		if (context->reachedEndPos)
		{
			log_info("stream_apply_stdin: reached end position %X/%X",
					 LSN_FORMAT_ARGS(context->endpos));
			break;
		}
	}

	return true;
}
