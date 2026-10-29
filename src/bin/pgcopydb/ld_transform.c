/*
 * src/bin/pgcopydb/ld_transform.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <math.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "libpq-fe.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#include "lookup3.h"
#include "parson.h"

#include "catalog.h"
#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "env_utils.h"
#include "ld_pgoutput.h"
#include "ld_store.h"
#include "ld_stream.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


static bool canCoalesceLogicalTransactionStatement(LogicalTransaction *txn,
												   LogicalTransactionStatement *new);
static bool stream_transform_open_transaction(StreamSpecs *specs);
static bool stream_transform_write_replay_begin(StreamSpecs *specs,
												uint64_t endlsn);
static bool stream_transform_write_replay_stmts(StreamSpecs *specs);
static bool coalesceLogicalTransactionStatement(LogicalTransaction *txn,
												LogicalTransactionStatement *new);

static bool markGeneratedColumnsFromTransaction(GeneratedColumnsCache *cache,
												LogicalTransaction *txn);
static bool markGeneratedColumnsFromStatement(GeneratedColumnsCache *cache,
											  LogicalTransactionStatement *stmt);

static bool stream_transform_locate_by_key(StreamContext *privateContext,
										   LogicalTransactionStatement *stmt);

static bool prepareGeneratedColumnsCache_hook(void *ctx, SourceTable *table);

static bool prepareGeneratedColumnsCache(StreamSpecs *specs);

static GeneratedColumnSet * lookupGeneratedColumnsForTable(GeneratedColumnsCache *cache,
														   const char *nspname,
														   const char *relname);

static bool stream_transform_cdc_file_hook(StreamSpecs *specs,
										   ReplayDBOutputMessage *output,
										   bool *stop);

static bool stream_transform_prepare_message(StreamSpecs *specs,
											 ReplayDBOutputMessage *output);

static bool stream_write_insert(ReplayDBStmt *replayStmt,
								LogicalMessageInsert *insert);

static bool stream_write_update(bool replayNoOpUpdates,
								ReplayDBStmt *replayStmt,
								LogicalMessageUpdate *update);

static bool stream_write_delete(ReplayDBStmt *replayStmt,
								LogicalMessageDelete *delete);

static bool stream_write_truncate(ReplayDBStmt *replayStmt,
								  LogicalMessageTruncate *truncate);

static bool stream_write_ddl(ReplayDBStmt *replayStmt,
							 LogicalMessageDDL *ddl);

static bool stream_write_file_txn(FILE *out,
								  LogicalTransaction *txn,
								  bool replayNoOpUpdates);

static bool stream_write_file_message(FILE *out,
									  LogicalMessage *msg,
									  LogicalMessageMetadata *metadata,
									  bool replayNoOpUpdates);


/*
 * stream_transform_context_init initializes StreamContext for the transform
 * operation.
 */
bool
stream_transform_context_init(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);


	/*
	 * The inline transform needs the catalog handles on the private context:
	 * sourceDB for table/primary-key lookups (e.g. a test_decoding UPDATE or
	 * DELETE that carries no old-key section), outputDB to read from and
	 * replayDB to write to.  stream_init_context() sets these for the
	 * receive/stdout paths; the apply (catchup) path uses this function
	 * instead, so set them here too — otherwise sourceDB is NULL and
	 * lookupOrCacheSourceTable() dereferences it.
	 */
	privateContext->sourceDB = specs->sourceDB;
	privateContext->outputDB = specs->outputDB;
	privateContext->replayDB = specs->replayDB;

	/* record which output plugin we are transforming — used for dispatch */
	privateContext->plugin = specs->slot.plugin;


	privateContext->transformPGSQL = &(specs->transformPGSQL);


	/*
	 * Initialize and open the connection to the target database.  When the
	 * target URI is "-" (stdout sentinel) or absent there is no target
	 * connection; pgsql_escape_identifier handles the NULL-connection case
	 * with a local quoting fallback.
	 */

	const char *tpguri = specs->connStrings->target_pguri;


	if (tpguri != NULL && !streq(tpguri, "-"))
	{
		if (!pgsql_init(privateContext->transformPGSQL, (char *) tpguri,
						PGSQL_CONN_TARGET))
		{
			/* errors have already been logged */
			return false;
		}


		if (!pgsql_open_connection(privateContext->transformPGSQL))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Prepare the generated columns cache, which helps to skip the generated
	 * columns in the SQL output.
	 */

	if (!prepareGeneratedColumnsCache(specs))
	{
		/* errors have already been logged */
		return false;
	}


	return true;
}


/*
 * stream_transform_from_outputdb is the public entry point for the inline
 * transform step that is now run inside the apply process.
 *
 * It sets the output iteration starting position (previousLSN, i.e. the last
 * committed LSN on the target), then calls ld_store_iter_output() with the
 * static hook to parse each JSON row from outputDB and write the corresponding
 * parameterised SQL into replayDB (stmt + replay tables).
 *
 * One call is a pass: it keeps going, a transaction at a time, while the next
 * thing in outputDB is a complete transaction, up to
 * STREAM_TRANSFORM_BATCH_MAX_TXNS of them, and commits what it wrote to
 * replayDB once at the end.  A marker (KEEPALIVE) ends the pass: it has a
 * cursor of its own that only the apply process advances.
 *
 * KEEPALIVE and SWITCH markers are selected by keepaliveLSN instead, the
 * position of the last marker the apply consumed.  previousLSN cannot serve:
 * a marker carries the server's walEnd and sits above transactions that
 * commit below it, so skipping past one with previousLSN would drop them.
 *
 * After this call returns, specs->private.midTxnEndpos may be true; the caller
 * must check that flag and exit cleanly without advancing previousLSN if so.
 */
bool
stream_transform_from_outputdb(StreamSpecs *specs,
							   uint64_t previousLSN,
							   uint64_t keepaliveLSN)
{
	DatabaseCatalog *replayDB = specs->replayDB;

	specs->private.keepaliveLSN = keepaliveLSN;


	/* reset the mid-transaction endpos flag before each pass */
	specs->private.midTxnEndpos = false;


	DatabaseCatalog *outputDB = specs->outputDB;

	/*
	 * One SQLite transaction on replayDB for the whole pass: a commit is a
	 * sync, and doing one per statement made it the cost of transforming a
	 * transaction.  And one on outputDB, which only reads: it gives the pass
	 * a single snapshot of what receive has committed, and the pass reads
	 * without the catalog semaphore (see ld_store_iter_output).
	 */
	if (!catalog_begin(replayDB, false))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_begin(outputDB, false))
	{
		/* errors have already been logged */
		(void) catalog_execute(replayDB, "ROLLBACK");
		return false;
	}

	uint64_t startLSN = previousLSN;
	bool success = true;

	/*
	 * A transaction that an earlier process transformed part of and never
	 * completed is still in replayDB, its BEGIN row without an end LSN. It is
	 * of no use to this one, which transforms it again from the start, and
	 * would be taken for the open transaction of this process (and everything
	 * after it dropped along with it): clear it before writing anything.
	 */
	if (specs->private.staleOpenDroppedFrom != replayDB)
	{
		bool dropped = false;

		if (!ld_store_replay_drop_open(replayDB, &dropped))
		{
			/* errors have already been logged */
			(void) catalog_execute(replayDB, "ROLLBACK");
			(void) catalog_commit(outputDB);
			return false;
		}

		if (dropped)
		{
			specs->private.openGeneration++;
		}

		specs->private.staleOpenDroppedFrom = replayDB;
	}

	for (int count = 0; count < STREAM_TRANSFORM_BATCH_MAX_TXNS; count++)
	{
		/* tell ld_store_iter_output where to start (last transformed commit) */
		specs->sentinel.replay_lsn = startLSN;
		specs->private.transformedCommitLSN = InvalidXLogRecPtr;

		if (!ld_store_iter_output(specs, &stream_transform_cdc_file_hook))
		{
			/* errors have already been logged */
			success = false;
			break;
		}

		if (specs->private.transformedCommitLSN == InvalidXLogRecPtr ||
			specs->private.midTxnEndpos)
		{
			break;
		}

		startLSN = specs->private.transformedCommitLSN;

		if (specs->sentinel.endpos != InvalidXLogRecPtr &&
			specs->sentinel.endpos <= startLSN)
		{
			break;
		}
	}

	/*
	 * What the loop stopped on may be a transaction that receive has not
	 * reached the COMMIT of: transform what receive has committed of it so
	 * far, rather than wait for the rest of it (the transform of a large
	 * transaction then overlaps its receive).
	 */
	if (success &&
		!specs->private.midTxnEndpos &&
		specs->private.pendingXid != 0)
	{
		success = stream_transform_open_transaction(specs);
	}

	if (!success)
	{
		/* what was written is rolled back below: start the transaction over */
		LogicalMessage empty = { 0 };

		specs->private.currentMsg = empty;
		specs->private.openXid = 0;
		specs->private.openReplayBeginId = 0;
		specs->private.openGeneration++;
	}

	/* the caller's cursor is what it was: it advances as it applies */
	specs->sentinel.replay_lsn = previousLSN;

	/* nothing was written to outputDB: this just ends the snapshot */
	(void) catalog_commit(outputDB);

	if (!(success ? catalog_commit(replayDB) :
		  catalog_execute(replayDB, "ROLLBACK")))
	{
		/* errors have already been logged */
		return false;
	}

	return success;
}


/*
 * stream_transform_cdc_file_hook is an iterator callback function.
 */
static bool
stream_transform_cdc_file_hook(StreamSpecs *specs,
							   ReplayDBOutputMessage *output,
							   bool *stop)
{
	CopyDBSentinel *sentinel = &(specs->sentinel);

	/* parse the logical decoding output */
	if (!stream_transform_prepare_message(specs, output))
	{
		/* errors have already been logged */
		return false;
	}

	/* insert the message into the SQLite replayDB (stmt, replay) */
	if (!stream_transform_write_transaction(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/* track progress at this message's LSN */
	uint64_t lsn = output->lsn;

	/*
	 * A ROLLBACK is not recorded: ld_store_lookup_output_after_lsn moves past
	 * COMMIT rows only, so a pass could not continue from one.
	 */
	if (output->action == STREAM_ACTION_COMMIT)
	{
		specs->private.transformedCommitLSN = lsn;
	}

	/*
	 * Record transaction progress in the in-memory apply pipeline state owned
	 * by the apply driver loop (stream_apply_replaydb).  The driver snapshots
	 * this struct each iteration and compares afterwards to decide whether the
	 * transform stage made progress, then syncs it to sourceDB once in a while.
	 *
	 * applyState is NULL in the stdout/file apply path, which has no driver.
	 */
	PipelineStateEntry *as = specs->private.applyState;

	if (as != NULL)
	{
		switch (output->action)
		{
			case STREAM_ACTION_BEGIN:
			{
				as->last_xid = output->xid;
				as->last_txn_begin_lsn = output->lsn;
				as->last_txn_end_lsn = InvalidXLogRecPtr;
				as->last_txn_complete = false;
				as->last_txn_processed = false;
				break;
			}

			case STREAM_ACTION_COMMIT:
			case STREAM_ACTION_ROLLBACK:
			{
				as->last_xid = output->xid;
				as->last_txn_end_lsn = output->lsn;
				as->last_txn_complete = true;

				/* transformed into replay, not yet applied to the target */
				as->last_txn_processed = false;
				break;
			}

			default:
			{
				/* noop */
				break;
			}
		}
	}

	/*
	 * Signal the caller to stop once we have reached the endpos.
	 */
	if (sentinel->endpos != InvalidXLogRecPtr &&
		sentinel->endpos <= lsn)
	{
		*stop = true;

		log_info("Inline transform reached end position %X/%X at %X/%X",
				 LSN_FORMAT_ARGS(sentinel->endpos),
				 LSN_FORMAT_ARGS(lsn));
	}

	return true;
}


/*
 * stream_transform_prepare_message prepares a message with metadata taken from
 * the replayDB output table, and parses the actual Logical Decoding message
 * parts.
 */
bool
stream_transform_prepare_message(StreamSpecs *specs,
								 ReplayDBOutputMessage *output)
{
	StreamContext *privateContext = &(specs->private);
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	/* first re-build the metadata from the SQLite row */
	LogicalMessageMetadata outputMetadata = {
		.action = output->action,
		.xid = output->xid,
		.lsn = output->lsn,
		.jsonBuffer = output->jsonBuffer
	};

	*metadata = outputMetadata;

	/* copy the timestamp over */
	strlcpy(metadata->timestamp, output->timestamp, PG_MAX_TIMESTAMP);

	log_debug("stream_transform_prepare_message: %lld %c xid=%lld lsn=%X/%X",
			  (long long) output->id,
			  metadata->action,
			  (long long) metadata->xid,
			  LSN_FORMAT_ARGS(metadata->lsn));

	/*
	 * pgoutput stores structured column data in pgoutput_col instead of a
	 * text blob.  For TCL messages (BEGIN/COMMIT/ROLLBACK) we call the normal
	 * parseMessage path with a non-NULL sentinel message so error log lines
	 * don't crash on NULL.  For DML messages we call parsePgoutputMessage
	 * directly, which queries pgoutput_col.
	 */
	if (privateContext->plugin == STREAM_PLUGIN_PGOUTPUT)
	{
		if (StreamActionIsDML(metadata->action))
		{
			/*
			 * The stmt pointer must be set before parsePgoutputMessage so it
			 * can populate stmt->stmt.insert / .update / .delete.
			 * parseMessage would normally allocate and set privateContext->stmt;
			 * replicate that here.
			 */
			LogicalTransactionStatement *pgstmt = (LogicalTransactionStatement *)
												  calloc(1, sizeof(
															 LogicalTransactionStatement))
			;
			if (pgstmt == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}
			pgstmt->action = metadata->action;
			pgstmt->xid = metadata->xid;
			pgstmt->lsn = metadata->lsn;
			strlcpy(pgstmt->timestamp, metadata->timestamp,
					sizeof(pgstmt->timestamp));
			privateContext->stmt = pgstmt;

			if (!parsePgoutputMessage(privateContext,
									  privateContext->outputDB,
									  (int64_t) output->id,
									  output->jsonBuffer,
									  output->nspname,
									  output->relname,
									  output->old_type))
			{
				log_error("Failed to parse pgoutput columns for output id=%lld",
						  (long long) output->id);
				return false;
			}

			LogicalMessage *mesg = &(privateContext->currentMsg);
			if (!mesg->isTransaction)
			{
				log_error("pgoutput DML received outside of transaction");
				return false;
			}
			if (!stream_transform_locate_by_key(privateContext, pgstmt))
			{
				/* errors have already been logged */
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(
				&(mesg->command.tx), pgstmt);
		}
		else if (metadata->action == STREAM_ACTION_MESSAGE)
		{
			/*
			 * Unlike DML, a pgoutput MESSAGE row carries its content as a
			 * real JSON envelope in the output->jsonBuffer column (see
			 * ld_store_insert_pgoutput_message / preparePgoutputMessage) —
			 * same as test_decoding/wal2json — so use the common path.
			 */
			JSON_Value *json = json_parse_string(output->jsonBuffer);

			if (!parseMessage(privateContext, output->jsonBuffer, json))
			{
				log_error("Failed to parse pgoutput MESSAGE id=%lld: %.1024s%s",
						  (long long) output->id,
						  output->jsonBuffer != NULL ? output->jsonBuffer : "(null)",
						  output->jsonBuffer != NULL &&
						  strlen(output->jsonBuffer) > 1024 ? "..." : "");
				return false;
			}
		}
		else
		{
			/* BEGIN, COMMIT, ROLLBACK, KEEPALIVE, etc. — use the common path.
			 * Pass an empty string as message so log lines don't segfault. */
			if (!parseMessage(privateContext, "", NULL))
			{
				log_error("Failed to parse pgoutput TCL message id=%lld",
						  (long long) output->id);
				return false;
			}
		}

		return true;
	}

	JSON_Value *json = json_parse_string(output->jsonBuffer);

	if (!parseMessage(privateContext, output->jsonBuffer, json))
	{
		log_error("Failed to parse JSON message: %.1024s%s",
				  output->jsonBuffer != NULL ? output->jsonBuffer : "(null)",
				  output->jsonBuffer != NULL && strlen(output->jsonBuffer) > 1024
				  ? "..." : "");
		return false;
	}

	return true;
}


/*
 * stream_transform_write_transaction updates the SQLite replayDB with the stmt
 * and replay messages as processed from the logical decoding plugin output.
 */
bool
stream_transform_write_transaction(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);
	LogicalMessage *currentMsg = &(privateContext->currentMsg);
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	switch (metadata->action)
	{
		case STREAM_ACTION_COMMIT:
		case STREAM_ACTION_ROLLBACK:
		case STREAM_ACTION_KEEPALIVE:
		case STREAM_ACTION_SWITCH:
		case STREAM_ACTION_ENDPOS:
		{
			/* insert the transaction into the SQLite replayDB (stmt, replay) */
			if (!stream_transform_write_replay_stmt(specs))
			{
				/* errors have already been logged */
				return false;
			}

			/* then prepare a new transaction, reusing the same memory area */
			LogicalMessage empty = { 0 };
			*currentMsg = empty;

			log_debug("stream_transform_write_transaction: transaction written, "
					  "resetting current message");

			return true;
		}

		/*
		 * Skip DML messages, we update one full transaction at a time to allow
		 * for INSERT rewrites with multiple-values and other SQL level
		 * optimisations.
		 */
		default:
		{
			return true;
		}
	}

	return true;
}


/*
 * stream_write_file_txn writes a completed LogicalTransaction to an already-
 * open SQL FILE in the PREPARE/EXECUTE format used by the file-based
 * `pgcopydb stream transform <json> <sql>` command.
 *
 * This restores the SQL-file output capability that was present before the
 * SQLite pipeline refactoring removed stream_write_message().
 */
static bool
stream_write_file_txn(FILE *out, LogicalTransaction *txn, bool replayNoOpUpdates)
{
#define FFORMAT(stream, fmt, ...) \
	{ if (fformat(stream, fmt, __VA_ARGS__) == -1) { \
		  log_error("Failed to write to stream: %m"); \
		  return false; } \
	}

	/* write BEGIN only for non-continued, non-empty transactions */
	bool sentBEGIN = false;

	LogicalTransactionStatement *currentStmt = txn->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		if (currentStmt->action == STREAM_ACTION_SWITCH ||
			currentStmt->action == STREAM_ACTION_KEEPALIVE ||
			currentStmt->action == STREAM_ACTION_ENDPOS)
		{
			/* skip internal messages embedded in the transaction */
			continue;
		}

		if (!sentBEGIN && !txn->continued)
		{
			if (txn->commitLSN != InvalidXLogRecPtr)
			{
				FFORMAT(out,
						"%s{\"xid\":%lld,\"lsn\":\"%X/%X\","
						"\"timestamp\":\"%s\","
						"\"commit_lsn\":\"%X/%X\"}\n",
						OUTPUT_BEGIN,
						(long long) txn->xid,
						LSN_FORMAT_ARGS(txn->beginLSN),
						txn->timestamp,
						LSN_FORMAT_ARGS(txn->commitLSN));
			}
			else
			{
				FFORMAT(out,
						"%s{\"xid\":%lld,\"lsn\":\"%X/%X\","
						"\"timestamp\":\"%s\"}\n",
						OUTPUT_BEGIN,
						(long long) txn->xid,
						LSN_FORMAT_ARGS(txn->beginLSN),
						txn->timestamp);
			}

			sentBEGIN = true;
		}

		/* generate the SQL template + args via the same helpers used for SQLite */
		ReplayDBStmt replayStmt = {
			.action = currentStmt->action,
			.xid = currentStmt->xid,
			.lsn = currentStmt->lsn
		};

		strlcpy(replayStmt.timestamp, currentStmt->timestamp,
				sizeof(replayStmt.timestamp));

		switch (currentStmt->action)
		{
			case STREAM_ACTION_INSERT:
			{
				if (!stream_write_insert(&replayStmt,
										 &(currentStmt->stmt.insert)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				if (!stream_write_update(replayNoOpUpdates,
										 &replayStmt,
										 &(currentStmt->stmt.update)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				if (!stream_write_delete(&replayStmt,
										 &(currentStmt->stmt.delete)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			case STREAM_ACTION_TRUNCATE:
			{
				if (!stream_write_truncate(&replayStmt,
										   &(currentStmt->stmt.truncate)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			default:
			{
				log_error("BUG: stream_write_file_txn: unexpected action %c",
						  currentStmt->action);
				return false;
			}
		}

		if (replayStmt.stmt != NULL)
		{
			char hash[9] = { 0 };

			sformat(hash, sizeof(hash), "%x", replayStmt.hash);

			FFORMAT(out, "PREPARE %s AS %s;\n", hash, replayStmt.stmt);

			if (replayStmt.data != NULL)
			{
				FFORMAT(out, "EXECUTE %s%s;\n", hash, replayStmt.data);
			}

			free(replayStmt.stmt);
			free(replayStmt.data);
		}
	}

	/* write COMMIT or ROLLBACK */
	if (txn->rollback)
	{
		FFORMAT(out,
				"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
				OUTPUT_ROLLBACK,
				(long long) txn->xid,
				LSN_FORMAT_ARGS(txn->rollbackLSN),
				txn->timestamp);
	}
	else if (txn->commit || sentBEGIN)
	{
		FFORMAT(out,
				"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
				OUTPUT_COMMIT,
				(long long) txn->xid,
				LSN_FORMAT_ARGS(txn->commitLSN),
				txn->timestamp);
	}

	if (fflush(out) != 0)
	{
		log_error("Failed to flush SQL file: %m");
		return false;
	}

	return true;

#undef FFORMAT
}


/*
 * stream_write_file_message writes the current LogicalMessage to an open SQL
 * FILE in the PREPARE/EXECUTE format.  Called by stream_transform_write_message
 * when a sqlFile is open (file-to-file transform path only).
 */
static bool
stream_write_file_message(FILE *out,
						  LogicalMessage *msg,
						  LogicalMessageMetadata *metadata,
						  bool replayNoOpUpdates)
{
#define FFORMAT(stream, fmt, ...) \
	{ if (fformat(stream, fmt, __VA_ARGS__) == -1) { \
		  log_error("Failed to write to stream: %m"); \
		  return false; } \
	}

	if (msg->isTransaction)
	{
		return stream_write_file_txn(out, &(msg->command.tx), replayNoOpUpdates);
	}

	/* non-transaction messages: SWITCH / KEEPALIVE / ENDPOS */
	switch (msg->action)
	{
		case STREAM_ACTION_SWITCH:
		{
			FFORMAT(out, "%s{\"lsn\":\"%X/%X\"}\n",
					OUTPUT_SWITCHWAL,
					LSN_FORMAT_ARGS(msg->command.switchwal.lsn));
			break;
		}

		case STREAM_ACTION_KEEPALIVE:
		{
			FFORMAT(out, "%s{\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
					OUTPUT_KEEPALIVE,
					LSN_FORMAT_ARGS(msg->command.keepalive.lsn),
					msg->command.keepalive.timestamp);
			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			FFORMAT(out, "%s{\"lsn\":\"%X/%X\"}\n",
					OUTPUT_ENDPOS,
					LSN_FORMAT_ARGS(msg->command.endpos.lsn));
			break;
		}

		default:
		{
			/* COMMIT/ROLLBACK of a non-isTransaction message: skip */
			break;
		}
	}

	return true;

#undef FFORMAT
}


/*
 * stream_transform_write_message checks if we need to flush-out the current
 * message down to file, and maybe also stdout (Unix PIPE).
 */
static bool
stream_transform_write_message(StreamContext *privateContext,
							   uint64_t *currentMsgIndex)
{
	LogicalMessage *currentMsg = &(privateContext->currentMsg);
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	/*
	 * Is it time to close the current message and prepare a new one?
	 *
	 * If not, just skip writing the current message/transaction to the SQL
	 * file, we need a full transaction in-memory to be able to do that. Or at
	 * least a partial transaction within known boundaries.
	 */
	if (metadata->action != STREAM_ACTION_COMMIT &&
		metadata->action != STREAM_ACTION_ROLLBACK &&
		metadata->action != STREAM_ACTION_KEEPALIVE &&
		metadata->action != STREAM_ACTION_SWITCH &&
		metadata->action != STREAM_ACTION_ENDPOS)
	{
		return true;
	}

	LogicalTransaction *txn = &(currentMsg->command.tx);

	if (metadata->action == STREAM_ACTION_COMMIT)
	{
		/* now write the COMMIT message even when txn is continued */
		txn->commit = true;
	}

	/*
	 * Before serializing the transaction to disk or stdout, we need to find
	 * the generated columns from the transactionn and mark them as such.
	 *
	 * It will help to set the value of the generated columns to DEFAULT in the
	 * SQL output.
	 */
	GeneratedColumnsCache *cache = privateContext->generatedColumnsCache;

	if (currentMsg->isTransaction && cache != NULL)
	{
		if (!markGeneratedColumnsFromTransaction(cache, txn))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * When stream_transform_file has opened an output SQL file, write the
	 * current message there now.  The SQLite-based pipeline does not set
	 * sqlFile (it uses stream_transform_write_replay_stmt instead), so this
	 * branch is only taken by the explicit `pgcopydb stream transform
	 * <json> <sql>` file-to-file path.
	 */
	if (privateContext->sqlFile != NULL)
	{
		bool replayNoOpUpdates = privateContext->specs != NULL &&
								 privateContext->specs->replayNoOpUpdates;

		if (!stream_write_file_message(privateContext->sqlFile, currentMsg,
									   metadata, replayNoOpUpdates))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (metadata->action == STREAM_ACTION_COMMIT ||
		metadata->action == STREAM_ACTION_ROLLBACK)
	{
		/* then prepare a new one, reusing the same memory area */
		LogicalMessage empty = { 0 };

		*currentMsg = empty;
		++(*currentMsgIndex);
	}
	else if (currentMsg->isTransaction)
	{
		/*
		 * A SWITCH WAL or a KEEPALIVE or an ENDPOS message happened in the
		 * middle of a transaction: we need to mark the new transaction as
		 * a continued part of the previous one.
		 */
		log_debug("stream_transform_line: continued transaction at %c: %X/%X",
				  metadata->action,
				  LSN_FORMAT_ARGS(metadata->lsn));

		LogicalMessage new = { 0 };

		new.isTransaction = true;
		new.action = STREAM_ACTION_BEGIN;

		LogicalTransaction *oldTxn = &(currentMsg->command.tx);
		LogicalTransaction *newTxn = &(new.command.tx);

		newTxn->continued = true;

		newTxn->xid = oldTxn->xid;
		newTxn->beginLSN = oldTxn->beginLSN;
		strlcpy(newTxn->timestamp, oldTxn->timestamp, sizeof(newTxn->timestamp));

		newTxn->first = NULL;

		*currentMsg = new;
	}

	return true;
}


/*
 * stream_transform_message transforms a single JSON message from our streaming
 * output into a SQL statement, and appends it to the given opened transaction.
 */
bool
stream_transform_message(StreamContext *privateContext, char *message)
{
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	JSON_Value *json = json_parse_string(message);

	if (!parseMessageMetadata(metadata, message, json, false))
	{
		/* errors have already been logged */
		return false;
	}

	if (!parseMessage(privateContext, message, json))
	{
		log_error("Failed to parse JSON message: %.1024s%s",
				  message,
				  strlen(message) > 1024 ? "..." : "");
		return false;
	}

	return true;
}


/*
 * stream_transform_rotate prepares the output file where we store the SQL
 * commands on-disk, which is important for restartability of the process.
 */
bool
stream_transform_rotate(StreamContext *privateContext)
{
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	/*
	 * When streaming from stdin to stdout (or other streams), we also maintain
	 * our SQL file on-disk using the WAL file naming strategy from Postgres,
	 * allowing the whole logical decoding follower client to restart.
	 */
	char jsonFileName[MAXPGPATH] = { 0 };
	char sqlFileName[MAXPGPATH] = { 0 };

	if (!stream_compute_pathnames(privateContext->WalSegSz,
								  privateContext->timeline,
								  metadata->lsn,
								  privateContext->paths.dir,
								  jsonFileName,
								  sqlFileName))
	{
		/* errors have already been logged */
		return false;
	}

	/* in most cases, the file name is still the same */
	if (streq(privateContext->sqlFileName, sqlFileName))
	{
		if (privateContext->sqlFile == NULL)
		{
			log_fatal("BUG: privateContext->sqlFile == NULL");
			return false;
		}
		return true;
	}

	/* we might be opening the file for the first time, that's not a switch */
	if (privateContext->sqlFile != NULL &&
		metadata->action != STREAM_ACTION_SWITCH)
	{
		log_error("stream_transform_rotate: BUG, rotation asked on action %c",
				  metadata->action);
		return false;
	}

	/* if we had a SQL file opened, close it now */
	if (!IS_EMPTY_STRING_BUFFER(privateContext->sqlFileName) &&
		privateContext->sqlFile != NULL)
	{
		log_debug("Closing file \"%s\"", privateContext->sqlFileName);

		if (fclose(privateContext->sqlFile) != 0)
		{
			log_error("Failed to close file \"%s\": %m",
					  privateContext->sqlFileName);
			return false;
		}

		/* reset the jsonFile FILE * pointer to NULL, it's closed now */
		privateContext->sqlFile = NULL;

		log_notice("Closed file \"%s\"", privateContext->sqlFileName);
	}

	log_notice("Now transforming changes to \"%s\"", sqlFileName);
	strlcpy(privateContext->walFileName, jsonFileName, MAXPGPATH);
	strlcpy(privateContext->sqlFileName, sqlFileName, MAXPGPATH);

	privateContext->sqlFile =
		fopen_with_umask(sqlFileName, "ab", FOPEN_FLAGS_A, 0644);

	if (privateContext->sqlFile == NULL)
	{
		/* errors have already been logged */
		log_error("Failed to open file \"%s\": %m", sqlFileName);
		return false;
	}

	return true;
}


/*
 * stream_transform_file transforms a JSON formatted file as received from the
 * wal2json logical decoding plugin into an SQL file ready for applying to the
 * target database.
 */
bool
stream_transform_file(StreamSpecs *specs, char *jsonfilename, char *sqlfilename)
{
	StreamContext *privateContext = &(specs->private);
	StreamContent content = { 0 };

	log_notice("Transforming JSON file \"%s\" into SQL file \"%s\"",
			   jsonfilename,
			   sqlfilename);

	strlcpy(content.filename, jsonfilename, sizeof(content.filename));

	/*
	 * Read the JSON-lines file that we received from streaming logical
	 * decoding messages, and parse the JSON messages into our internal
	 * representation structure.
	 */
	char *contents = NULL;
	long size = 0L;

	if (!read_file(content.filename, &contents, &size))
	{
		/* errors have already been logged */
		return false;
	}

	if (!splitLines(&(content.lbuf), contents))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("stream_transform_file: read %lld lines from \"%s\"",
			  (long long) content.lbuf.count,
			  content.filename);

	/*
	 * If the file contains zero lines, we're done already, Also malloc(zero)
	 * leads to "corrupted size vs. prev_size" run-time errors.
	 */
	if (content.lbuf.count == 0)
	{
		return true;
	}

	/*
	 * The output is written to a temp/partial file which is renamed after
	 * close, so that another tool that would want to read the file won't read
	 * partial JSON messages in there.
	 */
	char tempfilename[MAXPGPATH] = { 0 };

	sformat(tempfilename, sizeof(tempfilename), "%s.partial", sqlfilename);

	privateContext->sqlFile =
		fopen_with_umask(tempfilename, "w", FOPEN_FLAGS_W, 0644);

	if (privateContext->sqlFile == NULL)
	{
		log_error("Failed to open file \"%s\"", tempfilename);
		return false;
	}

	log_debug("stream_transform_file writing to \"%s\"", tempfilename);

	uint64_t currentMsgIndex = 0;

	/* we might need to access to the last message metadata after the loop */
	LogicalMessage *currentMsg = &(privateContext->currentMsg);
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	/* we skip KEEPALIVE message in the beginning of the file */
	bool firstMessage = true;

	for (uint64_t i = 0; i < content.lbuf.count; i++)
	{
		char *message = content.lbuf.lines[i];

		LogicalMessageMetadata empty = { 0 };
		*metadata = empty;

		log_trace("stream_transform_file[%4lld]: %s", (long long) i, message);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, false))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * Our SQL file might begin with DML messages, in that case it's a
		 * transaction that continues over a file boundary.
		 */
		if (firstMessage &&
			(metadata->action == STREAM_ACTION_COMMIT ||
			 metadata->action == STREAM_ACTION_ROLLBACK ||
			 metadata->action == STREAM_ACTION_INSERT ||
			 metadata->action == STREAM_ACTION_UPDATE ||
			 metadata->action == STREAM_ACTION_DELETE ||
			 metadata->action == STREAM_ACTION_TRUNCATE))
		{
			LogicalMessage new = { 0 };

			new.isTransaction = true;
			new.action = STREAM_ACTION_BEGIN;

			LogicalTransaction *txn = &(new.command.tx);
			txn->continued = true;

			/*
			 * test_decoding DML logical messages will always have xid = 0.
			 * We handle that in parseMessage STREAM_ACTION_COMMIT by using
			 * the xid from the COMMIT message.
			 */
			txn->xid = metadata->xid;
			txn->first = NULL;

			*currentMsg = new;
		}

		if (!parseMessage(privateContext, message, json))
		{
			log_error("Failed to parse JSON message: %s", message);
			return false;
		}


		/*
		 * Prepare a new message when we just read the COMMIT message of an
		 * opened transaction, closing it, or when we just read a standalone
		 * non-transactional message (such as a KEEPALIVE or a SWITCH WAL or an
		 * ENDPOS message).
		 */
		if (!stream_transform_write_message(privateContext, &currentMsgIndex))
		{
			log_error("Failed to transform and flush the current message, "
					  "see above for details");
			return false;
		}

		/*
		 * skip KEEPALIVE messages at beginning of files in our continued
		 * transaction logic
		 */
		if (firstMessage && metadata->action != STREAM_ACTION_KEEPALIVE)
		{
			firstMessage = false;
		}
	}

	if (fclose(privateContext->sqlFile) == EOF)
	{
		log_error("Failed to close file \"%s\"", tempfilename);
		return false;
	}

	/* reset the sqlFile FILE * pointer to NULL, it's closed now */
	privateContext->sqlFile = NULL;

	log_debug("stream_transform_file: mv \"%s\" \"%s\"",
			  tempfilename, sqlfilename);

	if (rename(tempfilename, sqlfilename) != 0)
	{
		log_error("Failed to move \"%s\" to \"%s\": %m",
				  tempfilename,
				  sqlfilename);
		return false;
	}

	log_info("Transformed %lld JSON messages into SQL file \"%s\"",
			 (long long) content.lbuf.count,
			 sqlfilename);

	return true;
}


/*
 * stream_compute_pathnames computes the WAL.json and WAL.sql filenames from
 * the given LSN, which is expected to be the first LSN processed in the file
 * we need to find the name of.
 */
bool
stream_compute_pathnames(uint32_t WalSegSz,
						 uint32_t timeline,
						 uint64_t lsn,
						 char *dir,
						 char *walFileName,
						 char *sqlFileName)
{
	char wal[MAXPGPATH] = { 0 };

	/* compute the WAL filename that would host the current LSN */
	XLogSegNo segno;
	XLByteToSeg(lsn, segno, WalSegSz);
	XLogFileName(wal, timeline, segno, WalSegSz);

	log_trace("stream_compute_pathnames: %X/%X: %s", LSN_FORMAT_ARGS(lsn), wal);

	sformat(walFileName, MAXPGPATH, "%s/%s.json", dir, wal);
	sformat(sqlFileName, MAXPGPATH, "%s/%s.sql", dir, wal);

	return true;
}


/*
 * parseMessage parses a JSON message as emitted by the logical decoding output
 * plugin (either test_decoding or wal2json) into our own internal
 * representation, that can be later output as SQL text.
 */
bool
parseMessage(StreamContext *privateContext, char *message, JSON_Value *json)
{
	LogicalMessage *mesg = &(privateContext->currentMsg);
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	if (mesg == NULL)
	{
		log_error("BUG: parseMessage called with a NULL LogicalMessage");
		return false;
	}

	if (metadata == NULL)
	{
		log_error("BUG: parseMessage called with a NULL LogicalMessageMetadata");
		return false;
	}

	if (message == NULL && StreamActionIsDML(metadata->action))
	{
		log_error("BUG: parseMessage called with a NULL message");
		return false;
	}

	if (json == NULL && StreamActionIsDML(metadata->action))
	{
		log_error("BUG: parseMessage called with a NULL JSON_Value");
		return false;
	}

	LogicalTransaction *txn = NULL;

	if (mesg->isTransaction)
	{
		txn = &(mesg->command.tx);
	}

	/*
	 * Check that XID make sense for DML actions (Insert, Update, Delete,
	 * Truncate).
	 */
	if (StreamActionIsDML(metadata->action))
	{
		if (mesg->isTransaction)
		{
			if (txn->xid > 0 &&
				metadata->xid > 0 &&
				txn->xid != metadata->xid)
			{
				log_debug("%s", message);
				log_error("BUG: logical message xid is %lld, which is different "
						  "from the current transaction xid %lld",
						  (long long) metadata->xid,
						  (long long) txn->xid);

				return false;
			}
		}
		else
		{
			log_debug("%.1024s", message);
			log_error("BUG: logical message %c received with !isTransaction",
					  metadata->action);
			return false;
		}
	}

	/*
	 * All messages except for BEGIN/COMMIT/ROLLBACK (Transaction Control
	 * Language, or TCL) need a LogicalTransactionStatement to represent them
	 * within the current transaction.
	 */
	LogicalTransactionStatement *stmt = NULL;

	if (!StreamActionIsTCL(metadata->action))
	{
		stmt = (LogicalTransactionStatement *)
			   calloc(1, sizeof(LogicalTransactionStatement));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->action = metadata->action;
		stmt->xid = metadata->xid;
		stmt->lsn = metadata->lsn;

		strlcpy(stmt->timestamp, metadata->timestamp, sizeof(stmt->timestamp));

		/* publish the statement in the privateContext */
		privateContext->stmt = stmt;
	}

	switch (metadata->action)
	{
		/* begin messages only use pgcopydb internal metadata */
		case STREAM_ACTION_BEGIN:
		{
			if (mesg->isTransaction)
			{
				/*
				 * A new BEGIN arrived while a previous transaction was still
				 * open in memory.  This can happen due to a pipeline race: the
				 * range iterator saw only B (not C/T yet), set isTransaction,
				 * then the iterator ended; on the next pass it finds B again
				 * now that C is also visible.  Treat the abandoned partial
				 * transaction as an implicit rollback and start fresh.
				 */
				log_debug("BEGIN %lld while previous transaction open "
						  "(implicit rollback of incomplete txn)",
						  (long long) metadata->xid);

				LogicalMessage empty = { 0 };
				*mesg = empty;
			}

			mesg->isTransaction = true;
			mesg->action = metadata->action;

			txn = &(mesg->command.tx);

			txn->xid = metadata->xid;
			txn->beginLSN = metadata->lsn;

			/*
			 * The timestamp is overwritten at COMMIT as that's what we need
			 * for replication origin tracking.
			 */
			strlcpy(txn->timestamp, metadata->timestamp, sizeof(txn->timestamp));
			txn->first = NULL;

			if (metadata->lsn == InvalidXLogRecPtr ||
				IS_EMPTY_STRING_BUFFER(txn->timestamp))
			{
				log_fatal("Failed to parse BEGIN message: %s", message);
				return false;
			}

			break;
		}

		/* commit messages only use pgcopydb internal metadata */
		case STREAM_ACTION_COMMIT:
		{
			if (!mesg->isTransaction)
			{
				log_error("Failed to parse COMMIT: no transaction in progress");
				return false;
			}

			/* update the timestamp for tracking in replication origin */
			strlcpy(txn->timestamp, metadata->timestamp, sizeof(txn->timestamp));
			txn->commitLSN = metadata->lsn;

			/*
			 * Unlike wal2json, test_decoding don't have xid in the DML logical
			 * messages. So we use the xid from the COMMIT message to update the
			 * transaction xid.
			 */
			txn->xid = metadata->xid;
			txn->commit = true;

			break;
		}

		case STREAM_ACTION_ROLLBACK:
		{
			if (!mesg->isTransaction)
			{
				log_error("Failed to parse ROLLBACK: no transaction in progress");
				return false;
			}

			txn->rollbackLSN = metadata->lsn;
			txn->rollback = true;
			break;
		}

		/* switch wal messages are pgcopydb internal messages */
		case STREAM_ACTION_SWITCH:
		{
			if (mesg->isTransaction)
			{
				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/* maintain the LogicalMessage copy of the metadata */
				mesg->action = metadata->action;
				mesg->lsn = metadata->lsn;
				mesg->command.switchwal.lsn = metadata->lsn;
				strlcpy(mesg->timestamp, metadata->timestamp, PG_MAX_TIMESTAMP);
			}

			break;
		}

		/* keepalive messages are pgcopydb internal messages */
		case STREAM_ACTION_KEEPALIVE:
		{
			if (mesg->isTransaction)
			{
				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/* maintain the LogicalMessage copy of the metadata */
				mesg->action = metadata->action;
				mesg->lsn = metadata->lsn;
				strlcpy(mesg->timestamp, metadata->timestamp, PG_MAX_TIMESTAMP);
			}

			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			if (mesg->isTransaction)
			{
				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/* maintain the LogicalMessage copy of the metadata */
				mesg->action = metadata->action;
				mesg->lsn = metadata->lsn;
				strlcpy(mesg->timestamp, metadata->timestamp, PG_MAX_TIMESTAMP);
			}

			break;
		}

		/*
		 * A pg_logical_emit_message() message. All three plugins filtered
		 * by the "pgcopydb" prefix already, at decode time (pgoutput in
		 * preparePgoutputMessage, wal2json in parseWal2jsonMessageActionAndXid,
		 * test_decoding in parseTestDecodingMessageActionAndXid) -- getting
		 * here means the prefix matched. What's left is unwrapping the
		 * envelope JSON text, which each plugin carries differently:
		 *
		 *  - pgoutput stores the envelope bytes directly as jsonBuffer, so
		 *    json IS the envelope already.
		 *  - wal2json's jsonBuffer is its own JSON object, with our envelope
		 *    JSON text nested under the "content" key.
		 *  - test_decoding's jsonBuffer is the raw decoder line, JSON-string
		 *    escaped; our envelope JSON text follows "content:" in there.
		 */
		case STREAM_ACTION_MESSAGE:
		{
			/*
			 * A message is not always transactional: pg_logical_emit_message()
			 * takes a "transactional" argument, and when false, Postgres
			 * delivers it to the output plugin outright, bypassing the
			 * reorder buffer's commit-ordered replay entirely -- no
			 * preceding BEGIN at all (see ReorderBufferQueueMessage's
			 * non-transactional branch and message_cb_wrapper calling
			 * message_cb directly, in
			 * src/backend/replication/logical/reorderbuffer.c and logical.c).
			 *
			 * Our own event trigger always calls pg_logical_emit_message()
			 * with transactional=true (cli_stream.c), so a "pgcopydb"-prefixed
			 * "ddl" envelope is always transactional in practice; this check
			 * only rejects a different, non-transactional emitter spoofing
			 * our prefix, rather than assuming every message is one of ours.
			 */
			if (!mesg->isTransaction)
			{
				log_error("Failed to parse MESSAGE: no transaction in progress");
				return false;
			}

			JSON_Object *jsobj = NULL;

			switch (privateContext->plugin)
			{
				case STREAM_PLUGIN_PGOUTPUT:
				{
					jsobj = json_value_get_object(json);
					break;
				}

				case STREAM_PLUGIN_WAL2JSON:
				{
					JSON_Object *outer = json_value_get_object(json);
					const char *content = outer != NULL ?
										  json_object_get_string(outer, "content") : NULL;

					if (content == NULL)
					{
						log_error("Failed to parse wal2json message, "
								  "missing \"content\": %.1024s",
								  message != NULL ? message : "(null)");
						return false;
					}

					JSON_Value *envelope = json_parse_string(content);
					jsobj = json_value_get_object(envelope);
					break;
				}

				case STREAM_PLUGIN_TEST_DECODING:
				{
					const char *line = json_value_get_string(json);

					bool transactional = false;
					char prefix[PG_NAMEDATALEN] = { 0 };
					const char *contentPtr = NULL;
					size_t contentSize = 0;

					if (line == NULL ||
						!parseTestDecodingMessageEnvelopeHeader(line,
																&transactional,
																prefix,
																sizeof(prefix),
																&contentPtr,
																&contentSize))
					{
						log_error("Failed to parse test_decoding message: "
								  "%.1024s", message != NULL ? message : "(null)");
						return false;
					}

					char *content = strndup(contentPtr, contentSize);

					if (content == NULL)
					{
						log_error(ALLOCATION_FAILED_ERROR);
						return false;
					}

					JSON_Value *envelope = json_parse_string(content);
					free(content);
					jsobj = json_value_get_object(envelope);
					break;
				}

				default:
				{
					log_error("BUG: parseMessage called with unknown plugin %d",
							  privateContext->plugin);
					return false;
				}
			}

			const char *type = jsobj != NULL ?
							   json_object_get_string(jsobj, "type") : NULL;

			if (type == NULL)
			{
				log_error("Failed to parse message envelope, missing \"type\": "
						  "%.1024s", message != NULL ? message : "(null)");
				return false;
			}

			if (strcmp(type, "ddl") == 0)
			{
				const char *body = json_object_get_string(jsobj, "body");

				if (body == NULL)
				{
					log_error("Failed to parse ddl message, missing \"body\": "
							  "%.1024s", message != NULL ? message : "(null)");
					return false;
				}

				stmt->stmt.ddl.body = strdup(body);

				if (stmt->stmt.ddl.body == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/*
				 * Unrecognised type: log and skip, not an error, so an
				 * older pgcopydb binary tolerates a newer source emitting
				 * a type it does not know about yet. stmt was allocated
				 * above but never appended; free it.
				 */
				log_warn("Ignoring message of unknown type \"%s\"", type);
				free(stmt);
				privateContext->stmt = NULL;
			}

			break;
		}

		/* now handle DML messages from the output plugin */
		default:
		{
			if (!mesg->isTransaction)
			{
				log_error("Failed to parse action %c: no transaction in progress",
						  metadata->action);
				return false;
			}

			/*
			 * Dispatch to the correct DML parser based on the output plugin
			 * that was recorded at slot creation time.  This avoids inspecting
			 * the JSON shape of each individual message; the plugin is fixed
			 * for the lifetime of the replication slot.
			 */
			switch (privateContext->plugin)
			{
				case STREAM_PLUGIN_TEST_DECODING:
				{
					if (!parseTestDecodingMessage(privateContext, message, json))
					{
						log_error("Failed to parse test_decoding message, "
								  "see above for details");
						return false;
					}
					break;
				}

				case STREAM_PLUGIN_WAL2JSON:
				{
					if (!parseWal2jsonMessage(privateContext, message, json))
					{
						log_error("Failed to parse wal2json message, "
								  "see above for details");
						return false;
					}
					break;
				}

				default:
				{
					log_error("Failed to parse DML message: "
							  "unknown output plugin %d",
							  privateContext->plugin);
					return false;
				}
			}

			if (!stream_transform_locate_by_key(privateContext, stmt))
			{
				/* errors have already been logged */
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}
	}

	return true;
}


/*
 * appendValuesArray appends the first entry of newValuesArray into
 * lastValuesArray, resizing the backing array with doubling if needed.
 * The moved entry is removed from newValuesArray (count set to 0) so that
 * it is not freed when the source statement is released.
 */
static bool
appendValuesArray(LogicalMessageValuesArray *lastValuesArray,
				  LogicalMessageValuesArray *newValuesArray)
{
	int capacity = lastValuesArray->capacity;
	LogicalMessageValues *array = lastValuesArray->array;

	/*
	 * Check if the current LogicalMessageValues array has enough space to hold
	 * the values from the new statement. If not, resize the lastValuesArray
	 * using realloc.
	 */
	if (capacity < (lastValuesArray->count + 1))
	{
		/*
		 * Additionally, we allocate more space than currently needed to avoid
		 * repeated reallocation on every new value append. This trade-off
		 * increases memory usage slightly but reduces the reallocation overhead
		 * and potential heap memory fragmentation.
		 */
		capacity *= 2;
		array = (LogicalMessageValues *)
				realloc(array, sizeof(LogicalMessageValues) * capacity);

		if (array == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		lastValuesArray->array = array;
		lastValuesArray->capacity = capacity;
	}

	/*
	 * Move the new value from the 'newValuesArray' to the 'lastValuesArray' of
	 * the existing statement. Additionally, set the count of the 'newValuesArray'
	 * to 0 to prevent it from being deallocated by FreeLogicalMessageTupleArray,
	 * as it has been moved to the 'lastValuesArray'.
	 */
	lastValuesArray->array[lastValuesArray->count++] = newValuesArray->array[0];
	newValuesArray->count = 0;

	return true;
}


/*
 * coalesceLogicalTransactionStatement appends a new entry to an existing tuple
 * array created during the last INSERT or DELETE statement in a logical
 * transaction.
 *
 * This functionality enables the generation of multi-values INSERT/COPY
 * commands and multi-row DELETE ... WHERE pk IN (...) statements, enhancing
 * efficiency.
 *
 * Important: Before invoking this function, ensure that validation is performed
 * using canCoalesceLogicalTransactionStatement.
 */
static bool
coalesceLogicalTransactionStatement(LogicalTransaction *txn,
									LogicalTransactionStatement *new)
{
	LogicalTransactionStatement *last = txn->last;

	if (last->action == STREAM_ACTION_INSERT)
	{
		return appendValuesArray(&(last->stmt.insert.new.array->values),
								 &(new->stmt.insert.new.array->values));
	}
	else if (last->action == STREAM_ACTION_DELETE)
	{
		return appendValuesArray(&(last->stmt.delete.old.array->values),
								 &(new->stmt.delete.old.array->values));
	}

	/* should not be reached */
	log_error("BUG: coalesceLogicalTransactionStatement called for action %c",
			  last->action);
	return false;
}


/*
 * canCoalesceLogicalTransactionStatement checks the new statement is
 * same as the last statement in the txn by comparing the relation name,
 * column count and column names.
 *
 * This acts as a validation function for coalesceLogicalTransactionStatement.
 */
static bool
canCoalesceLogicalTransactionStatement(LogicalTransaction *txn,
									   LogicalTransactionStatement *new)
{
	LogicalTransactionStatement *last = txn->last;

	/* Only coalesce same-action pairs */
	if (last->action != new->action)
	{
		return false;
	}

	if (last->action == STREAM_ACTION_INSERT)
	{
		LogicalMessageInsert *lastInsert = &last->stmt.insert;
		LogicalMessageInsert *newInsert = &new->stmt.insert;

		/* Last and current statements must target same relation */
		if (!streq(lastInsert->table.nspname, newInsert->table.nspname) ||
			!streq(lastInsert->table.relname, newInsert->table.relname))
		{
			return false;
		}

		LogicalMessageTuple *lastInsertColumns = lastInsert->new.array;
		LogicalMessageTuple *newInsertColumns = newInsert->new.array;

		/* Last and current statements must have same number of columns */
		if (lastInsertColumns->attributes.count != newInsertColumns->attributes.count)
		{
			return false;
		}

		LogicalMessageValuesArray *lastValuesArray = &(lastInsert->new.array->values);

		/*
		 * Check if adding the new statement would exceed libpq's limit on the
		 * total number of parameters allowed in a single PQsendPrepare call.
		 *
		 * TODO: This parameter limit check is not applicable for COPY
		 * operations. It should be removed once we switch to using COPY.
		 */
		if (((lastValuesArray->count + 1) * lastInsertColumns->attributes.count) >
			PQ_QUERY_PARAM_MAX_LIMIT)
		{
			return false;
		}

		/* Last and current statements cols must have same name and order */
		for (int i = 0; i < lastInsertColumns->attributes.count; i++)
		{
			LogicalMessageAttribute *lastAttr =
				&(lastInsertColumns->attributes.array[i]);
			LogicalMessageAttribute *newAttr =
				&(newInsertColumns->attributes.array[i]);

			if (!streq(lastAttr->attname, newAttr->attname))
			{
				return false;
			}
		}

		return true;
	}

	if (last->action == STREAM_ACTION_DELETE)
	{
		LogicalMessageDelete *lastDelete = &last->stmt.delete;
		LogicalMessageDelete *newDelete = &new->stmt.delete;

		/* Both must target the same relation */
		if (!streq(lastDelete->table.nspname, newDelete->table.nspname) ||
			!streq(lastDelete->table.relname, newDelete->table.relname))
		{
			return false;
		}

		LogicalMessageTuple *lastTuple = lastDelete->old.array;
		LogicalMessageTuple *newTuple = newDelete->old.array;

		/* Same number of identity columns */
		if (lastTuple->attributes.count != newTuple->attributes.count)
		{
			return false;
		}

		int colCount = newTuple->attributes.count;
		LogicalMessageValuesArray *lastValuesArray = &(lastTuple->values);

		/* Parameter count limit: same reasoning as INSERT */
		if (((lastValuesArray->count + 1) * colCount) > PQ_QUERY_PARAM_MAX_LIMIT)
		{
			return false;
		}

		/*
		 * The multi-row DELETE uses WHERE (k1, k2) IN (($1,$2), ($3,$4)), which
		 * does not handle NULLs (NULL = NULL is false in SQL).  For primary-key
		 * tables this never matters: PRIMARY KEY implies NOT NULL.  For
		 * REPLICA IDENTITY FULL or nullable REPLICA IDENTITY INDEX columns,
		 * fall back to per-row by refusing to coalesce any NULL-bearing row.
		 *
		 * Check the incoming (new) row unconditionally.  Check the existing
		 * batch's first row when the batch has exactly one row (i.e., the first
		 * coalesce into this batch): once count > 1 the batch is null-free by
		 * construction (every prior new row passed the same check).
		 */
		LogicalMessageValues *newValues = &(newTuple->values.array[0]);
		for (int v = 0; v < newValues->cols; v++)
		{
			if (newValues->array[v].isNull)
			{
				return false;
			}
		}

		if (lastValuesArray->count == 1)
		{
			LogicalMessageValues *existingFirstRow = &(lastValuesArray->array[0]);
			for (int v = 0; v < existingFirstRow->cols; v++)
			{
				if (existingFirstRow->array[v].isNull)
				{
					return false;
				}
			}
		}

		/* Same identity column names in the same order */
		for (int i = 0; i < colCount; i++)
		{
			if (!streq(lastTuple->attributes.array[i].attname,
					   newTuple->attributes.array[i].attname))
			{
				return false;
			}
		}

		return true;
	}

	return false;
}


/*
 * streamLogicalTransactionAppendStatement appends a statement to the current
 * transaction.
 *
 * There are two ways to append a statement to an existing transaction:
 *
 *  1. it's a new statement altogether, we just append to the linked-list
 *
 *  2. it's the same statement as the previous one, we only add an entry to the
 *     already existing tuple array created on the previous statement
 *
 * This allows to then generate multi-values insert commands, for instance.
 */
bool
streamLogicalTransactionAppendStatement(LogicalTransaction *txn,
										LogicalTransactionStatement *stmt)
{
	if (txn == NULL)
	{
		log_error("BUG: streamLogicalTransactionAppendStatement "
				  "called with a NULL LogicalTransaction");
		return false;
	}

	if (stmt == NULL)
	{
		log_error("BUG: streamLogicalTransactionAppendStatement "
				  "called with a NULL LogicalTransactionStatement");
		return false;
	}

	if (txn->first == NULL)
	{
		txn->first = stmt;
		txn->last = stmt;

		stmt->prev = NULL;
		stmt->next = NULL;
	}
	else
	{
		if (canCoalesceLogicalTransactionStatement(txn, stmt))
		{
			if (!coalesceLogicalTransactionStatement(txn, stmt))
			{
				/* errors have already been logged */
				return false;
			}
		}
		else
		{
			/* update the current last entry of the linked-list */
			txn->last->next = stmt;

			/* the new statement now becomes the last entry of the linked-list */
			stmt->prev = txn->last;
			stmt->next = NULL;
			txn->last = stmt;
		}
	}
	++txn->count;

	return true;
}


/*
 * allocateLogicalMessageTuple allocates memory for count columns (and values)
 * for the given LogicalMessageTuple.
 */
bool
AllocateLogicalMessageTuple(LogicalMessageTuple *tuple, int count)
{
	tuple->attributes.count = count;

	if (count == 0)
	{
		LogicalMessageValuesArray *valuesArray = &(tuple->values);
		valuesArray->count = 0;
		valuesArray->capacity = 0;
		valuesArray->array = NULL;

		tuple->attributes.array = NULL;

		return true;
	}

	tuple->attributes.array = (LogicalMessageAttribute *) calloc(count,
																 sizeof(
																	 LogicalMessageAttribute));

	if (tuple->attributes.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Allocate the tuple values, an array of VALUES, as in SQL.
	 *
	 * It actually supports multi-values clauses (single column names array,
	 * multiple VALUES matching the same metadata definition).
	 *
	 * The goal is to be able to represent VALUES(a1, b1, c1), (a2, b2, c2).
	 *
	 * Refer coalesceLogicalTransactionStatement for more details.
	 */
	LogicalMessageValuesArray *valuesArray = &(tuple->values);

	valuesArray->count = 1;
	valuesArray->capacity = 1;
	valuesArray->array =
		(LogicalMessageValues *) calloc(1, sizeof(LogicalMessageValues));

	if (valuesArray->array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* allocate one VALUES entry */
	LogicalMessageValues *values = &(tuple->values.array[0]);
	values->cols = count;
	values->array =
		(LogicalMessageValue *) calloc(count, sizeof(LogicalMessageValue));

	if (values->array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	return true;
}


/*
 * stream_transform_write_replay_stmt writes the current message to the
 * replayDB stmt and replay tables.
 */
bool
stream_transform_write_replay_stmt(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);
	LogicalMessage *msg = &(privateContext->currentMsg);

	if (msg->isTransaction)
	{
		if (!stream_transform_write_replay_txn(specs))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		ReplayDBStmt replayStmt = {
			.action = msg->action,
			.xid = msg->xid,
			.lsn = msg->lsn
		};

		strlcpy(replayStmt.timestamp, msg->timestamp, PG_MAX_TIMESTAMP);

		if (replayStmt.action != STREAM_ACTION_SWITCH &&
			replayStmt.action != STREAM_ACTION_KEEPALIVE &&
			replayStmt.action != STREAM_ACTION_ENDPOS)
		{
			log_error("BUG: Failed to write SQL for unexpected "
					  "LogicalMessage action %d",
					  replayStmt.action);
			return false;
		}

		if (!ld_store_insert_replay_stmt(specs->replayDB, &replayStmt))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * stream_transform_write_replay_txn writes the rest of a transaction to the
 * replayDB stmt and replay tables, with its COMMIT or ROLLBACK row: what the
 * transform has not already written as the transaction was still open.
 */
bool
stream_transform_write_replay_txn(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);
	LogicalMessage *msg = &(privateContext->currentMsg);
	LogicalTransaction *txn = &(msg->command.tx);

	uint64_t endlsn = txn->commit ? txn->commitLSN : txn->rollbackLSN;

	log_debug("stream_transform_write_replay_txn: lsn %X/%X endlsn %X/%X",
			  LSN_FORMAT_ARGS(txn->beginLSN),
			  LSN_FORMAT_ARGS(endlsn));

	if (!stream_transform_write_replay_begin(specs, endlsn) ||
		!stream_transform_write_replay_stmts(specs))
	{
		/* errors have already been logged */
		return false;
	}

	ReplayDBStmt end = {
		.action = txn->rollback ? STREAM_ACTION_ROLLBACK : STREAM_ACTION_COMMIT,
		.xid = txn->xid,
		.lsn = txn->rollback ? txn->rollbackLSN : txn->commitLSN
	};

	strlcpy(end.timestamp, txn->timestamp, sizeof(end.timestamp));

	if (!ld_store_insert_replay_stmt(specs->replayDB, &end))
	{
		/* errors have already been logged */
		return false;
	}

	/* the transaction is complete: no longer open */
	privateContext->openXid = 0;
	privateContext->openBeginId = 0;
	privateContext->openLastId = 0;
	privateContext->openReplayBeginId = 0;

	return true;
}


/*
 * stream_transform_write_replay_begin writes the BEGIN row of the current
 * transaction to replayDB.  The apply process takes a transaction by the end
 * LSN of its BEGIN row, which a transaction that is still open does not have
 * yet: it is written without, and invisible to apply until it is set.
 */
static bool
stream_transform_write_replay_begin(StreamSpecs *specs, uint64_t endlsn)
{
	StreamContext *privateContext = &(specs->private);
	LogicalTransaction *txn = &(privateContext->currentMsg.command.tx);

	if (privateContext->openReplayBeginId != 0)
	{
		if (endlsn == InvalidXLogRecPtr)
		{
			return true;
		}

		return ld_store_replay_set_endlsn(specs->replayDB,
										  privateContext->openReplayBeginId,
										  endlsn);
	}

	ReplayDBStmt begin = {
		.action = STREAM_ACTION_BEGIN,
		.xid = txn->xid,
		.lsn = txn->beginLSN,
		.endlsn = endlsn
	};

	strlcpy(begin.timestamp, txn->timestamp, sizeof(begin.timestamp));

	if (!ld_store_insert_replay_stmt(specs->replayDB, &begin))
	{
		/* errors have already been logged */
		return false;
	}

	if (endlsn == InvalidXLogRecPtr)
	{
		privateContext->openReplayBeginId =
			sqlite3_last_insert_rowid(specs->replayDB->db);
	}

	return true;
}


/*
 * stream_transform_write_replay_stmts writes the statements the current
 * transaction has gathered so far to replayDB, and lets go of them: a
 * transaction that is still open goes on from an empty list.
 */
static bool
stream_transform_write_replay_stmts(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);
	LogicalMessage *msg = &(privateContext->currentMsg);
	LogicalTransaction *txn = &(msg->command.tx);

	GeneratedColumnsCache *cache = privateContext->generatedColumnsCache;

	if (cache != NULL)
	{
		if (!markGeneratedColumnsFromTransaction(cache, txn))
		{
			log_error("Failed to mark generated columns from transaction");
			return false;
		}
	}

	LogicalTransactionStatement *currentStmt = txn->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		ReplayDBStmt stmt = {
			.action = currentStmt->action,
			.xid = currentStmt->xid,
			.lsn = currentStmt->lsn
		};

		strlcpy(stmt.timestamp, currentStmt->timestamp, PG_MAX_TIMESTAMP);

		switch (currentStmt->action)
		{
			case STREAM_ACTION_INSERT:
			{
				if (!stream_write_insert(&stmt, &(currentStmt->stmt.insert)))
				{
					/* errors have already been logged */
					return false;
				}

				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				if (!stream_write_update(specs->replayNoOpUpdates,
										 &stmt, &(currentStmt->stmt.update)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				if (!stream_write_delete(&stmt, &(currentStmt->stmt.delete)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			case STREAM_ACTION_TRUNCATE:
			{
				if (!stream_write_truncate(&stmt, &(currentStmt->stmt.truncate)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			case STREAM_ACTION_MESSAGE:
			{
				if (!stream_write_ddl(&stmt, &(currentStmt->stmt.ddl)))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}

			default:
			{
				log_error("BUG: Failed to write unexepected SQL action %d",
						  currentStmt->action);
				return false;
			}
		}

		if (!ld_store_insert_replay_stmt(specs->replayDB, &stmt))
		{
			/* errors have already been logged */
			return false;
		}
	}

	txn->first = NULL;
	txn->last = NULL;
	txn->count = 0;

	return true;
}


/*
 * stream_transform_open_transaction transforms what receive has committed to
 * outputDB of a transaction it has not reached the COMMIT of, and writes it to
 * replayDB as a transaction that cannot be applied yet.  The pass that finds
 * the COMMIT row writes the rest, and makes it visible to apply.
 *
 * Such a transaction is complete once decoded and committed: with pgoutput
 * (and the other plugins pgcopydb reads) the server only sends a transaction
 * once it has committed.  What receive spools of it is a prefix of that.
 */
static bool
stream_transform_open_transaction(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	if (privateContext->openXid != privateContext->pendingXid ||
		privateContext->openBeginId != privateContext->pendingBeginId)
	{
		/*
		 * The first piece of this transaction, or receive replaced it (it
		 * went back to the server for it).  Whatever replayDB holds of an
		 * earlier attempt is not part of it.
		 */
		LogicalMessage empty = { 0 };

		privateContext->currentMsg = empty;

		bool dropped = false;

		if (!ld_store_replay_drop_open(specs->replayDB, &dropped))
		{
			/* errors have already been logged */
			return false;
		}

		if (dropped)
		{
			privateContext->openGeneration++;
		}

		privateContext->openXid = privateContext->pendingXid;
		privateContext->openBeginId = privateContext->pendingBeginId;
		privateContext->openLastId = privateContext->pendingBeginId - 1;
		privateContext->openReplayBeginId = 0;
	}

	if (!ld_store_iter_output_open(specs, &stream_transform_cdc_file_hook))
	{
		/* errors have already been logged */
		return false;
	}

	LogicalMessage *msg = &(privateContext->currentMsg);

	if (!msg->isTransaction)
	{
		/* the BEGIN row has not come through: nothing to write yet */
		return true;
	}

	return stream_transform_write_replay_begin(specs, InvalidXLogRecPtr) &&
		   stream_transform_write_replay_stmts(specs);
}


/*
 * stream_write_insert writes an INSERT statement into the replayDB.
 */
static bool
stream_write_insert(ReplayDBStmt *replayStmt, LogicalMessageInsert *insert)
{
	strlcpy(replayStmt->nspname, insert->table.nspname, sizeof(replayStmt->nspname));
	strlcpy(replayStmt->relname, insert->table.relname, sizeof(replayStmt->relname));

	/* loop over INSERT statements targeting the same table */
	for (int s = 0; s < insert->new.count; s++)
	{
		LogicalMessageTuple *stmt = &(insert->new.array[s]);

		PQExpBuffer buf = createPQExpBuffer();

		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		/*
		 * First, the PREPARE part.
		 */
		appendPQExpBuffer(buf, "INSERT INTO %s.%s ",
						  insert->table.nspname,
						  insert->table.relname);

		/* loop over column names and add them to the out stream */
		appendPQExpBuffer(buf, "%s", "(");

		LogicalMessageAttribute *attr = &(stmt->attributes.array[0]);

		for (int c = 0; c < stmt->attributes.count; c++)
		{
			/* skip generated columns */
			if (!attr[c].isgenerated)
			{
				appendPQExpBuffer(buf, "%s%s",
								  c > 0 ? ", " : "",
								  attr[c].attname);
			}
		}

		appendPQExpBuffer(buf, "%s", ")");

		/*
		 * See https://www.postgresql.org/docs/current/sql-insert.html
		 *
		 * OVERRIDING SYSTEM VALUE
		 *
		 * If this clause is specified, then any values supplied for identity
		 * columns will override the default sequence-generated values.
		 *
		 * For an identity column defined as GENERATED ALWAYS, it is an error
		 * to insert an explicit value (other than DEFAULT) without specifying
		 * either OVERRIDING SYSTEM VALUE or OVERRIDING USER VALUE. (For an
		 * identity column defined as GENERATED BY DEFAULT, OVERRIDING SYSTEM
		 * VALUE is the normal behavior and specifying it does nothing, but
		 * PostgreSQL allows it as an extension.)
		 */
		appendPQExpBufferStr(buf, " overriding system value VALUES ");

		int pos = 0;

		for (int r = 0; r < stmt->values.count; r++)
		{
			LogicalMessageValues *values = &(stmt->values.array[r]);

			/* now loop over column values for this VALUES row */
			appendPQExpBuffer(buf, "%s(", r > 0 ? ", " : "");

			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				/*
				 * Instead of skipping the generated column, we could have
				 * set the value to DEFAULT. But, PG13 doesn't allow multi
				 * value INSERT with DEFAULT for generated columns.
				 *
				 * TODO: Once we stop supporting PG13, set the value to DEFAULT
				 * for generated columns similar to UPDATE.
				 */
				if (!attr[v].isgenerated)
				{
					appendPQExpBuffer(buf, "%s$%d",
									  v > 0 ? ", " : "",
									  ++pos);

					if (!stream_add_value_in_json_array(value, jsArray))
					{
						/* errors have already been logged */
						destroyPQExpBuffer(buf);
						return false;
					}
				}
			}

			appendPQExpBufferStr(buf, ")");
		}

		if (PQExpBufferBroken(buf))
		{
			log_error("Failed to transform INSERT statement: Out of Memory");
			destroyPQExpBuffer(buf);
			return false;
		}

		/* compute the hash and prepare the JSONB data array */
		uint32_t hash = hashlittle(buf->data, buf->len, 5381);
		char *serialized_string = json_serialize_to_string(js);

		replayStmt->hash = hash;
		replayStmt->stmt = strdup(buf->data);
		replayStmt->data = strdup(serialized_string);

		destroyPQExpBuffer(buf);
		json_free_serialized_string(serialized_string);
	}

	return true;
}


/*
 * stream_write_update writes an UPDATE statement.
 */
static bool
stream_write_update(bool replayNoOpUpdates,
					ReplayDBStmt *replayStmt,
					LogicalMessageUpdate *update)
{
	strlcpy(replayStmt->nspname, update->table.nspname, sizeof(replayStmt->nspname));
	strlcpy(replayStmt->relname, update->table.relname, sizeof(replayStmt->relname));

	if (update->old.count != update->new.count)
	{
		log_error("Failed to write UPDATE statement "
				  "with %d old rows and %d new rows",
				  update->old.count,
				  update->new.count);
		return false;
	}

	/* loop over UPDATE statements targeting the same table */
	for (int s = 0; s < update->old.count; s++)
	{
		LogicalMessageTuple *old = &(update->old.array[s]);
		LogicalMessageTuple *new = &(update->new.array[s]);

		if (old->values.count == 0 && new->values.count == 0)
		{
			log_trace("stream_write_update: Skipping empty UPDATE statement");
			continue;
		}
		else if (old->values.count != new->values.count ||
				 old->values.count != 1 ||
				 new->values.count != 1)
		{
			log_error("Failed to write multi-values UPDATE statement "
					  "with %d old rows and %d new rows",
					  old->values.count,
					  new->values.count);
			return false;
		}

		PQExpBuffer buf = createPQExpBuffer();

		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		/*
		 * First, the PREPARE part.
		 */
		appendPQExpBuffer(buf, "UPDATE %s.%s SET ",
						  update->table.nspname,
						  update->table.relname);
		int pos = 0;

		int skipColumnCount = 0;

		for (int r = 0; r < new->values.count; r++)
		{
			LogicalMessageValues *values = &(new->values.array[r]);

			bool first = true;


			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageAttribute *attr = &(new->attributes.array[v]);
				LogicalMessageValue *value = &(values->array[v]);

				if (new->attributes.count <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  new->attributes.count);
					destroyPQExpBuffer(buf);
					return false;
				}

				/*
				 * Avoid SET "id" = 1 WHERE "id" = 1 ; so for that we lookup
				 * for a column with the same name in the old parts, and with
				 * the same value too.
				 */
				bool skip = false;

				for (int oc = 0; oc < old->attributes.count; oc++)
				{
					LogicalMessageAttribute *oldAttr = &(old->attributes.array[oc]);
					if (streq(oldAttr->attname, attr->attname))
					{
						/* only works because old->values.count == 1 */
						LogicalMessageValue *oldValue =
							&(old->values.array[0].array[oc]);

						if (!replayNoOpUpdates &&
							LogicalMessageValueEq(oldValue, value))
						{
							skip = true;
							break;
						}
					}
				}

				if (skip || attr->isidentityalways)
				{
					/*
					 * Skip this column from the SET clause.
					 *
					 * For value-unchanged columns (skip=true): the old and new
					 * values are equal, so setting it is a no-op.
					 *
					 * For GENERATED ALWAYS AS IDENTITY columns: PostgreSQL
					 * rejects "SET col = $N" with "can only be updated to
					 * DEFAULT".  Unlike INSERT (where OVERRIDING SYSTEM VALUE
					 * lets us supply the value), there is no equivalent for
					 * UPDATE, so we must omit these columns from the SET clause.
					 * The identity value is stable across normal UPDATEs, so
					 * omitting it is correct.
					 */
					++skipColumnCount;
				}
				else
				{
					if (attr->isgenerated)
					{
						appendPQExpBuffer(buf, "%s%s = DEFAULT",
										  first ? "" : ", ",
										  attr->attname);
					}
					else
					{
						appendPQExpBuffer(buf, "%s%s = $%d",
										  first ? "" : ", ",
										  attr->attname,
										  ++pos);

						if (!stream_add_value_in_json_array(value, jsArray))
						{
							/* errors have already been logged */
							destroyPQExpBuffer(buf);
							return false;
						}
					}

					if (first)
					{
						first = false;
					}
				}
			}
		}

		appendPQExpBufferStr(buf, " WHERE ");

		/*
		 * With REPLICA IDENTITY FULL the old tuple is the whole row, and the
		 * row is located by its primary key alone when the table has one, as
		 * native logical replication and pglogical do: the target may differ
		 * from the source in other columns.
		 */
		bool keyOnly = false;

		for (int oc = 0; oc < old->attributes.count; oc++)
		{
			keyOnly = keyOnly || old->attributes.array[oc].iskey;
		}

		int whereCount = 0;

		for (int r = 0; r < old->values.count; r++)
		{
			LogicalMessageValues *values = &(old->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageAttribute *attr = &(old->attributes.array[v]);
				LogicalMessageValue *value = &(values->array[v]);

				if (old->attributes.count <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  old->attributes.count);
					destroyPQExpBuffer(buf);
					return false;
				}

				if (keyOnly && !attr->iskey)
				{
					continue;
				}

				if (value->isNull)
				{
					/*
					 * Attributes with the value `NULL` require `IS NULL`
					 * instead of `=` in the WHERE clause.
					 */
					appendPQExpBuffer(buf, "%s%s IS NULL",
									  whereCount++ > 0 ? " and " : "",
									  attr->attname);
				}
				else
				{
					appendPQExpBuffer(buf, "%s%s = $%d",
									  whereCount++ > 0 ? " and " : "",
									  attr->attname,
									  ++pos);

					if (!stream_add_value_in_json_array(value, jsArray))
					{
						/* errors have already been logged */
						destroyPQExpBuffer(buf);
						return false;
					}
				}
			}
		}

		if (PQExpBufferBroken(buf))
		{
			log_error("Failed to transform UPDATE statement: Out of Memory");
			destroyPQExpBuffer(buf);
			return false;
		}

		/*
		 * When all column values in the SET clause are equal to those in the
		 * WHERE clause, we remove all columns from the SET clause. This results
		 * in an invalid UPDATE statement like the one shown below:
		 *
		 * UPDATE table SET WHERE "id" = 1;
		 *
		 * Usually, the above could happen when REPLICA IDENTITY is set to FULL,
		 * and the UPDATE statement executed with the same values as the old ones.
		 * For e.g.
		 * UPDATE table SET "id" = 1 WHERE "id" = 1;
		 *
		 * Skip the UPDATE statement in such cases.
		 */
		bool skipUpdate = (skipColumnCount == new->attributes.count);

		if (skipUpdate)
		{
			log_debug("Skipping no-op UPDATE statement: "
					  "all column values are unchanged");

			destroyPQExpBuffer(buf);
			return true;
		}

		/* compute the hash and prepare the JSONB data array */
		uint32_t hash = hashlittle(buf->data, buf->len, 5381);
		char *serialized_string = json_serialize_to_string(js);

		replayStmt->hash = hash;
		replayStmt->stmt = strdup(buf->data);
		replayStmt->data = strdup(serialized_string);

		destroyPQExpBuffer(buf);
		json_free_serialized_string(serialized_string);
	}

	return true;
}


/*
 * stream_write_delete writes a DELETE statement.
 *
 * When multiple DELETE rows for the same table have been coalesced into a
 * single LogicalMessageDelete (old->values.count > 1), a single batched
 * statement is emitted:
 *
 *   Single-column identity:
 *     DELETE FROM ns.tbl WHERE col IN ($1, $2, $3)
 *
 *   Multi-column identity:
 *     DELETE FROM ns.tbl WHERE (k1, k2) IN (($1, $2), ($3, $4))
 *
 * The batch form is only used when all identity values are non-NULL, which
 * canCoalesceLogicalTransactionStatement guarantees.  A single row with NULL
 * identity values falls through to the per-row path which uses IS NULL.
 */
static bool
stream_write_delete(ReplayDBStmt *replayStmt, LogicalMessageDelete *delete)
{
	strlcpy(replayStmt->nspname, delete->table.nspname, sizeof(replayStmt->nspname));
	strlcpy(replayStmt->relname, delete->table.relname, sizeof(replayStmt->relname));

	/* loop over distinct identity schemas (count == 1 in all practical cases) */
	for (int s = 0; s < delete->old.count; s++)
	{
		LogicalMessageTuple *old = &(delete->old.array[s]);
		int rowCount = old->values.count;
		int colCount = old->attributes.count;

		PQExpBuffer buf = createPQExpBuffer();
		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		appendPQExpBuffer(buf, "DELETE FROM %s.%s WHERE ",
						  delete->table.nspname,
						  delete->table.relname);

		if (rowCount == 1)
		{
			/*
			 * Single-row path: col1 = $1 AND col2 = $2 ...
			 * NULL identity values are rendered as col IS NULL.
			 */
			LogicalMessageValues *values = &(old->values.array[0]);
			int pos = 0;

			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);
				LogicalMessageAttribute *attr = &(old->attributes.array[v]);

				if (colCount <= v)
				{
					log_error("Failed to write DELETE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  colCount);
					destroyPQExpBuffer(buf);
					return false;
				}

				if (value->isNull)
				{
					appendPQExpBuffer(buf, "%s%s IS NULL",
									  v > 0 ? " and " : "",
									  attr->attname);
				}
				else
				{
					appendPQExpBuffer(buf, "%s%s = $%d",
									  v > 0 ? " and " : "",
									  attr->attname,
									  ++pos);

					if (!stream_add_value_in_json_array(value, jsArray))
					{
						destroyPQExpBuffer(buf);
						return false;
					}
				}
			}
		}
		else
		{
			/*
			 * Multi-row batch path.  All values are non-NULL (enforced by
			 * canCoalesceLogicalTransactionStatement).
			 */
			int pos = 0;

			if (colCount == 1)
			{
				/* WHERE col IN ($1, $2, $3) */
				appendPQExpBuffer(buf, "%s IN (",
								  old->attributes.array[0].attname);

				for (int r = 0; r < rowCount; r++)
				{
					LogicalMessageValues *values = &(old->values.array[r]);

					appendPQExpBuffer(buf, "%s$%d", r > 0 ? ", " : "", ++pos);

					if (!stream_add_value_in_json_array(&values->array[0], jsArray))
					{
						destroyPQExpBuffer(buf);
						return false;
					}
				}

				appendPQExpBuffer(buf, ")");
			}
			else
			{
				/* WHERE (k1, k2) IN (($1, $2), ($3, $4)) */
				appendPQExpBuffer(buf, "(");
				for (int v = 0; v < colCount; v++)
				{
					appendPQExpBuffer(buf, "%s%s",
									  v > 0 ? ", " : "",
									  old->attributes.array[v].attname);
				}
				appendPQExpBuffer(buf, ") IN (");

				for (int r = 0; r < rowCount; r++)
				{
					LogicalMessageValues *values = &(old->values.array[r]);

					appendPQExpBuffer(buf, "%s(", r > 0 ? ", " : "");

					for (int v = 0; v < colCount; v++)
					{
						appendPQExpBuffer(buf, "%s$%d",
										  v > 0 ? ", " : "",
										  ++pos);

						if (!stream_add_value_in_json_array(&values->array[v],
															jsArray))
						{
							destroyPQExpBuffer(buf);
							return false;
						}
					}

					appendPQExpBuffer(buf, ")");
				}

				appendPQExpBuffer(buf, ")");
			}
		}

		if (PQExpBufferBroken(buf))
		{
			log_error("Failed to transform DELETE statement: Out of Memory");
			destroyPQExpBuffer(buf);
			return false;
		}

		/* compute the hash and prepare the JSONB data array */
		uint32_t hash = hashlittle(buf->data, buf->len, 5381);
		char *serialized_string = json_serialize_to_string(js);

		replayStmt->hash = hash;
		replayStmt->stmt = strdup(buf->data);
		replayStmt->data = strdup(serialized_string);

		destroyPQExpBuffer(buf);
		json_free_serialized_string(serialized_string);
	}

	return true;
}


/*
 * stream_write_truncate writes a TRUNCATE statement into the replayDB.
 *
 * Each call emits exactly one relation per statement; the data model
 * (LogicalMessageTruncate.table) is singular by construction. The apply path
 * in ld_apply.c (STREAM_ACTION_TRUNCATE) relies on this invariant to detect
 * the partitioned-target case via a single regclass lookup. If this is ever
 * changed to emit multi-relation TRUNCATE statements, update the apply path
 * accordingly.
 */
static bool
stream_write_truncate(ReplayDBStmt *replayStmt, LogicalMessageTruncate *truncate)
{
	strlcpy(replayStmt->nspname, truncate->table.nspname, sizeof(replayStmt->nspname));
	strlcpy(replayStmt->relname, truncate->table.relname, sizeof(replayStmt->relname));

	PQExpBuffer buf = createPQExpBuffer();

	printfPQExpBuffer(buf, "TRUNCATE ONLY %s.%s\n",
					  truncate->table.nspname,
					  truncate->table.relname);

	if (PQExpBufferBroken(buf))
	{
		log_error("Failed to transform TRUNCATE statement: Out of Memory");
		destroyPQExpBuffer(buf);
		return false;
	}

	/* compute the hash and prepare the JSONB data array */
	uint32_t hash = hashlittle(buf->data, buf->len, 5381);

	replayStmt->hash = hash;
	replayStmt->stmt = strdup(buf->data);

	destroyPQExpBuffer(buf);

	return true;
}


/*
 * stream_write_ddl writes a decoded DDL message (STREAM_ACTION_MESSAGE with
 * envelope type "ddl") into the replayDB. The body is the literal DDL SQL
 * text; it is executed verbatim at apply time (ld_apply.c), with no
 * PREPARE/EXECUTE caching, unlike DML statements.
 */
static bool
stream_write_ddl(ReplayDBStmt *replayStmt, LogicalMessageDDL *ddl)
{
	if (ddl->body == NULL)
	{
		log_error("BUG: stream_write_ddl called with a NULL body");
		return false;
	}

	replayStmt->stmt = strdup(ddl->body);

	if (replayStmt->stmt == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * ld_store_insert_replay_stmt only stores (and later re-reads) a
	 * statement when replayStmt->hash > 0 — see its hashParamType check.
	 * Without a non-zero hash the stmt text is written to the "stmt" table
	 * but the replay row's stmt_hash stays NULL, so the apply side would
	 * read back an empty statement (silent no-op). Hash the same way
	 * stream_write_truncate does.
	 */
	replayStmt->hash = hashlittle(replayStmt->stmt, strlen(replayStmt->stmt), 5381);

	return true;
}


/*
 * stream_values_as_json_array fills-in a JSON array with the string
 * representation of the given values.
 */
bool
stream_add_value_in_json_array(LogicalMessageValue *value, JSON_Array *jsArray)
{
	if (value == NULL)
	{
		log_error("BUG: stream_values_as_json_array value is NULL");
		return false;
	}

	if (value->isNull)
	{
		json_array_append_null(jsArray);
	}
	else
	{
		switch (value->oid)
		{
			case BOOLOID:
			{
				char *string = value->val.boolean ? "t" : "f";
				json_array_append_string(jsArray, string);
				break;
			}

			case INT8OID:
			{
				char string[BUFSIZE] = { 0 };

				sformat(string, sizeof(string), "%lld",
						(long long) value->val.int8);

				json_array_append_string(jsArray, string);
				break;
			}

			case FLOAT8OID:
			{
				char string[BUFSIZE] = { 0 };

				if (fmod(value->val.float8, 1) == 0.0)
				{
					sformat(string, sizeof(string), "%lld",
							(long long) value->val.float8);
				}
				else
				{
					/*
					 * IEEE 754 double precision requires 17 significant decimal
					 * digits to round-trip without loss.  The previous "%f"
					 * format produced only 6 decimal places, silently corrupting
					 * values like -216237.00000035969.  Trailing zeros are
					 * stripped so that the serialised form stays compact and
					 * matches what wal2json / Postgres itself would emit.
					 */
					sformat(string, sizeof(string), "%.17f", value->val.float8);

					char *p = string + strlen(string) - 1;

					while (p > string && *p == '0')
					{
						*p-- = '\0';
					}

					if (*p == '.')
					{
						*p = '\0';
					}
				}

				json_array_append_string(jsArray, string);
				break;
			}

			case TEXTOID:
			case BYTEAOID:
			{
				json_array_append_string(jsArray, value->val.str);
				break;
			}

			default:
			{
				log_error("BUG: stream_values_as_json_array value with oid %d",
						  value->oid);
				return false;
			}
		}
	}

	return true;
}


/*
 * LogicalMessageValueEq compares two LogicalMessageValue instances and return
 * true when they represent the same value. NULL are considered Equal, like in
 * the SQL operator IS NOT DISTINCT FROM.
 */
bool
LogicalMessageValueEq(LogicalMessageValue *a, LogicalMessageValue *b)
{
	if (a->oid != b->oid)
	{
		return false;
	}

	if (a->isNull != b->isNull)
	{
		return false;
	}

	if (a->isNull && b->isNull)
	{
		return true;
	}

	switch (a->oid)
	{
		case BOOLOID:
		{
			return a->val.boolean == b->val.boolean;
		}

		case INT8OID:
		{
			return a->val.int8 == b->val.int8;
		}

		case FLOAT8OID:
		{
			return a->val.float8 == b->val.float8;
		}

		case TEXTOID:
		case BYTEAOID:
		{
			return a->isQuoted == b->isQuoted &&
				   streq(a->val.str, b->val.str);
		}

		default:
		{
			log_error("BUG: LogicalMessageValueEq a.oid == %d", a->oid);
			return false;
		}
	}

	/* makes compiler happy */
	return false;
}


/*
 * Identifiers such as schema, table, column comes from various
 * sources(e.g. wal2json, test_decoding and source catalog) and some of them
 * already escapes identifiers and few don't.
 * We need to check if the identifier is already quoted or not before
 * escaping it.
 * Whatever we are here is not a fool proof escaping mechanism, but a best
 * effort to make sure that the identifiers are normalized by quoting them
 * if it is not already quoted.
 *
 * Here is an example:
 * foo -> "foo"
 * "foo" -> "foo"
 * foo"bar -> "foo"bar"
 * "foo -> ""foo"
 *
 * The goal of this normalization is to make sure that the identifiers are
 * comparable in the context of Hash Table.
 */
#define NORMALIZED_PG_NAMEDATA_COPY(dst, src) \
	{ \
		int len = strlen(src); \
		if (src[0] == '"' && src[len - 1] == '"') \
		{ \
			strlcpy(dst, src, PG_NAMEDATALEN); \
		} \
		else \
		{ \
			sformat(dst, PG_NAMEDATALEN, "\"%s\"", src); \
		} \
	}

/*
 * lookupGeneratedColumnsForTable lookup the generated columns set for the given
 * table "nspname.relname".
 *
 * Returns a GeneratedColumnSet if the table has generated columns, NULL
 * otherwise.
 *
 * NOTE: There is no error condition, if the cache is NULL, it means that we
 * don't have any generated columns in the catalog.
 */
static GeneratedColumnSet *
lookupGeneratedColumnsForTable(GeneratedColumnsCache *cache,
							   const char *nspname,
							   const char *relname)
{
	/*
	 * NULL cache means that we don't have any generated columns in the
	 * catalog.
	 */
	if (cache == NULL)
	{
		return NULL;
	}

	GeneratedColumnsCache *item = NULL;

	GeneratedColumnsCache_Lookup key = { 0 };

	NORMALIZED_PG_NAMEDATA_COPY(key.nspname, nspname);
	NORMALIZED_PG_NAMEDATA_COPY(key.relname, relname);

	HASH_FIND(hh, cache, &key, sizeof(GeneratedColumnsCache_Lookup), item);

	if (item == NULL)
	{
		return NULL;
	}

	if (item->columns == NULL)
	{
		log_error("BUG: Table \"%s.%s\" is in the cache but columns are NULL",
				  nspname, relname);
		return NULL;
	}

	log_trace("Table \"%s.%s\" has generated columns", nspname, relname);

	return item->columns;
}


/*
 * unquoteIdentifier copies an identifier to dst without the double quotes
 * around it, and with the double quotes inside it undoubled.
 */
static void
unquoteIdentifier(const char *src, char *dst, size_t size)
{
	size_t len = strlen(src);

	if (len < 2 || src[0] != '"' || src[len - 1] != '"')
	{
		strlcpy(dst, src, size);
		return;
	}

	size_t pos = 0;

	for (size_t i = 1; i < len - 1 && pos < size - 1; i++)
	{
		if (src[i] == '"' && src[i + 1] == '"')
		{
			++i;
		}

		dst[pos++] = src[i];
	}

	dst[pos] = '\0';
}


/*
 * lookupKeyColumns returns the primary key columns of a table, from the
 * cache or else from the source catalog, where the lookup is done once per
 * table. The names are the ones used in the statements, quoted or not.
 */
static KeyColumnsCache *
lookupKeyColumns(StreamContext *privateContext,
				 const char *nspname,
				 const char *relname)
{
	char qname[4 * PG_NAMEDATALEN + 4] = { 0 };

	sformat(qname, sizeof(qname), "%s.%s", nspname, relname);

	KeyColumnsCache *item = NULL;

	HASH_FIND_STR(privateContext->keyColumnsCache, qname, item);

	if (item != NULL)
	{
		return item;
	}

	item = (KeyColumnsCache *) calloc(1, sizeof(KeyColumnsCache));

	if (item == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return NULL;
	}

	strlcpy(item->qname, qname, sizeof(item->qname));

	DatabaseCatalog *sourceDB = privateContext->sourceDB;

	if (sourceDB != NULL)
	{
		char nsp[PG_NAMEDATALEN] = { 0 };
		char rel[PG_NAMEDATALEN] = { 0 };

		unquoteIdentifier(nspname, nsp, sizeof(nsp));
		unquoteIdentifier(relname, rel, sizeof(rel));

		SourceTable table = { 0 };

		if (!catalog_lookup_s_table_by_name(sourceDB, nsp, rel, &table))
		{
			/* errors have already been logged */
			free(item);
			return NULL;
		}

		/* a table that is not in our catalog has no key to use */
		if (table.oid != 0)
		{
			if (!catalog_s_table_fetch_attrs(sourceDB, &table))
			{
				log_error("Failed to fetch table %s attribute list", table.qname);
				free(item);
				return NULL;
			}

			for (int i = 0; i < table.attributes.count; i++)
			{
				SourceTableAttribute *attr = &(table.attributes.array[i]);

				if (!attr->attisprimary)
				{
					continue;
				}

				KeyColumn *key = (KeyColumn *) calloc(1, sizeof(KeyColumn));

				if (key == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					free(item);
					return NULL;
				}

				strlcpy(key->attname, attr->attname, sizeof(key->attname));
				HASH_ADD_STR(item->columns, attname, key);
				++item->count;
			}
		}
	}

	HASH_ADD_STR(privateContext->keyColumnsCache, qname, item);

	return item;
}


/*
 * isKeyColumn returns true when attname, as a statement spells it, is one of
 * the key columns.
 */
static bool
isKeyColumn(KeyColumnsCache *keys, const char *attname)
{
	char name[PG_NAMEDATALEN] = { 0 };

	unquoteIdentifier(attname, name, sizeof(name));

	KeyColumn *key = NULL;

	HASH_FIND_STR(keys->columns, name, key);

	return key != NULL;
}


/*
 * stream_transform_locate_by_key makes an UPDATE or a DELETE that comes with
 * the whole old row (REPLICA IDENTITY FULL) locate its row by primary key, as
 * native logical replication and pglogical do. Matching every column of the
 * old row finds nothing as soon as the target differs from the source in a
 * column, such as one a trigger sets, and the change is then silently lost.
 *
 * An UPDATE keeps the whole old row, which the SET clause is built from, and
 * has its key columns marked. A DELETE keeps its key columns only, before the
 * statement can be coalesced with the ones that come next. A table without a
 * primary key keeps the whole old row, and so does a tuple that does not hold
 * every key column.
 */
static bool
stream_transform_locate_by_key(StreamContext *privateContext,
							   LogicalTransactionStatement *stmt)
{
	LogicalMessageRelation *table = NULL;
	LogicalMessageTupleArray *olds = NULL;

	if (stmt->action == STREAM_ACTION_UPDATE)
	{
		table = &(stmt->stmt.update.table);
		olds = &(stmt->stmt.update.old);
	}
	else if (stmt->action == STREAM_ACTION_DELETE)
	{
		table = &(stmt->stmt.delete.table);
		olds = &(stmt->stmt.delete.old);
	}
	else
	{
		return true;
	}

	KeyColumnsCache *keys =
		lookupKeyColumns(privateContext, table->nspname, table->relname);

	if (keys == NULL)
	{
		/* errors have already been logged */
		return false;
	}

	if (keys->count == 0)
	{
		return true;
	}

	for (int t = 0; t < olds->count; t++)
	{
		LogicalMessageTuple *old = &(olds->array[t]);

		int found = 0;

		for (int c = 0; c < old->attributes.count; c++)
		{
			if (isKeyColumn(keys, old->attributes.array[c].attname))
			{
				++found;
			}
		}

		/* the old row is already the key, or does not have all of it */
		if (found != keys->count || found == old->attributes.count)
		{
			continue;
		}

		if (stmt->action == STREAM_ACTION_UPDATE)
		{
			for (int c = 0; c < old->attributes.count; c++)
			{
				LogicalMessageAttribute *attr = &(old->attributes.array[c]);

				attr->iskey = isKeyColumn(keys, attr->attname);
			}

			continue;
		}

		LogicalMessageValues *values = &(old->values.array[0]);
		int kept = 0;

		for (int c = 0; c < old->attributes.count; c++)
		{
			if (!isKeyColumn(keys, old->attributes.array[c].attname))
			{
				continue;
			}

			old->attributes.array[kept] = old->attributes.array[c];
			values->array[kept] = values->array[c];
			++kept;
		}

		old->attributes.count = kept;
		values->cols = kept;
	}

	return true;
}


/*
 * prepareGeneratedColumnsCache_hook is a callback function that populates the
 * generated columns cache from the catalog.
 */
static bool
prepareGeneratedColumnsCache_hook(void *ctx, SourceTable *table)
{
	StreamSpecs *specs = (StreamSpecs *) ctx;
	StreamContext *privateContext = &(specs->private);
	DatabaseCatalog *sourceDB = specs->sourceDB;

	if (!catalog_s_table_fetch_attrs(sourceDB, table))
	{
		log_error("Failed to fetch attributes for table \"%s\".%s",
				  table->nspname, table->relname);
		return false;
	}

	GeneratedColumnsCache *item = (GeneratedColumnsCache *)
								  calloc(1, sizeof(GeneratedColumnsCache));

	if (item == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	for (int i = 0; i < table->attributes.count; i++)
	{
		SourceTableAttribute *attr = &(table->attributes.array[i]);

		if (attr->attisgenerated || attr->attidentity == 'a')
		{
			/* Add a generated or identity-always column to the set */
			GeneratedColumnSet *generatedColumn = (GeneratedColumnSet *)
												  calloc(1, sizeof(GeneratedColumnSet));
			if (generatedColumn == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			NORMALIZED_PG_NAMEDATA_COPY(generatedColumn->attname, attr->attname);
			generatedColumn->isidentityalways = (attr->attidentity == 'a');
			HASH_ADD_STR(item->columns, attname, generatedColumn);
		}
	}

	NORMALIZED_PG_NAMEDATA_COPY(item->nspname, table->nspname);
	NORMALIZED_PG_NAMEDATA_COPY(item->relname, table->relname);

	/*
	 * Prepare keylen as per https://troydhanson.github.io/uthash/userguide.html#_compound_keys
	 */
	unsigned keylen = offsetof(GeneratedColumnsCache, relname) + /* offset of last key field */
					  sizeof(item->relname) -    /* size of last key field */
					  offsetof(GeneratedColumnsCache, nspname); /* offset of first key field */

	/* Add the table to the GeneratedColumnsCache. */
	HASH_ADD(hh,
			 privateContext->generatedColumnsCache,
			 nspname,
			 keylen,
			 item);

	return true;
}


/*
 * prepareGeneratedColumnsCache fills-in the cache with the tables having
 * generated columns.
 */
static bool
prepareGeneratedColumnsCache(StreamSpecs *specs)
{
	/*
	 * TODO: GeneratedColumn must be retrieved from the target catalog
	 * because the schema of the target can be different from the source.
	 */
	if (!catalog_iter_s_table_generated_columns(specs->sourceDB,
												specs,
												&prepareGeneratedColumnsCache_hook))
	{
		log_error("Failed to prepare a generated column cache for our catalog,"
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * markGeneratedColumnsFromTransaction marks the generated columns in the
 * transaction.
 */
static bool
markGeneratedColumnsFromTransaction(GeneratedColumnsCache *cache,
									LogicalTransaction *txn)
{
	LogicalTransactionStatement *stmt = txn->first;

	for (; stmt != NULL; stmt = stmt->next)
	{
		if (!markGeneratedColumnsFromStatement(cache, stmt))
		{
			return false;
		}
	}

	return true;
}


/*
 * markGeneratedColumnsFromStatement marks the generated columns in the
 * given statement after looking up the cache.
 */
static bool
markGeneratedColumnsFromStatement(GeneratedColumnsCache *cache,
								  LogicalTransactionStatement *stmt)
{
	LogicalMessageTupleArray *columns = NULL;
	const char *nspname = NULL;
	const char *relname = NULL;

	if (stmt->action == STREAM_ACTION_INSERT)
	{
		columns = &(stmt->stmt.insert.new);
		nspname = stmt->stmt.insert.table.nspname;
		relname = stmt->stmt.insert.table.relname;
	}
	else if (stmt->action == STREAM_ACTION_UPDATE)
	{
		columns = &(stmt->stmt.update.new);
		nspname = stmt->stmt.update.table.nspname;
		relname = stmt->stmt.update.table.relname;
	}
	else
	{
		/*
		 * Only INSERT and UPDATE statements can update the table
		 * generated columns.
		 */
		return true;
	}

	GeneratedColumnSet *tableColumns = lookupGeneratedColumnsForTable(cache, nspname,
																	  relname);

	if (tableColumns == NULL)
	{
		/* no generated or identity columns in this table */
		return true;
	}

	for (int i = 0; i < columns->count; i++)
	{
		LogicalMessageTuple *tuple = &(columns->array[i]);

		for (int c = 0; c < tuple->attributes.count; c++)
		{
			LogicalMessageAttribute *attr = &(tuple->attributes.array[c]);

			char attnameNormalized[PG_NAMEDATALEN] = { 0 };
			NORMALIZED_PG_NAMEDATA_COPY(attnameNormalized, attr->attname);

			GeneratedColumnSet *entry = NULL;
			HASH_FIND_STR(tableColumns, attnameNormalized, entry);

			if (entry != NULL)
			{
				attr->isidentityalways = entry->isidentityalways;
				attr->isgenerated = !entry->isidentityalways;
			}
		}
	}

	return true;
}
