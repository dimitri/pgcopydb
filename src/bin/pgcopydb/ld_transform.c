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


typedef struct TransformStreamCtx
{
	StreamContext *context;
	uint64_t currentMsgIndex;
} TransformStreamCtx;

static bool stream_transform_stream_internal(StreamSpecs *specs);

static bool stream_transform_from_queue_internal(StreamSpecs *specs);

static bool canCoalesceLogicalTransactionStatement(LogicalTransaction *txn,
												   LogicalTransactionStatement *new);
static bool coalesceLogicalTransactionStatement(LogicalTransaction *txn,
												LogicalTransactionStatement *new);

static bool markGeneratedColumnsFromTransaction(GeneratedColumnsCache *cache,
												LogicalTransaction *txn);
static bool markGeneratedColumnsFromStatement(GeneratedColumnsCache *cache,
											  LogicalTransactionStatement *stmt);

static bool prepareGeneratedColumnsCache_hook(void *ctx, SourceTable *table);

static bool prepareGeneratedColumnsCache(StreamSpecs *specs);

static bool isGeneratedColumn(GeneratedColumnSet *columns, const char *attname);

static GeneratedColumnSet * lookupGeneratedColumnsForTable(GeneratedColumnsCache *cache,
														   const char *nspname,
														   const char *relname);

/*
 * stream_transform_context_init initializes StreamContext for the transform
 * operation.
 */
bool
stream_transform_context_init(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	privateContext->transformPGSQL = &(specs->transformPGSQL);

	/* initialize our connection to the target database */
	if (!pgsql_init(privateContext->transformPGSQL,
					specs->connStrings->target_pguri,
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
 * stream_transform_stream transforms a JSON formatted input stream (read line
 * by line) as received from the wal2json logical decoding plugin into an SQL
 * stream ready for applying to the target database.
 */
bool
stream_transform_stream(StreamSpecs *specs)
{
	if (!stream_transform_context_init(specs))
	{
		/* errors have already been logged */
		return false;
	}

	bool success = stream_transform_stream_internal(specs);

	pgsql_finish(&(specs->transformPGSQL));

	return success;
}


/*
 * stream_transform_stream_internal implements the core of
 * stream_transform_stream
 */
static bool
stream_transform_stream_internal(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	/*
	 * Resume operations by reading the current transform target file, if it
	 * already exists, and make sure to grab the current sentinel endpos LSN
	 * when it has been set.
	 */
	if (!stream_transform_resume(specs))
	{
		log_error("Failed to resume streaming from %X/%X",
				  LSN_FORMAT_ARGS(privateContext->startpos));
		return false;
	}

	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	if (privateContext->endpos != InvalidXLogRecPtr &&
		privateContext->endpos <= metadata->lsn)
	{
		log_info("Transform reached end position %X/%X at %X/%X",
				 LSN_FORMAT_ARGS(privateContext->endpos),
				 LSN_FORMAT_ARGS(metadata->lsn));
		return true;
	}

	/*
	 * Now read from the input PIPE and parse lines, writing SQL to disk at
	 * transaction boundaries. The read_from_stream() function finishes upon
	 * PIPE being closed on the writing side.
	 */
	TransformStreamCtx ctx = {
		.context = privateContext,
		.currentMsgIndex = 0
	};

	ReadFromStreamContext context = {
		.callback = stream_transform_line,
		.ctx = &ctx
	};

	if (!read_from_stream(privateContext->in, &context))
	{
		log_error("Failed to transform JSON messages from input stream, "
				  "see above for details");
		return false;
	}

	/* we might have stopped reading mid-file, let's close it. */
	if (privateContext->sqlFile != NULL)
	{
		if (fclose(privateContext->sqlFile) != 0)
		{
			log_error("Failed to close file \"%s\": %m",
					  privateContext->sqlFileName);
			return false;
		}

		/* reset the sqlFile FILE * pointer to NULL, it's closed now */
		privateContext->sqlFile = NULL;

		log_notice("Closed file \"%s\"", privateContext->sqlFileName);
	}

	log_notice("Transformed %lld messages and %lld transactions",
			   (long long) context.lineno,
			   (long long) ctx.currentMsgIndex + 1);

	return true;
}


/*
 * stream_transform_resume allows resuming operation when a SQL file is already
 * existing on-disk.
 */
bool
stream_transform_resume(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	/*
	 * Now grab the current sentinel values, specifically the current endpos.
	 *
	 * The pgcopydb sentinel table also contains an endpos. The --endpos
	 * command line option (found in specs->endpos) prevails, but when it's not
	 * been used, we have a look at the sentinel value.
	 */
	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_get(specs->sourceDB, &sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	log_notice("stream_transform_resume: "
			   "startpos %X/%X endpos %X/%X "
			   "write_lsn %X/%X flush_lsn %X/%X replay_lsn %X/%X",
			   LSN_FORMAT_ARGS(sentinel.startpos),
			   LSN_FORMAT_ARGS(sentinel.endpos),
			   LSN_FORMAT_ARGS(sentinel.write_lsn),
			   LSN_FORMAT_ARGS(sentinel.flush_lsn),
			   LSN_FORMAT_ARGS(sentinel.replay_lsn));

	if (specs->endpos == InvalidXLogRecPtr)
	{
		specs->endpos = sentinel.endpos;
	}
	else if (specs->endpos != sentinel.endpos)
	{
		log_warn("Sentinel endpos is %X/%X, overriden by --endpos %X/%X",
				 LSN_FORMAT_ARGS(sentinel.endpos),
				 LSN_FORMAT_ARGS(specs->endpos));
	}

	if (specs->endpos != InvalidXLogRecPtr)
	{
		log_info("Transform process is setup to end at LSN %X/%X",
				 LSN_FORMAT_ARGS(specs->endpos));
	}

	/* if we have a startpos, that's better than using 0/0 at init time */
	if (specs->startpos == InvalidXLogRecPtr)
	{
		if (sentinel.startpos != InvalidXLogRecPtr)
		{
			specs->startpos = sentinel.startpos;

			log_notice("Resuming transform at LSN %X/%X from sentinel",
					   LSN_FORMAT_ARGS(specs->startpos));
		}
	}

	/*
	 * Initialize our private context from the updated specs.
	 */
	if (!stream_init_context(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/* we need timeline and wal_segment_size to compute WAL filenames */
	if (specs->system.timeline == 0)
	{
		if (!stream_read_context(specs))
		{
			log_error("Failed to read the streaming context information "
					  "from the source database and internal catalogs, "
					  "see above for details");
			return false;
		}
	}

	privateContext->WalSegSz = specs->WalSegSz;
	privateContext->timeline = specs->system.timeline;

	log_debug("Source database wal_segment_size is %u", specs->WalSegSz);
	log_debug("Source database timeline is %d", specs->system.timeline);

	char jsonFileName[MAXPGPATH] = { 0 };
	char sqlFileName[MAXPGPATH] = { 0 };

	if (!stream_compute_pathnames(privateContext->WalSegSz,
								  privateContext->timeline,
								  privateContext->startpos,
								  privateContext->paths.dir,
								  jsonFileName,
								  sqlFileName))
	{
		/* errors have already been logged */
		return false;
	}

	log_notice("Transforming from %X/%X in \"%s\"",
			   LSN_FORMAT_ARGS(privateContext->startpos),
			   sqlFileName);

	/*
	 * If the JSON file already exists on-disk, make sure to read it file again
	 * now. The previous round of streaming might have stopped at an endpos
	 * that fits in the middle of a transaction.
	 *
	 * We can think about this as "cache invalidation" of the SQL file on-disk.
	 */
	if (file_exists(jsonFileName))
	{
		if (!stream_transform_file(specs, jsonFileName, sqlFileName))
		{
			log_error("Failed to resume transforming from existing file \"%s\"",
					  sqlFileName);
			return false;
		}
	}

	return true;
}


/*
 * stream_transform_line is a callback function for the ReadFromStreamContext
 * and read_from_stream infrastructure. It's called on each line read from a
 * stream such as a unix pipe.
 */
bool
stream_transform_line(void *ctx, const char *line, bool *stop)
{
	TransformStreamCtx *transformCtx = (TransformStreamCtx *) ctx;
	StreamContext *privateContext = transformCtx->context;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	static uint64_t lineno = 0;

	log_trace("stream_transform_line[%lld]: %s", (long long) ++lineno, line);

	/* clean-up from whatever was read previously */
	LogicalMessageMetadata empty = { 0 };
	*metadata = empty;

	if (!stream_transform_message(privateContext, (char *) line))
	{
		/* errors have already been logged */
		return false;
	}

	if (privateContext->sqlFile == NULL)
	{
		if (!stream_transform_rotate(privateContext))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Is it time to close the current message and prepare a new one?
	 */
	if (!stream_transform_write_message(privateContext,
										&(transformCtx->currentMsgIndex)))
	{
		log_error("Failed to transform and flush the current message, "
				  "see above for details");
		return false;
	}

	/* rotate the SQL file when receiving a SWITCH WAL message */
	if (metadata->action == STREAM_ACTION_SWITCH)
	{
		if (!stream_transform_rotate(privateContext))
		{
			/* errors have already been logged */
			return false;
		}
	}
	/* at ENDPOS check that it's the current sentinel value and exit */
	else if (metadata->action == STREAM_ACTION_ENDPOS)
	{
		CopyDBSentinel sentinel = { 0 };

		if (!sentinel_get(privateContext->sourceDB, &sentinel))
		{
			/* errors have already been logged */
			return false;
		}

		if (sentinel.endpos != InvalidXLogRecPtr &&
			sentinel.endpos <= metadata->lsn)
		{
			*stop = true;

			log_info("Transform process reached ENDPOS %X/%X",
					 LSN_FORMAT_ARGS(metadata->lsn));
		}
	}

	if (privateContext->endpos != InvalidXLogRecPtr &&
		privateContext->endpos <= metadata->lsn)
	{
		*stop = true;

		log_info("Transform reached end position %X/%X at %X/%X",
				 LSN_FORMAT_ARGS(privateContext->endpos),
				 LSN_FORMAT_ARGS(metadata->lsn));
	}

	return true;
}


/*
 * stream_transform_write_message checks if we need to flush-out the current
 * message down to file, and maybe also stdout (Unix PIPE).
 */
bool
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

	/* now write the transaction out */
	if (privateContext->out != NULL)
	{
		if (!stream_write_message(privateContext->out, currentMsg))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* now write the transaction out also to file on-disk */
	if (!stream_write_message(privateContext->sqlFile, currentMsg))
	{
		/* errors have already been logged */
		return false;
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
 * stream_transform_worker is a worker process that loops over messages
 * received from a queue, each message contains the WAL.json and the WAL.sql
 * file names. When receiving such a message, the WAL.json file is transformed
 * into the WAL.sql file.
 */
bool
stream_transform_worker(StreamSpecs *specs)
{
	/*
	 * The timeline and wal segment size are determined when connecting to the
	 * source database, and stored to local files at that time. When the Stream
	 * Transform Worker process is created, that information is read from our
	 * local files and internal catalogs.
	 */
	if (!stream_read_context(specs))
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_debug("Stream Transform Worker startup was interrupted");
			return true;
		}

		log_error("Failed to read the streaming context information "
				  "from the source database and internal catalogs, "
				  "see above for details");
		return false;
	}

	return stream_transform_from_queue(specs);
}


/*
 * stream_transform_from_queue loops over messages from a System V queue, each
 * message contains the WAL.json and the WAL.sql file names. When receiving
 * such a message, the WAL.json file is transformed into the WAL.sql file.
 */
bool
stream_transform_from_queue(StreamSpecs *specs)
{
	DatabaseCatalog *sourceDB = specs->sourceDB;

	if (!stream_init_context(specs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_transform_context_init(specs))
	{
		/* errors have already been logged */
		return false;
	}

	bool success = stream_transform_from_queue_internal(specs);

	pgsql_finish(&(specs->transformPGSQL));

	if (!catalog_close(sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	return success;
}


/*
 * stream_transform_from_queue_internal implements the core of
 * stream_transform_from_queue
 */
static bool
stream_transform_from_queue_internal(StreamSpecs *specs)
{
	Queue *transformQueue = &(specs->transformQueue);

	int errors = 0;
	bool stop = false;

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(transformQueue, &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			/*
			 * It's part of the supervision protocol to return true here, so
			 * that the follow sub-processes supervisor can then switch from
			 * catchup mode to replay mode.
			 */
			log_debug("stream_transform_from_queue was asked to stop");
			return true;
		}

		if (!recv_ok)
		{
			/* errors have already been logged */
			return false;
		}

		switch (mesg.type)
		{
			case QMSG_TYPE_STOP:
			{
				stop = true;
				log_debug("stream_transform_from_queue: STOP");
				break;
			}

			case QMSG_TYPE_STREAM_TRANSFORM:
			{
				log_debug("stream_transform_from_queue: %X/%X",
						  LSN_FORMAT_ARGS(mesg.data.lsn));

				if (!stream_transform_file_at_lsn(specs, mesg.data.lsn))
				{
					/* errors have already been logged, break from the loop */
					++errors;
					break;
				}

				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on %s queue %d",
						  mesg.type,
						  transformQueue->name,
						  transformQueue->qId);
				++errors;
				break;
			}
		}
	}

	bool success = (stop == true && errors == 0);

	if (errors > 0)
	{
		log_error("Stream transform worker encountered %d errors, "
				  "see above for details",
				  errors);
	}

	return success;
}


/*
 * stream_transform_file_at_lsn computes the JSON and SQL filenames at given
 * LSN position in the WAL, and transform the JSON file into an SQL file.
 */
bool
stream_transform_file_at_lsn(StreamSpecs *specs, uint64_t lsn)
{
	char walFileName[MAXPGPATH] = { 0 };
	char sqlFileName[MAXPGPATH] = { 0 };

	if (!stream_compute_pathnames(specs->WalSegSz,
								  specs->system.timeline,
								  lsn,
								  specs->paths.dir,
								  walFileName,
								  sqlFileName))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_transform_file(specs, walFileName, sqlFileName))
	{
		/* errors have already been logged */
		return false;
	}

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
 * vacuum_add_table sends a message to the VACUUM process queue to process
 * given table.
 */
bool
stream_transform_add_file(Queue *queue, uint64_t firstLSN)
{
	QMessage mesg = {
		.type = QMSG_TYPE_STREAM_TRANSFORM,
		.data.lsn = firstLSN
	};

	log_debug("stream_transform_add_file[%d]: %X/%X",
			  queue->qId,
			  LSN_FORMAT_ARGS(mesg.data.lsn));

	if (!queue_send(queue, &mesg))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * vacuum_send_stop sends the STOP message to the Stream Transform worker.
 */
bool
stream_transform_send_stop(Queue *queue)
{
	QMessage stop = { .type = QMSG_TYPE_STOP };

	log_debug("Send STOP message to Transform Queue %d", queue->qId);

	if (!queue_send(queue, &stop))
	{
		/* errors have already been logged */
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

	if (message == NULL)
	{
		log_error("BUG: parseMessage called with a NULL message");
		return false;
	}

	if (json == NULL)
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
	 * Check that XID make sense, except for SWITCH messages, which don't have
	 * XID information, only have LSN information.
	 */
	if (metadata->action == STREAM_ACTION_INSERT ||
		metadata->action == STREAM_ACTION_UPDATE ||
		metadata->action == STREAM_ACTION_DELETE ||
		metadata->action == STREAM_ACTION_TRUNCATE)
	{
		if (mesg->isTransaction)
		{
			if (txn->xid > 0 && metadata->xid > 0 && txn->xid != metadata->xid)
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
	 * All messages except for BEGIN/COMMIT/ROLLBACL need a
	 * LogicalTransactionStatement to represent them within the
	 * current transaction.
	 */
	LogicalTransactionStatement *stmt = NULL;

	if (metadata->action != STREAM_ACTION_BEGIN &&
		metadata->action != STREAM_ACTION_COMMIT &&
		metadata->action != STREAM_ACTION_ROLLBACK)
	{
		stmt = (LogicalTransactionStatement *)
			   calloc(1, sizeof(LogicalTransactionStatement));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->action = metadata->action;

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
				log_error("Failed to parse BEGIN: "
						  "transaction already in progress");
				return false;
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
			stmt->stmt.switchwal.lsn = metadata->lsn;

			if (mesg->isTransaction)
			{
				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/* copy the stmt over, then free the extra allocated memory */
				mesg->action = metadata->action;
				mesg->command.switchwal = stmt->stmt.switchwal;
			}

			break;
		}

		/* keepalive messages are pgcopydb internal messages */
		case STREAM_ACTION_KEEPALIVE:
		{
			stmt->stmt.keepalive.lsn = metadata->lsn;

			strlcpy(stmt->stmt.keepalive.timestamp,
					metadata->timestamp,
					sizeof(stmt->stmt.keepalive.timestamp));

			if (mesg->isTransaction)
			{
				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/* copy the stmt over, then free the extra allocated memory */
				mesg->action = metadata->action;
				mesg->command.keepalive = stmt->stmt.keepalive;
			}

			break;
		}

		case STREAM_ACTION_ENDPOS:
		{
			stmt->stmt.endpos.lsn = metadata->lsn;

			if (mesg->isTransaction)
			{
				(void) streamLogicalTransactionAppendStatement(txn, stmt);
			}
			else
			{
				/* copy the stmt over, then free the extra allocated memory */
				mesg->action = metadata->action;
				mesg->command.endpos = stmt->stmt.endpos;
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
			 * When using test_decoding, we append the received message as a
			 * JSON string in the "message" object key. When using wal2json, we
			 * use the raw JSON message as a json object in the "message"
			 * object key.
			 */
			JSON_Value_Type jsmesgtype =
				json_value_get_type(
					json_object_get_value(
						json_value_get_object(json),
						"message"));

			switch (jsmesgtype)
			{
				case JSONString:
				{
					if (!parseTestDecodingMessage(privateContext, message, json))
					{
						log_error("Failed to parse test_decoding message, "
								  "see above for details");
						return false;
					}

					break;
				}

				case JSONObject:
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
					log_error("Failed to parse JSON message with "
							  "unknown JSON type %d",
							  jsmesgtype);
					return false;
				}
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}
	}

	return true;
}


/*
 * coalesceLogicalTransactionStatement appends a new entry to an existing tuple
 * array created during the last INSERT statement in a logical transaction.
 *
 * This functionality enables the generation of multi-values INSERT or COPY
 * commands, enhancing efficiency.
 *
 * Important: Before invoking this function, ensure that validation is performed
 * using canCoalesceLogicalTransactionStatement.
 */
static bool
coalesceLogicalTransactionStatement(LogicalTransaction *txn,
									LogicalTransactionStatement *new)
{
	LogicalTransactionStatement *last = txn->last;

	LogicalMessageValuesArray *lastValuesArray =
		&(last->stmt.insert.new.array->values);

	LogicalMessageValuesArray *newValuesArray =
		&(new->stmt.insert.new.array->values);

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

	/* TODO: Support UPDATE and DELETE */
	if (last->action != STREAM_ACTION_INSERT ||
		new->action != STREAM_ACTION_INSERT)
	{
		return false;
	}

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
	 * Check if adding the new statement would exceed libpq's limit on the total
	 * number of parameters allowed in a single PQsendPrepare call.
	 * If it would exceed the limit, return false to indicate that coalescing
	 * should not be performed.
	 *
	 * TODO: This parameter limit check is not applicable for COPY operations.
	 * It should be removed once we switch to using COPY.
	 */
	if (((lastValuesArray->count + 1) * lastInsertColumns->attributes.count) >
		PQ_QUERY_PARAM_MAX_LIMIT)
	{
		return false;
	}


	/* Last and current statements cols must have same name and order */
	for (int i = 0; i < lastInsertColumns->attributes.count; i++)
	{
		LogicalMessageAttribute *lastAttr = &(lastInsertColumns->attributes.array[i]);
		LogicalMessageAttribute *newAttr = &(newInsertColumns->attributes.array[i]);
		if (!streq(lastAttr->attname, newAttr->attname))
		{
			return false;
		}
	}

	return true;
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
 * stream_write_message writes the LogicalMessage statement(s) as SQL to the
 * already open out stream.
 */
bool
stream_write_message(FILE *out, LogicalMessage *msg)
{
	if (msg->isTransaction)
	{
		return stream_write_transaction(out, &(msg->command.tx));
	}
	else
	{
		switch (msg->action)
		{
			case STREAM_ACTION_SWITCH:
			{
				if (!stream_write_switchwal(out, &(msg->command.switchwal)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_KEEPALIVE:
			{
				if (!stream_write_keepalive(out, &(msg->command.keepalive)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_ENDPOS:
			{
				if (!stream_write_endpos(out, &(msg->command.endpos)))
				{
					return false;
				}
				break;
			}

			default:
			{
				log_error("BUG: Failed to write SQL for LogicalMessage action %d",
						  msg->action);
				return false;
			}
		}
	}

	return true;
}


/*
 * stream_write_transaction writes the LogicalTransaction statements as SQL to
 * the already open out stream.
 */
bool
stream_write_transaction(FILE *out, LogicalTransaction *txn)
{
	/*
	 * Logical decoding also outputs empty transactions that act here kind of
	 * like a keepalive stream. These transactions might represent activity in
	 * other databases or background activity in the source Postgres instance
	 * where the LSN is moving forward. We want to replay them.
	 */
	if (!txn->continued && txn->count == 0)
	{
		if (!stream_write_begin(out, txn))
		{
			return false;
		}

		if (!stream_write_commit(out, txn))
		{
			return false;
		}

		return true;
	}

	/*
	 * Now we deal with non-empty transactions.
	 *
	 * SWITCH WAL commands might appear eigher in the middle of a transaction
	 * or in between two transactions, depending on when the LSN WAL file
	 * switch happens on the source server.
	 */
	bool sentBEGIN = false;
	bool splitTx = false;

	LogicalTransactionStatement *currentStmt = txn->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		switch (currentStmt->action)
		{
			case STREAM_ACTION_SWITCH:
			{
				if (sentBEGIN)
				{
					splitTx = true;
				}

				if (!stream_write_switchwal(out, &(currentStmt->stmt.switchwal)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_KEEPALIVE:
			{
				if (sentBEGIN)
				{
					splitTx = true;
				}

				if (!stream_write_keepalive(out, &(currentStmt->stmt.keepalive)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_ENDPOS:
			{
				if (sentBEGIN)
				{
					splitTx = true;
				}

				if (!stream_write_endpos(out, &(currentStmt->stmt.endpos)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_INSERT:
			{
				if (!sentBEGIN && !txn->continued)
				{
					if (!stream_write_begin(out, txn))
					{
						return false;
					}
					sentBEGIN = true;
				}

				if (!stream_write_insert(out, &(currentStmt->stmt.insert)))
				{
					return false;
				}

				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				if (!sentBEGIN && !txn->continued)
				{
					if (!stream_write_begin(out, txn))
					{
						return false;
					}
					sentBEGIN = true;
				}

				if (!stream_write_update(out, &(currentStmt->stmt.update)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				if (!sentBEGIN && !txn->continued)
				{
					if (!stream_write_begin(out, txn))
					{
						return false;
					}
					sentBEGIN = true;
				}

				if (!stream_write_delete(out, &(currentStmt->stmt.delete)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_TRUNCATE:
			{
				if (!sentBEGIN && !txn->continued)
				{
					if (!stream_write_begin(out, txn))
					{
						return false;
					}
					sentBEGIN = true;
				}

				if (!stream_write_truncate(out, &(currentStmt->stmt.truncate)))
				{
					return false;
				}
				break;
			}

			default:
			{
				log_error("BUG: Failed to write SQL action %d",
						  currentStmt->action);
				return false;
			}
		}
	}

	/*
	 * Some transactions might be spanning over multiple WAL.{json,sql} files,
	 * because it just happened at the boundary LSN. In that case we don't want
	 * to send the COMMIT message yet.
	 *
	 * Continued transaction are then represented using several instances of
	 * our LogicalTransaction data structure, and the last one of the series
	 * then have the txn->commit metadata forcibly set to true: here we also
	 * need to obey that.
	 */
	if ((sentBEGIN && !splitTx) || txn->commit)
	{
		if (!stream_write_commit(out, txn))
		{
			return false;
		}
	}

	if (txn->rollback)
	{
		if (!stream_write_rollback(out, txn))
		{
			return false;
		}
	}

	/* flush out stream at transaction boundaries */
	if (fflush(out) != 0)
	{
		log_error("Failed to flush stream output: %m");
		return false;
	}

	return true;
}


#define FFORMAT(stream, fmt, ...) \
	{ if (fformat(stream, fmt, __VA_ARGS__) == -1) { \
		  log_error("Failed to write to stream: %m"); \
		  return false; } \
	}

/*
 * stream_write_begin writes a BEGIN statement to the already open out stream.
 */
bool
stream_write_begin(FILE *out, LogicalTransaction *txn)
{
	/* include commit_lsn only if the transaction has commitLSN */
	if (txn->commitLSN != InvalidXLogRecPtr)
	{
		FFORMAT(out,
				"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\",\"commit_lsn\":\"%X/%X\"}\n",
				OUTPUT_BEGIN,
				(long long) txn->xid,
				LSN_FORMAT_ARGS(txn->beginLSN),
				txn->timestamp,
				LSN_FORMAT_ARGS(txn->commitLSN));
	}
	else
	{
		FFORMAT(out,
				"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
				OUTPUT_BEGIN,
				(long long) txn->xid,
				LSN_FORMAT_ARGS(txn->beginLSN),
				txn->timestamp);
	}

	/* keep compiler happy */
	return true;
}


/*
 * stream_write_rollback writes a COMMIT statement to the already open out
 * stream.
 */
bool
stream_write_rollback(FILE *out, LogicalTransaction *txn)
{
	FFORMAT(out,
			"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_ROLLBACK,
			(long long) txn->xid,
			LSN_FORMAT_ARGS(txn->rollbackLSN),
			txn->timestamp);

	return true;
}


/*
 * stream_write_commit writes a COMMIT statement to the already open out
 * stream.
 */
bool
stream_write_commit(FILE *out, LogicalTransaction *txn)
{
	FFORMAT(out,
			"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_COMMIT,
			(long long) txn->xid,
			LSN_FORMAT_ARGS(txn->commitLSN),
			txn->timestamp);

	return true;
}


/*
 * stream_write_switchwal writes a SWITCH statement to the already open out
 * stream.
 */
bool
stream_write_switchwal(FILE *out, LogicalMessageSwitchWAL *switchwal)
{
	FFORMAT(out, "%s{\"lsn\":\"%X/%X\"}\n",
			OUTPUT_SWITCHWAL,
			LSN_FORMAT_ARGS(switchwal->lsn));

	return true;
}


/*
 * stream_write_keepalive writes a KEEPALIVE statement to the already open out
 * stream.
 */
bool
stream_write_keepalive(FILE *out, LogicalMessageKeepalive *keepalive)
{
	FFORMAT(out, "%s{\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_KEEPALIVE,
			LSN_FORMAT_ARGS(keepalive->lsn),
			keepalive->timestamp);

	return true;
}


/*
 * stream_write_endpos writes a SWITCH statement to the already open out
 * stream.
 */
bool
stream_write_endpos(FILE *out, LogicalMessageEndpos *endpos)
{
	FFORMAT(out, "%s{\"lsn\":\"%X/%X\"}\n",
			OUTPUT_ENDPOS,
			LSN_FORMAT_ARGS(endpos->lsn));

	return true;
}


/*
 * stream_write_insert writes an INSERT statement to the already open out
 * stream.
 */
bool
stream_write_insert(FILE *out, LogicalMessageInsert *insert)
{
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

		uint32_t hash = hashlittle(buf->data, buf->len, 5381);

		FFORMAT(out, "PREPARE %x AS %s;\n", hash, buf->data);

		destroyPQExpBuffer(buf);

		/*
		 * Second, the EXECUTE part.
		 */
		char *serialized_string = json_serialize_to_string(js);

		FFORMAT(out, "EXECUTE %x%s;\n", hash, serialized_string);

		json_free_serialized_string(serialized_string);
	}

	return true;
}


/*
 * stream_write_update writes an UPDATE statement to the already open out
 * stream.
 */
bool
stream_write_update(FILE *out, LogicalMessageUpdate *update)
{
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

						if (LogicalMessageValueEq(oldValue, value))
						{
							skip = true;
							break;
						}
					}
				}

				if (!skip)
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

				if (value->isNull)
				{
					/*
					 * Attributes with the value `NULL` require `IS NULL` instead of `=`
					 * in the WHERE clause.
					 */
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
						/* errors have already been logged */
						destroyPQExpBuffer(buf);
						return false;
					}
				}
			}
		}

		if (PQExpBufferBroken(buf))
		{
			log_error("Failed to transform INSERT statement: Out of Memory");
			destroyPQExpBuffer(buf);
			return false;
		}

		uint32_t hash = hashlittle(buf->data, buf->len, 5381);

		FFORMAT(out, "PREPARE %x AS %s;\n", hash, buf->data);

		destroyPQExpBuffer(buf);

		/*
		 * Second, the EXECUTE part.
		 */
		char *serialized_string = json_serialize_to_string(js);

		FFORMAT(out, "EXECUTE %x%s;\n", hash, serialized_string);

		json_free_serialized_string(serialized_string);
	}

	return true;
}


/*
 * stream_write_delete writes an DELETE statement to the already open out
 * stream.
 */
bool
stream_write_delete(FILE *out, LogicalMessageDelete *delete)
{
	/* loop over DELETE statements targeting the same table */
	for (int s = 0; s < delete->old.count; s++)
	{
		LogicalMessageTuple *old = &(delete->old.array[s]);

		PQExpBuffer buf = createPQExpBuffer();
		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		/*
		 * First, the PREPARE part.
		 */
		appendPQExpBuffer(buf, "DELETE FROM %s.%s WHERE ",
						  delete->table.nspname,
						  delete->table.relname);

		int pos = 0;

		for (int r = 0; r < old->values.count; r++)
		{
			LogicalMessageValues *values = &(old->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);
				LogicalMessageAttribute *attr = &(old->attributes.array[v]);

				if (old->attributes.count <= v)
				{
					log_error("Failed to write DELETE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  old->attributes.count);
					destroyPQExpBuffer(buf);
					return false;
				}

				if (value->isNull)
				{
					/*
					 * Attributes with the value `NULL` require `IS NULL` instead of `=`
					 * in the WHERE clause.
					 */
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
						/* errors have already been logged */
						destroyPQExpBuffer(buf);
						return false;
					}
				}
			}
		}

		uint32_t hash = hashlittle(buf->data, buf->len, 5381);

		FFORMAT(out, "PREPARE %x AS %s;\n", hash, buf->data);

		destroyPQExpBuffer(buf);

		/*
		 * Second, the EXECUTE part.
		 */
		char *serialized_string = json_serialize_to_string(js);

		FFORMAT(out, "EXECUTE %x%s;\n", hash, serialized_string);

		json_free_serialized_string(serialized_string);
	}

	return true;
}


/*
 * stream_write_truncate writes an TRUNCATE statement to the already open out
 * stream.
 */
bool
stream_write_truncate(FILE *out, LogicalMessageTruncate *truncate)
{
	FFORMAT(out,
			"TRUNCATE ONLY %s.%s\n",
			truncate->table.nspname,
			truncate->table.relname);

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
					sformat(string, sizeof(string), "%f", value->val.float8);
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
 * stream_write_sql_escape_string_constant writes given str to out and follows
 * the Postgres syntax for String Constants With C-Style Escapes, as documented
 * at the following URL:
 *
 * https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
 */
bool
stream_write_sql_escape_string_constant(FILE *out, const char *str)
{
	FFORMAT(out, "%s", "E'");

	for (int i = 0; str[i] != '\0'; i++)
	{
		switch (str[i])
		{
			case '\b':
			{
				FFORMAT(out, "%s", "\\b");
				break;
			}

			case '\f':
			{
				FFORMAT(out, "%s", "\\f");
				break;
			}

			case '\n':
			{
				FFORMAT(out, "%s", "\\n");
				break;
			}

			case '\r':
			{
				FFORMAT(out, "%s", "\\r");
				break;
			}

			case '\t':
			{
				FFORMAT(out, "%s", "\\t");
				break;
			}

			case '\'':
			case '\\':
			{
				FFORMAT(out, "\\%c", str[i]);
				break;
			}

			default:
			{
				FFORMAT(out, "%c", str[i]);
				break;
			}
		}
	}

	FFORMAT(out, "%s", "'");

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
 * isGeneratedColumn checks whether the given "attname" is a generated column.
 *
 * Returns true if the column is generated, false otherwise.
 *
 * NOTE: There is no error condition, if the columns is NULL, it means that we
 * don't have any generated columns in the catalog.
 */
static bool
isGeneratedColumn(GeneratedColumnSet *columns, const char *attname)
{
	char attnameNormalized[PG_NAMEDATALEN] = { 0 };

	NORMALIZED_PG_NAMEDATA_COPY(attnameNormalized, attname);

	GeneratedColumnSet *generatedColumns = NULL;

	HASH_FIND_STR(columns, attnameNormalized, generatedColumns);

	if (generatedColumns != NULL)
	{
		log_trace("Column \"%s\" is generated", attnameNormalized);

		return true;
	}

	return false;
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

		if (attr->attisgenerated)
		{
			/* Add a generated column to the GeneratedColumnSet */
			GeneratedColumnSet *generatedColumn = (GeneratedColumnSet *)
												  calloc(1, sizeof(GeneratedColumnSet));
			if (generatedColumn == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			NORMALIZED_PG_NAMEDATA_COPY(generatedColumn->attname, attr->attname);
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

	GeneratedColumnSet *generatedColumns = lookupGeneratedColumnsForTable(cache, nspname,
																		  relname);

	if (generatedColumns == NULL)
	{
		/* no generated columns in this table */
		return true;
	}

	for (int i = 0; i < columns->count; i++)
	{
		LogicalMessageTuple *tuple = &(columns->array[i]);

		for (int c = 0; c < tuple->attributes.count; c++)
		{
			LogicalMessageAttribute *attr = &(tuple->attributes.array[c]);

			if (isGeneratedColumn(generatedColumns, attr->attname))
			{
				attr->isgenerated = true;
			}
		}
	}

	return true;
}
