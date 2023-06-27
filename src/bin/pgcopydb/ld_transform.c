/*
 * src/bin/pgcopydb/ld_transform.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/select.h>
#include <sys/time.h>
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
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


typedef struct TransformStreamCtx
{
	StreamContext *context;
	uint64_t currentMsgIndex;
} TransformStreamCtx;


/*
 * stream_transform_stream transforms a JSON formatted input stream (read line
 * by line) as received from the wal2json logical decoding plugin into an SQL
 * stream ready for applying to the target database.
 */
bool
stream_transform_stream(StreamSpecs *specs)
{
	StreamContext *privateContext =
		(StreamContext *) calloc(1, sizeof(StreamContext));

	if (privateContext == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!stream_init_context(privateContext, specs))
	{
		/* errors have already been logged */
		return false;
	}

	/* we need timeline and wal_segment_size to compute WAL filenames */
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

	privateContext->WalSegSz = specs->WalSegSz;
	privateContext->timeline = specs->system.timeline;

	log_debug("Source database wal_segment_size is %u", specs->WalSegSz);
	log_debug("Source database timeline is %d", specs->system.timeline);

	if (!stream_transform_resume(specs, privateContext))
	{
		log_error("Failed to resume streaming from %X/%X",
				  LSN_FORMAT_ARGS(privateContext->startpos));
		return false;
	}

	TransformStreamCtx ctx = {
		.context = privateContext,
		.currentMsgIndex = 0
	};

	ReadFromStreamContext context = {
		.callback = stream_transform_line,
		.ctx = &ctx
	};

	/* switch out stream from block buffered to line buffered mode */
	if (setvbuf(privateContext->out, NULL, _IOLBF, 0) != 0)
	{
		log_error("Failed to set stdout to line buffered mode: %m");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

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

		/* reset the jsonFile FILE * pointer to NULL, it's closed now */
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
stream_transform_resume(StreamSpecs *specs, StreamContext *privateContext)
{
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
	 * If the target SQL file already exists on-disk, make sure to read the
	 * JSON file again now. The previous round of streaming might have stopped
	 * at an endpos that fits in the middle of a transaction.
	 */
	if (file_exists(sqlFileName))
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

	/*
	 * If we're in a continued transaction, it means that the earlier write
	 * of this txn's BEGIN statement didn't have the COMMIT LSN. Therefore,
	 * we need to maintain that LSN as a separate metadata file. This is
	 * necessary because the COMMIT LSN is required later in the apply
	 * process.
	 */
	if (txn->continued && txn->commit)
	{
		writeTxnMetadataFile(txn, privateContext->paths.dir);
	}

	(void) FreeLogicalMessage(currentMsg);

	if (metadata->action == STREAM_ACTION_COMMIT)
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

		LogicalTransaction *old = &(currentMsg->command.tx);
		LogicalTransaction *txn = &(new.command.tx);

		txn->continued = true;

		txn->xid = old->xid;
		txn->beginLSN = old->beginLSN;
		strlcpy(txn->timestamp, old->timestamp, sizeof(txn->timestamp));

		txn->first = NULL;

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
		json_value_free(json);
		return false;
	}

	if (!parseMessage(privateContext, message, json))
	{
		log_error("Failed to parse JSON message: %s", message);
		json_value_free(json);
		return false;
	}

	json_value_free(json);

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
	log_notice("Started Stream Transform worker %d [%d]", getpid(), getppid());

	/*
	 * The timeline and wal segment size are determined when connecting to the
	 * source database, and stored to local files at that time. When the Stream
	 * Transform Worker process is created, that information is read from our
	 * local files.
	 */
	if (!stream_read_context(&(specs->paths), &(specs->system), &(specs->WalSegSz)))
	{
		log_error("Failed to read the streaming context information "
				  "from the source database, see above for details");
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
	StreamContent content = { 0 };
	long size = 0L;

	log_notice("Transforming JSON file \"%s\" into SQL file \"%s\"",
			   jsonfilename,
			   sqlfilename);

	strlcpy(content.filename, jsonfilename, sizeof(content.filename));

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

	log_debug("stream_transform_file: read %d lines from \"%s\"",
			  content.count,
			  content.filename);

	/*
	 * Read the JSON-lines file that we received from streaming logical
	 * decoding messages, and parse the JSON messages into our internal
	 * representation structure.
	 */
	StreamContext *privateContext =
		(StreamContext *) calloc(1, sizeof(StreamContext));

	if (privateContext == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!stream_init_context(privateContext, specs))
	{
		/* errors have already been logged */
		return false;
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

	for (int i = 0; i < content.count; i++)
	{
		char *message = content.lines[i];

		LogicalMessageMetadata empty = { 0 };
		*metadata = empty;

		log_trace("stream_transform_file[%2d]: %s", i, message);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, false))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		/*
		 * Our SQL file might begin with DML messages, in that case it's a
		 * transaction that continues over a file boundary.
		 */
		if (i == 0 &&
			(metadata->action == STREAM_ACTION_COMMIT ||
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
			txn->xid = metadata->xid;
			txn->first = NULL;

			*currentMsg = new;
		}

		if (!parseMessage(privateContext, message, json))
		{
			log_error("Failed to parse JSON message: %s", message);
			json_value_free(json);
			return false;
		}

		json_value_free(json);

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
	}

	if (fclose(privateContext->sqlFile) == EOF)
	{
		log_error("Failed to write file \"%s\"", tempfilename);
		return false;
	}

	log_debug("stream_transform_file: mv \"%s\" \"%s\"",
			  tempfilename, sqlfilename);

	if (rename(tempfilename, sqlfilename) != 0)
	{
		log_error("Failed to move \"%s\" to \"%s\": %m",
				  tempfilename,
				  sqlfilename);
		return false;
	}

	log_info("Transformed %d JSON messages into SQL file \"%s\"",
			 content.count,
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
			log_debug("%s", message);
			log_error("BUG: logical message %c received with !isTransaction",
					  metadata->action);
			return false;
		}
	}

	/*
	 * All messages except for BEGIN/COMMIT need a LogicalTransactionStatement
	 * to represent them within the current transaction.
	 */
	LogicalTransactionStatement *stmt = NULL;

	if (metadata->action != STREAM_ACTION_BEGIN &&
		metadata->action != STREAM_ACTION_COMMIT)
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
			txn->commit = true;

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
				free(stmt);
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
				free(stmt);
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
				free(stmt);
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
 *
 * TODO: at the moment we don't pack several statements that look alike into
 * the same one.
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
		if (txn->last != NULL)
		{
			/* update the current last entry of the linked-list */
			txn->last->next = stmt;
		}

		/* the new statement now becomes the last entry of the linked-list */
		stmt->prev = txn->last;
		stmt->next = NULL;
		txn->last = stmt;
	}

	++txn->count;

	return true;
}


/*
 * FreeLogicalMessage frees the malloc'ated memory areas of a LogicalMessage.
 */
void
FreeLogicalMessage(LogicalMessage *msg)
{
	if (msg->isTransaction)
	{
		FreeLogicalTransaction(&(msg->command.tx));
	}
}


/*
 * FreeLogicalTransaction frees the malloc'ated memory areas of a
 * LogicalTransaction.
 */
void
FreeLogicalTransaction(LogicalTransaction *tx)
{
	LogicalTransactionStatement *currentStmt = tx->first;

	for (; currentStmt != NULL;)
	{
		switch (currentStmt->action)
		{
			case STREAM_ACTION_INSERT:
			{
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.insert.new));
				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.update.old));
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.update.new));
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				FreeLogicalMessageTupleArray(&(currentStmt->stmt.delete.old));
				break;
			}

			/* no malloc'ated area in a BEGIN, COMMIT, or TRUNCATE statement */
			default:
			{
				break;
			}
		}

		LogicalTransactionStatement *stmt = currentStmt;
		currentStmt = currentStmt->next;

		free(stmt);
	}

	tx->first = NULL;
}


/*
 * FreeLogicalMessageTupleArray frees the malloc'ated memory areas of a
 * LogicalMessageTupleArray.
 */
void
FreeLogicalMessageTupleArray(LogicalMessageTupleArray *tupleArray)
{
	for (int s = 0; s < tupleArray->count; s++)
	{
		LogicalMessageTuple *tuple = &(tupleArray->array[s]);

		(void) FreeLogicalMessageTuple(tuple);
	}
}


/*
 * FreeLogicalMessageTuple frees the malloc'ated memory areas of a
 * LogicalMessageTuple.
 */
void
FreeLogicalMessageTuple(LogicalMessageTuple *tuple)
{
	free(tuple->columns);

	for (int r = 0; r < tuple->values.count; r++)
	{
		LogicalMessageValues *values = &(tuple->values.array[r]);

		for (int v = 0; v < values->cols; v++)
		{
			LogicalMessageValue *value = &(values->array[v]);

			if ((value->oid == TEXTOID || value->oid == BYTEAOID) &&
				!value->isNull)
			{
				free(value->val.str);
			}
		}

		free(tuple->values.array);
	}
}


/*
 * allocateLogicalMessageTuple allocates memory for count columns (and values)
 * for the given LogicalMessageTuple.
 */
bool
AllocateLogicalMessageTuple(LogicalMessageTuple *tuple, int count)
{
	tuple->cols = count;
	tuple->columns = (char **) calloc(count, sizeof(char *));

	if (tuple->columns == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Allocate the tuple values, an array of VALUES, as in SQL.
	 *
	 * TODO: actually support multi-values clauses (single column names array,
	 * multiple VALUES matching the same metadata definition). At the moment
	 * it's always a single VALUES entry: VALUES(a, b, c).
	 *
	 * The goal is to be able to represent VALUES(a1, b1, c1), (a2, b2, c2).
	 */
	LogicalMessageValuesArray *valuesArray = &(tuple->values);

	valuesArray->count = 1;
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

	/* flush out stream at transaction boundaries */
	if (fflush(out) != 0)
	{
		log_error("Failed to flush stream output: %m");
		return false;
	}

	return true;
}


/*
 * stream_write_begin writes a BEGIN statement to the already open out stream.
 */
bool
stream_write_begin(FILE *out, LogicalTransaction *txn)
{
	int ret;

	/* include commit_lsn only if the transaction has commitLSN */
	if (txn->commitLSN != InvalidXLogRecPtr)
	{
		ret =
			fformat(out,
					"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\",\"commit_lsn\":\"%X/%X\"}\n",
					OUTPUT_BEGIN,
					(long long) txn->xid,
					LSN_FORMAT_ARGS(txn->beginLSN),
					txn->timestamp,
					LSN_FORMAT_ARGS(txn->commitLSN));
	}
	else
	{
		ret =
			fformat(out,
					"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
					OUTPUT_BEGIN,
					(long long) txn->xid,
					LSN_FORMAT_ARGS(txn->beginLSN),
					txn->timestamp);
	}

	return ret != -1;
}


/*
 * stream_write_commit writes a COMMIT statement to the already open out
 * stream.
 */
bool
stream_write_commit(FILE *out, LogicalTransaction *txn)
{
	int ret =
		fformat(out,
				"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
				OUTPUT_COMMIT,
				(long long) txn->xid,
				LSN_FORMAT_ARGS(txn->commitLSN),
				txn->timestamp);

	return ret != -1;
}


/*
 * stream_write_switchwal writes a SWITCH statement to the already open out
 * stream.
 */
bool
stream_write_switchwal(FILE *out, LogicalMessageSwitchWAL *switchwal)
{
	int ret =
		fformat(out, "%s{\"lsn\":\"%X/%X\"}\n",
				OUTPUT_SWITCHWAL,
				LSN_FORMAT_ARGS(switchwal->lsn));

	return ret != -1;
}


/*
 * stream_write_keepalive writes a KEEPALIVE statement to the already open out
 * stream.
 */
bool
stream_write_keepalive(FILE *out, LogicalMessageKeepalive *keepalive)
{
	int ret =
		fformat(out, "%s{\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
				OUTPUT_KEEPALIVE,
				LSN_FORMAT_ARGS(keepalive->lsn),
				keepalive->timestamp);

	return ret != -1;
}


/*
 * stream_write_endpos writes a SWITCH statement to the already open out
 * stream.
 */
bool
stream_write_endpos(FILE *out, LogicalMessageEndpos *endpos)
{
	int ret =
		fformat(out, "%s{\"lsn\":\"%X/%X\"}\n",
				OUTPUT_ENDPOS,
				LSN_FORMAT_ARGS(endpos->lsn));

	return ret != -1;
}


/*
 * stream_write_insert writes an INSERT statement to the already open out
 * stream.
 */
#define FFORMAT(str, fmt, ...) \
	{ if (fformat(str, fmt, __VA_ARGS__) == -1) { return false; } \
	}

bool
stream_write_insert(FILE *out, LogicalMessageInsert *insert)
{
	/* loop over INSERT statements targeting the same table */
	for (int s = 0; s < insert->new.count; s++)
	{
		LogicalMessageTuple *stmt = &(insert->new.array[s]);

		FFORMAT(out, "INSERT INTO \"%s\".\"%s\" ",
				insert->nspname,
				insert->relname);

		/* loop over column names and add them to the out stream */
		FFORMAT(out, "%s", "(");
		for (int c = 0; c < stmt->cols; c++)
		{
			FFORMAT(out, "%s\"%s\"", c > 0 ? ", " : "", stmt->columns[c]);
		}
		FFORMAT(out, "%s", ")");

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
		FFORMAT(out, "%s", " overriding system value ");

		/* now loop over VALUES rows */
		FFORMAT(out, "%s", "VALUES ");

		for (int r = 0; r < stmt->values.count; r++)
		{
			LogicalMessageValues *values = &(stmt->values.array[r]);

			/* now loop over column values for this VALUES row */
			FFORMAT(out, "%s(", r > 0 ? ", " : "");
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				FFORMAT(out, "%s", v > 0 ? ", " : "");

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}

			FFORMAT(out, "%s", ")");
		}

		FFORMAT(out, "%s", ";\n");
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

		FFORMAT(out, "UPDATE \"%s\".\"%s\" ", update->nspname, update->relname);

		if (old->values.count != new->values.count ||
			old->values.count != 1 ||
			new->values.count != 1)
		{
			log_error("Failed to write multi-values UPDATE statement "
					  "with %d old rows and %d new rows",
					  old->values.count,
					  new->values.count);
			return false;
		}

		FFORMAT(out, "%s", "SET ");

		for (int r = 0; r < new->values.count; r++)
		{
			LogicalMessageValues *values = &(new->values.array[r]);

			bool first = true;

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				const char *colname = new->columns[v];
				LogicalMessageValue *value = &(values->array[v]);

				if (new->cols <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  new->cols);
					return false;
				}

				/*
				 * Avoid SET "id" = 1 WHERE "id" = 1 ; so for that we lookup
				 * for a column with the same name in the old parts, and with
				 * the same value too.
				 */
				bool skip = false;

				for (int oc = 0; oc < old->cols; oc++)
				{
					if (streq(old->columns[oc], colname))
					{
						/* only works because old->values.count == 1 */
						LogicalMessageValue *oldValue =
							&(old->values.array[0].array[v]);

						if (LogicalMessageValueEq(oldValue, value))
						{
							skip = true;
							break;
						}
					}
				}

				if (!skip)
				{
					FFORMAT(out, "%s", first ? "" : ", ");
					FFORMAT(out, "\"%s\" = ", colname);

					if (first)
					{
						first = false;
					}

					if (!stream_write_value(out, value))
					{
						/* errors have already been logged */
						return false;
					}
				}
			}
		}

		FFORMAT(out, "%s", " WHERE ");

		for (int r = 0; r < old->values.count; r++)
		{
			LogicalMessageValues *values = &(old->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (old->cols <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  old->cols);
					return false;
				}

				FFORMAT(out, "%s", v > 0 ? " and " : "");
				FFORMAT(out, "\"%s\" = ", old->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		FFORMAT(out, "%s", ";\n");
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

		FFORMAT(out,
				"DELETE FROM \"%s\".\"%s\"",
				delete->nspname,
				delete->relname);

		FFORMAT(out, "%s", " WHERE ");

		for (int r = 0; r < old->values.count; r++)
		{
			LogicalMessageValues *values = &(old->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (old->cols <= v)
				{
					log_error("Failed to write DELETE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  old->cols);
					return false;
				}

				FFORMAT(out, "%s", v > 0 ? " and " : "");
				FFORMAT(out, "\"%s\" = ", old->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		FFORMAT(out, "%s", ";\n");
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
	fformat(out, "TRUNCATE ONLY %s.%s\n", truncate->nspname, truncate->relname);

	return true;
}


/*
 * stream_write_value writes the given LogicalMessageValue to the out stream.
 */
bool
stream_write_value(FILE *out, LogicalMessageValue *value)
{
	if (value == NULL)
	{
		log_error("BUG: stream_write_value value is NULL");
		return false;
	}

	if (value->isNull)
	{
		fformat(out, "NULL");
	}
	else
	{
		switch (value->oid)
		{
			case BOOLOID:
			{
				fformat(out, "'%s'", value->val.boolean ? "t" : "f");
				break;
			}

			case INT8OID:
			{
				fformat(out, "%lld", (long long) value->val.int8);
				break;
			}

			case FLOAT8OID:
			{
				fformat(out, "%g", value->val.float8);
				break;
			}

			case BYTEAOID:
			{
				if (value->isQuoted)
				{
					fformat(out, "%s", value->val.str);
				}
				else
				{
					fformat(out, "'%s'", value->val.str);
				}

				break;
			}

			case TEXTOID:
			{
				if (value->isQuoted)
				{
					fformat(out, "%s", value->val.str);
				}
				else
				{
					const char *str = value->val.str;
					if (!stream_write_sql_escape_string_constant(out, str))
					{
						log_error("Failed to write escaped string: E'%s'", str);
						return false;
					}
				}
				break;
			}

			default:
			{
				log_error("BUG: stream_write_insert value with oid %d",
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
	fformat(out, "E'");

	for (int i = 0; str[i] != '\0'; i++)
	{
		switch (str[i])
		{
			case '\b':
			{
				fformat(out, "\\b");
				break;
			}

			case '\f':
			{
				fformat(out, "\\f");
				break;
			}

			case '\n':
			{
				fformat(out, "\\n");
				break;
			}

			case '\r':
			{
				fformat(out, "\\r");
				break;
			}

			case '\t':
			{
				fformat(out, "\\t");
				break;
			}

			case '\'':
			case '\\':
			{
				fformat(out, "\\%c", str[i]);
				break;
			}

			default:
			{
				fformat(out, "%c", str[i]);
				break;
			}
		}
	}

	fformat(out, "'");

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
 *  computeTxnMetadataFilename computes the file path for transaction metadata
 *  based on its transaction id
 */
bool
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
 * writeTxnMetadataFile writes the transaction metadata to a file in the given
 * directory
 */
bool
writeTxnMetadataFile(LogicalTransaction *txn, const char *dir)
{
	char txnfilename[MAXPGPATH] = { 0 };

	if (!computeTxnMetadataFilename(txn->xid, dir, txnfilename))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("stream_write_commit_metadata_file: writing transaction "
			  "metadata file \"%s\" with commit lsn %X/%X",
			  txnfilename,
			  LSN_FORMAT_ARGS(txn->commitLSN));

	char contents[BUFSIZE] = { 0 };

	sformat(contents, BUFSIZE,
			"{\"xid\":%lld,\"commit_lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			(long long) txn->xid,
			LSN_FORMAT_ARGS(txn->commitLSN),
			txn->timestamp);

	/* write the metadata to txnfilename */
	if (!write_file(contents, strlen(contents), txnfilename))
	{
		log_error("Failed to write file \"%s\"", txnfilename);
		return false;
	}

	return true;
}
