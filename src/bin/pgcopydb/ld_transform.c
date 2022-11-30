/*
 * src/bin/pgcopydb/ld_transform.c
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
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


/*
 * stream_transform_start_worker creates a sub-process that transform JSON
 * files into SQL files as needed, consuming requests from a queue.
 */
bool
stream_transform_start_worker(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/*
	 * Flush stdio channels just before fork, to avoid double-output
	 * problems.
	 */
	fflush(stdout);
	fflush(stderr);

	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a stream transform worker process: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			if (!stream_transform_worker(context))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			privateContext->subprocess = fpid;
			break;
		}
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
stream_transform_worker(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	int errors = 0;
	bool stop = false;

	log_notice("Started Stream Transform worker %d [%d]", getpid(), getppid());

	while (!stop)
	{
		QMessage mesg = { 0 };

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			return false;
		}

		if (!queue_receive(&(privateContext->transformQueue), &mesg))
		{
			/* errors have already been logged */
			break;
		}

		switch (mesg.type)
		{
			case QMSG_TYPE_STOP:
			{
				stop = true;
				log_debug("Stop message received by stream transform worker");
				break;
			}

			case QMSG_TYPE_STREAM_TRANSFORM:
			{
				log_debug("stream_transform_worker received transform %X/%X",
						  LSN_FORMAT_ARGS(mesg.data.lsn));

				if (!stream_compute_pathnames(context, mesg.data.lsn))
				{
					/* errors have already been logged, break from the loop */
					++errors;
					break;
				}

				if (!stream_transform_file(privateContext->walFileName,
										   privateContext->sqlFileName))
				{
					/* errors have already been logged, break from the loop */
					++errors;
					break;
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on vacuum queue %d",
						  mesg.type,
						  privateContext->transformQueue.qId);
				break;
			}
		}
	}

	return stop == true && errors == 0;
}


/*
 * stream_compute_pathnames computes the WAL.json and WAL.sql filenames from
 * the given LSN, which is expected to be the first LSN processed in the file
 * we need to find the name of.
 */
bool
stream_compute_pathnames(LogicalStreamContext *context, uint64_t lsn)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	char wal[MAXPGPATH] = { 0 };

	/*
	 * The timeline and wal segment size are determined when connecting to the
	 * source database, and stored to local files at that time. When the Stream
	 * Transform Worker process is created, we don't have that information yet,
	 * so the first time we process an LSN from the queue we go and fetch the
	 * information from our local files.
	 */
	if (context->timeline == 0)
	{
		uint32_t WalSegSz;
		IdentifySystem system = { 0 };

		if (!stream_read_context(&(privateContext->paths), &system, &WalSegSz))
		{
			log_error("Failed to read the streaming context information "
					  "from the source database, see above for details");
			return false;
		}

		context->timeline = system.timeline;
		context->WalSegSz = WalSegSz;
	}

	/* compute the WAL filename that would host the current LSN */
	XLogSegNo segno;
	XLByteToSeg(lsn, segno, context->WalSegSz);
	XLogFileName(wal, context->timeline, segno, context->WalSegSz);

	sformat(privateContext->walFileName,
			sizeof(privateContext->walFileName),
			"%s/%s.json",
			privateContext->paths.dir,
			wal);

	sformat(privateContext->sqlFileName,
			sizeof(privateContext->sqlFileName),
			"%s/%s.sql",
			privateContext->paths.dir,
			wal);

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
stream_transform_file(char *jsonfilename, char *sqlfilename)
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

	content.messages =
		(LogicalMessageMetadata *) calloc(content.count,
										  sizeof(LogicalMessageMetadata));

	if (content.messages == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/* {action: B} {action: C} {action: X} */
	int maxTxnsCount = (content.count / 2) + 1;
	LogicalTransactionArray txns = { 0 };

	/* the actual count is maintained in the for loop below */
	txns.count = 0;
	txns.array =
		(LogicalTransaction *) calloc(maxTxnsCount, sizeof(LogicalTransaction));

	if (txns.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Read the JSON-lines file that we received from streaming logical
	 * decoding messages with wal2json, and parse the JSON messages into our
	 * internal representation structure.
	 */
	int currentTxIndex = 0;
	LogicalTransaction *currentTx = &(txns.array[currentTxIndex]);

	for (int i = 0; i < content.count; i++)
	{
		char *message = content.lines[i];
		LogicalMessageMetadata *metadata = &(content.messages[i]);

		log_trace("stream_transform_file[%2d]: %s", i, message);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, false))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		if (!parseMessage(currentTx, metadata, message, json))
		{
			log_error("Failed to parse JSON message: %s", message);
			json_value_free(json);
			return false;
		}

		json_value_free(json);

		log_trace("stream_transform_file[%2d]: %c %3d %X/%X [%2d]: %3d %X/%X",
				  i,
				  metadata->action,
				  metadata->xid,
				  LSN_FORMAT_ARGS(metadata->lsn),
				  currentTxIndex,
				  currentTx->xid,
				  LSN_FORMAT_ARGS(currentTx->beginLSN));

		/* it is time to close the current transaction and prepare a new one? */
		if (metadata->action == STREAM_ACTION_COMMIT)
		{
			++txns.count;
			++currentTxIndex;

			if ((maxTxnsCount - 1) < currentTxIndex)
			{
				log_error("Parsing transaction %d, which is more than the "
						  "maximum allocated transaction count %d",
						  currentTxIndex + 1,
						  maxTxnsCount);
				return false;
			}

			currentTx = &(txns.array[currentTxIndex]);
		}
	}

	/*
	 * We might have a last pending transaction with a COMMIT message to be
	 * found in a a later file.
	 */
	if (currentTx->count > 0)
	{
		++txns.count;
	}

	/* free dynamic memory that's not needed anymore */
	free(content.lines);
	free(content.messages);

	log_debug("stream_transform_file read %d transactions", txns.count);

	/*
	 * Now that we have read and parsed the JSON file into our internal
	 * structure that represents SQL transactions with statements, output the
	 * content in the SQL format.
	 */
	FILE *sql = fopen_with_umask(sqlfilename, "w", FOPEN_FLAGS_W, 0644);

	if (sql == NULL)
	{
		log_error("Failed to create and open file \"%s\"", sqlfilename);
		return false;
	}

	for (int i = 0; i < txns.count; i++)
	{
		LogicalTransaction *currentTx = &(txns.array[i]);

		if (!stream_write_transaction(sql, currentTx))
		{
			/* errors have already been logged */
			return false;
		}

		(void) FreeLogicalTransaction(currentTx);
	}

	if (fclose(sql) == EOF)
	{
		log_error("Failed to write file \"%s\"", sqlfilename);
		return false;
	}

	log_info("Transformed JSON messages into SQL file \"%s\"",
			 sqlfilename);

	return true;
}


/*
 * parseMessage parses a JSON message as emitted by the logical decoding output
 * plugin (either test_decoding or wal2json) into our own internal
 * representation, that can be later output as SQL text.
 */
bool
parseMessage(LogicalTransaction *txn,
			 LogicalMessageMetadata *metadata,
			 char *message,
			 JSON_Value *json)
{
	if (txn == NULL)
	{
		log_error("BUG: parseMessage called with a NULL LogicalTransaction");
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

	/*
	 * Check that XID make sense, except for SWITCH messages, which don't have
	 * XID information, only have LSN information.
	 */
	if (metadata->action != STREAM_ACTION_SWITCH &&
		metadata->action != STREAM_ACTION_KEEPALIVE)
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

	/*
	 * All messages except for BEGIN/COMMIT need a LogicalTransactionStatement
	 * to represent them within the current transaction.
	 */
	LogicalTransactionStatement *stmt = NULL;

	if (metadata->action != STREAM_ACTION_BEGIN &&
		metadata->action != STREAM_ACTION_COMMIT)
	{
		stmt = (LogicalTransactionStatement *)
			   malloc(sizeof(LogicalTransactionStatement));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->action = metadata->action;
	}

	switch (metadata->action)
	{
		/* begin messages only use pgcopydb internal metadata */
		case STREAM_ACTION_BEGIN:
		{
			txn->xid = metadata->xid;
			txn->beginLSN = metadata->lsn;
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
			txn->commitLSN = metadata->lsn;

			break;
		}

		/* switch wal messages are pgcopydb internal messages */
		case STREAM_ACTION_SWITCH:
		{
			stmt->stmt.switchwal.lsn = metadata->lsn;

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		/* keepalive messages are pgcopydb internal messages */
		case STREAM_ACTION_KEEPALIVE:
		{
			stmt->stmt.keepalive.lsn = metadata->lsn;

			strlcpy(stmt->stmt.keepalive.timestamp,
					metadata->timestamp,
					sizeof(stmt->stmt.keepalive.timestamp));

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		/* now handle DML messages from the output plugin */
		default:
		{
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
					if (!parseTestDecodingMessage(stmt, metadata, message, json))
					{
						log_error("Failed to parse test_decoding message, "
								  "see above for details");
						return false;
					}

					break;
				}

				case JSONObject:
				{
					if (!parseWal2jsonMessage(stmt, metadata, message, json))
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
 * FreeLogicalTransaction frees the malloc'ated memory areas of a
 * LogicalTransaction.
 */
void
FreeLogicalTransaction(LogicalTransaction *tx)
{
	LogicalTransactionStatement *currentStmt = tx->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
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
	}
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
		LogicalMessageTuple *stmt = &(tupleArray->array[s]);

		free(stmt->columns);

		for (int r = 0; r < stmt->values.count; r++)
		{
			LogicalMessageValues *values = &(stmt->values.array[r]);

			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (value->oid == TEXTOID)
				{
					free(value->val.str);
				}
			}

			free(stmt->values.array);
		}
	}
}


/*
 * stream_write_transaction writes the LogicalTransaction statements as SQL to
 * the already open out stream.
 */
bool
stream_write_transaction(FILE *out, LogicalTransaction *txn)
{
	/*
	 * SWITCH WAL commands might appear eigher in the middle of a transaction
	 * or in between two transactions, depending on when the LSN WAL file
	 * switch happens on the source server.
	 *
	 * When the SWITCH WAL happens in between transactions, our internal
	 * representation makes it look like a transaction with a single SWITCH
	 * statement, and in that case we don't want to output BEGIN and COMMIT
	 * statements at all.
	 */
	if (txn->count == 0)
	{
		return true;
	}

	bool sentBEGIN = false;
	LogicalTransactionStatement *currentStmt = txn->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		switch (currentStmt->action)
		{
			case STREAM_ACTION_SWITCH:
			{
				if (!stream_write_switchwal(out, &(currentStmt->stmt.switchwal)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_KEEPALIVE:
			{
				if (!stream_write_keepalive(out, &(currentStmt->stmt.keepalive)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_INSERT:
			{
				if (!sentBEGIN)
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
				if (!sentBEGIN)
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
				if (!sentBEGIN)
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
				if (!sentBEGIN)
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

	if (sentBEGIN)
	{
		if (!stream_write_commit(out, txn))
		{
			return false;
		}
	}

	return true;
}


/*
 * stream_write_switchwal writes a SWITCH statement to the already open out
 * stream.
 */
bool
stream_write_begin(FILE *out, LogicalTransaction *txn)
{
	fformat(out,
			"%s{\"xid\":%lld,\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_BEGIN,
			(long long) txn->xid,
			LSN_FORMAT_ARGS(txn->beginLSN),
			txn->timestamp);

	return true;
}


/*
 * stream_write_switchwal writes a SWITCH statement to the already open out
 * stream.
 */
bool
stream_write_commit(FILE *out, LogicalTransaction *txn)
{
	fformat(out,
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
	fformat(out, "%s{\"lsn\":\"%X/%X\"}\n",
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
	fformat(out, "%s{\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
			OUTPUT_KEEPALIVE,
			LSN_FORMAT_ARGS(keepalive->lsn),
			keepalive->timestamp);

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

		fformat(out, "INSERT INTO \"%s\".\"%s\" ",
				insert->nspname,
				insert->relname);

		/* loop over column names and add them to the out stream */
		fformat(out, "(");
		for (int c = 0; c < stmt->cols; c++)
		{
			fformat(out, "%s\"%s\"", c > 0 ? ", " : "", stmt->columns[c]);
		}
		fformat(out, ")");

		/* now loop over VALUES rows */
		fformat(out, " VALUES ");

		for (int r = 0; r < stmt->values.count; r++)
		{
			LogicalMessageValues *values = &(stmt->values.array[r]);

			/* now loop over column values for this VALUES row */
			fformat(out, "%s(", r > 0 ? ", " : "");
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				fformat(out, "%s", v > 0 ? ", " : "");

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}

			fformat(out, ")");
		}

		fformat(out, ";\n");
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

		fformat(out, "UPDATE \"%s\".\"%s\" ", update->nspname, update->relname);

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

		fformat(out, "SET ");

		for (int r = 0; r < new->values.count; r++)
		{
			LogicalMessageValues *values = &(new->values.array[r]);

			/* now loop over column values for this VALUES row */
			for (int v = 0; v < values->cols; v++)
			{
				LogicalMessageValue *value = &(values->array[v]);

				if (new->cols <= v)
				{
					log_error("Failed to write UPDATE statement with more "
							  "VALUES (%d) than COLUMNS (%d)",
							  values->cols,
							  new->cols);
					return false;
				}

				fformat(out, "%s", v > 0 ? ", " : "");
				fformat(out, "\"%s\" = ", new->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		fformat(out, " WHERE ");

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

				fformat(out, "%s", v > 0 ? " and " : "");
				fformat(out, "\"%s\" = ", old->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		fformat(out, ";\n");
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

		fformat(out,
				"DELETE FROM \"%s\".\"%s\"",
				delete->nspname,
				delete->relname);

		fformat(out, " WHERE ");

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

				fformat(out, "%s", v > 0 ? " and " : "");
				fformat(out, "\"%s\" = ", old->columns[v]);

				if (!stream_write_value(out, value))
				{
					/* errors have already been logged */
					return false;
				}
			}
		}

		fformat(out, ";\n");
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
				fformat(out, "'%s' ", value->val.boolean ? "t" : "f");
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

			case TEXTOID:
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
