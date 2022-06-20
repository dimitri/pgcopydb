/*
 * src/bin/pgcopydb/stream.c
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


static bool updateStreamCounters(StreamContext *context,
								 LogicalMessageMetadata *metadata);

static bool SetColumnNamesAndValues(LogicalMessageTuple *tuple,
									const char *message,
									JSON_Value *json,
									const char *name);

static bool streamLogicalTransactionAppendStatement(LogicalTransaction *txn,
													LogicalTransactionStatement *stmt
													);


/*
 * stream_init_specs initializes Change Data Capture streaming specifications
 * from a copyDBSpecs structure.
 */
bool
stream_init_specs(StreamSpecs *specs,
				  char *cdcdir,
				  char *source_pguri,
				  char *target_pguri,
				  char *slotName,
				  uint64_t endpos)
{
	/* just copy into StreamSpecs what's been initialized in copySpecs */
	strlcpy(specs->cdcdir, cdcdir, MAXCONNINFO);
	strlcpy(specs->source_pguri, source_pguri, MAXCONNINFO);
	strlcpy(specs->target_pguri, target_pguri, MAXCONNINFO);
	strlcpy(specs->slotName, slotName, sizeof(specs->slotName));

	specs->endpos = endpos;

	if (!buildReplicationURI(specs->source_pguri, specs->logrep_pguri))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * startLogicalStreaming opens a replication connection to the given source
 * database and issues the START REPLICATION command there.
 */
bool
startLogicalStreaming(StreamSpecs *specs)
{
	/* wal2json options we want to use for the plugin */
	KeyVal options = {
		.count = 5,
		.keywords = {
			"format-version",
			"include-xids",
			"include-lsn",
			"include-transaction",
			"include-timestamp"
		},
		.values = {
			"2",
			"true",
			"true",
			"true",
			"true"
		}
	};

	/* prepare the stream options */
	LogicalStreamClient stream = { 0 };

	stream.pluginOptions = options;
	stream.writeFunction = &streamWrite;
	stream.flushFunction = &streamFlush;
	stream.closeFunction = &streamClose;

	StreamContext privateContext = { 0 };
	LogicalStreamContext context = { 0 };

	privateContext.cdcdir = specs->cdcdir;
	privateContext.startLSN = specs->startLSN;

	context.private = (void *) &(privateContext);

	/*
	 * Read possibly already existing file to initialize the start LSN from a
	 * previous run of our command.
	 */
	StreamContent latestStreamedContent = { 0 };

	if (!stream_read_latest(specs, &latestStreamedContent))
	{
		/* errors have already been logged */
		return false;
	}

	if (latestStreamedContent.count > 0)
	{
		LogicalMessageMetadata *latest =
			&(latestStreamedContent.messages[latestStreamedContent.count - 1]);

		specs->startLSN = latest->nextlsn;

		log_info("Resuming streaming at LSN %X/%X",
				 LSN_FORMAT_ARGS(specs->startLSN));
	}

	/*
	 * In case of being disconnected or other transient errors, reconnect and
	 * continue streaming.
	 */
	bool retry = true;

	while (retry)
	{
		if (!pgsql_init_stream(&stream,
							   specs->logrep_pguri,
							   specs->slotName,
							   specs->startLSN,
							   specs->endpos))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_start_replication(&stream))
		{
			/* errors have already been logged */
			return false;
		}

		/* ignore errors, try again unless asked to stop */
		bool cleanExit = pgsql_stream_logical(&stream, &context);

		if (cleanExit || asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			retry = false;
		}

		if (cleanExit)
		{
			log_info("Streaming is now finished after processing %lld message%s",
					 (long long) privateContext.counters.total,
					 privateContext.counters.total > 0 ? "s" : "");
		}
		else if (!(asked_to_stop || asked_to_stop_fast || asked_to_quit))
		{
			log_warn("Streaming got interrupted at %X/%X, reconnecting in 1s",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn));
		}
		else
		{
			log_warn("Streaming got interrupted at %X/%X "
					 "after processing %lld message%s",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn),
					 (long long) privateContext.counters.total,
					 privateContext.counters.total > 0 ? "s" : "");
		}

		/* sleep for one entire second before retrying */
		(void) pg_usleep(1 * 1000 * 1000);
	}

	return true;
}


/*
 * streamWrite is a callback function for our LogicalStreamClient.
 *
 * This function is called for each message received in pgsql_stream_logical.
 * It records the logical message to file. The message is expected to be in
 * JSON format, from the wal2json logical decoder.
 */
bool
streamWrite(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/* get the segment number from the current_record_lsn */
	XLogSegNo segno;
	char wal[MAXPGPATH] = { 0 };
	char walFileName[MAXPGPATH] = { 0 };

	/* compute the WAL filename that would host the current LSN */
	XLByteToSeg(context->cur_record_lsn, segno, context->WalSegSz);
	XLogFileName(wal, context->timeline, segno, context->WalSegSz);

	sformat(walFileName, sizeof(walFileName), "%s/%s.json",
			privateContext->cdcdir,
			wal);

	if (strcmp(privateContext->walFileName, walFileName) != 0)
	{
		/* if we had a WAL file opened, close it now */
		if (!IS_EMPTY_STRING_BUFFER(privateContext->walFileName) &&
			privateContext->jsonFile != NULL)
		{
			log_debug("closing %s", privateContext->walFileName);

			if (fclose(privateContext->jsonFile) != 0)
			{
				/* errors have already been logged */
				return false;
			}
		}

		log_info("Now streaming changes to \"%s\"", walFileName);
		strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);

		/*
		 * When the target file already exists, open it in append mode.
		 */
		int flags = file_exists(walFileName) ? FOPEN_FLAGS_A : FOPEN_FLAGS_W;

		privateContext->jsonFile =
			fopen_with_umask(walFileName, "ab", flags, 0644);

		if (privateContext->jsonFile == NULL)
		{
			/* errors have already been logged */
			log_error("Failed to open file \"%s\": %m",
					  privateContext->walFileName);
			return false;
		}

		/*
		 * Also maintain the "latest" symbolic link to the latest file where
		 * we've been streaming changes in.
		 */
		char latest[MAXPGPATH] = { 0 };

		sformat(latest, sizeof(latest), "%s/latest", privateContext->cdcdir);

		if (!create_symbolic_link(privateContext->walFileName, latest))
		{
			/* errors have already been logged */
			return false;
		}
	}

	LogicalMessageMetadata metadata = { 0 };
	JSON_Value *json = json_parse_string(context->buffer);

	if (!parseMessageMetadata(&metadata, context->buffer, json))
	{
		/* errors have already been logged */
		if (privateContext->jsonFile != NULL)
		{
			if (fclose(privateContext->jsonFile) != 0)
			{
				log_error("Failed to close file \"%s\": %m",
						  privateContext->walFileName);
			}
		}

		json_value_free(json);
		return false;
	}

	json_value_free(json);

	(void) updateStreamCounters(privateContext, &metadata);

	/*
	 * Write the logical decoding message to disk, appending to the already
	 * opened file we track in the privateContext.
	 */
	if (privateContext->jsonFile != NULL)
	{
		long bytes_left = strlen(context->buffer);
		long bytes_written = 0;

		while (bytes_left > 0)
		{
			int ret;

			ret = fwrite(context->buffer + bytes_written,
						 sizeof(char),
						 bytes_left,
						 privateContext->jsonFile);

			if (ret < 0)
			{
				log_error("Failed to write %ld bytes to file \"%s\": %m",
						  bytes_left,
						  privateContext->walFileName);
				return false;
			}

			/* Write was successful, advance our position */
			bytes_written += ret;
			bytes_left -= ret;
		}

		if (fwrite("\n", sizeof(char), 1, privateContext->jsonFile) != 1)
		{
			log_error("Failed to write 1 byte to file \"%s\": %m",
					  privateContext->walFileName);

			if (privateContext->jsonFile != NULL)
			{
				if (fclose(privateContext->jsonFile) != 0)
				{
					log_error("Failed to close file \"%s\": %m",
							  privateContext->walFileName);
				}
			}
			return false;
		}

		/* update the LSN tracking that's reported in the feedback */
		context->tracking->written_lsn = context->cur_record_lsn;
	}

	log_debug("Received action %c for XID %u in LSN %X/%X",
			  metadata.action,
			  metadata.xid,
			  LSN_FORMAT_ARGS(metadata.lsn));

	return true;
}


/*
 * streamFlush is a callback function for our LogicalStreamClient.
 *
 * This function is called when it's time to flush the data that's currently
 * being written to disk, by calling fsync(). This is triggerred either on a
 * time basis from within the writeFunction callback, or when it's
 * time_to_abort in pgsql_stream_logical.
 */
bool
streamFlush(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/* when there is currently no file opened, just skip the flush operation */
	if (privateContext->jsonFile == NULL)
	{
		return true;
	}

	if (context->tracking->flushed_lsn < context->tracking->written_lsn)
	{
		int fd = fileno(privateContext->jsonFile);

		if (fsync(fd) != 0)
		{
			log_error("Failed to fsync file \"%s\": %m",
					  privateContext->walFileName);
			return false;
		}

		context->tracking->flushed_lsn = context->tracking->written_lsn;

		log_debug("Flushed up to %X/%X in file \"%s\"",
				  LSN_FORMAT_ARGS(context->tracking->flushed_lsn),
				  privateContext->walFileName);
	}

	return true;
}


/*
 * streamClose is a callback function for our LogicalStreamClient.
 *
 * This function is called when it's time to close the currently opened file
 * before quitting. On the way out, a call to streamFlush is included.
 */
bool
streamClose(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/* when there is currently no file opened, just skip the close operation */
	if (privateContext->jsonFile == NULL)
	{
		return true;
	}

	if (!streamFlush(context))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("streamClose: closing file \"%s\"", privateContext->walFileName);

	if (fclose(privateContext->jsonFile) != 0)
	{
		log_error("Failed to close file \"%s\": %m",
				  privateContext->walFileName);
	}

	return true;
}


/*
 * parseMessageMetadata parses just the metadata of the JSON replication
 * message we got from wal2json.
 */
bool
parseMessageMetadata(LogicalMessageMetadata *metadata,
					 const char *buffer,
					 JSON_Value *json)
{
	JSON_Object *jsobj = json_value_get_object(json);

	if (json_type(json) != JSONObject)
	{
		log_error("Failed to parse JSON message: %s", buffer);
		return false;
	}

	/* action is one of "B", "C", "I", "U", "D", "T" */
	char *action = (char *) json_object_get_string(jsobj, "action");

	if (action == NULL || strlen(action) != 1)
	{
		log_error("Failed to parse action \"%s\" in JSON message: %s",
				  action ? "NULL" : action,
				  buffer);
		return false;
	}

	metadata->action = StreamActionFromChar(action[0]);

	if (metadata->action == STREAM_ACTION_UNKNOWN)
	{
		/* errors have already been logged */
		return false;
	}

	/* message entries {action: "M"} do not have xid, lsn, nextlsn fields */
	if (metadata->action == STREAM_ACTION_MESSAGE)
	{
		log_debug("Skipping message: %s", buffer);
		return true;
	}

	double xid = json_object_get_number(jsobj, "xid");
	metadata->xid = (uint32_t) xid;

	char *lsn = (char *) json_object_get_string(jsobj, "lsn");

	if (lsn == NULL)
	{
		log_error("Failed to parse JSON message: \"%s\"", buffer);
		return false;
	}

	if (!parseLSN(lsn, &(metadata->lsn)))
	{
		log_error("Failed to parse LSN \"%s\"", lsn);
		return false;
	}

	char *nextlsn = (char *) json_object_get_string(jsobj, "nextlsn");

	if (nextlsn != NULL)
	{
		if (!parseLSN(nextlsn, &(metadata->nextlsn)))
		{
			log_error("Failed to parse LSN \"%s\"", nextlsn);
			return false;
		}
	}

	return true;
}


/*
 * stream_read_file reads a JSON file that is expected to contain messages
 * received via logical decoding when using the wal2json output plugin with the
 * format-version 2.
 */
bool
stream_read_file(StreamContent *content)
{
	long size = 0L;

	if (!read_file(content->filename, &(content->buffer), &size))
	{
		/* errors have already been logged */
		return false;
	}

	content->count =
		splitLines(content->buffer, content->lines, MAX_STREAM_CONTENT_COUNT);

	if (content->count >= MAX_STREAM_CONTENT_COUNT)
	{
		log_error("Failed to split file \"%s\" in lines: pgcopydb support only "
				  "files with up to %d lines, and more were found",
				  content->filename,
				  MAX_STREAM_CONTENT_COUNT);
		free(content->buffer);
		return false;
	}

	for (int i = 0; i < content->count; i++)
	{
		char *message = content->lines[i];
		LogicalMessageMetadata *metadata = &(content->messages[i]);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		json_value_free(json);
	}

	return true;
}


/*
 * stream_read_latest reads the "latest" file that was written into, if any,
 * using the symbolic link named "latest". When the file exists, its content is
 * parsed as an array of LogicalMessageMetadata.
 *
 * One message per physical line is expected (wal2json uses Postgres internal
 * function escape_json which deals with escaping newlines and other special
 * characters).
 */
bool
stream_read_latest(StreamSpecs *specs, StreamContent *content)
{
	char latest[MAXPGPATH] = { 0 };

	sformat(latest, sizeof(latest), "%s/latest", specs->cdcdir);

	if (!file_exists(latest))
	{
		return true;
	}

	if (!normalize_filename(latest,
							content->filename,
							sizeof(content->filename)))
	{
		/* errors have already been logged  */
		return false;
	}

	log_info("Resuming streaming from latest file \"%s\"", content->filename);

	return stream_read_file(content);
}


/*
 * updateStreamCounters increment the counter that matches the received
 * message.
 */
static bool
updateStreamCounters(StreamContext *context, LogicalMessageMetadata *metadata)
{
	++context->counters.total;

	switch (metadata->action)
	{
		case STREAM_ACTION_BEGIN:
		{
			++context->counters.begin;
			break;
		}

		case STREAM_ACTION_COMMIT:
		{
			++context->counters.commit;
			break;
		}

		case STREAM_ACTION_INSERT:
		{
			++context->counters.insert;
			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			++context->counters.update;
			break;
		}

		case STREAM_ACTION_DELETE:
		{
			++context->counters.delete;
			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			++context->counters.truncate;
			break;
		}

		default:
		{
			log_debug("Skipping counters for message action \"%c\"",
					  metadata->action);
			break;
		}
	}

	return true;
}


/*
 * buildReplicationURI builds a connection string that includes
 * replication=database from the connection string that's passed as input.
 */
bool
buildReplicationURI(const char *pguri, char *repl_pguri)
{
	URIParams params = { 0 };
	bool checkForCompleteURI = false;

	KeyVal replicationParams = {
		.count = 1,
		.keywords = { "replication" },
		.values = { "database" }
	};

	/* if replication is already found, we override it to value "1" */
	if (!parse_pguri_info_key_vals(pguri,
								   &replicationParams,
								   &params,
								   checkForCompleteURI))
	{
		/* errors have already been logged */
		return false;
	}

	if (!buildPostgresURIfromPieces(&params, repl_pguri))
	{
		log_error("Failed to produce the replication connection string");
		return false;
	}

	return true;
}


/*
 * StreamActionFromChar parses an action character as expected in a wal2json
 * entry and returns our own internal enum value for it.
 */
StreamAction
StreamActionFromChar(char action)
{
	switch (action)
	{
		case 'B':
		{
			return STREAM_ACTION_BEGIN;
		}

		case 'C':
		{
			return STREAM_ACTION_COMMIT;
		}

		case 'I':
		{
			return STREAM_ACTION_INSERT;
		}

		case 'U':
		{
			return STREAM_ACTION_UPDATE;
		}

		case 'D':
		{
			return STREAM_ACTION_DELETE;
		}

		case 'T':
		{
			return STREAM_ACTION_TRUNCATE;
		}

		case 'M':
		{
			return STREAM_ACTION_MESSAGE;
		}

		default:
		{
			log_error("Failed to parse JSON message action: \"%c\"", action);
			return STREAM_ACTION_UNKNOWN;
		}
	}

	/* keep compiler happy */
	return STREAM_ACTION_UNKNOWN;
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

	strlcpy(content.filename, jsonfilename, sizeof(content.filename));

	if (!read_file(content.filename, &(content.buffer), &size))
	{
		/* errors have already been logged */
		return false;
	}

	content.count =
		splitLines(content.buffer, content.lines, MAX_STREAM_CONTENT_COUNT);

	if (content.count >= MAX_STREAM_CONTENT_COUNT)
	{
		log_error("Failed to split file \"%s\" in lines: pgcopydb support only "
				  "files with up to %d lines, and more were found",
				  content.filename,
				  MAX_STREAM_CONTENT_COUNT);
		free(content.buffer);
		return false;
	}

	int maxTxnsCount = content.count / 2; /* {action: B} {action: C} */
	LogicalTransactionArray txns = { 0 };

	size_t arraySize = maxTxnsCount * sizeof(LogicalTransaction);

	txns.array = (LogicalTransaction *) malloc(arraySize);
	bzero((void *) &(txns.array), arraySize);

	int currentTxIndex = 0;
	LogicalTransaction *currentTx = &(txns.array[currentTxIndex]);

	for (int i = 0; i < content.count; i++)
	{
		char *message = content.lines[i];
		LogicalMessageMetadata *metadata = &(content.messages[i]);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json))
		{
			/* errors have already been logged */
			json_value_free(json);
			return false;
		}

		if (!parseMessage(currentTx, metadata, message, json))
		{
			log_error("Failed to parse JSON message: %s", message);
			return false;
		}

		json_value_free(json);

		/* it is time to close the current transaction and prepare a new one? */
		if (metadata->action == STREAM_ACTION_COMMIT)
		{
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

	return true;
}


/*
 * parseMessage parses a JSON message as emitted by wal2json into our own
 * internal representation, that can be later output as SQL text.
 */
bool
parseMessage(LogicalTransaction *txn,
			 LogicalMessageMetadata *metadata,
			 char *message,
			 JSON_Value *json)
{
	if (txn->xid > 0 && txn->xid != metadata->xid)
	{
		log_debug("%s", message);
		log_error("BUG: logical message xid is %lld, which is different "
				  "from the current transaction xid %lld",
				  (long long) metadata->xid,
				  (long long) txn->xid);

		return false;
	}

	/* common entries for supported statements */
	LogicalTransactionStatement *stmt = NULL;

	JSON_Object *jsobj = NULL;
	char *schema = NULL;
	char *table = NULL;

	if (metadata->action == STREAM_ACTION_TRUNCATE ||
		metadata->action == STREAM_ACTION_INSERT ||
		metadata->action == STREAM_ACTION_UPDATE ||
		metadata->action == STREAM_ACTION_DELETE)
	{
		stmt = (LogicalTransactionStatement *)
			   malloc(sizeof(LogicalTransactionStatement));

		if (stmt == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		stmt->action = metadata->action;

		jsobj = json_value_get_object(json);
		schema = (char *) json_object_get_string(jsobj, "schema");
		table = (char *) json_object_get_string(jsobj, "table");

		if (schema == NULL || table == NULL)
		{
			log_error("Failed to parse truncate message missing "
					  "schema or table property: %s",
					  message);
			return false;
		}
	}

	switch (metadata->action)
	{
		case STREAM_ACTION_BEGIN:
		{
			txn->xid = metadata->xid;
			txn->beginLSN = metadata->lsn;
			txn->first = NULL;

			break;
		}

		case STREAM_ACTION_COMMIT:
		{
			txn->commitLSN = metadata->lsn;

			break;
		}

		case STREAM_ACTION_TRUNCATE:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.truncate.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.truncate.relname, table, NAMEDATALEN);

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		case STREAM_ACTION_INSERT:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

			stmt->stmt.insert.new.count = 1;
			stmt->stmt.insert.new.array =
				(LogicalMessageTuple *) malloc(sizeof(LogicalMessageTupleArray));

			LogicalMessageTuple zero = { 0 };
			LogicalMessageTuple *tuple = &(stmt->stmt.insert.new.array[0]);

			*tuple = zero;

			if (!SetColumnNamesAndValues(tuple, message, json, "columns"))
			{
				log_error("Failed to parse INSERT columns for logical "
						  "message %s",
						  message);
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		default:
		{
			log_error("Unknown message action %d", metadata->action);
			return false;
		}
	}

	/* keep compiler happy */
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
static bool
streamLogicalTransactionAppendStatement(LogicalTransaction *txn,
										LogicalTransactionStatement *stmt)
{
	if (txn->first == NULL)
	{
		txn->first = stmt;
		txn->last = stmt;
	}
	else
	{
		/* update the current last entry of the linked-list */
		txn->last->next = stmt;

		/* the new statement now becomes the last entry of the linked-list */
		stmt->prev = txn->last;
		txn->last = stmt;
	}

	return true;
}


/*
 * SetColumnNames parses the "columns" (or "identity") JSON object from a
 * wal2json logical replication message and fills-in our internal
 * representation for a tuple.
 */
static bool
SetColumnNamesAndValues(LogicalMessageTuple *tuple,
						const char *message,
						JSON_Value *json,
						const char *name)
{
	JSON_Object *jsobj = json_value_get_object(json);
	JSON_Array *jscols = json_object_get_array(jsobj, name);

	int count = json_array_get_count(jscols);

	if (MAX_COLUMN_COUNT < count)
	{
		log_error("Failed to parse logical decoding message that contains "
				  "%d columns, maximum supported by pgcopydb is %d",
				  count,
				  MAX_COLUMN_COUNT);
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
	LogicalMessageValuesArray *values = &(tuple->values);
	values->count = 1;
	values->array =
		(LogicalMessageValues *) malloc(sizeof(LogicalMessageValues));

	bzero((void *) &(values->array), sizeof(LogicalMessageValues));

	/* allocate one VALUES entry */
	LogicalMessageValues *value = &(tuple->values.array[0]);
	value->count = count;
	value->array =
		(LogicalMessageValue *) malloc(count * sizeof(LogicalMessageValue));

	bzero((void *) &(value->array), count * sizeof(LogicalMessageValue));

	for (int i = 0; i < count; i++)
	{
		JSON_Object *jscol = json_array_get_object(jscols, i);
		const char *colname = json_object_get_string(jscol, "name");

		strlcpy(tuple->columns[i], colname, NAMEDATALEN);

		JSON_Value *jsval = json_object_get_value(jscol, "value");

		switch (json_value_get_type(jsval))
		{
			case JSONNull:
			{
				/* default to TEXTOID to send NULLs over the wire */
				value->array[i].oid = TEXTOID;
				value->array[i].isNull = true;
				break;
			}

			case JSONBoolean:
			{
				bool x = json_value_get_boolean(jsval);

				value->array[i].oid = BOOLOID;
				value->array[i].val.boolean = x;
				value->array[i].isNull = false;
				break;
			}

			case JSONNumber:
			{
				double x = json_value_get_number(jsval);

				value->array[i].oid = FLOAT8OID;
				value->array[i].val.float8 = x;
				value->array[i].isNull = false;
				break;
			}

			case JSONString:
			{
				const char *x = json_value_get_string(jsval);

				value->array[i].oid = TEXTOID;
				value->array[i].val.str = strdup(x);
				value->array[i].isNull = false;
				break;
			}

			default:
			{
				log_error("Failed to parse column \"%s\" "
						  "JSON type for \"value\": %s",
						  colname,
						  message);
				return false;
			}
		}
	}

	return true;
}
