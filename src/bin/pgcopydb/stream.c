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
									JSON_Array *jscols);

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

	log_debug("stream_transform_file: read %d lines from \"%s\"",
			  content.count,
			  content.filename);

	int maxTxnsCount = content.count / 2; /* {action: B} {action: C} */
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

		log_debug("stream_transform_file[%d]: %s", i, message);

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
			json_value_free(json);
			return false;
		}

		json_value_free(json);

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

	JSON_Object *jsobj = json_value_get_object(json);
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
			JSON_Array *jscols = json_object_get_array(jsobj, "columns");

			stmt->action = metadata->action;

			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

			stmt->stmt.insert.new.count = 1;
			stmt->stmt.insert.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.insert.new.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			LogicalMessageTuple *tuple = &(stmt->stmt.insert.new.array[0]);

			if (!SetColumnNamesAndValues(tuple, message, jscols))
			{
				log_error("Failed to parse INSERT columns for logical "
						  "message %s",
						  message);
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		case STREAM_ACTION_UPDATE:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

			stmt->stmt.update.old.count = 1;
			stmt->stmt.update.new.count = 1;

			stmt->stmt.update.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			stmt->stmt.update.new.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.update.old.array == NULL ||
				stmt->stmt.update.new.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);
			JSON_Array *jsids = json_object_get_array(jsobj, "identity");

			if (!SetColumnNamesAndValues(old, message, jsids))
			{
				log_error("Failed to parse UPDATE identity (old) for logical "
						  "message %s",
						  message);
				return false;
			}

			LogicalMessageTuple *new = &(stmt->stmt.update.new.array[0]);
			JSON_Array *jscols = json_object_get_array(jsobj, "columns");

			if (!SetColumnNamesAndValues(new, message, jscols))
			{
				log_error("Failed to parse UPDATE columns (new) for logical "
						  "message %s",
						  message);
				return false;
			}

			(void) streamLogicalTransactionAppendStatement(txn, stmt);

			break;
		}

		case STREAM_ACTION_DELETE:
		{
			stmt->action = metadata->action;
			strlcpy(stmt->stmt.insert.nspname, schema, NAMEDATALEN);
			strlcpy(stmt->stmt.insert.relname, table, NAMEDATALEN);

			stmt->stmt.delete.old.count = 1;
			stmt->stmt.delete.old.array =
				(LogicalMessageTuple *) calloc(1, sizeof(LogicalMessageTuple));

			if (stmt->stmt.update.old.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			LogicalMessageTuple *old = &(stmt->stmt.update.old.array[0]);
			JSON_Array *jsids = json_object_get_array(jsobj, "identity");

			if (!SetColumnNamesAndValues(old, message, jsids))
			{
				log_error("Failed to parse DELETE identity (old) for logical "
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
						JSON_Array *jscols)
{
	int count = json_array_get_count(jscols);

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

	/*
	 * Now that our memory areas are allocated and initialized to zeroes, fill
	 * them in with the values from the JSON message.
	 */
	for (int i = 0; i < tuple->cols; i++)
	{
		LogicalMessageValue *valueColumn = &(values->array[i]);

		JSON_Object *jscol = json_array_get_object(jscols, i);
		const char *colname = json_object_get_string(jscol, "name");

		if (jscol == NULL || colname == NULL)
		{
			log_debug("cols[%d]: count = %d, jscols %p, "
					  "json_array_get_count(jscols) == %lld",
					  i,
					  count,
					  jscols,
					  (long long) json_array_get_count(jscols));

			log_error("Failed to parse JSON columns array");
			return false;
		}

		tuple->columns[i] = strndup(colname, NAMEDATALEN);

		if (tuple->columns[i] == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		JSON_Value *jsval = json_object_get_value(jscol, "value");

		switch (json_value_get_type(jsval))
		{
			case JSONNull:
			{
				/* default to TEXTOID to send NULLs over the wire */
				valueColumn->oid = TEXTOID;
				valueColumn->isNull = true;
				break;
			}

			case JSONBoolean:
			{
				bool x = json_value_get_boolean(jsval);

				valueColumn->oid = BOOLOID;
				valueColumn->val.boolean = x;
				valueColumn->isNull = false;
				break;
			}

			case JSONNumber:
			{
				double x = json_value_get_number(jsval);

				valueColumn->oid = FLOAT8OID;
				valueColumn->val.float8 = x;
				valueColumn->isNull = false;
				break;
			}

			case JSONString:
			{
				const char *x = json_value_get_string(jsval);

				valueColumn->oid = TEXTOID;
				valueColumn->val.str = strdup(x);
				valueColumn->isNull = false;
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
stream_write_transaction(FILE *out, LogicalTransaction *tx)
{
	fformat(out, "BEGIN; -- {\"xid\":%lld,\"lsn\":\"%X/%X\"}\n",
			(long long) tx->xid,
			LSN_FORMAT_ARGS(tx->beginLSN));

	LogicalTransactionStatement *currentStmt = tx->first;

	for (; currentStmt != NULL; currentStmt = currentStmt->next)
	{
		switch (currentStmt->action)
		{
			case STREAM_ACTION_INSERT:
			{
				if (!stream_write_insert(out, &(currentStmt->stmt.insert)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_UPDATE:
			{
				if (!stream_write_update(out, &(currentStmt->stmt.update)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_DELETE:
			{
				if (!stream_write_delete(out, &(currentStmt->stmt.delete)))
				{
					return false;
				}
				break;
			}

			case STREAM_ACTION_TRUNCATE:
			{
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

	fformat(out, "COMMIT; -- {\"xid\": %lld,\"lsn\":\"%X/%X\"}\n",
			(long long) tx->xid,
			LSN_FORMAT_ARGS(tx->commitLSN));

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
			fformat(out, "%s%s", c > 0 ? ", " : "", stmt->columns[c]);
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
	fformat(out, "TRUNCATE %s.%s\n", truncate->nspname, truncate->relname);

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
				fformat(out, "'%s'", value->val.str);
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
