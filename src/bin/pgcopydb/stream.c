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
#include "schema.h"
#include "signals.h"
#include "stream.h"
#include "string_utils.h"
#include "summary.h"


static bool updateStreamCounters(StreamContext *context,
								 LogicalMessageMetadata *metadata);

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
		.count = 4,
		.keywords = {
			"format-version",
			"include-xids",
			"include-lsn",
			"include-transaction"
		},
		.values = {
			"2",
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

		if (!parseLSN(latest->nextlsn, &(specs->startLSN)))
		{
			log_error("Failed to parse start LSN \"%s\" to resume "
					  "streaming from file \"%s\"",
					  latest->nextlsn,
					  latestStreamedContent.filename);
			return false;
		}

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

	if (!parseMessageMetadata(&metadata, context->buffer))
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

		return false;
	}

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

	log_debug("Received action %c for XID %u in LSN %s",
			  metadata.action,
			  metadata.xid,
			  metadata.lsn);

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
parseMessageMetadata(LogicalMessageMetadata *metadata, const char *buffer)
{
	JSON_Value *json = json_parse_string(buffer);
	JSON_Object *jsobj = json_value_get_object(json);

	if (json_type(json) != JSONObject)
	{
		log_error("Failed to parse JSON message: %s", buffer);
		json_value_free(json);
		return false;
	}

	/* action is one of "B", "C", "I", "U", "D", "T" */
	char *action = (char *) json_object_get_string(jsobj, "action");

	if (action == NULL || strlen(action) != 1)
	{
		log_error("Failed to parse action \"%s\" in JSON message: %s",
				  action ? "NULL" : action,
				  buffer);
		json_value_free(json);
		return false;
	}

	switch (action[0])
	{
		case 'B':
		{
			metadata->action = STREAM_ACTION_BEGIN;
			break;
		}

		case 'C':
		{
			metadata->action = STREAM_ACTION_COMMIT;
			break;
		}

		case 'I':
		{
			metadata->action = STREAM_ACTION_INSERT;
			break;
		}

		case 'U':
		{
			metadata->action = STREAM_ACTION_UPDATE;
			break;
		}

		case 'D':
		{
			metadata->action = STREAM_ACTION_DELETE;
			break;
		}

		case 'T':
		{
			metadata->action = STREAM_ACTION_TRUNCATE;
			break;
		}

		default:
		{
			log_error("Failed to parse JSON message action: \"%s\"", action);
			return false;
		}
	}

	double xid = json_object_get_number(jsobj, "xid");
	metadata->xid = (uint32_t) xid;

	char *lsn = (char *) json_object_get_string(jsobj, "lsn");

	if (lsn == NULL)
	{
		log_error("Failed to parse JSON message: \"%s\"", buffer);
		json_value_free(json);
		return false;
	}

	strlcpy(metadata->lsn, lsn, sizeof(metadata->lsn));

	char *nextlsn = (char *) json_object_get_string(jsobj, "nextlsn");

	if (nextlsn != NULL)
	{
		strlcpy(metadata->nextlsn, nextlsn, sizeof(metadata->nextlsn));
	}

	json_value_free(json);
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

		if (!parseMessageMetadata(metadata, message))
		{
			/* errors have already been logged */
			return false;
		}
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
