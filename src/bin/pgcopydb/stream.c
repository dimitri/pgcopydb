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
stream_init_specs(CopyDataSpec *copySpecs, StreamSpecs *specs, char *slotName)
{
	/* just copy into StreamSpecs what's been initialized in copySpecs */
	specs->cfPaths = copySpecs->cfPaths;
	specs->pgPaths = copySpecs->pgPaths;

	strlcpy(specs->source_pguri, copySpecs->source_pguri, MAXCONNINFO);
	strlcpy(specs->target_pguri, copySpecs->target_pguri, MAXCONNINFO);

	strlcpy(specs->slotName, slotName, sizeof(specs->slotName));

	specs->sourceSnapshot = copySpecs->sourceSnapshot;

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

	privateContext.cfPaths = &(specs->cfPaths);
	privateContext.startLSN = specs->startLSN;

	context.private = (void *) &(privateContext);

	if (!pgsql_init_stream(&stream,
						   specs->logrep_pguri,
						   specs->slotName,
						   specs->startLSN,
						   0))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_start_replication(&stream))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_stream_logical(&stream, &context))
	{
		/* errors have already been logged */
		return false;
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

	XLByteToSeg(context->cur_record_lsn, segno, context->WalSegSz);

	/* compute the WAL filename that would host the current LSN */
	XLogFileName(wal, context->timeline, segno, context->WalSegSz);

	sformat(walFileName, sizeof(walFileName), "%s/%s.json",
			privateContext->cfPaths->cdcdir,
			wal);

	if (strcmp(privateContext->walFileName, walFileName) != 0)
	{
		/* if we had a WAL file opened, close it now */
		if (!IS_EMPTY_STRING_BUFFER(privateContext->walFileName) &&
			privateContext->jsonFile != NULL)
		{
			log_info("Close %s", privateContext->walFileName);

			if (fclose(privateContext->jsonFile) != 0)
			{
				/* errors have already been logged */
				return false;
			}
		}

		log_info(" Open %s", walFileName);
		strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);

		/*
		 * TODO
		 *
		 * At the moment if the file already exists, we just continue writing
		 * to it, considering that we are receiving the next bits of
		 * information from the server. That might not be the case though, so
		 * we might want to check the LSN that we're going to write, and maybe
		 * skip it.
		 */
		privateContext->jsonFile =
			fopen_with_umask(walFileName, "ab", FOPEN_FLAGS_A, 0644);

		if (privateContext->jsonFile == NULL)
		{
			/* errors have already been logged */
			log_error("Failed to open file \"%s\": %m",
					  privateContext->walFileName);
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
		log_error("Failed to parse JSON message: \"%s\"", buffer);
		json_value_free(json);
		return false;
	}

	/* action is one of "B", "C", "I", "U", "D", "T" */
	char *action = (char *) json_object_get_string(jsobj, "action");

	if (action == NULL || strlen(action) != 1)
	{
		log_error("Failed to parse JSON message: \"%s\"", buffer);
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

	char *lsn = (char *) json_object_get_string(jsobj, "lsn");

	if (lsn == NULL)
	{
		log_error("Failed to parse JSON message: \"%s\"", buffer);
		json_value_free(json);
		return false;
	}

	strlcpy(metadata->lsn, lsn, sizeof(metadata->lsn));

	double xid = json_object_get_number(jsobj, "xid");
	metadata->xid = (uint32_t) xid;

	json_value_free(json);
	return true;
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
 * buildReplicationURI builds a connection string that includes replication=1
 * from the connection string that's passed as input.
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
