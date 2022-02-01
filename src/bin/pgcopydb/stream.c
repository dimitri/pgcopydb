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
 * startLogicalStreaming opens a replication connection to the given source
 * database and issues the START REPLICATION command there.
 */
bool
startLogicalStreaming(const char *pguri, const char *slotName, uint64_t startLSN)
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

	char replicationURI[MAXCONNINFO] = { 0 };

	if (!buildReplicationURI(pguri, replicationURI))
	{
		/* errors have already been logged */
		return false;
	}

	/* prepare the stream options */
	LogicalStreamClient stream = { 0 };

	stream.pluginOptions = options;
	stream.receiverFunction = &streamToFiles;

	StreamContext privateContext = { 0 };
	LogicalStreamContext context = { 0 };

	privateContext.startLSN = startLSN;

	context.private = (void *) &(privateContext);

	if (!pgsql_init_stream(&stream, replicationURI, slotName, startLSN, 0))
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
 * streamToFiles is a callback function for our LogicalStreamClient.
 *
 * This function is called for each message received in pgsql_stream_logical.
 * It records the logical message to file. The message is expected to be in
 * JSON format, from the wal2json logical decoder.
 */
bool
streamToFiles(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/* get the segment number from the current_record_lsn */
	XLogSegNo segno;
	char walFileName[MAXPGPATH] = { 0 };

	XLByteToSeg(context->cur_record_lsn, segno, context->WalSegSz);

	/* compute the WAL filename that would host the current LSN */
	XLogFileName(walFileName, context->timeline, segno, context->WalSegSz);

	if (strcmp(privateContext->walFileName, walFileName) != 0)
	{
		/* if we had a WAL file opened, close it now */
		if (!IS_EMPTY_STRING_BUFFER(privateContext->walFileName) &&
			privateContext->jsonFile != NULL)
		{
			log_info("Close %s", privateContext->walFileName);
		}

		log_info("Open %s", walFileName);
		strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);
	}

	LogicalMessageMetadata metadata = { 0 };

	if (!parseMessageMetadata(&metadata, context->buffer))
	{
		/* errors have already been logged */
		return false;
	}

	(void) updateStreamCounters(privateContext, &metadata);

	log_info("Received action %c for XID %u in LSN %s",
			 metadata.action,
			 metadata.xid,
			 metadata.lsn);

	if (privateContext->counters.insert > 10)
	{
		return false;
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
