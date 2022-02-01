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

	log_info("received: %s", context->buffer);

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
