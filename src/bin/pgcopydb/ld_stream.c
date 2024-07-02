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
#include "ld_store.h"
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
 * stream_init_specs initializes Change Data Capture streaming specifications
 * from a copyDBSpecs structure.
 */
bool
stream_init_specs(StreamSpecs *specs,
				  CDCPaths *paths,
				  ConnStrings *connStrings,
				  ReplicationSlot *slot,
				  char *origin,
				  uint64_t endpos,
				  LogicalStreamMode mode,
				  DatabaseCatalog *sourceDB,
				  DatabaseCatalog *replayDB,
				  bool stdin,
				  bool stdout,
				  bool logSQL)
{
	/* just copy into StreamSpecs what's been initialized in copySpecs */
	specs->mode = mode;
	specs->stdIn = stdin;
	specs->stdOut = stdout;
	specs->logSQL = logSQL;

	specs->paths = *paths;
	specs->endpos = endpos;

	/*
	 * Open the specified sourceDB catalog.
	 */
	specs->sourceDB = sourceDB;
	specs->replayDB = replayDB;

	if (!catalog_init(specs->sourceDB))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Copy the given ReplicationSlot: it comes from command line parsing, or
	 * from a previous command that created it and saved information to file.
	 * Such a sprevious command could be: pgcopydb snapshot --follow.
	 */
	specs->slot = *slot;

	switch (specs->slot.plugin)
	{
		case STREAM_PLUGIN_TEST_DECODING:
		{
			KeyVal options = {
				.count = 1,
				.keywords = {
					"include-xids"
				},
				.values = {
					"true"
				}
			};

			specs->pluginOptions = options;
			break;
		}

		case STREAM_PLUGIN_WAL2JSON:
		{
			KeyVal options = {
				/* we ignore the last keyword and value if the option is not set */
				.count = specs->slot.wal2jsonNumericAsString ? 7 : 6,
				.keywords = {
					"format-version",
					"include-xids",
					"include-schemas",
					"include-transaction",
					"include-types",
					"filter-tables",
					"numeric-data-types-as-string"
				},
				.values = {
					"2",
					"true",
					"true",
					"true",
					"true",
					"pgcopydb.*",
					"true"
				}
			};

			specs->pluginOptions = options;
			break;
		}

		default:
		{
			log_error("Unknown logical decoding output plugin \"%s\"",
					  OutputPluginToString(slot->plugin));
			return false;
		}
	}

	strlcpy(specs->origin, origin, sizeof(specs->origin));

	specs->connStrings = connStrings;

	if (!buildReplicationURI(specs->connStrings->source_pguri,
							 &(specs->connStrings->logrep_pguri)))
	{
		/* errors have already been logged */
		return false;
	}

	log_trace("stream_init_specs: %s(%d)",
			  OutputPluginToString(slot->plugin),
			  specs->pluginOptions.count);

	/*
	 * Now prepare for the follow mode sub-process management.
	 */
	bool replayMode = specs->mode == STREAM_MODE_REPLAY;

	FollowSubProcess prefetch = {
		.name = replayMode ? "receive" : "prefetch",
		.command = &follow_start_prefetch,
		.pid = -1
	};

	FollowSubProcess transform = {
		.name = "transform",
		.command = &follow_start_transform,
		.pid = -1
	};

	FollowSubProcess catchup = {
		.name = replayMode ? "replay" : "catchup",
		.command = &follow_start_catchup,
		.pid = -1
	};

	specs->prefetch = prefetch;
	specs->transform = transform;
	specs->catchup = catchup;

	switch (specs->mode)
	{
		/*
		 * Create the message queue needed to communicate JSON files to
		 * transform to SQL files on prefetch/catchup mode. See the supervisor
		 * process implemented in function followDB() for the clean-up code
		 * that unlinks the message queue.
		 */
		case STREAM_MODE_PREFETCH:
		case STREAM_MODE_CATCHUP:
		{
			if (!queue_create(&(specs->transformQueue), "transform"))
			{
				log_error("Failed to create the transform queue");
				return false;
			}
			break;
		}

		/*
		 * Create the unix pipes needed for inter-process communication (data
		 * flow) in replay mode. We override command line arguments for
		 * --to-stdout and --from-stdin when stream mode is set to
		 * STREAM_MODE_REPLAY.
		 */
		case STREAM_MODE_REPLAY:
		{
			specs->stdIn = true;
			specs->stdOut = true;
			break;
		}

		/* other stream modes don't need special treatment here */
		default:
		{
			/* pass */
			break;
		}
	}

	return true;
}


/*
 * stream_init_for_mode initializes StreamSpecs bits that relate to the
 * streaming mode choosen, allowing to switch back and forth between CATCHUP
 * and REPLAY modes.
 */
bool
stream_init_for_mode(StreamSpecs *specs, LogicalStreamMode mode)
{
	if (specs->mode == STREAM_MODE_CATCHUP && mode == STREAM_MODE_REPLAY)
	{
		specs->stdIn = true;
		specs->stdOut = true;
	}
	else if (specs->mode == STREAM_MODE_REPLAY && mode == STREAM_MODE_CATCHUP)
	{
		specs->stdIn = false;
		specs->stdOut = false;

		/* we keep the transform queue around */
	}
	else
	{
		log_error("BUG: stream_init_for_mode(%d, %d)", specs->mode, mode);
		return false;
	}

	/* the re-init for the new mode has been done now, register that */
	specs->mode = mode;

	return true;
}


/*
 * LogicalStreamModeToString returns a string representation for the mode.
 */
char *
LogicalStreamModeToString(LogicalStreamMode mode)
{
	switch (mode)
	{
		case STREAM_MODE_UNKNOW:
		{
			return "unknown stream mode";
		}

		case STREAM_MODE_RECEIVE:
		{
			return "receive";
		}

		case STREAM_MODE_PREFETCH:
		{
			return "prefetch";
		}

		case STREAM_MODE_CATCHUP:
		{
			return "catchup";
		}

		case STREAM_MODE_REPLAY:
		{
			return "replay";
		}

		default:
		{
			log_error("BUG: LogicalStreamModeToString(%d)", mode);
			return "unknown stream mode";
		}
	}

	/* keep compiler happy */
	return "unknown stream mode";
}


/*
 * stream_check_in_out checks that the stdIn and stdOut file descriptors are
 * still valid: EBADF could happen when a PIPE is Broken for lack of a
 * reader/writer process.
 */
bool
stream_check_in_out(StreamSpecs *specs)
{
	if (specs->stdIn)
	{
		char buf[0];

		if (read(fileno(specs->in), buf, 0) != 0)
		{
			log_error("Failed to read from input PIPE: %m");
			return false;
		}
	}

	if (specs->stdOut)
	{
		char buf[0];

		if (fwrite(buf, sizeof(char), 0, specs->in) != 0)
		{
			log_error("Failed to write to output PIPE: %m");
			return false;
		}

		if (fflush(specs->out) != 0)
		{
			log_error("Failed to flush output PIPE: %m");
			return false;
		}
	}

	return true;
}


/*
 * stream_init_context initializes a LogicalStreamContext.
 */
bool
stream_init_context(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	privateContext->endpos = specs->endpos;
	privateContext->startpos = specs->startpos;

	privateContext->mode = specs->mode;

	privateContext->transformQueue = &(specs->transformQueue);

	privateContext->paths = specs->paths;

	privateContext->connStrings = specs->connStrings;

	/*
	 * TODO: get rid of WalSegSz entirely. In the meantime, have it set to a
	 * fixed value as in the old Postgres versions.
	 */
	privateContext->WalSegSz = 16 * 1024 * 1024;

	/*
	 * When using PIPEs for inter-process communication, makes sure the PIPEs
	 * are ready for us to use and not broken, as in EBADF.
	 */
	privateContext->stdIn = specs->stdIn;
	privateContext->stdOut = specs->stdOut;

	privateContext->in = specs->in;
	privateContext->out = specs->out;

	if (!stream_check_in_out(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * When streaming is resumed, transactions are sent in full even if we wrote
	 * and flushed a transactions partially in previous command. This implies
	 * that, if the last message is B/I/U/D/T, the streaming resumes from the
	 * same transaction and there's a need to skip some messages.
	 *
	 * However, note that if the last message is COMMIT, the streaming will
	 * resume from the next transaction.
	 */
	privateContext->metadata.action = STREAM_ACTION_UNKNOWN;
	privateContext->previous.action = STREAM_ACTION_UNKNOWN;

	privateContext->lastWriteTime = 0;

	/*
	 * Initializing maxWrittenLSN as startpos at the beginning of migration or
	 * when resuming from interruption where it will be equal to
	 * consistent_point or LSN of last message in latest.json respectively.
	 *
	 * maxWrittenLSN helps in ensuring that we don't write to a previous JSON
	 * file during streaming. Even though we haven't written anything before the
	 * beginning of migration, initializing with startpos serves as sensible
	 * boundary. This is because apply process starts applying changes from the
	 * SQL file with name computed from startpos.
	 *
	 * This initialization is particularly useful during the beginning of
	 * migration, where some messages may have LSNs less than the
	 * consistent_point. These messages may be located in a previous WAL file
	 * compared to the startpos, and we ensure that we start writing to a file
	 * of startpos.
	 */
	privateContext->maxWrittenLSN = specs->startpos;

	/* transform needs some catalog lookups (pkey, type oid) */
	privateContext->sourceDB = specs->sourceDB;

	/* replayDB is needed too */
	privateContext->replayDB = specs->replayDB;

	return true;
}


/*
 * stream_init_timeline registers the timeline history information into our
 * SQLite catalogs, and opens (and initializes if needed) the current replayDB
 * SQLite file.
 */
bool
stream_init_timeline(StreamSpecs *specs, LogicalStreamClient *stream)
{
	DatabaseCatalog *sourceDB = specs->sourceDB;
	StreamContext *privateContext = &(specs->private);

	privateContext->timeline = stream->system.timeline;

	/* insert the timeline history into our catalogs */
	if (stream->system.timelines.count == 0)
	{
		if (!ld_store_insert_timeline_history(sourceDB,
											  1,
											  InvalidXLogRecPtr,
											  InvalidXLogRecPtr))
		{
			log_error("Failed to initialize source database timeline history, "
					  "see above for details");
			return false;
		}
	}
	else
	{
		for (int i = 0; i < stream->system.timelines.count; i++)
		{
			uint32_t tli = stream->system.timelines.history[i].tli;
			uint64_t startpos = stream->system.timelines.history[i].begin;
			uint64_t endpos = stream->system.timelines.history[i].end;

			if (!ld_store_insert_timeline_history(sourceDB,
												  tli,
												  startpos,
												  endpos))
			{
				log_error("Failed to initialize source database timeline history, "
						  "see above for details");
				return false;
			}
		}
	}

	/* now that we have the current timeline and startpos lsn */
	if (!ld_store_open_replaydb(specs))
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
	/* prepare the stream options */
	LogicalStreamClient stream = { 0 };

	stream.pluginOptions = specs->pluginOptions;
	stream.writeFunction = &streamWrite;
	stream.flushFunction = &streamFlush;
	stream.closeFunction = &streamClose;
	stream.feedbackFunction = &streamFeedback;
	stream.keepaliveFunction = &streamKeepalive;

	/*
	 * Read possibly already existing file to initialize the start LSN from a
	 * previous run of our command.
	 */
	if (!streamCheckResumePosition(specs))
	{
		/* errors have already been logged */
		return false;
	}

	LogicalStreamContext context = { 0 };

	if (!stream_init_context(specs))
	{
		/* errors have already been logged */
		return false;
	}

	StreamContext *privateContext = &(specs->private);
	context.private = (void *) privateContext;

	log_notice("Connecting to logical decoding replication stream");

	/*
	 * In case of being disconnected or other transient errors, reconnect and
	 * continue streaming.
	 */
	bool retry = true;
	uint64_t retries = 0;
	uint64_t waterMarkLSN = InvalidXLogRecPtr;

	while (retry)
	{
		if (!stream_check_in_out(specs))
		{
			/* errors have already been logged */
			return false;
		}

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("Streaming process has been signaled to stop");
			return false;
		}

		if (!pgsql_init_stream(&stream,
							   specs->connStrings->logrep_pguri,
							   specs->slot.plugin,
							   specs->slot.slotName,
							   specs->startpos,
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

		if (!stream_init_timeline(specs, &stream))
		{
			/* errors have already been logged */
			return false;
		}

		/* ignore errors, try again unless asked to stop */
		bool cleanExit = pgsql_stream_logical(&stream, &context);

		if ((cleanExit && context.endpos != InvalidXLogRecPtr) ||
			asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			retry = false;
		}

		if (cleanExit && context.endpos != InvalidXLogRecPtr)
		{
			log_info("Streamed up to write_lsn %X/%X, flush_lsn %X/%X, stopping: "
					 "endpos is %X/%X",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn),
					 LSN_FORMAT_ARGS(context.tracking->flushed_lsn),
					 LSN_FORMAT_ARGS(context.endpos));
		}
		else if (cleanExit && context.endpos == InvalidXLogRecPtr)
		{
			log_info("Streamed up to write_lsn %X/%X, flush_lsn %X/%X, "
					 "reconnecting in 1s ",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn),
					 LSN_FORMAT_ARGS(context.tracking->flushed_lsn));
		}
		else if (retries > 0 &&
				 context.tracking->written_lsn == waterMarkLSN)
		{
			log_warn("Streaming got interrupted at %X/%X, and did not make "
					 "any progress from previous attempt, stopping now",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn));

			return false;
		}
		else if (retry)
		{
			log_warn("Streaming got interrupted at %X/%X, reconnecting in 1s",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn));
		}
		else
		{
			log_warn("Streaming got interrupted at %X/%X",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn));
		}

		/* if we are going to retry, we need to rollback the last txn */
		context.onRetry = retry;

		/* sleep for one entire second before retrying */
		if (retry)
		{
			++retries;
			waterMarkLSN = context.tracking->written_lsn;

			(void) pg_usleep(1 * 1000 * 1000); /* 1s */
		}
	}

	return true;
}


/*
 * streamCheckResumePosition checks that the resume position on the replication
 * slot on the source database is in-sync with the lastest on-file LSN we have.
 */
bool
streamCheckResumePosition(StreamSpecs *specs)
{
	/*
	 * We might have specifications for when to start in the pgcopydb sentinel
	 * table. The sentinel only applies to STREAM_MODE_PREFETCH, in
	 * STREAM_MODE_RECEIVE we bypass that mechanism entirely.
	 *
	 * When STREAM_MODE_PREFETCH is set, it is expected that the pgcopydb
	 * sentinel table has been setup before starting the logical decoding
	 * client.
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

	if (specs->endpos == InvalidXLogRecPtr)
	{
		specs->endpos = sentinel.endpos;
	}
	else
	{
		if (sentinel.endpos != InvalidXLogRecPtr &&
			sentinel.endpos != specs->endpos)
		{
			log_warn("Sentinel endpos was %X/%X and is now updated to "
					 "--endpos option %X/%X",
					 LSN_FORMAT_ARGS(sentinel.endpos),
					 LSN_FORMAT_ARGS(specs->endpos));
		}

		if (!sentinel_update_endpos(specs->sourceDB, specs->endpos))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (specs->endpos != InvalidXLogRecPtr)
	{
		log_info("Streaming is setup to end at LSN %X/%X",
				 LSN_FORMAT_ARGS(specs->endpos));
	}

	if (sentinel.startpos != InvalidXLogRecPtr)
	{
		specs->startpos = sentinel.startpos;

		log_info("Resuming streaming at LSN %X/%X (sentinel startpos)",
				 LSN_FORMAT_ARGS(specs->startpos));
	}

	PGSQL src = { 0 };

	if (!pgsql_init(&src, specs->connStrings->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	bool flush = false;
	uint64_t lsn = 0;

	if (!pgsql_replication_slot_exists(&src, specs->slot.slotName, &flush, &lsn))
	{
		/* errors have already been logged */
		return false;
	}

	int logLevel = lsn == specs->startpos ? LOG_NOTICE : LOG_INFO;

	log_level(logLevel,
			  "Replication slot \"%s\" current lsn is %X/%X",
			  specs->slot.slotName,
			  LSN_FORMAT_ARGS(lsn));

	/*
	 * The receive process knows how to skip over LSNs that have already been
	 * fetched in a previous run. What we are not able to do is fill-in a gap
	 * between what we have on-disk and what the replication slot can send us.
	 */
	if (specs->startpos < lsn)
	{
		log_error("Failed to resume replication: sentinel.startpos is %X/%X "
				  "and replication slot LSN is %X/%X",
				  LSN_FORMAT_ARGS(specs->startpos),
				  LSN_FORMAT_ARGS(lsn));

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
	LogicalMessageMetadata *metadata = &(privateContext->metadata);
	DatabaseCatalog *replayDB = privateContext->replayDB;

	if (!prepareMessageMetadataFromContext(context))
	{
		log_error("Failed to prepare Logical Message Metadata from context, "
				  "see above for details");
		return false;
	}

	if (metadata->filterOut)
	{
		/* message has already been logged */
		return true;
	}

	/* update the LSN tracking that's reported in the feedback */
	context->tracking->written_lsn = context->cur_record_lsn;

	/* write the actual JSON message to file, unless instructed not to */
	if (!metadata->skipping)
	{
		if (context->onRetry)
		{
			/*
			 * When retrying due to a transient network error or server conn
			 * failure, we need to rollback the last incomplete transaction.
			 *
			 * Otherwise, we would end up with a partial transaction in the
			 * JSON file, and the transform process would fail to process it.
			 */
			if (privateContext->transactionInProgress)
			{
				InternalMessage rollback = {
					.action = STREAM_ACTION_ROLLBACK,
					.lsn = context->cur_record_lsn
				};

				if (!ld_store_insert_internal_message(replayDB, &rollback))
				{
					/* errors have already been logged */
					return false;
				}
			}

			context->onRetry = false;
		}

		/* insert the message to our current SQLite logical decoding file */
		if (!ld_store_insert_message(replayDB, metadata))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (metadata->xid > 0)
	{
		log_debug("Received action %c for XID %u at LSN %X/%X",
				  metadata->action,
				  metadata->xid,
				  LSN_FORMAT_ARGS(metadata->lsn));
	}
	else
	{
		log_debug("Received action %c at LSN %X/%X",
				  metadata->action,
				  LSN_FORMAT_ARGS(metadata->lsn));
	}

	/*
	 * Maintain the transaction progress based on the BEGIN and COMMIT messages
	 * received from replication slot.
	 */
	if (metadata->action == STREAM_ACTION_BEGIN)
	{
		privateContext->transactionInProgress = true;
	}
	else if (metadata->action == STREAM_ACTION_COMMIT)
	{
		privateContext->transactionInProgress = false;
	}

	/*
	 * We are not expecting STREAM_ACTION_ROLLBACK here. It's a custom
	 * message we write directly to the "latest" file using
	 * stream_write_internal_message to abort the last incomplete transaction.
	 */
	else if (metadata->action == STREAM_ACTION_ROLLBACK)
	{
		log_error("BUG: STREAM_ACTION_ROLLBACK is not expected here");
		return false;
	}

	return true;
}


/*
 * streamRotate decides if the received message should be appended to the
 * already opened file or to a new file, and then opens that file and takes
 * care of preparing the new file descriptor.
 *
 * A "latest" symbolic link is also maintained.
 */
bool
streamRotateFile(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	DatabaseCatalog *replayDB = privateContext->replayDB;

	/* get the segment number from the current_record_lsn */
	XLogSegNo segno;
	char wal[MAXPGPATH] = { 0 };
	char walFileName[MAXPGPATH] = { 0 };
	char partialFileName[MAXPGPATH] = { 0 };

	/* skip LSN 0/0 at the start of streaming */
	if (context->cur_record_lsn == InvalidXLogRecPtr)
	{
		return true;
	}

	/*
	 * Determine the LSN to calculate walFileName in which to write the current
	 * message.
	 *
	 * This walFileName calculation later ensures safe transaction formation in
	 * the transform/apply process by always appending messages here to the
	 * latest file and preventing rotation to earlier files.
	 *
	 * In most cases, jsonFileLSN should be the same as cur_record_lsn. However,
	 * occasionally, current messages may have LSNs lower than the previous
	 * ones. This can occur due to concurrent transactions with interleaved
	 * LSNs. Since the logical decoding protocol sends the complete transaction
	 * at commit time, the LSNs for messages within one transaction could be
	 * lower than those of the previously streamed transactions. In such cases,
	 * we use maximum LSN of the messages written so far to the disk in order to
	 * write to the current file.
	 *
	 * Here is a oversimplified visualization of three concurrent transactions.
	 * In this scenario, we receive complete transactions in the order txn-1 ->
	 * txn-3 -> txn-2, based on their COMMIT order. When we start with
	 * maxWrittenLSN as LSN AB..00, the first message of txn-1 (A9..01) and the
	 * remaining messages for this transaction will be written to AB.json file.
	 * As we continue, the maxWrittenLSN becomes AB..01, so the next transaction
	 * (txn-3) has its first message with LSN AA..02, which is less than
	 * maxWrittenLSN, so we continue writing to AB..01. This process continues
	 * for txn-2 and subsequent txns.
	 *
	 *      +----------+----------+----------+
	 *      |  txn-1   |  txn-2   | txn-3    |
	 *   |  +--------------------------------+
	 *   |  | B A9..01 |          |          |
	 *   |  |          | B A9..02 |          |
	 *   |  |          |          |          |
	 *   |  | ---SWITCH WAL from A9 to AA--- |
	 *   |  |          |          |          |
	 *   |  | I AA..01 |          |          |
	 *   |  |          |          | B AA..02 |
	 *   |  |          |          | I AA..03 |
	 * TIME |          | I AA..04 |          |
	 *   |  |          |          +          |
	 *   |  | ---SWITCH WAL from AA to AB--- |
	 *   |  |          |          |          |
	 *   |  | I AB..00 |          |          |
	 *   |  | C AB..01 |          |          |
	 *   v  |          |          | C AB..02 |
	 *      |          | I AB..03 |          |
	 *      |          | C AB..04 |          |
	 *      +----------+----------+----------+
	 */
	uint64_t jsonFileLSN;

	/*
	 * jsonFileLSN is greater of the max LSN of messages written so far and the
	 * current record.
	 */
	if (privateContext->maxWrittenLSN != InvalidXLogRecPtr)
	{
		/* cur_record_lsn leads to current file, skipping rotation, or to a new file */
		if (privateContext->maxWrittenLSN <= context->cur_record_lsn)
		{
			jsonFileLSN = context->cur_record_lsn;
		}

		/* maxWrittenLSN always points to the current file and skips rotation */
		else
		{
			jsonFileLSN = privateContext->maxWrittenLSN;
		}
	}
	else
	{
		jsonFileLSN = context->cur_record_lsn;
	}

	/* compute the WAL filename that would host the current message */
	XLByteToSeg(jsonFileLSN, segno, privateContext->WalSegSz);
	XLogFileName(wal, context->timeline, segno, privateContext->WalSegSz);

	sformat(walFileName, sizeof(walFileName), "%s/%s.json",
			privateContext->paths.dir,
			wal);

	sformat(partialFileName, sizeof(partialFileName), "%s/%s.json.partial",
			privateContext->paths.dir,
			wal);

	/* in most cases, the file name is still the same */
	if (streq(privateContext->walFileName, walFileName))
	{
		return true;
	}

	/* if we had a WAL file opened, close it now */
	if (!IS_EMPTY_STRING_BUFFER(privateContext->partialFileName) &&
		privateContext->jsonFile != NULL)
	{
		bool time_to_abort = false;

		InternalMessage switchwal = {
			.action = STREAM_ACTION_SWITCH,
			.lsn = jsonFileLSN
		};

		if (!ld_store_insert_internal_message(replayDB, &switchwal))
		{
			/* errors have already been logged */
			return false;
		}

		if (!streamCloseFile(context, time_to_abort))
		{
			/* errors have already been logged */
			return false;
		}
	}

	strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);
	strlcpy(privateContext->partialFileName, partialFileName, MAXPGPATH);

	/* when dealing with a new JSON name, also prepare the SQL name */
	sformat(privateContext->sqlFileName, sizeof(privateContext->sqlFileName),
			"%s/%s.sql",
			privateContext->paths.dir,
			wal);

	/* the jsonFileLSN is the firstLSN for this file */
	privateContext->firstLSN = jsonFileLSN;

	/*
	 * When the target file already exists, open it in append mode.
	 */
	if (file_exists(walFileName))
	{
		if (!unlink_file(partialFileName))
		{
			log_error("Failed to unlink stale partial file \"%s\", "
					  "see above for details",
					  partialFileName);
			return false;
		}

		if (!duplicate_file(walFileName, partialFileName))
		{
			log_error("Failed to duplicate pre-existing file \"%s\" into "
					  "current partial file \"%s\", see above for details",
					  walFileName,
					  partialFileName);
			return false;
		}

		privateContext->jsonFile =
			fopen_with_umask(partialFileName, "ab", FOPEN_FLAGS_A, 0644);
	}
	else if (file_exists(partialFileName))
	{
		/* previous run might have been interrupted before rename */
		log_notice("Found pre-existing partial file \"%s\"", partialFileName);

		privateContext->jsonFile =
			fopen_with_umask(partialFileName, "ab", FOPEN_FLAGS_A, 0644);
	}
	else
	{
		privateContext->jsonFile =
			fopen_with_umask(partialFileName, "ab", FOPEN_FLAGS_W, 0644);
	}

	if (privateContext->jsonFile == NULL)
	{
		/* errors have already been logged */
		log_error("Failed to open file \"%s\": %m",
				  privateContext->partialFileName);
		return false;
	}

	log_notice("Now streaming changes to \"%s\"", partialFileName);

	return true;
}


/*
 * streamCloseFile closes the current file where the stream messages are
 * written to. It's called from either streamWrite or streamClose logical
 * stream client callback functions.
 */
bool
streamCloseFile(LogicalStreamContext *context, bool time_to_abort)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	DatabaseCatalog *replayDB = privateContext->replayDB;

	/*
	 * Before closing the JSON file, when we have reached endpos add a pgcopydb
	 * 'E' message to signal transform and replay processes to skip replaying
	 * the possibly opened transaction for now.
	 *
	 * Note that as the user can edit the endpos and restart pgcopydb, we neex
	 * to be able to stop replay because of endpos and still skip replaying a
	 * partial transaction.
	 */
	if (time_to_abort &&
		privateContext->jsonFile != NULL &&
		privateContext->endpos != InvalidXLogRecPtr &&
		privateContext->endpos <= context->cur_record_lsn)
	{
		InternalMessage endpos = {
			.action = STREAM_ACTION_ENDPOS,
			.lsn = context->cur_record_lsn
		};

		log_warn("streamCloseFile: insert ENDPOS at %X/%X",
				 LSN_FORMAT_ARGS(endpos.lsn));

		if (!ld_store_insert_internal_message(replayDB, &endpos))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * On graceful exit, ROLLBACK the last incomplete transaction.
	 * As we resume from a consistent point, there's no concern about
	 * the transaction being rolled back here.
	 *
	 * TODO: For process crashes (e.g., segmentation faults), this
	 * method won't work, potentially leaving incomplete transactions.
	 * To handle this, we should read the last message from the "latest"
	 * file and rollback any incomplete transaction found.
	 */
	if (time_to_abort &&
		privateContext->jsonFile != NULL &&
		privateContext->transactionInProgress)
	{
		InternalMessage rollback = {
			.action = STREAM_ACTION_ROLLBACK,
			.lsn = context->cur_record_lsn
		};

		if (!ld_store_insert_internal_message(replayDB, &rollback))
		{
			/* errors have already been logged */
			return false;
		}
	}


	/*
	 * If we have a JSON file currently opened, then close it.
	 *
	 * Some situations exist where there is no JSON file currently opened and
	 * we still want to transform the latest JSON file into SQL: we might reach
	 * endpos at startup, for instance.
	 */
	if (privateContext->jsonFile != NULL)
	{
		log_debug("Closing file \"%s\"", privateContext->partialFileName);

		if (fclose(privateContext->jsonFile) != 0)
		{
			log_error("Failed to close file \"%s\": %m",
					  privateContext->partialFileName);
			return false;
		}

		/* reset the jsonFile FILE * pointer to NULL, it's closed now */
		privateContext->jsonFile = NULL;

		/* rename the .json.partial file to .json only */
		log_debug("streamCloseFile: mv \"%s\" \"%s\"",
				  privateContext->partialFileName,
				  privateContext->walFileName);

		if (rename(privateContext->partialFileName,
				   privateContext->walFileName) != 0)
		{
			log_error("Failed to rename \"%s\" to \"%s\": %m",
					  privateContext->partialFileName,
					  privateContext->walFileName);
			return false;
		}

		log_notice("Closed file \"%s\"", privateContext->walFileName);
	}

	/* in prefetch mode, kick-in a transform process */
	switch (privateContext->mode)
	{
		case STREAM_MODE_RECEIVE:
		{
			/* nothing else to do in that streaming mode */
			break;
		}

		case STREAM_MODE_PREFETCH:
		case STREAM_MODE_CATCHUP:
		{
			/*
			 * Now is the time to transform the JSON file into SQL.
			 */
			if (privateContext->firstLSN != InvalidXLogRecPtr)
			{
				if (!stream_transform_add_file(privateContext->transformQueue,
											   privateContext->firstLSN))
				{
					log_error("Failed to add LSN %X/%X to the transform queue",
							  LSN_FORMAT_ARGS(privateContext->firstLSN));
					return false;
				}
			}

			/*
			 * While streaming logical decoding JSON messages, the transforming
			 * of the previous JSON file happens in parallel to the receiving
			 * of the current one.
			 *
			 * When it's time_to_abort, we need to make sure the current file
			 * has been transformed before exiting.
			 */
			if (time_to_abort)
			{
				if (!stream_transform_send_stop(privateContext->transformQueue))
				{
					log_error("Failed to send STOP to the transform queue");
					return false;
				}
			}

			break;
		}

		case STREAM_MODE_REPLAY:
		{
			/* nothing else to do in that streaming mode */
			break;
		}

		default:
		{
			log_error("BUG: unknown LogicalStreamMode %d", privateContext->mode);
			return false;
		}
	}

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

	log_debug("streamFlush: %X/%X %X/%X",
			  LSN_FORMAT_ARGS(context->tracking->written_lsn),
			  LSN_FORMAT_ARGS(context->cur_record_lsn));

	/* if needed, flush our current file now (fsync) */
	if (context->tracking->flushed_lsn < context->tracking->written_lsn)
	{
		/*
		 * When it's time to flush, inject a KEEPALIVE message to make sure we
		 * mark the progress made in terms of LSN. Since we skip empty
		 * transactions, we might be missing the last progress at endpos time
		 * without this.
		 */
		if (!streamKeepalive(context))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * streamKeepalive ensures we have a valid jsonFile by calling
		 * streamRotateFile, so we can safely call fsync here.
		 */
		int fd = fileno(privateContext->jsonFile);

		if (fsync(fd) != 0)
		{
			log_error("Failed to fsync file \"%s\": %m",
					  privateContext->partialFileName);
			return false;
		}

		context->tracking->flushed_lsn = context->tracking->written_lsn;

		log_debug("Flushed up to %X/%X in file \"%s\"",
				  LSN_FORMAT_ARGS(context->tracking->flushed_lsn),
				  privateContext->partialFileName);
	}

	/* at flush time also update our internal sentinel tracking */
	if (!stream_sync_sentinel(context))
	{
		/* errors have already been logged */
		return false;
	}
	return true;
}


/*
 * streamKeepalive is a callback function for our LogicalStreamClient.
 *
 * This function is called when receiving a logical decoding keepalive packet.
 */
bool
streamKeepalive(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	DatabaseCatalog *replayDB = privateContext->replayDB;

	/* skip LSN 0/0 at the start of streaming */
	if (context->cur_record_lsn == InvalidXLogRecPtr)
	{
		return true;
	}

	/* we might have to rotate to the next on-disk file */
	if (!streamRotateFile(context))
	{
		/* errors have already been logged */
		return false;
	}

	/* register progress made through receiving keepalive messages */
	if (privateContext->jsonFile != NULL)
	{
		InternalMessage keepalive = {
			.action = STREAM_ACTION_KEEPALIVE,
			.lsn = context->cur_record_lsn,
			.time = context->sendTime
		};

		if (!ld_store_insert_internal_message(replayDB, &keepalive))
		{
			/* errors have already been logged */
			return false;
		}

		log_trace("Inserted action KEEPALIVE for lsn %X/%X @%s",
				  LSN_FORMAT_ARGS(keepalive.lsn),
				  keepalive.timeStr);

		/* update the LSN tracking that's reported in the feedback */
		context->tracking->written_lsn = context->cur_record_lsn;

		/* time to update our lastWriteTime mark */
		privateContext->lastWriteTime = time(NULL);

		/* update the tracking for maximum LSN of messages written to disk so far */
		if (privateContext->maxWrittenLSN < context->cur_record_lsn)
		{
			privateContext->maxWrittenLSN = context->cur_record_lsn;
		}
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
	if (!streamFlush(context))
	{
		/* errors have already been logged */
		return false;
	}

	bool time_to_abort = true;

	if (!streamCloseFile(context, time_to_abort))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * streamFeedback is a callback function for our LogicalStreamClient.
 *
 * This function is called when it's time to send feedback to the source
 * Postgres instance, include write_lsn, flush_lsn, and replay_lsn. Once in a
 * while we fetch the replay_lsn from the pgcopydb sentinel table and sync with
 * the current progress.
 */
bool
streamFeedback(LogicalStreamContext *context)
{
	int feedbackInterval = 1 * 1000; /* 1s */

	if (!context->forceFeedback)
	{
		if (!feTimestampDifferenceExceeds(context->lastFeedbackSync,
										  context->now,
										  feedbackInterval))
		{
			return true;
		}
	}

	if (!stream_sync_sentinel(context))
	{
		/* errors have already been logged */
		return false;
	}

	/* mark that we just did a feedback sync */
	context->lastFeedbackSync = context->now;

	return true;
}


/*
 * stream_sync_sentinel syncs the sentinel values in our internal catalogs with
 * the current streaming protocol values.
 */
bool
stream_sync_sentinel(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_sync_recv(privateContext->sourceDB,
							context->tracking->written_lsn,
							context->tracking->flushed_lsn,
							&sentinel))
	{
		log_error("Failed to update sentinel at stream flush time, "
				  "see above for details");
		return false;
	}

	/*
	 * Update the main LogicalStreamClient parts, API with the lower-level
	 * logical decoding client.
	 */
	privateContext->apply = sentinel.apply;
	privateContext->endpos = sentinel.endpos;
	privateContext->startpos = sentinel.startpos;

	context->endpos = sentinel.endpos;
	context->tracking->applied_lsn = sentinel.replay_lsn;

	log_debug("stream_sync_sentinel: "
			  "write_lsn %X/%X flush_lsn %X/%X apply_lsn %X/%X "
			  "startpos %X/%X endpos %X/%X apply %s",
			  LSN_FORMAT_ARGS(context->tracking->written_lsn),
			  LSN_FORMAT_ARGS(context->tracking->flushed_lsn),
			  LSN_FORMAT_ARGS(context->tracking->applied_lsn),
			  LSN_FORMAT_ARGS(privateContext->startpos),
			  LSN_FORMAT_ARGS(privateContext->endpos),
			  privateContext->apply ? "enabled" : "disabled");

	return true;
}


/*
 * prepareMessageMetadataFromContext prepares the Logical Message Metadata from
 * the fields grabbbed in the logical streaming protocol.
 *
 * See XLogData (B) protocol message description at:
 *
 * https://www.postgresql.org/docs/current/protocol-replication.html
 */
bool
prepareMessageMetadataFromContext(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	LogicalMessageMetadata *metadata = &(privateContext->metadata);
	LogicalMessageMetadata *previous = &(privateContext->previous);

	/* ensure we have a new all-zero metadata structure for the new message */
	(void) memset(metadata, 0, sizeof(LogicalMessageMetadata));

	/* add the server start LSN to the LogicalMessageMetadata */
	metadata->lsn = context->cur_record_lsn;

	/* add the server sendTime to the LogicalMessageMetadata */
	if (!pgsql_timestamptz_to_string(context->sendTime,
									 metadata->timestamp,
									 sizeof(metadata->timestamp)))
	{
		log_error("Failed to format server send time %lld to time string",
				  (long long) context->sendTime);
		return false;
	}

	/* now parse metadata found in the output_plugin data buffer itself */
	if (!parseMessageActionAndXid(context))
	{
		log_error("Failed to parse header from logical decoding message: %s",
				  context->buffer);
		return false;
	}

	/* in case of filtering, early exit */
	if (metadata->filterOut)
	{
		return true;
	}

	if (!prepareMessageJSONbuffer(context))
	{
		log_error("Failed to prepare a JSON buffer from "
				  "logical decoding context buffer: %s, "
				  "see above for details",
				  context->buffer);
		return false;
	}

	/*
	 * Skip empty transactions, except every once in a while in order to
	 * continue tracking LSN progress in our replay system.
	 */
	uint64_t now = time(NULL);
	uint64_t elapsed = now - privateContext->lastWriteTime;

	metadata->recvTime = now;

	/* BEGIN message: always wait to see if next message is a COMMIT */
	if (metadata->action == STREAM_ACTION_BEGIN)
	{
		metadata->skipping = true;
	}

	/* COMMIT message and previous one is a BEGIN */
	else if (previous->action == STREAM_ACTION_BEGIN &&
			 metadata->action == STREAM_ACTION_COMMIT)
	{
		metadata->skipping = true;

		/* add a synthetic KEEPALIVE message once in a while */
		if (STREAM_EMPTY_TX_TIMEOUT <= elapsed)
		{
			if (!streamKeepalive(context))
			{
				/* errors have already been logged */
				return false;
			}
		}
	}

	/*
	 * NOT a COMMIT message and previous one is a BEGIN
	 *
	 * It probably means the transaction is an INSERT/UPDATE/DELETE/TRUNCATE or
	 * maybe even a SWITCH or something. In any case we want to now write the
	 * previous BEGIN message out in the JSON stream.
	 */
	else if (previous->action == STREAM_ACTION_BEGIN &&
			 metadata->action != STREAM_ACTION_COMMIT)
	{
		previous->skipping = false;
		metadata->skipping = false;

		/* insert the message to our current SQLite logical decoding file */
		if (!ld_store_insert_message(privateContext->replayDB, previous))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * Any other case: current message is not a BEGIN, previous message is not
	 * a BEGIN either.
	 *
	 * We don't need to keep track of the previous message anymore, and we need
	 * to prepare for the next iteration by copying the current message
	 * wholesale into the previous location.
	 */
	*previous = *metadata;

	return true;
}


/*
 * parseMessageXid retrieves the XID from the logical replication message found
 * in the buffer. It might be a buffer formatted by any supported output
 * plugin, at the moment either wal2json or test_decoding.
 *
 * Not all messages are supposed to have the XID information.
 */
bool
parseMessageActionAndXid(LogicalStreamContext *context)
{
	switch (context->plugin)
	{
		case STREAM_PLUGIN_TEST_DECODING:
		{
			return parseTestDecodingMessageActionAndXid(context);
		}

		case STREAM_PLUGIN_WAL2JSON:
		{
			return parseWal2jsonMessageActionAndXid(context);
		}

		default:
		{
			log_error("BUG in parseMessageActionAndXid: unknown plugin %d",
					  context->plugin);
			return false;
		}
	}

	return true;
}


/*
 * prepareMessageJSONbuffer prepares a buffer in the JSON format from the raw
 * message sent by the logical decoding buffer.
 */
bool
prepareMessageJSONbuffer(LogicalStreamContext *context)
{
	switch (context->plugin)
	{
		case STREAM_PLUGIN_TEST_DECODING:
		{
			return prepareTestDecodingMessage(context);
		}

		case STREAM_PLUGIN_WAL2JSON:
		{
			return prepareWal2jsonMessage(context);
		}

		default:
		{
			log_error("BUG in prepareMessageJSONbuffer: unknown plugin %d",
					  context->plugin);
			return NULL;
		}
	}

	/* keep compiler happy */
	return NULL;
}


/*
 * parseMessageMetadata parses just the metadata of the JSON replication
 * message we got from wal2json.
 */
bool
parseMessageMetadata(LogicalMessageMetadata *metadata,
					 const char *buffer,
					 JSON_Value *json,
					 bool skipAction)
{
	JSON_Object *jsobj = json_value_get_object(json);

	if (json_type(json) != JSONObject)
	{
		log_error("Failed to parse JSON message: %s", buffer);
		return false;
	}

	if (!skipAction)
	{
		/* action is one of "B", "C", "I", "U", "D", "T", "X" */
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

		/* message entries {action: "M"} do not have xid, lsn fields */
		if (metadata->action == STREAM_ACTION_MESSAGE)
		{
			log_debug("Skipping message: %s", buffer);
			return true;
		}
	}

	if (json_object_has_value_of_type(jsobj, "xid", JSONString))
	{
		const char *xid = json_object_get_string(jsobj, "xid");

		if (!stringToUInt32(xid, &(metadata->xid)))
		{
			log_error("Failed to parse XID \"%s\" in message: %s", xid, buffer);
			return false;
		}
	}
	else if (json_object_has_value_of_type(jsobj, "xid", JSONNumber))
	{
		double xid = json_object_get_number(jsobj, "xid");
		metadata->xid = (uint32_t) xid;
	}
	else
	{
		if (!skipAction &&
			(metadata->action == STREAM_ACTION_BEGIN ||
			 metadata->action == STREAM_ACTION_COMMIT))
		{
			log_error("Failed to parse XID for action %c in JSON message: %s",
					  metadata->action,
					  buffer);
			return false;
		}
	}

	if (json_object_has_value(jsobj, "lsn"))
	{
		char *lsn = (char *) json_object_get_string(jsobj, "lsn");

		if (lsn != NULL)
		{
			if (!parseLSN(lsn, &(metadata->lsn)))
			{
				log_error("Failed to parse LSN \"%s\"", lsn);
				return false;
			}
		}
	}

	if (json_object_has_value(jsobj, "commit_lsn"))
	{
		char *txnCommitLSN = (char *) json_object_get_string(jsobj, "commit_lsn");

		if (txnCommitLSN != NULL)
		{
			if (!parseLSN(txnCommitLSN, &(metadata->txnCommitLSN)))
			{
				log_error("Failed to parse LSN \"%s\"", txnCommitLSN);
				return false;
			}
		}
	}

	if (!skipAction &&
		metadata->lsn == InvalidXLogRecPtr &&
		(metadata->action == STREAM_ACTION_BEGIN ||
		 metadata->action == STREAM_ACTION_COMMIT))
	{
		log_error("Failed to parse LSN for action %c in message: %s",
				  metadata->action,
				  buffer);
		return false;
	}

	if (json_object_has_value(jsobj, "timestamp"))
	{
		char *timestamp = (char *) json_object_get_string(jsobj, "timestamp");

		if (timestamp != NULL)
		{
			size_t n = sizeof(metadata->timestamp);

			if (strlcpy(metadata->timestamp, timestamp, n) >= n)
			{
				log_error("Failed to parse JSON message timestamp value \"%s\" "
						  "which is %zu bytes long, "
						  "pgcopydb only support timestamps up to %zu bytes",
						  timestamp,
						  strlen(timestamp),
						  sizeof(metadata->timestamp));
				return false;
			}
		}
	}

	return true;
}


/*
 * buildReplicationURI builds a connection string that includes
 * replication=database from the connection string that's passed as input.
 */
bool
buildReplicationURI(const char *pguri, char **repl_pguri)
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
								   &connStringDefaults,
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

		case 'X':
		{
			return STREAM_ACTION_SWITCH;
		}

		case 'K':
		{
			return STREAM_ACTION_KEEPALIVE;
		}

		case 'E':
		{
			return STREAM_ACTION_ENDPOS;
		}

		case 'R':
		{
			return STREAM_ACTION_ROLLBACK;
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
 * StreamActionToString returns a text representation of the action.
 */
char *
StreamActionToString(StreamAction action)
{
	switch (action)
	{
		case STREAM_ACTION_UNKNOWN:
		{
			return "unknown";
		}

		case STREAM_ACTION_BEGIN:
		{
			return "BEGIN";
		}

		case STREAM_ACTION_COMMIT:
		{
			return "COMMIT";
		}

		case STREAM_ACTION_INSERT:
		{
			return "INSERT";
		}

		case STREAM_ACTION_UPDATE:
		{
			return "UPDATE";
		}

		case STREAM_ACTION_DELETE:
		{
			return "DELETE";
		}

		case STREAM_ACTION_TRUNCATE:
		{
			return "TRUNCATE";
		}

		case STREAM_ACTION_MESSAGE:
		{
			return "MESSAGE";
		}

		case STREAM_ACTION_SWITCH:
		{
			return "SWITCH";
		}

		case STREAM_ACTION_KEEPALIVE:
		{
			return "KEEPALIVE";
		}

		case STREAM_ACTION_ENDPOS:
		{
			return "ENDPOS";
		}

		case STREAM_ACTION_ROLLBACK:
		{
			return "ROLLBACK";
		}

		default:
		{
			log_error("Failed to parse message action: \"%c\"", action);
			return "unknown";
		}
	}

	/* keep compiler happy */
	return "unknown";
}


/*
 * StreamActionIsTCL returns true if the action is part of the Transaction
 * Control Language.
 */
bool
StreamActionIsTCL(StreamAction action)
{
	switch (action)
	{
		case STREAM_ACTION_BEGIN:
		case STREAM_ACTION_COMMIT:
		case STREAM_ACTION_ROLLBACK:
		{
			return true;
		}

		default:
			return false;
	}

	/* keep compiler happy */
	return false;
}


/*
 * StreamActionIsDML returns true if the action is a DML.
 */
bool
StreamActionIsDML(StreamAction action)
{
	switch (action)
	{
		case STREAM_ACTION_INSERT:
		case STREAM_ACTION_UPDATE:
		case STREAM_ACTION_DELETE:
		case STREAM_ACTION_TRUNCATE:
		{
			return true;
		}

		default:
			return false;
	}

	/* keep compiler happy */
	return false;
}


/*
 * StreamActionIsInternal returns true if the action is internal.
 */
bool
StreamActionIsInternal(StreamAction action)
{
	switch (action)
	{
		case STREAM_ACTION_ENDPOS:
		case STREAM_ACTION_KEEPALIVE:
		case STREAM_ACTION_SWITCH:
		{
			return true;
		}

		default:
			return false;
	}

	/* keep compiler happy */
	return false;
}


/*
 * stream_setup_source_database sets up the source database with a sentinel
 * table, and the target database with a replication origin.
 */
bool
stream_setup_databases(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	ReplicationSlot *slot = &(streamSpecs->slot);

	if (!stream_create_sentinel(copySpecs, slot->lsn, InvalidXLogRecPtr))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_create_origin(copySpecs, streamSpecs->origin, slot->lsn))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * stream_cleanup_source_database cleans up the source database and the target
 * database.
 */
bool
stream_cleanup_databases(CopyDataSpec *copySpecs, char *slotName, char *origin)
{
	PGSQL src = { 0 };
	PGSQL dst = { 0 };

	/*
	 * Cleanup the source database (replication slot, pgcopydb sentinel).
	 */
	if (!pgsql_init(&src, copySpecs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&src))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_drop_replication_slot(&src, slotName))
	{
		log_error("Failed to drop replication slot \"%s\"", slotName);
		return false;
	}

	log_info("Removing schema pgcopydb and its objects");

	if (!pgsql_execute(&src, "drop schema if exists pgcopydb cascade"))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_commit(&src))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * When we have dropped the replication slot, we can remove the slot file
	 * on-disk and also the snapshot file.
	 */
	log_notice("Removing slot file \"%s\"", copySpecs->cfPaths.cdc.slotfile);

	if (!unlink_file(copySpecs->cfPaths.cdc.slotfile))
	{
		log_error("Failed to unlink the slot file \"%s\"",
				  copySpecs->cfPaths.cdc.slotfile);
		return false;
	}

	log_notice("Removing snapshot file \"%s\"", copySpecs->cfPaths.snfile);

	if (!unlink_file(copySpecs->cfPaths.snfile))
	{
		log_error("Failed to unlink the snapshot file \"%s\"",
				  copySpecs->cfPaths.snfile);
		return false;
	}

	/*
	 * Now cleanup the target database (replication origin).
	 */
	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_replication_origin_drop(&dst, origin))
	{
		log_error("Failed to drop replication origin \"%s\"", origin);
		return false;
	}

	return true;
}


/*
 * stream_create_origin creates a replication origin on the target database.
 */
bool
stream_create_origin(CopyDataSpec *copySpecs, char *nodeName, uint64_t startpos)
{
	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, copySpecs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	uint32_t oid = 0;

	if (!pgsql_replication_origin_oid(&dst, nodeName, &oid))
	{
		/* errors have already been logged */
		return false;
	}

	if (oid == 0)
	{
		if (!pgsql_replication_origin_create(&dst, nodeName))
		{
			/* errors have already been logged */
			return false;
		}

		char startLSN[PG_LSN_MAXLENGTH] = { 0 };

		sformat(startLSN, sizeof(startLSN), "%X/%X", LSN_FORMAT_ARGS(startpos));

		if (!pgsql_replication_origin_advance(&dst, nodeName, startLSN))
		{
			/* errors have already been logged */
			return false;
		}

		log_info("Created logical replication origin \"%s\" at LSN %X/%X",
				 nodeName,
				 LSN_FORMAT_ARGS(startpos));
	}
	else
	{
		uint64_t lsn = 0;

		if (!pgsql_replication_origin_progress(&dst,
											   nodeName,
											   true,
											   &lsn))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * We accept the current target origin position when --resume has been
		 * used, and also when a --startpos has been given that matches exactly
		 * the current tracked position.
		 */
		bool acceptTrackedLSN = copySpecs->resume || lsn == startpos;

		log_level(acceptTrackedLSN ? LOG_INFO : LOG_ERROR,
				  "Replication origin \"%s\" already exists at LSN %X/%X",
				  nodeName,
				  LSN_FORMAT_ARGS(lsn));

		if (!acceptTrackedLSN)
		{
			/* errors have already been logged */
			pgsql_finish(&dst);
			return false;
		}
	}

	if (!pgsql_commit(&dst))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * stream_create_sentinel creates the pgcopydb sentinel table on the source
 * database and registers the startpos, usually the same as the LSN returned
 * from stream_create_repl_slot.
 */
bool
stream_create_sentinel(CopyDataSpec *copySpecs,
					   uint64_t startpos,
					   uint64_t endpos)
{
	if (copySpecs->resume)
	{
		log_info("Skipping creation of pgcopydb.sentinel (--resume)");
		return true;
	}

	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	if (!sentinel_setup(sourceDB, startpos, endpos))
	{
		log_error("Failed to create the sentinel table, see above for details");
		return false;
	}

	return true;
}


/*
 * stream_fetch_current_source_lsn connects to the given Postgres service and
 * fetches the current WAL LSN position by calling pg_current_wal_flush_lsn
 * there, or the variant of that function that is supported by this Postgres
 * version.
 */
bool
stream_fetch_current_lsn(uint64_t *lsn,
						 const char *pguri,
						 ConnectionType connectionType)
{
	PGSQL src = { 0 };

	if (!pgsql_init(&src, (char *) pguri, connectionType))
	{
		/* errors have already been logged */
		return false;
	}

	/* limit the amount of logging of the apply process */
	src.logSQL = false;

	uint64_t flushLSN = InvalidXLogRecPtr;

	if (!pgsql_begin(&src))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_server_version(&src))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_current_wal_flush_lsn(&src, &flushLSN))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_commit(&src))
	{
		/* errors have already been logged */
		return false;
	}

	*lsn = flushLSN;

	return true;
}
