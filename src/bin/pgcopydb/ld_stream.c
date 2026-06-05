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
#include "ld_ipc.h"
#include "ld_store.h"
#include "ld_stream.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pg_utils.h"
#include "pgsql_timeline.h"
#include "pidfile.h"
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

	if (specs->connStrings->source_pguri != NULL &&
		!buildReplicationURI(specs->connStrings->source_pguri,
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
	 *
	 * The process names reflect their actual function:
	 * - receive: receives changes from the PostgreSQL replication slot
	 * - transform: transforms logical decoding output to SQL statements
	 * - apply: applies the SQL statements to the target database
	 *
	 * In replay mode (no live Postgres), we use "replay" instead of "apply"
	 * since there's no target database involved.
	 */
	bool replayMode = specs->mode == STREAM_MODE_REPLAY;

	FollowSubProcess prefetch = {
		.name = replayMode ? "receive" : "receive",
		.command = &follow_start_prefetch,
		.pid = -1
	};

	FollowSubProcess transform = {
		.name = "transform",
		.command = &follow_start_transform,
		.pid = -1
	};

	FollowSubProcess catchup = {
		.name = replayMode ? "replay" : "apply",
		.command = &follow_start_catchup,
		.pid = -1
	};

	specs->prefetch = prefetch;
	specs->transform = transform;
	specs->catchup = catchup;

	/*
	 * In replay mode, the receive, transform, and apply processes communicate
	 * via Unix pipes.  Set up the stdin/stdout flags accordingly.
	 */
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		specs->stdIn = true;
		specs->stdOut = true;
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

	/* record the plugin so the receive and transform paths both dispatch correctly */
	privateContext->plugin = specs->slot.plugin;

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

	if (!parse_timeline_history_file(stream->system.timelineHistoryFilename,
									 sourceDB,
									 stream->system.timeline))
	{
		log_error("Failed to parse timeline history file \"%s\": "
				  "see above for details",
				  specs->system.timelineHistoryFilename);
		return false;
	}

	/* publish the stream client Identify System information in the specs */
	specs->system = stream->system;
	specs->private.timeline = stream->system.timeline;

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
							   specs->paths.dir,
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
 * stream_receive_from_file reads a JSON-lines fixture file and feeds every
 * message through the same streamWrite() path that a live replication
 * connection would use, populating the replayDB output table.
 *
 * Each line in the file must be a JSON object with the following fields:
 *
 *   {
 *     "lsn":       "0/1519348",
 *     "timestamp": "2026-01-01 12:00:00.000000+00",
 *     "message":   "<raw CDC message text>"
 *   }
 *
 * For test_decoding the message is plain text ("BEGIN 733", "table ...: ...");
 * for wal2json it is the raw JSON string that wal2json emits.
 *
 * The function returns false on the first parse or write error.
 */
bool
stream_receive_from_file(StreamSpecs *specs, const char *filename)
{
	if (!stream_init_context(specs))
	{
		/* errors have already been logged */
		return false;
	}

	StreamContext *privateContext = &(specs->private);

	LogicalStreamContext context = { 0 };
	context.private = (void *) privateContext;
	context.plugin = specs->slot.plugin;

	/*
	 * Bootstrap the tracking struct so streamWrite / streamFlush do not
	 * dereference a NULL pointer.
	 */
	LogicalTrackLSN tracking = { 0 };
	context.tracking = &tracking;

	char *contents = NULL;
	long size = 0L;

	if (!read_file(filename, &contents, &size))
	{
		/* errors have already been logged */
		return false;
	}

	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, contents))
	{
		/* errors have already been logged */
		free(contents);
		return false;
	}

	log_info("stream_receive_from_file: processing %lld lines from \"%s\"",
			 (long long) lbuf.count, filename);

	bool success = true;

	for (uint64_t i = 0; i < lbuf.count; i++)
	{
		char *line = lbuf.lines[i];

		/* skip empty lines */
		if (line == NULL || line[0] == '\0')
		{
			continue;
		}

		JSON_Value *json = json_parse_string(line);

		if (json == NULL)
		{
			log_error("stream_receive_from_file: failed to parse JSON line %lld:"
					  " %s", (long long) (i + 1), line);
			success = false;
			break;
		}

		JSON_Object *jsobj = json_value_get_object(json);

		if (jsobj == NULL)
		{
			log_error("stream_receive_from_file: line %lld is not a JSON object:"
					  " %s", (long long) (i + 1), line);
			json_value_free(json);
			success = false;
			break;
		}

		/* --- lsn --------------------------------------------------------- */
		const char *lsn_str = json_object_get_string(jsobj, "lsn");

		if (lsn_str == NULL)
		{
			log_error("stream_receive_from_file: line %lld missing \"lsn\"",
					  (long long) (i + 1));
			json_value_free(json);
			success = false;
			break;
		}

		uint64_t parsedLSN = 0;

		if (!parseLSN(lsn_str, &parsedLSN))
		{
			log_error("stream_receive_from_file: failed to parse lsn \"%s\" "
					  "on line %lld", lsn_str, (long long) (i + 1));
			json_value_free(json);
			success = false;
			break;
		}

		context.cur_record_lsn = parsedLSN;

		/* --- timestamp --------------------------------------------------- */
		/*
		 * Use a fixed PG-epoch timestamp so that fixture files are
		 * fully deterministic (no wall-clock dependency).  sendTime = 0
		 * corresponds to the Postgres epoch 2000-01-01 00:00:00 UTC.
		 */
		context.sendTime = 0;

		/* --- message ----------------------------------------------------- */
		const char *msg = json_object_get_string(jsobj, "message");

		if (msg == NULL)
		{
			log_error("stream_receive_from_file: line %lld missing \"message\"",
					  (long long) (i + 1));
			json_value_free(json);
			success = false;
			break;
		}

		/*
		 * context.buffer must point to a writable buffer; strlcpy into
		 * a local buffer then point context.buffer at it.
		 */
		static char msgbuf[BUFSIZE * 16];

		strlcpy(msgbuf, msg, sizeof(msgbuf));
		context.buffer = msgbuf;

		if (!streamWrite(&context))
		{
			log_error("stream_receive_from_file: streamWrite failed on line %lld",
					  (long long) (i + 1));
			json_value_free(json);
			success = false;
			break;
		}

		json_value_free(json);
	}

	free(contents);

	if (!success)
	{
		return false;
	}

	/*
	 * Inject a final ENDPOS keepalive so the replayDB output table gets a
	 * proper endpos marker, which the transform step uses to know it has
	 * consumed everything.
	 */
	if (specs->endpos != InvalidXLogRecPtr)
	{
		context.cur_record_lsn = (XLogRecPtr) specs->endpos;
		context.sendTime = 0;

		if (!streamKeepalive(&context))
		{
			/* errors have already been logged */
			return false;
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
	 * After a restart, sentinel.startpos is the original replication slot
	 * LSN (never updated), while the slot's confirmed_flush has already
	 * advanced to flush_lsn from the previous run.  startpos < slot_lsn is
	 * therefore normal and expected: it means the output table already
	 * contains rows up to flush_lsn and the transform process will consume
	 * them from transform_lsn independently.  Clamp our streaming start
	 * position to the slot's current LSN so we begin receiving new WAL from
	 * there and let PostgreSQL serve what it still has.
	 */
	if (specs->startpos < lsn)
	{
		log_notice("Prefetch restart: sentinel.startpos %X/%X is behind "
				   "replication slot LSN %X/%X; clamping to slot LSN",
				   LSN_FORMAT_ARGS(specs->startpos),
				   LSN_FORMAT_ARGS(lsn));

		specs->startpos = lsn;
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
			 * After a transient reconnect do NOT delete partial rows for the
			 * in-progress transaction.
			 *
			 * Previously this path deleted partial rows assuming "the slot
			 * will re-deliver the entire transaction from its BEGIN on the
			 * new connection."  That assumption fails when flush_lsn (now
			 * tied to transform_lsn, not written_lsn) has already advanced
			 * past the transaction's BEGIN lsn: the slot starts from
			 * confirmed_flush > BEGIN lsn and therefore never re-sends the
			 * BEGIN, leaving orphaned DML rows without a matching BEGIN in
			 * the output table.
			 *
			 * With flush_lsn = transform_lsn and INSERT OR REPLACE semantics
			 * on the output table the partial rows from the previous
			 * connection are safely completed by the re-delivered messages:
			 * the BEGIN is already in the table (or will be re-inserted
			 * idempotently if confirmed_flush < BEGIN lsn), and any
			 * re-delivered DML/COMMIT rows are upserted without duplication.
			 */
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

		/*
		 * Track the current transaction XID so that ROLLBACK internal messages
		 * inserted by streamCloseFile carry the correct xid regardless of plugin.
		 * test_decoding sets currentXid in its own parser but wal2json does not;
		 * updating here covers both paths uniformly.
		 */
		if (metadata->xid > 0)
		{
			privateContext->currentXid = metadata->xid;
		}
	}
	else if (metadata->action == STREAM_ACTION_COMMIT)
	{
		privateContext->transactionInProgress = false;
		privateContext->currentXid = 0;
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

	/*
	 * In the SQLite pipeline, the WAL segment name is used only to detect
	 * epoch boundaries (when to write a SWITCH message and notify the
	 * transform service).  We no longer open .json files on disk.
	 */
	sformat(walFileName, sizeof(walFileName), "%s/%s",
			privateContext->paths.dir,
			wal);

	/* in most cases, the WAL segment is still the same */
	if (streq(privateContext->walFileName, walFileName))
	{
		return true;
	}

	/*
	 * The WAL segment boundary changed.  In the SQLite pipeline there is a
	 * single output table that spans all WAL segments — no per-segment file
	 * rotation is performed and no SWITCH marker is needed.  We only call
	 * streamCloseFile() to flush any in-progress state for the current segment
	 * before continuing with the next one.
	 */
	if (!IS_EMPTY_STRING_BUFFER(privateContext->walFileName))
	{
		bool time_to_abort = false;

		if (!streamCloseFile(context, time_to_abort))
		{
			/* errors have already been logged */
			return false;
		}
	}

	strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);

	/* the jsonFileLSN is the firstLSN for this epoch */
	privateContext->firstLSN = jsonFileLSN;

	log_info("Now streaming changes for WAL segment \"%s\"", wal);

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
	log_notice("streamCloseFile: time_to_abort=%d, endpos=%X/%X, cur_record_lsn=%X/%X, "
			   "privateContext->endpos=%X/%X, replayDB=%p",
			   time_to_abort,
			   LSN_FORMAT_ARGS(privateContext->endpos),
			   LSN_FORMAT_ARGS(context->cur_record_lsn),
			   LSN_FORMAT_ARGS(privateContext->endpos),
			   replayDB);

	if (time_to_abort &&
		privateContext->endpos != InvalidXLogRecPtr &&
		privateContext->endpos <= context->cur_record_lsn)
	{
		InternalMessage endpos = {
			.action = STREAM_ACTION_ENDPOS,
			.lsn = context->cur_record_lsn
		};

		log_notice("streamCloseFile: INSERTING ENDPOS at %X/%X",
				   LSN_FORMAT_ARGS(endpos.lsn));

		if (!ld_store_insert_internal_message(replayDB, &endpos))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * Force a WAL checkpoint to ensure the ENDPOS message is immediately
		 * visible to other processes (transform, apply). Without this, the
		 * ENDPOS message might be buffered and not yet visible to readers,
		 * causing them to hang waiting for data that will never come.
		 *
		 * In SQLite WAL mode, writes are buffered and may not be immediately
		 * visible to concurrent readers until a checkpoint is performed or
		 * the connection is closed.
		 */
		if (replayDB != NULL && replayDB->db != NULL)
		{
			int rc = sqlite3_wal_checkpoint(replayDB->db, NULL);

			log_notice("streamCloseFile: sqlite3_wal_checkpoint returned %d", rc);

			if (rc != SQLITE_OK && rc != SQLITE_BUSY)
			{
				log_warn("streamCloseFile: sqlite3_wal_checkpoint failed: %s",
						 sqlite3_errmsg(replayDB->db));
				/* Continue anyway; checkpoint is not critical */
			}
		}

		/*
		 * Report ENDPOS to follow coordinator via IPC socket if available.
		 * Note: IPC reporting is optional - follow will detect ENDPOS via the
		 * output table query when it polls. The core fix is in the SQL query
		 * using >= instead of > to find ENDPOS messages at the exact endpos LSN.
		 */
	}
	else if (!time_to_abort)
	{
		log_notice("streamCloseFile: SKIPPED (time_to_abort=false)");
	}
	else if (privateContext->endpos == InvalidXLogRecPtr)
	{
		log_notice("streamCloseFile: SKIPPED (endpos=InvalidXLogRecPtr)");
	}
	else
	{
		log_notice("streamCloseFile: SKIPPED (endpos %X/%X > cur_record_lsn %X/%X)",
				   LSN_FORMAT_ARGS(privateContext->endpos),
				   LSN_FORMAT_ARGS(context->cur_record_lsn));
	}

	/*
	 * Do NOT delete partial transaction rows when aborting.
	 *
	 * Previously this deleted the in-progress transaction's rows so the
	 * "slot re-delivers from BEGIN on reconnect" assumption held.  That
	 * assumption breaks when flush_lsn (= transform_lsn) has already advanced
	 * past the transaction's BEGIN lsn: the slot starts from confirmed_flush
	 * and never re-delivers the BEGIN, leaving orphaned DML rows.
	 *
	 * With flush_lsn = transform_lsn the slot always re-delivers from
	 * transform_lsn (a transaction boundary).  Partial rows already in the
	 * output table are completed by re-delivered messages via INSERT OR
	 * REPLACE; the BEGIN row is either already present (BEGIN lsn <
	 * transform_lsn) or will be re-inserted (BEGIN lsn >= transform_lsn).
	 * No deletion is needed or safe.
	 */

	log_debug("streamCloseFile: WAL epoch boundary at LSN %X/%X",
			  LSN_FORMAT_ARGS(context->cur_record_lsn));

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
	log_debug("streamFlush: written %X/%X cur %X/%X",
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

		context->tracking->flushed_lsn = context->tracking->written_lsn;

		log_debug("Flushed up to %X/%X",
				  LSN_FORMAT_ARGS(context->tracking->flushed_lsn));
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
	if (replayDB != NULL && replayDB->db != NULL)
	{
		/*
		 * Use context->sendTime when available (from a real server keepalive
		 * packet). For synthetic keepalive messages (generated during flush or
		 * empty transaction skipping), sendTime may be 0, so fall back to the
		 * current local timestamp to ensure the KEEPALIVE always has a valid
		 * timestamp for the apply process.
		 */
		TimestampTz keepaliveTime = context->sendTime;

		if (keepaliveTime == 0)
		{
			keepaliveTime = feGetCurrentTimestamp();
		}

		InternalMessage keepalive = {
			.action = STREAM_ACTION_KEEPALIVE,
			.lsn = context->cur_record_lsn,
			.time = keepaliveTime
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

	if (context->endpos != sentinel.endpos)
	{
		log_debug("stream_sync_sentinel: updating endpos to %X/%X",
				  LSN_FORMAT_ARGS(sentinel.endpos));
	}

	context->endpos = sentinel.endpos;
	context->tracking->applied_lsn = sentinel.replay_lsn;

	/*
	 * Report transform_lsn as the flushed position in the PostgreSQL
	 * replication feedback message.
	 *
	 * PostgreSQL uses flush_lsn to advance the logical slot's confirmed_flush.
	 * confirmed_flush is the point from which the slot re-delivers WAL on
	 * reconnect (max(startlsn, confirmed_flush)).  Using written_lsn (the raw
	 * WAL receive position) here races ahead of what transform has actually
	 * processed: if prefetch is killed mid-segment, confirmed_flush ends up
	 * beyond the last row in the output table, creating a permanent gap that
	 * can never be recovered.
	 *
	 * Using transform_lsn ensures confirmed_flush advances only when transform
	 * has fully committed a transaction to the relay table.  On any restart
	 * the slot re-delivers from transform_lsn, which is always a transaction
	 * boundary (we sync transform_lsn only at COMMIT/ROLLBACK), and INSERT OR
	 * REPLACE handles idempotent re-insertion of already-present output rows.
	 *
	 * write_lsn is still reported as-is (how far prefetch has received) for
	 * monitoring; only flush_lsn (which drives confirmed_flush) is changed.
	 */
	if (sentinel.transform_lsn != InvalidXLogRecPtr)
		context->tracking->flushed_lsn = sentinel.transform_lsn;

	log_debug("stream_sync_sentinel: "
			 "write_lsn %X/%X flush_lsn(=transform_lsn) %X/%X apply_lsn %X/%X "
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
	 * When we have dropped the replication slot, we can remove the snapshot
	 * file. The replication slot metadata is stored in the SQLite catalog and
	 * will be cleaned up with it.
	 */
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
