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


static bool updateStreamCounters(StreamContext *context,
								 LogicalMessageMetadata *metadata);


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
				  SourceCatalog *catalog,
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

	specs->catalog = catalog;

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
				.count = 6,
				.keywords = {
					"format-version",
					"include-xids",
					"include-schemas",
					"include-transaction",
					"include-types",
					"filter-tables"
				},
				.values = {
					"2",
					"true",
					"true",
					"true",
					"true",
					"pgcopydb.*"
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
	privateContext->startposActionFromJSON = specs->startposActionFromJSON;

	privateContext->mode = specs->mode;

	privateContext->transformQueue = &(specs->transformQueue);

	privateContext->paths = specs->paths;
	privateContext->startpos = specs->startpos;

	privateContext->connStrings = specs->connStrings;

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

	if (specs->startposComputedFromJSON &&
		(specs->startposActionFromJSON == STREAM_ACTION_BEGIN ||
		 specs->startposActionFromJSON == STREAM_ACTION_INSERT ||
		 specs->startposActionFromJSON == STREAM_ACTION_UPDATE ||
		 specs->startposActionFromJSON == STREAM_ACTION_DELETE ||
		 specs->startposActionFromJSON == STREAM_ACTION_TRUNCATE))
	{
		privateContext->reachedStartPos = false;
	}
	else
	{
		privateContext->reachedStartPos = true;
	}

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
	privateContext->catalog = specs->catalog;

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

		log_debug("startLogicalStreaming: %s (%d)",
				  OutputPluginToString(specs->slot.plugin),
				  specs->pluginOptions.count);

		if (!pgsql_start_replication(&stream))
		{
			/* errors have already been logged */
			return false;
		}

		/* write the wal_segment_size and timeline history files */
		if (!stream_write_context(specs, &stream))
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
			log_info("Streamed up to write_lsn %X/%X, flush_lsn %X/%X, stopping: "
					 "endpos is %X/%X",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn),
					 LSN_FORMAT_ARGS(context.tracking->flushed_lsn),
					 LSN_FORMAT_ARGS(context.endpos));
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
			log_warn("Streaming got interrupted at %X/%X "
					 "after processing %lld message%s",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn),
					 (long long) privateContext->counters.total,
					 privateContext->counters.total > 0 ? "s" : "");
		}

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
	StreamContent latestStreamedContent = { 0 };

	if (!stream_read_latest(specs, &latestStreamedContent))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * When we don't have any file on-disk yet, we might have specifications
	 * for when to start in the pgcopydb sentinel table. The sentinel only
	 * applies to STREAM_MODE_PREFETCH, in STREAM_MODE_RECEIVE we bypass that
	 * mechanism entirely.
	 *
	 * When STREAM_MODE_PREFETCH is set, it is expected that the pgcopydb
	 * sentinel table has been setup before starting the logical decoding
	 * client.
	 *
	 * The pgcopydb sentinel table also contains an endpos. The --endpos
	 * command line option (found in specs->endpos) prevails, but when it's not
	 * been used, we have a look at the sentinel value.
	 */
	PGSQL src = { 0 };

	if (!pgsql_init(&src, specs->connStrings->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	CopyDBSentinel sentinel = { 0 };

	if (!pgsql_get_sentinel(&src, &sentinel))
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

		if (!pgsql_update_sentinel_endpos(&src, false, specs->endpos))
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

	if (latestStreamedContent.count == 0)
	{
		if (specs->mode == STREAM_MODE_RECEIVE)
		{
			return true;
		}

		if (sentinel.startpos != InvalidXLogRecPtr)
		{
			specs->startpos = sentinel.startpos;

			log_info("Resuming streaming at LSN %X/%X "
					 "from replication slot \"%s\"",
					 LSN_FORMAT_ARGS(specs->startpos),
					 specs->slot.slotName);
		}
	}
	else
	{
		/* lines are counted starting at zero */
		int lastLineNb = latestStreamedContent.count - 1;

		LogicalMessageMetadata *messages = latestStreamedContent.messages;
		LogicalMessageMetadata *latest = &(messages[lastLineNb]);

		/*
		 * We could have several messages following each-other with the same
		 * LSN, typically a sequence like:
		 *
		 *  {"action":"I","xid":"492","lsn":"0/244BEE0", ...}
		 *  {"action":"K","lsn":"0/244BEE0", ...}
		 *  {"action":"E","lsn":"0/244BEE0"}
		 *
		 * In that case we want to remember the latest message action as being
		 * INSERT rather than ENDPOS.
		 */
		int lineNb = lastLineNb;

		for (; lineNb > 0; lineNb--)
		{
			LogicalMessageMetadata *previous = &(messages[lineNb]);

			if (previous->lsn == latest->lsn)
			{
				latest = previous;
			}
			else
			{
				break;
			}
		}

		specs->startpos = latest->lsn;
		specs->startposComputedFromJSON = true;
		specs->startposActionFromJSON = latest->action;

		log_info("Resuming streaming at LSN %X/%X "
				 "from first message with that LSN read in JSON file \"%s\", "
				 "line %d",
				 LSN_FORMAT_ARGS(specs->startpos),
				 latestStreamedContent.filename,
				 lineNb);

		char *latestMessage = latestStreamedContent.lines[lineNb];
		log_notice("Resume replication from latest message: %s", latestMessage);
	}

	bool flush = false;
	uint64_t lsn = 0;

	if (!pgsql_replication_slot_exists(&src, specs->slot.slotName, &flush, &lsn))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * The receive process knows how to skip over LSNs that have already been
	 * fetched in a previous run. What we are not able to do is fill-in a gap
	 * between what we have on-disk and what the replication slot can send us.
	 */
	if (specs->startpos < lsn)
	{
		log_error("Failed to resume replication: on-disk next LSN is %X/%X  "
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
		bool previous = false;

		if (!stream_write_json(context, previous))
		{
			/* errors have already been logged */
			return false;
		}

		/* update internal transaction counters */
		(void) updateStreamCounters(privateContext, metadata);
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

	return true;
}


/*
 * stream_write_json writes the current (or previous) Logical Message to disk.
 */
bool
stream_write_json(LogicalStreamContext *context, bool previous)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata =
		previous ? &(privateContext->previous) : &(privateContext->metadata);

	/* we might have to rotate to the next on-disk file */
	if (!streamRotateFile(context))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Write the logical decoding message to disk, appending to the already
	 * opened file we track in the privateContext.
	 */
	if (privateContext->jsonFile == NULL)
	{
		log_error("Failed to write Logical Message: jsonFile is NULL");
		return false;
	}

	/* prepare a in-memory buffer with the whole data formatted in JSON */
	PQExpBuffer buffer = createPQExpBuffer();

	if (buffer == NULL)
	{
		log_fatal("Failed to allocate memory to prepare JSON message");
		return false;
	}

	appendPQExpBuffer(buffer,
					  "{\"action\":\"%c\","
					  "\"xid\":\"%lld\","
					  "\"lsn\":\"%X/%X\","
					  "\"timestamp\":\"%s\","
					  "\"message\":",
					  metadata->action,
					  (long long) metadata->xid,
					  LSN_FORMAT_ARGS(metadata->lsn),
					  metadata->timestamp);

	appendPQExpBuffer(buffer, "%s}\n", metadata->jsonBuffer);

	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(buffer))
	{
		log_error("Failed to prepare JSON message: out of memory");
		destroyPQExpBuffer(buffer);
		return false;
	}

	/* then add the logical output plugin data, inside our own JSON format */
	if (!write_to_stream(privateContext->jsonFile, buffer->data, buffer->len))
	{
		log_error("Failed to write to file \"%s\": see above for details",
				  privateContext->partialFileName);
		destroyPQExpBuffer(buffer);
		return false;
	}

	/* time to update our lastWriteTime mark */
	privateContext->lastWriteTime = time(NULL);

	/* update the tracking for maximum LSN of messages written to disk so far */
	if (privateContext->maxWrittenLSN < metadata->lsn)
	{
		privateContext->maxWrittenLSN = metadata->lsn;
	}

	/*
	 * Now if specs->stdOut is true we want to also write all the same things
	 * again to stdout this time. We don't expect buffered IO to stdout, so we
	 * don't loop and retry short writes there.
	 */
	if (privateContext->stdOut)
	{
		if (!write_to_stream(privateContext->out, buffer->data, buffer->len))
		{
			log_error("Failed to write JSON message to stdout: "
					  "see above for details");
			log_debug("JSON message: %s", buffer->data);

			destroyPQExpBuffer(buffer);
			return false;
		}

		/* flush stdout at transaction boundaries */
		if (metadata->action == STREAM_ACTION_COMMIT)
		{
			if (fflush(privateContext->out) != 0)
			{
				log_error("Failed to flush standard output: %m");
				destroyPQExpBuffer(buffer);
				return false;
			}
		}
	}

	destroyPQExpBuffer(buffer);
	free(metadata->jsonBuffer);

	return true;
}


/*
 * stream_write_internal_message outputs an internal message for pgcopydb
 * operations into our current stream output(s).
 */
bool
stream_write_internal_message(LogicalStreamContext *context,
							  InternalMessage *message)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	long buflen = 0;
	char buffer[BUFSIZE] = { 0 };

	/* not all internal message require a timestamp field */
	if (message->time > 0)
	{
		/* add the server sendTime to the LogicalMessageMetadata */
		if (!pgsql_timestamptz_to_string(message->time,
										 message->timeStr,
										 sizeof(message->timeStr)))
		{
			log_error("Failed to format server send time %lld to time string",
					  (long long) message->time);
			return false;
		}

		char *fmt =
			"{\"action\":\"%c\",\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n";

		buflen = sformat(buffer, sizeof(buffer), fmt,
						 message->action,
						 LSN_FORMAT_ARGS(message->lsn),
						 message->timeStr);
	}
	else
	{
		buflen = sformat(buffer, sizeof(buffer),
						 "{\"action\":\"%c\",\"lsn\":\"%X/%X\"}\n",
						 message->action,
						 LSN_FORMAT_ARGS(message->lsn));
	}

	if (!write_to_stream(privateContext->jsonFile, buffer, buflen))
	{
		log_error("Failed to write internal message: %.1024s%s",
				  buffer,
				  buflen > 1024 ? "..." : "");
		return false;
	}

	/* skip NOTICE logs for KEEPALIVE messages */
	if (message->action != STREAM_ACTION_KEEPALIVE)
	{
		log_notice("Inserted action %s for lsn %X/%X in \"%s\"",
				   StreamActionToString(message->action),
				   LSN_FORMAT_ARGS(message->lsn),
				   privateContext->partialFileName);
	}

	/*
	 * When streaming to a Unix pipe don't forget to also stream the SWITCH
	 * WAL message there, so that the transform process forwards it.
	 */
	if (privateContext->stdOut)
	{
		if (!write_to_stream(privateContext->out, buffer, buflen))
		{
			log_error("Failed to write JSON message (%ld bytes) to stdout: %m",
					  buflen);
			log_debug("JSON message: %.1024s%s",
					  buffer,
					  buflen > 1024 ? "..." : "");
			return false;
		}
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
	XLByteToSeg(jsonFileLSN, segno, context->WalSegSz);
	XLogFileName(wal, context->timeline, segno, context->WalSegSz);

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

		if (!stream_write_internal_message(context, &switchwal))
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

	/*
	 * Also maintain the "latest" symbolic link to the latest file where
	 * we've been streaming changes in.
	 */
	if (!stream_update_latest_symlink(privateContext,
									  privateContext->partialFileName))
	{
		log_error("Failed to update latest symlink to \"%s\", "
				  "see above for details",
				  privateContext->partialFileName);
		return false;
	}

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

		if (!stream_write_internal_message(context, &endpos))
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

		/* and also update the "latest" symlink, we need it for --resume */
		if (!stream_update_latest_symlink(privateContext,
										  privateContext->walFileName))
		{
			log_error("Failed to update latest symlink to \"%s\", "
					  "see above for details",
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

	/* when there is currently no file opened, just skip the flush operation */
	if (privateContext->jsonFile == NULL)
	{
		return true;
	}

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

		if (!stream_write_internal_message(context, &keepalive))
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
 * while we fetch the replay_lsn from the pgcopydb sentinel table on the source
 * database, and sync with the current progress.
 */
bool
streamFeedback(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

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

	PGSQL src = { 0 };
	char *pguri = privateContext->connStrings->source_pguri;

	if (!pgsql_init(&src, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	CopyDBSentinel sentinel = { 0 };

	if (!pgsql_sync_sentinel_recv(&src,
								  context->tracking->written_lsn,
								  context->tracking->flushed_lsn,
								  &sentinel))
	{
		/* errors have already been logged */
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

	context->lastFeedbackSync = context->now;

	log_debug("streamFeedback: written %X/%X flushed %X/%X applied %X/%X "
			  " startpos %X/%X endpos %X/%X apply %s",
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

	/*
	 * When streaming resumed for a partially applied txn, have we reached a
	 * message that wasn't flushed in the previous command?
	 */
	if (!privateContext->reachedStartPos)
	{
		/*
		 * Also the same LSN might be assigned to a BEGIN message, a COMMIT
		 * message, and a KEEPALIVE message. Avoid skipping what looks like the
		 * same message as the latest flushed in our JSON file when it's
		 * actually a new message.
		 */
		privateContext->reachedStartPos =
			privateContext->startpos < metadata->lsn ||
			(privateContext->startpos == metadata->lsn &&
			 metadata->action != privateContext->startposActionFromJSON);
	}

	if (!privateContext->reachedStartPos)
	{
		metadata->filterOut = true;

		log_debug("Skipping write for action %c for XID %u at LSN %X/%X: "
				  "startpos %X/%X not been reached",
				  metadata->action,
				  metadata->xid,
				  LSN_FORMAT_ARGS(metadata->lsn),
				  LSN_FORMAT_ARGS(privateContext->startpos));

		*previous = *metadata;

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

		bool previous = true;

		if (!stream_write_json(context, previous))
		{
			/* errors have been logged */
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
						  "which is %lu bytes long, "
						  "pgcopydb only support timestamps up to %lu bytes",
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

	content->count = countLines(content->buffer);
	content->lines = (char **) calloc(content->count, sizeof(char *));
	content->count = splitLines(content->buffer, content->lines, content->count);

	if (content->lines == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	content->messages =
		(LogicalMessageMetadata *) calloc(content->count,
										  sizeof(LogicalMessageMetadata));

	if (content->messages == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	for (int i = 0; i < content->count; i++)
	{
		char *message = content->lines[i];
		LogicalMessageMetadata *metadata = &(content->messages[i]);

		JSON_Value *json = json_parse_string(message);

		if (!parseMessageMetadata(metadata, message, json, false))
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

	sformat(latest, sizeof(latest), "%s/latest", specs->paths.dir);

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
 * stream_update_latest_symlink updates the latest symbolic link to the given
 * filename, that must already exists on the file system.
 */
bool
stream_update_latest_symlink(StreamContext *privateContext,
							 const char *filename)
{
	char latest[MAXPGPATH] = { 0 };

	sformat(latest, sizeof(latest), "%s/latest", privateContext->paths.dir);

	if (!unlink_file(latest))
	{
		/* errors have already been logged */
		return false;
	}

	if (!create_symbolic_link((char *) filename, latest))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("stream_update_latest_symlink: \"%s\" -> \"%s\"",
			  latest,
			  privateContext->partialFileName);

	return true;
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
			log_trace("Skipping counters for message action \"%c\"",
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
		freeURIParams(&params);
		return false;
	}

	freeURIParams(&params);
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

	char *sql[] = {
		"create schema if not exists pgcopydb",
		"drop table if exists pgcopydb.sentinel",
		"create table pgcopydb.sentinel"
		"(startpos pg_lsn, endpos pg_lsn, apply bool, "
		" write_lsn pg_lsn, flush_lsn pg_lsn, replay_lsn pg_lsn)",
		NULL
	};

	char *index = "create unique index on pgcopydb.sentinel((1))";

	PGSQL *pgsql = &(copySpecs->sourceSnapshot.pgsql);

	if (!pgsql_init(pgsql, copySpecs->connStrings.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	/* create the schema and the table for pgcopydb.sentinel */
	for (int i = 0; sql[i] != NULL; i++)
	{
		log_info("%s", sql[i]);

		if (!pgsql_execute(pgsql, sql[i]))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* now insert the sentinel values (startpos, endpos, false as apply) */
	char *insert =
		"insert into pgcopydb.sentinel "
		"(startpos, endpos, apply, write_lsn, flush_lsn, replay_lsn) "
		"values($1, $2, $3, '0/0', '0/0', '0/0')";

	char startLSN[PG_LSN_MAXLENGTH] = { 0 };
	char endLSN[PG_LSN_MAXLENGTH] = { 0 };

	sformat(startLSN, sizeof(startLSN), "%X/%X", LSN_FORMAT_ARGS(startpos));
	sformat(endLSN, sizeof(endLSN), "%X/%X", LSN_FORMAT_ARGS(endpos));

	int paramCount = 3;
	Oid paramTypes[3] = { LSNOID, LSNOID, BOOLOID };
	const char *paramValues[3] = { startLSN, endLSN, "false" };

	if (!pgsql_execute_with_params(pgsql, insert,
								   paramCount, paramTypes, paramValues,
								   NULL, NULL))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_execute(pgsql, index))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_commit(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * stream_write_context writes the wal_segment_size and timeline history to
 * files.
 */
bool
stream_write_context(StreamSpecs *specs, LogicalStreamClient *stream)
{
	IdentifySystem *system = &(stream->system);
	char wal_segment_size[BUFSIZE] = { 0 };

	/* also cache the system and WalSegSz in the StreamSpecs */
	specs->system = stream->system;
	specs->WalSegSz = stream->WalSegSz;

	int bytes =
		sformat(wal_segment_size, sizeof(wal_segment_size), "%lld",
				(long long) stream->WalSegSz);

	if (!write_file(wal_segment_size, bytes, specs->paths.walsegsizefile))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("Wrote wal_segment_size %s into \"%s\"",
			  wal_segment_size,
			  specs->paths.walsegsizefile);

	char tli[BUFSIZE] = { 0 };

	bytes = sformat(tli, sizeof(tli), "%d", system->timeline);

	if (!write_file(tli, bytes, specs->paths.tlifile))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("Wrote tli %s timeline file \"%s\"", tli, specs->paths.tlifile);

	if (!write_file(system->timelines.content,
					strlen(system->timelines.content),
					specs->paths.tlihistfile))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("Wrote timeline history file \"%s\"", specs->paths.tlihistfile);

	return true;
}


/*
 * stream_cleanup_context removes the context files that are created upon
 * connecting to the source database with the logical replication protocol.
 */
bool
stream_cleanup_context(StreamSpecs *specs)
{
	bool success = true;

	success = success && unlink_file(specs->paths.walsegsizefile);
	success = success && unlink_file(specs->paths.tlifile);
	success = success && unlink_file(specs->paths.tlihistfile);

	return success;
}


/*
 * stream_read_context reads the stream context back from files
 * wal_segment_size and timeline history.
 */
bool
stream_read_context(CDCPaths *paths,
					IdentifySystem *system,
					uint32_t *WalSegSz)
{
	char *wal_segment_size = NULL;
	char *tli = NULL;
	char *history = NULL;

	long size = 0L;

	/*
	 * We need to read the 3 streaming context files that the receive process
	 * prepares when connecting to the source database. Because the catchup
	 * process might get here early, we implement a retry loop in case the
	 * files have not been created yet.
	 */
	ConnectionRetryPolicy retryPolicy = { 0 };

	int maxT = 10;              /* 10s */
	int maxSleepTime = 1500;    /* 1.5s */
	int baseSleepTime = 100;    /* 100ms */

	(void) pgsql_set_retry_policy(&retryPolicy,
								  maxT,
								  -1, /* unbounded number of attempts */
								  maxSleepTime,
								  baseSleepTime);

	while (!pgsql_retry_policy_expired(&retryPolicy))
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			return false;
		}

		if (file_exists(paths->walsegsizefile) &&
			file_exists(paths->tlifile) &&
			file_exists(paths->tlihistfile))
		{
			/*
			 * File did exist, but might have been deleted now (race condition
			 * at prefetch and transform processes start-up).
			 */
			bool success = true;

			success = success &&
					  read_file(paths->walsegsizefile, &wal_segment_size, &size);

			success = success &&
					  read_file(paths->tlifile, &tli, &size);

			success = success &&
					  read_file(paths->tlihistfile, &history, &size);

			if (success)
			{
				/* success: break out of the retry loop */
				break;
			}
		}

		int sleepTimeMs =
			pgsql_compute_connection_retry_sleep_time(&retryPolicy);

		log_debug("stream_read_context: waiting for context files "
				  "to have been created, retrying in %dms",
				  sleepTimeMs);

		/* we have milliseconds, pg_usleep() wants microseconds */
		(void) pg_usleep(sleepTimeMs * 1000);
	}

	/* did retry policy expire before the files are created? */
	if (!(file_exists(paths->walsegsizefile) &&
		  file_exists(paths->tlifile) &&
		  file_exists(paths->tlihistfile)))
	{
		log_error("Failed to read stream context file: retry policy expired");
		return false;
	}

	/*
	 * Now that we could read the file contents, parse it.
	 */
	if (!stringToUInt(wal_segment_size, WalSegSz))
	{
		/* errors have already been logged */
		free(wal_segment_size);
		free(tli);
		free(history);
		return false;
	}

	if (!stringToUInt(tli, &(system->timeline)))
	{
		/* errors have already been logged */
		free(wal_segment_size);
		free(tli);
		free(history);
		return false;
	}

	if (!parseTimeLineHistory(paths->tlihistfile, history, system))
	{
		/* errors have already been logged */
		free(wal_segment_size);
		free(tli);
		free(history);
		return false;
	}

	free(wal_segment_size);
	free(tli);
	free(history);

	return true;
}
