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
				  char *source_pguri,
				  char *target_pguri,
				  char *plugin,
				  char *slotName,
				  char *origin,
				  uint64_t endpos,
				  LogicalStreamMode mode,
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

	specs->plugin = OutputPluginFromString(plugin);

	switch (specs->plugin)
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
			log_error("Unknown logical decoding output plugin \"%s\"", plugin);
			return false;
		}
	}

	strlcpy(specs->source_pguri, source_pguri, MAXCONNINFO);
	strlcpy(specs->target_pguri, target_pguri, MAXCONNINFO);
	strlcpy(specs->slotName, slotName, sizeof(specs->slotName));
	strlcpy(specs->origin, origin, sizeof(specs->origin));

	if (!buildReplicationURI(specs->source_pguri, specs->logrep_pguri))
	{
		/* errors have already been logged */
		return false;
	}

	log_trace("stream_init_specs: %s(%d)", plugin, specs->pluginOptions.count);

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
		/* we keep the transform queue around */
		(void) 0;
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
 * stream_init_context initializes a LogicalStreamContext.
 */
bool
stream_init_context(StreamContext *privateContext, StreamSpecs *specs)
{
	privateContext->endpos = specs->endpos;
	privateContext->startpos = specs->startpos;

	privateContext->mode = specs->mode;
	privateContext->stdIn = specs->stdIn;
	privateContext->stdOut = specs->stdOut;

	privateContext->in = specs->in;
	privateContext->out = specs->out;

	privateContext->transformQueue = &(specs->transformQueue);

	privateContext->paths = specs->paths;
	privateContext->startpos = specs->startpos;

	strlcpy(privateContext->source_pguri,
			specs->source_pguri,
			sizeof(privateContext->source_pguri));

	privateContext->metadata.action = STREAM_ACTION_UNKNOWN;
	privateContext->previous.action = STREAM_ACTION_UNKNOWN;

	privateContext->lastWrite = 0;

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
	StreamContext privateContext = { 0 };

	if (!stream_init_context(&privateContext, specs))
	{
		/* errors have already been logged */
		return false;
	}

	context.private = (void *) &(privateContext);

	if (specs->stdOut)
	{
		/* switch stdout from block buffered to line buffered mode */
		if (setvbuf(specs->out, NULL, _IOLBF, 0) != 0)
		{
			log_error("Failed to set stdout to line buffered mode: %m");
			return false;
		}
	}

	log_notice("Connecting to logical decoding replication stream");

	/*
	 * In case of being disconnected or other transient errors, reconnect and
	 * continue streaming.
	 */
	bool retry = true;

	while (retry)
	{
		if (!pgsql_init_stream(&stream,
							   specs->logrep_pguri,
							   specs->plugin,
							   specs->slotName,
							   specs->startpos,
							   specs->endpos))
		{
			/* errors have already been logged */
			return false;
		}

		log_debug("startLogicalStreaming: %s (%d)",
				  OutputPluginToString(specs->plugin),
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
			log_info("Streamed up to write_lsn %X/%X, flush_lsn %X/%X, stopping",
					 LSN_FORMAT_ARGS(context.tracking->written_lsn),
					 LSN_FORMAT_ARGS(context.tracking->flushed_lsn));
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
		if (retry)
		{
			(void) pg_usleep(1 * 1000 * 1000);
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

	if (!pgsql_init(&src, specs->source_pguri, PGSQL_CONN_SOURCE))
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

		if (specs->endpos != InvalidXLogRecPtr)
		{
			log_info("Streaming is setup to end at LSN %X/%X",
					 LSN_FORMAT_ARGS(specs->endpos));
		}
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
					 specs->slotName);
		}
	}
	else
	{
		LogicalMessageMetadata *latest =
			&(latestStreamedContent.messages[latestStreamedContent.count - 1]);

		specs->startpos = latest->lsn;

		log_info("Resuming streaming at LSN %X/%X "
				 "from last message read in JSON file \"%s\", line %d",
				 LSN_FORMAT_ARGS(specs->startpos),
				 latestStreamedContent.filename,
				 latestStreamedContent.count - 1);
	}

	bool flush = false;
	uint64_t lsn = 0;

	if (!pgsql_replication_slot_exists(&src, specs->slotName, &flush, &lsn))
	{
		/* errors have already been logged */
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
	long bytes_left = buffer->len;
	long bytes_written = 0;

	while (bytes_left > 0)
	{
		int ret;

		ret = fwrite(buffer->data + bytes_written,
					 sizeof(char),
					 bytes_left,
					 privateContext->jsonFile);

		if (ret < 0)
		{
			log_error("Failed to write %ld bytes to file \"%s\": %m",
					  bytes_left,
					  privateContext->partialFileName);
			return false;
		}

		/* Write was successful, advance our position */
		bytes_written += ret;
		bytes_left -= ret;
	}

	/* time to update our lastWrite mark */
	privateContext->lastWrite = time(NULL);

	/*
	 * Now if specs->stdOut is true we want to also write all the same things
	 * again to stdout this time. We don't expect buffered IO to stdout, so we
	 * don't loop and retry short writes there.
	 */
	if (privateContext->stdOut)
	{
		int ret =
			fwrite(buffer->data, sizeof(char), buffer->len, privateContext->out);

		if (ret != buffer->len)
		{
			log_error("Failed to write JSON message (%ld bytes) to stdout: %m",
					  buffer->len);
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
				return false;
			}
		}
	}

	destroyPQExpBuffer(buffer);
	free(metadata->jsonBuffer);

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

	/* compute the WAL filename that would host the current LSN */
	XLByteToSeg(context->cur_record_lsn, segno, context->WalSegSz);
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

		char buffer[BUFSIZE] = { 0 };

		/*
		 * Add an extra empty transaction with the first lsn of the next file
		 * to allow for the transform and apply process to follow along.
		 */
		long buflen = sformat(buffer, sizeof(buffer),
							  "{\"action\":\"X\",\"lsn\":\"%X/%X\"}\n",
							  LSN_FORMAT_ARGS(context->cur_record_lsn));

		fformat(privateContext->jsonFile, "%s", buffer);

		log_notice("Inserted action SWITCH for lsn %X/%X in \"%s\"",
				   LSN_FORMAT_ARGS(context->cur_record_lsn),
				   privateContext->partialFileName);

		if (!streamCloseFile(context, time_to_abort))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * When streaming to a Unix pipe don't forget to also stream the SWITCH
		 * WAL message there, so that the transform process forwards it.
		 */
		if (privateContext->stdOut)
		{
			int ret =
				fwrite(buffer, sizeof(char), buflen, privateContext->out);

			if (ret != buflen)
			{
				log_error("Failed to write JSON message (%ld bytes) to stdout: %m",
						  buflen);
				log_debug("JSON message: %s", buffer);
				return false;
			}
		}
	}

	strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);
	strlcpy(privateContext->partialFileName, partialFileName, MAXPGPATH);

	/* when dealing with a new JSON name, also prepare the SQL name */
	sformat(privateContext->sqlFileName, sizeof(privateContext->sqlFileName),
			"%s/%s.sql",
			privateContext->paths.dir,
			wal);

	/* the context->cur_record_lsn is the firstLSN for this file */
	privateContext->firstLSN = context->cur_record_lsn;

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
		char sendTimeStr[BUFSIZE] = { 0 };

		/* add the server sendTime to the LogicalMessageMetadata */
		if (!pgsql_timestamptz_to_string(context->sendTime,
										 sendTimeStr,
										 sizeof(sendTimeStr)))
		{
			log_error("Failed to format server send time %lld to time string",
					  (long long) context->sendTime);
			return false;
		}

		char buffer[BUFSIZE] = { 0 };

		long buflen =
			sformat(
				buffer,
				sizeof(buffer),
				"{\"action\":\"K\",\"lsn\":\"%X/%X\",\"timestamp\":\"%s\"}\n",
				LSN_FORMAT_ARGS(context->cur_record_lsn),
				sendTimeStr);

		fformat(privateContext->jsonFile, "%s", buffer);

		log_trace("Inserted action KEEPALIVE for lsn %X/%X @%s",
				  LSN_FORMAT_ARGS(context->cur_record_lsn),
				  sendTimeStr);

		/* update the LSN tracking that's reported in the feedback */
		context->tracking->written_lsn = context->cur_record_lsn;

		/* time to update our lastWrite mark */
		privateContext->lastWrite = time(NULL);

		/*
		 * When streaming to a Unix pipe don't forget to also stream the
		 * KEEPALIVE message there, so that the transform process forwards it.
		 */
		if (privateContext->stdOut)
		{
			int ret =
				fwrite(buffer, sizeof(char), buflen, privateContext->out);

			if (ret != buflen)
			{
				log_error("Failed to write JSON message (%ld bytes) to stdout: %m",
						  buflen);
				log_debug("JSON message: %s", buffer);
				return false;
			}
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

	if (!feTimestampDifferenceExceeds(context->lastFeedbackSync,
									  context->now,
									  feedbackInterval))
	{
		return true;
	}

	PGSQL src = { 0 };

	if (!pgsql_init(&src, privateContext->source_pguri, PGSQL_CONN_SOURCE))
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
			  " endpos %X/%X apply %s",
			  LSN_FORMAT_ARGS(context->tracking->written_lsn),
			  LSN_FORMAT_ARGS(context->tracking->flushed_lsn),
			  LSN_FORMAT_ARGS(context->tracking->applied_lsn),
			  LSN_FORMAT_ARGS(context->endpos),
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
	uint64_t elapsed = now - privateContext->lastWrite;

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

		case 'X':
		{
			return STREAM_ACTION_SWITCH;
		}

		case 'K':
		{
			return STREAM_ACTION_KEEPALIVE;
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
 * stream_setup_source_database sets up the source database with a replication
 * slot, a sentinel table, and the target database with a replication origin.
 */
bool
stream_setup_databases(CopyDataSpec *copySpecs,
					   StreamOutputPlugin plugin, char *slotName, char *origin)
{
	uint64_t lsn = 0;

	if (!stream_create_repl_slot(copySpecs, plugin, slotName, &lsn))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_create_sentinel(copySpecs, lsn, InvalidXLogRecPtr))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_create_origin(copySpecs, origin, lsn))
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
	if (!pgsql_init(&src, copySpecs->source_pguri, PGSQL_CONN_SOURCE))
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
	 * Now cleanup the target database (replication origin).
	 */
	if (!pgsql_init(&dst, copySpecs->target_pguri, PGSQL_CONN_TARGET))
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
 * stream_create_repl_slot creates a replication slot on the source database.
 */
bool
stream_create_repl_slot(CopyDataSpec *copySpecs,
						StreamOutputPlugin plugin,
						char *slotName,
						uint64_t *lsn)
{
	PGSQL *pgsql = &(copySpecs->sourceSnapshot.pgsql);

	/*
	 * When using Postgres 9.6, we're using the logical decoding replication
	 * protocol command CREATE_REPLICATION_SLOT to both create the replication
	 * and also export a snapshot.
	 */
	if (copySpecs->sourceSnapshot.exportedCreateSlotSnapshot)
	{
		bool slotExists = false;

		if (!pgsql_init(pgsql, copySpecs->source_pguri, PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_begin(pgsql))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_server_version(pgsql))
		{
			/* errors have already been logged */
			return false;
		}

		log_info("Postgres server version %s (%d)",
				 pgsql->pgversion, pgsql->pgversion_num);

		if (!pgsql_replication_slot_exists(pgsql, slotName, &slotExists, lsn))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_commit(pgsql))
		{
			/* errors have already been logged */
			return false;
		}

		if (!slotExists)
		{
			log_error("Logical replication slot \"%s\" does not exist, "
					  "it is expected to have been created by the replication "
					  "command CREATE_REPLICATION_SLOT when using Postgres %s",
					  slotName,
					  copySpecs->sourceSnapshot.stream.pgsql.pgversion);
			return false;
		}

		return true;
	}

	/*
	 * When --snapshot has been used, open a transaction using that snapshot.
	 */
	if (!IS_EMPTY_STRING_BUFFER(copySpecs->sourceSnapshot.snapshot))
	{
		if (!copydb_set_snapshot(copySpecs))
		{
			/* errors have already been logged */
			log_fatal("Failed to use given --snapshot \"%s\"",
					  copySpecs->sourceSnapshot.snapshot);
			return false;
		}
	}
	else
	{
		if (!pgsql_init(pgsql, copySpecs->source_pguri, PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_begin(pgsql))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * The pgsql_create_replication_slot SQL query text depends on the source
	 * Postgres version, because the "lsn" column used to be named
	 * "xlog_position" in 9.6. Make sure to retrieve the source server version
	 * now.
	 */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	bool slotExists = false;

	if (!pgsql_replication_slot_exists(pgsql, slotName, &slotExists, lsn))
	{
		/* errors have already been logged */
		return false;
	}

	if (slotExists)
	{
		if (!copySpecs->resume)
		{
			log_error("Failed to create replication slot \"%s\": already exists",
					  slotName);
			pgsql_rollback(pgsql);
			return false;
		}

		log_info("Logical replication slot \"%s\" already exists at LSN %X/%X",
				 slotName,
				 LSN_FORMAT_ARGS(*lsn));

		pgsql_commit(pgsql);
		return true;
	}
	else
	{
		if (!pgsql_create_replication_slot(pgsql, slotName, plugin, lsn))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_commit(pgsql))
		{
			/* errors have already been logged */
			return false;
		}

		log_info("Created logical replication slot \"%s\" with plugin \"%s\" "
				 "at LSN %X/%X",
				 slotName,
				 OutputPluginToString(plugin),
				 LSN_FORMAT_ARGS(*lsn));
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

	if (!pgsql_init(&dst, copySpecs->target_pguri, PGSQL_CONN_TARGET))
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

	if (!pgsql_init(pgsql, copySpecs->source_pguri, PGSQL_CONN_SOURCE))
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
			exit(EXIT_CODE_SOURCE);
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
	long size = 0L;

	/*
	 * We need to read the 3 streaming context files that the receive process
	 * prepares when connecting to the source database. Because the catchup
	 * process might get here early, we implement a retry loop in case the
	 * files have not been created yet.
	 */
	ConnectionRetryPolicy retryPolicy = { 0 };

	int maxT = 10;              /* 10s */
	int maxSleepTime = 1000;    /* 1s */
	int baseSleepTime = 100;    /* 100ms */

	(void) pgsql_set_retry_policy(&retryPolicy,
								  maxT,
								  -1, /* unbounded number of attempts */
								  maxSleepTime,
								  baseSleepTime);

	while (!pgsql_retry_policy_expired(&retryPolicy))
	{
		if (file_exists(paths->walsegsizefile) &&
			file_exists(paths->tlifile) &&
			file_exists(paths->tlihistfile))
		{
			/* success: break out of the retry loop */
			break;
		}

		int sleepTimeMs =
			pgsql_compute_connection_retry_sleep_time(&retryPolicy);

		log_debug("stream_read_context: waiting for context files "
				  "to have been created, retrying in %dms",
				  sleepTimeMs);

		/* we have milliseconds, pg_usleep() wants microseconds */
		(void) pg_usleep(sleepTimeMs * 1000);
	}

	/* we don't want to retry anymore, error out if files still don't exist */
	if (!read_file(paths->walsegsizefile, &wal_segment_size, &size))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToUInt(wal_segment_size, WalSegSz))
	{
		/* errors have already been logged */
		return false;
	}

	char *tli;

	if (!read_file(paths->tlifile, &tli, &size))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToUInt(tli, &(system->timeline)))
	{
		/* errors have already been logged */
		return false;
	}

	char *history = NULL;

	if (!read_file(paths->tlihistfile, &history, &size))
	{
		/* errors have already been logged */
		return false;
	}

	if (!parseTimeLineHistory(paths->tlihistfile, history, system))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
