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


/*
 * stream_init_specs initializes Change Data Capture streaming specifications
 * from a copyDBSpecs structure.
 */
bool
stream_init_specs(StreamSpecs *specs,
				  CDCPaths *paths,
				  char *source_pguri,
				  char *target_pguri,
				  char *slotName,
				  char *origin,
				  uint64_t endpos,
				  LogicalStreamMode mode)
{
	/* just copy into StreamSpecs what's been initialized in copySpecs */
	specs->mode = mode;
	specs->paths = *paths;
	specs->endpos = endpos;

	strlcpy(specs->source_pguri, source_pguri, MAXCONNINFO);
	strlcpy(specs->target_pguri, target_pguri, MAXCONNINFO);
	strlcpy(specs->slotName, slotName, sizeof(specs->slotName));
	strlcpy(specs->origin, origin, sizeof(specs->origin));

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
		.count = 6,
		.keywords = {
			"format-version",
			"include-xids",
			"include-lsn",
			"include-transaction",
			"include-timestamp",
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

	/* prepare the stream options */
	LogicalStreamClient stream = { 0 };

	stream.pluginOptions = options;
	stream.writeFunction = &streamWrite;
	stream.flushFunction = &streamFlush;
	stream.closeFunction = &streamClose;
	stream.feedbackFunction = &streamFeedback;

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

	privateContext.mode = specs->mode;
	privateContext.paths = specs->paths;
	privateContext.startpos = specs->startpos;

	strlcpy(privateContext.source_pguri,
			specs->source_pguri,
			sizeof(privateContext.source_pguri));

	context.private = (void *) &(privateContext);

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

		specs->startpos = latest->nextlsn;

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

	/* we might have to rotate to the next on-disk file */
	if (!streamRotateFile(context))
	{
		/* errors have already been logged */
		return false;
	}

	LogicalMessageMetadata *metadata = &(privateContext->metadata);
	JSON_Value *json = json_parse_string(context->buffer);

	/* ensure we have a new all-zero metadata structure for the new message */
	(void) memset(metadata, 0, sizeof(LogicalMessageMetadata));

	if (!parseMessageMetadata(metadata, context->buffer, json, false))
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

	(void) updateStreamCounters(privateContext, metadata);

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

	log_debug("Received action %c for XID %u in LSN %X/%X, Next LSN %X/%X",
			  metadata->action,
			  metadata->xid,
			  LSN_FORMAT_ARGS(metadata->lsn),
			  LSN_FORMAT_ARGS(metadata->nextlsn));

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

	/* compute the WAL filename that would host the current LSN */
	XLByteToSeg(context->cur_record_lsn, segno, context->WalSegSz);
	XLogFileName(wal, context->timeline, segno, context->WalSegSz);

	sformat(walFileName, sizeof(walFileName), "%s/%s.json",
			privateContext->paths.dir,
			wal);

	/* in most cases, the file name is still the same */
	if (strcmp(privateContext->walFileName, walFileName) == 0)
	{
		return true;
	}

	/* if we had a WAL file opened, close it now */
	if (!IS_EMPTY_STRING_BUFFER(privateContext->walFileName) &&
		privateContext->jsonFile != NULL)
	{
		bool time_to_abort = false;

		/*
		 * Here we might have an early WAL file rotation, either because of
		 * archive_timeout or a call to pg_switch_wal() for instance. In case
		 * the current message nextlsn doesn't belong to the new file we're
		 * about to create, add an extra empty transaction with the expected
		 * nextlsn.
		 */
		if (privateContext->metadata.nextlsn < context->cur_record_lsn)
		{
			fformat(privateContext->jsonFile,
					"{\"action\":\"X\",\"lsn\":\"%X/%X\",\"nextlsn\":\"%X/%X\"}\n",
					LSN_FORMAT_ARGS(privateContext->metadata.nextlsn),
					LSN_FORMAT_ARGS(context->cur_record_lsn));

			log_debug("Inserted action SWITCH for nextlsn %X/%X",
					  LSN_FORMAT_ARGS(context->cur_record_lsn));
		}

		if (!streamCloseFile(context, time_to_abort))
		{
			/* errors have already been logged */
			return false;
		}
	}

	log_info("Now streaming changes to \"%s\"", walFileName);
	strlcpy(privateContext->walFileName, walFileName, MAXPGPATH);

	/* when dealing with a new JSON name, also prepare the SQL name */
	sformat(privateContext->sqlFileName, sizeof(privateContext->sqlFileName),
			"%s/%s.sql",
			privateContext->paths.dir,
			wal);

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

	sformat(latest, sizeof(latest), "%s/latest", privateContext->paths.dir);

	if (file_exists(latest))
	{
		if (!unlink_file(latest))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!create_symbolic_link(privateContext->walFileName, latest))
	{
		/* errors have already been logged */
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

	if (privateContext->jsonFile == NULL)
	{
		return true;
	}

	log_debug("Closing file \"%s\"", privateContext->walFileName);

	if (fclose(privateContext->jsonFile) != 0)
	{
		log_error("Failed to close file \"%s\": %m",
				  privateContext->walFileName);
		return false;
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
		{
			/*
			 * Now is the time to transform the JSON file into SQL.
			 *
			 * The transformation of the file uses enough CPU that we'd prefer
			 * to start it in a subprocess.
			 */
			if (!streamTransformFileInSubprocess(context))
			{
				/* errors have already been logged */
				return false;
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
				if (!streamWaitForSubprocess(context))
				{
					/* errors have already been logged */
					return false;
				}
			}

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

	int feedbackInterval = 10 * 1000; /* a minute */

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

	context->endpos = privateContext->endpos;
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

		/* message entries {action: "M"} do not have xid, lsn, nextlsn fields */
		if (metadata->action == STREAM_ACTION_MESSAGE)
		{
			log_debug("Skipping message: %s", buffer);
			return true;
		}
	}

	if (metadata->action != STREAM_ACTION_SWITCH)
	{
		double xid = json_object_get_number(jsobj, "xid");
		metadata->xid = (uint32_t) xid;
	}

	char *lsn = (char *) json_object_get_string(jsobj, "lsn");

	if (lsn == NULL)
	{
		log_error("Failed to parse JSON message LSN: \"%s\"", buffer);
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
			log_error("Failed to parse Next LSN \"%s\"", nextlsn);
			return false;
		}
	}

	char *timestamp = (char *) json_object_get_string(jsobj, "timestamp");

	if (timestamp != NULL)
	{
		size_t n = sizeof(metadata->timestamp);

		if (strlcpy(metadata->timestamp, timestamp, n) >= n)
		{
			log_error("Failed to parse JSON message timestamp value \"%s\" "
					  "which is %lu bytes long, pgcopydb only support timestamps "
					  "up to %lu bytes",
					  timestamp,
					  strlen(timestamp),
					  sizeof(metadata->timestamp));
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

		case 'X':
		{
			return STREAM_ACTION_SWITCH;
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
 * streamTransformFileInSubprocess creates an auxiliary process to run the
 * stream_transform_file() function on the just closed JSON file.
 */
bool
streamTransformFileInSubprocess(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/*
	 * First, wait any already started subprocess.
	 *
	 * The assumption here is that by the time we have received another WAL
	 * size content of JSON messages, the transform subprocess should be
	 * finished already.
	 */
	if (!streamWaitForSubprocess(context))
	{
		/* errors have already been logged */
		return false;
	}

	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to transform "
					  "JSON file \"%s\" into SQL: %m",
					  privateContext->walFileName);
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			if (!stream_transform_file(privateContext->walFileName,
									   privateContext->sqlFileName))
			{
				/* errors have already been logged */
				return false;
			}

			/* and we're done */
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			privateContext->subprocess = fpid;
			log_info("Starting subprocess %d to prepare \"%s\"",
					 fpid,
					 privateContext->sqlFileName);
			break;
		}
	}

	return true;
}


/*
 * streamWaitForSubprocess calls waitpid() and blocks until the current
 * subprocess is reported terminated by the Operating System.
 */
bool
streamWaitForSubprocess(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;

	/* if a subprocess had been started before, wait until it's done. */
	if (privateContext->subprocess > 0)
	{
		int status = 0;

		if (waitpid(privateContext->subprocess, &status, 0) == -1)
		{
			log_error("Failed to wait for pid %d: %m",
					  privateContext->subprocess);
			return false;
		}

		if (WEXITSTATUS(status) != 0)
		{
			log_error("Failed to transform previous JSON file into SQL, "
					  "see above for details");
			return false;
		}

		log_debug("Transform subprocess %d exited successfully [%d]",
				  privateContext->subprocess,
				  status);

		privateContext->subprocess = 0;
	}

	return true;
}


/*
 * stream_setup_source_database sets up the source database with a replication
 * slot, a sentinel table, and the target database with a replication origin.
 */
bool
stream_setup_databases(CopyDataSpec *copySpecs, char *slotName, char *origin)
{
	uint64_t lsn = 0;

	if (!stream_create_repl_slot(copySpecs, slotName, &lsn))
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
stream_create_repl_slot(CopyDataSpec *copySpecs, char *slotName, uint64_t *lsn)
{
	PGSQL *pgsql = &(copySpecs->sourceSnapshot.pgsql);

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
		if (!pgsql_create_replication_slot(pgsql,
										   slotName,
										   REPLICATION_PLUGIN,
										   lsn))
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
				 REPLICATION_PLUGIN,
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
stream_read_context(StreamSpecs *specs,
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

	(void) pgsql_set_retry_policy(&retryPolicy,
								  CATCHINGUP_SLEEP_MS,
								  -1, /* unbounded number of attempts */
								  CATCHINGUP_SLEEP_MS / 1000,
								  CATCHINGUP_SLEEP_MS / 1000);

	while (!pgsql_retry_policy_expired(&retryPolicy))
	{
		if (file_exists(specs->paths.walsegsizefile) &&
			file_exists(specs->paths.tlifile) &&
			file_exists(specs->paths.tlihistfile))
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
	if (!read_file(specs->paths.walsegsizefile, &wal_segment_size, &size))
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

	if (!read_file(specs->paths.tlifile, &tli, &size))
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

	if (!read_file(specs->paths.tlihistfile, &history, &size))
	{
		/* errors have already been logged */
		return false;
	}

	if (!parseTimeLineHistory(specs->paths.tlihistfile, history, system))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
