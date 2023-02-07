/*
 * src/bin/pgcopydb/ld_replay.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parson.h"

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "ld_stream.h"
#include "log.h"
#include "parsing_utils.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"


/*
 * stream_replay sets 3 sub-processes up to implement "live replay" of the
 * changes from the source database directly to the target database.
 *
 * The process is split three-ways and sub-processes then communicate data
 * using a unix pipe mechanism, as if running the following synthetic command
 * line:
 *
 *    pgcopydb stream receive --to-stdout
 *  | pgcopydb stream transform - -
 *  | pgcopydb stream apply --from-stdin
 *
 */
bool
stream_replay(StreamSpecs *specs)
{
	if (specs->mode != STREAM_MODE_REPLAY)
	{
		log_fatal("BUG: stream_replay called with specs->mode %d", specs->mode);
		return false;
	}

	log_error("pgcopydb stream replay is not implemented yet");
	return false;
}


/*
 * stream_apply_replay implements "live replay" of the changes from the source
 * database directly to the target database.
 */
bool
stream_apply_replay(StreamSpecs *specs)
{
	StreamApplyContext context = { 0 };

	if (specs->mode == STREAM_MODE_REPLAY)
	{
		log_error("BUG: stream_apply_replay called with specs->mode %d",
				  specs->mode);
		return false;
	}

	if (!specs->stdin)
	{
		log_error("BUG: stream_apply_replay requires specs->stdin");
		return false;
	}

	/*
	 * Even though we're using the "live streaming" mode here, ensure that
	 * we're good to: the pgcyopdb sentinel table is expected to have allowed
	 * applying changes.
	 */
	if (!stream_apply_wait_for_sentinel(specs, &context))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stream_read_context(&(specs->paths),
							 &(context.system),
							 &(context.WalSegSz)))
	{
		log_error("Failed to read the streaming context information "
				  "from the source database, see above for details");
		return false;
	}

	log_debug("Source database wal_segment_size is %u", context.WalSegSz);
	log_debug("Source database timeline is %d", context.system.timeline);

	if (!setupReplicationOrigin(&context,
								&(specs->paths),
								specs->source_pguri,
								specs->target_pguri,
								specs->origin,
								specs->endpos,
								context.apply))
	{
		log_error("Failed to setup replication origin on the target database");
		return false;
	}

	log_info("Replaying changes from LSN %X/%X",
			 LSN_FORMAT_ARGS(context.previousLSN));

	if (context.endpos != InvalidXLogRecPtr)
	{
		log_info("Stopping at endpos LSN %X/%X",
				 LSN_FORMAT_ARGS(context.endpos));
	}

	/* count lines */
	uint64_t lineno = 0;

	int countFdsReadyToRead, nfds; /* see man select(2) */
	fd_set readFileDescriptorSet;

	int fd = fileno(stdin);
	nfds = fd + 1;

	bool doneReading = false;

	while (!doneReading)
	{
		struct timeval timeout = { 0, 100 * 1000 }; /* 100 ms */

		/*
		 * When asked_to_stop || asked_to_stop_fast still continue reading
		 * through EOF on the input stream, then quit normally.
		 */
		if (asked_to_quit)
		{
			return false;
		}

		FD_ZERO(&readFileDescriptorSet);
		FD_SET(fd, &readFileDescriptorSet);

		countFdsReadyToRead =
			select(nfds, &readFileDescriptorSet, NULL, NULL, &timeout);

		if (countFdsReadyToRead == -1)
		{
			if (errno == EINTR || errno == EAGAIN)
			{
				continue;
			}
			else
			{
				log_error("Failed to select on file descriptor %d: %m", fd);
				return false;
			}
		}

		if (countFdsReadyToRead == 0)
		{
			continue;
		}

		/*
		 * data is expected to be written one line at a time, if any data is
		 * available per select(2) call, then we should be able to read an
		 * entire line now.
		 */
		if (FD_ISSET(fd, &readFileDescriptorSet))
		{
			log_debug("Replay process reading from input stream %d", fd);

			/*
			 * Allocate a 128 MB string to receive data from the input stream.
			 *
			 * TODO: That's a large allocation but should allow us to accept
			 * logical decoding traffic. The general case would be to loop over
			 * memory chunks and then split and re-join chunks into JSON lines,
			 * or use a streaming JSON parsing API.
			 */
			size_t s = 128 * 1024 * 1024;
			char *buf = calloc(s + 1, sizeof(char));
			size_t bytes = read(fd, buf, s);

			if (bytes == -1)
			{
				log_error("Failed to read from input stream: %m");
				free(buf);
				return false;
			}
			else if (bytes == 0)
			{
				free(buf);
				doneReading = true;
				continue;
			}
			else if (bytes == s)
			{
				char bytesPretty[BUFSIZE] = { 0 };

				(void) pretty_print_bytes(bytesPretty, BUFSIZE, bytes);

				log_error("Failed to read from input stream, message is larger "
						  "than pgcopydb limit: %s",
						  bytesPretty);
				return false;
			}

			log_debug("stream_apply_replay read %lld bytes from input",
					  (long long) bytes);

			char *lines[BUFSIZE] = { 0 };
			int lineCount = splitLines(buf, lines, BUFSIZE);

			log_debug("stream_apply_replay read %d lines", lineCount);

			for (int i = 0; i < lineCount; i++)
			{
				if (asked_to_quit)
				{
					free(buf);
					return false;
				}

				const char *sql = lines[i];

				/* we count stream input lines as if reading from a file */
				++lineno;

				LogicalMessageMetadata metadata = { 0 };

				if (!parseSQLAction(sql, &metadata))
				{
					/* errors have already been logged */
					free(buf);
					return false;
				}

				if (!stream_apply_sql(&context, &metadata, sql))
				{
					/* errors have already been logged */
					free(buf);
					return false;
				}

				/* update progres on source database when needed */
				switch (metadata.action)
				{
					case STREAM_ACTION_COMMIT:
					case STREAM_ACTION_SWITCH:
					case STREAM_ACTION_KEEPALIVE:
					{
						(void) stream_apply_sync_sentinel(&context);
						break;
					}

					default:
					{
						/* skip BEGIN and DML commands here */
						break;
					}
				}

				/*
				 * When syncing with the pgcopydb sentinel we might receive a
				 * new endpos, and it might mean we're done already.
				 */
				if (!context.reachedEndPos &&
					context.endpos != InvalidXLogRecPtr &&
					context.endpos <= context.previousLSN)
				{
					context.reachedEndPos = true;

					log_info("Applied reached end position %X/%X at %X/%X",
							 LSN_FORMAT_ARGS(context.endpos),
							 LSN_FORMAT_ARGS(context.previousLSN));
				}

				if (context.reachedEndPos)
				{
					/* information has already been logged */
					break;
				}
			}

			free(buf);
		}

		doneReading = feof(stdin) != 0;
	}

	/* we might still have to disconnect now */
	(void) pgsql_finish(&(context.pgsql));

	log_notice("Replayed %lld messages", (long long) lineno);

	return true;
}
