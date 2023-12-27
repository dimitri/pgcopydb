/*
 * src/bin/pgcopydb/follow.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli_common.h"
#include "cli_root.h"
#include "ld_stream.h"
#include "log.h"
#include "progress.h"
#include "signals.h"


/*
 * follow_export_snapshot opens a snapshot that we're going to re-use in all
 * our connections to the source database. When the --snapshot option has been
 * used, instead of exporting a new snapshot, we can just re-use it.
 */
bool
follow_export_snapshot(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	/*
	 * When using logical decoding, we need to create our replication slot and
	 * fetch the snapshot from that logical replication command.
	 */
	char *logrep_pguri = streamSpecs->connStrings->logrep_pguri;

	if (!copydb_create_logical_replication_slot(copySpecs,
												logrep_pguri,
												&(streamSpecs->slot)))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_setup_databases ensures that both source and target databases are
 * setup for logical decoding operations (replication slot, replication origin
 * tracking, pgcopydb.sentinel table).
 */
bool
follow_setup_databases(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	/*
	 * We want to make sure to use a private PGSQL client connection
	 * instance when connecting to the source database now, as the main
	 * connection is currently active holding a snapshot for the whole
	 * process.
	 */
	CopyDataSpec setupSpecs = { 0 };
	TransactionSnapshot snapshot = { 0 };

	/* copy our structure wholesale */
	setupSpecs = *copySpecs;

	/* ensure we use a new snapshot and connection in setupSpecs */
	if (!copydb_copy_snapshot(copySpecs, &snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	setupSpecs.sourceSnapshot = snapshot;

	/*
	 * Now create the replication slot and the pgcopydb sentinel table on
	 * the source database, and the origin (replication progress tracking)
	 * on the target database.
	 */
	if (!stream_setup_databases(&setupSpecs, streamSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_reset_sequences resets the sequences on the target database to match
 * the source database at this very moment (not in any pre-established
 * snapshot). Postgres logical decoding lacks support for syncing sequences.
 *
 * This step is implement as if running the following command:
 *
 *   $ pgcopydb copy sequences --resume --not-consistent
 *
 * The whole idea is to fetch the "new" current values of the sequences, not
 * the ones that were current when the main snapshot was exported.
 */
bool
follow_reset_sequences(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	CopyDataSpec seqSpecs = { 0 };

	/* copy our structure wholesale */
	seqSpecs = *copySpecs;

	/* then force some options such as --resume --not-consistent */
	seqSpecs.restart = false;
	seqSpecs.resume = true;
	seqSpecs.consistent = false;
	seqSpecs.section = DATA_SECTION_SET_SEQUENCES;

	/* we don't want to re-use any snapshot */
	TransactionSnapshot snapshot = { 0 };

	seqSpecs.sourceSnapshot = snapshot;

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&seqSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_copy_all_sequences(&seqSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_init_sentinel sets the sentinel endpos to the command line --endpos
 * option, when given.
 */
bool
follow_init_sentinel(StreamSpecs *specs, CopyDBSentinel *sentinel)
{
	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, specs->connStrings->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	if (specs->endpos != InvalidXLogRecPtr)
	{
		if (!pgsql_update_sentinel_endpos(&pgsql, false, specs->endpos))
		{
			/* errors have already been logged */
			return false;
		}
	}

	if (!pgsql_get_sentinel(&pgsql, sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_commit(&pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * follow_get_sentinel refreshes the given CopyDBSentinel with the current
 * values from the pgcopydb.sentinel table on the source database.
 */
bool
follow_get_sentinel(StreamSpecs *specs, CopyDBSentinel *sentinel, bool verbose)
{
	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, specs->connStrings->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_get_sentinel(&pgsql, sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	/* always accept the startpos and apply values from the sentinel */
	specs->startpos = sentinel->startpos;

	/* the endpos might have changed on the sentinel table */
	if (sentinel->endpos != InvalidXLogRecPtr &&
		sentinel->endpos != specs->endpos)
	{
		specs->endpos = sentinel->endpos;

		log_info("Current sentinel replay_lsn is %X/%X, "
				 "endpos has now been set to %X/%X",
				 LSN_FORMAT_ARGS(sentinel->replay_lsn),
				 LSN_FORMAT_ARGS(sentinel->endpos));
	}
	else if (verbose)
	{
		if (sentinel->endpos != InvalidXLogRecPtr)
		{
			log_info("Current sentinel replay_lsn is %X/%X, endpos is %X/%X",
					 LSN_FORMAT_ARGS(sentinel->replay_lsn),
					 LSN_FORMAT_ARGS(sentinel->endpos));
		}
		else if (sentinel->replay_lsn != InvalidXLogRecPtr)
		{
			log_info("Current sentinel replay_lsn is %X/%X",
					 LSN_FORMAT_ARGS(sentinel->replay_lsn));
		}
	}

	return true;
}


/*
 * follow_main_loop implements the main loop for the follow sub-process
 * management. It loops between two modes of operations:
 *
 *  1. prefetch + catchup
 *  2. live replay using Unix pipes between sub-processes
 *
 * When the catchup process needs to read a file on-disk that does not exist
 * yet, it quits with EXIT_CODE_QUIT (success) and the loop terminate the other
 * subprocesses and switch to the live replay mode of operations.
 *
 * When a sub-process ends abnormally then the main process terminates the
 * sibling worker processes and restart in the other mode.
 *
 * Each time we switch from a mode of operations to another, a catchup from
 * disk is done to ensure we don't miss applying what has already been
 * received.
 */
bool
follow_main_loop(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	/*
	 * Remove the possibly still existing stream context files from
	 * previous round of operations (--resume, etc). We want to make
	 * sure that the catchup process reads the files created on this
	 * connection.
	 */
	if (!stream_cleanup_context(streamSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * In case of successful exit from the follow sub-processes, we
	 * switch back and forth between CATCHUP and REPLAY modes and
	 * continue replaying changes. In case of error, we stop.
	 */
	LogicalStreamMode modeArray[] = {
		STREAM_MODE_CATCHUP,
		STREAM_MODE_REPLAY
	};

	int count = sizeof(modeArray) / sizeof(modeArray[0]);

	uint64_t loop = 0;
	LogicalStreamMode currentMode = modeArray[0];
	LogicalStreamMode previousMode = STREAM_MODE_UNKNOW;

	while (true)
	{
		if (!followDB(copySpecs, streamSpecs))
		{
			log_error("Failed to follow changes from source, "
					  "see above for details");
			return false;
		}

		if (asked_to_quit)
		{
			log_error("Main follow process received SIGQUIT, exiting");
			return false;
		}

		bool done = false;

		if (!follow_reached_endpos(streamSpecs, &done))
		{
			/* errors have already been logged */
			return false;
		}

		if (done)
		{
			log_info("Follow mode is now done, "
					 "reached replay_lsn %X/%X with endpos %X/%X",
					 LSN_FORMAT_ARGS(streamSpecs->sentinel.endpos),
					 LSN_FORMAT_ARGS(streamSpecs->sentinel.replay_lsn));

			return true;
		}

		/* switch to the next mode, increment loop counter */
		previousMode = currentMode;
		currentMode = modeArray[++loop % count];

		/*
		 * Whatever the current/previous mode was, we need to
		 * ensure to catch-up with files on-disk before switching
		 * to another mode of operations.
		 */
		if (!follow_prepare_mode_switch(streamSpecs, previousMode, currentMode))
		{
			/* errors have already been logged */
			return false;
		}

		/* we could have reached endpos in this step: */
		if (!follow_reached_endpos(streamSpecs, &done))
		{
			/* errors have already been logged */
			return false;
		}

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_warn("Main follow process was asked to terminate, exiting");
			return true;
		}

		if (done)
		{
			log_info("Follow mode is now done, "
					 "reached replay_lsn %X/%X with endpos %X/%X",
					 LSN_FORMAT_ARGS(streamSpecs->sentinel.endpos),
					 LSN_FORMAT_ARGS(streamSpecs->sentinel.replay_lsn));

			return true;
		}

		log_info("Restarting logical decoding follower in %s mode",
				 LogicalStreamModeToString(currentMode));

		/* and re-init our streamSpecs for the new mode */
		if (!stream_init_for_mode(streamSpecs, currentMode))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* keep compiler happy */
	log_warn("BUG: follow_main_loop reached out of loop");
	return true;
}


/*
 * follow_reached_endpos sets done to true when endpos has been reached.
 */
bool
follow_reached_endpos(StreamSpecs *streamSpecs, bool *done)
{
	bool verbose = true;
	CopyDBSentinel *sentinel = &(streamSpecs->sentinel);

	if (!follow_get_sentinel(streamSpecs, sentinel, verbose))
	{
		log_error("Failed to get sentinel values");
		return false;
	}

	if (sentinel->endpos != InvalidXLogRecPtr &&
		sentinel->endpos <= sentinel->replay_lsn)
	{
		/* follow_get_sentinel logs replay_lsn and endpos already */
		*done = true;

		log_info("Current endpos %X/%X has been reached at %X/%X",
				 LSN_FORMAT_ARGS(sentinel->endpos),
				 LSN_FORMAT_ARGS(sentinel->replay_lsn));
	}

	return true;
}


/*
 * follow_prepare_mode_switch prepares for the next mode of operation. We need
 * to make sure that all that was streamed in our JSON file has been
 * transformed and replayed from file before changing our mode of operations.
 */
bool
follow_prepare_mode_switch(StreamSpecs *streamSpecs,
						   LogicalStreamMode previousMode,
						   LogicalStreamMode currentMode)
{
	log_info("Catching-up from existing on-disk files");

	if (streamSpecs->system.timeline == 0)
	{
		if (!stream_read_context(&(streamSpecs->paths),
								 &(streamSpecs->system),
								 &(streamSpecs->WalSegSz)))
		{
			log_error("Failed to read the streaming context information "
					  "from the source database, see above for details");
			return false;
		}
	}

	/*
	 * If the previous mode was catch-up, then before proceeding, we might need
	 * to empty the transform queue where the STOP message was sent.
	 */
	if (previousMode == STREAM_MODE_CATCHUP)
	{
		Queue *transformQueue = &(streamSpecs->transformQueue);
		QueueStats qStats = { 0 };

		if (!queue_stats(transformQueue, &qStats))
		{
			log_error("Failed to get the transform queue stats, "
					  "see above for details");
			return false;
		}

		if (qStats.msg_qnum > 0)
		{
			log_notice("Processing %lld messages from the transform queue",
					   (long long) qStats.msg_qnum);

			FollowSubProcess *transform = &(streamSpecs->transform);

			if (!follow_start_subprocess(streamSpecs, transform))
			{
				log_error("Failed to start the transform process");
				return false;
			}

			if (!follow_wait_subprocesses(streamSpecs))
			{
				log_error("Failed to transform %lld messages from the queue, "
						  "see above for details",
						  (long long) qStats.msg_qnum);
				return false;
			}
		}
	}

	/*
	 * Then catch-up with what's been stream and transformed already, which
	 * means replaying the files that have already been prepared on-disk.
	 */
	LogicalStreamMode mode = streamSpecs->mode;
	streamSpecs->mode = STREAM_MODE_CATCHUP;

	FollowSubProcess *catchup = &(streamSpecs->catchup);

	if (!follow_start_subprocess(streamSpecs, catchup))
	{
		streamSpecs->mode = mode;
		log_error("Failed to start the %s process", catchup->name);
		return false;
	}

	if (!follow_wait_subprocesses(streamSpecs))
	{
		streamSpecs->mode = mode;
		log_error("Failed to catchup with on-disk files, "
				  "see above for details");
		return false;
	}

	/* re-install the streamSpecs->mode as it was before getting there */
	streamSpecs->mode = mode;

	return true;
}


/*
 * followDB implements a logical decoding client for streaming changes from the
 * source database into the target database.
 *
 * The source database is expected to have been setup already so that the
 * replication slot using wal2json is ready, the pgcopydb.sentinel table
 * exists, and the target database replication origin has been created too.
 */
bool
followDB(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	int errors = 0;

	if (streamSpecs->mode < STREAM_MODE_PREFETCH)
	{
		log_error("BUG: followDB with stream mode %d", streamSpecs->mode);
		return false;
	}

	/*
	 * Before starting sub-processes, clean-up intermediate files from previous
	 * round. Here that's the stream context with WAL segment size and timeline
	 * history, which are fetched from the source server to compute WAL file
	 * names. The current timeline can only change at a server restart or a
	 * failover, both with trigger a reconnect.
	 */
	if (!stream_cleanup_context(streamSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Prepare the sub-process communication mechanisms, when needed:
	 *
	 *   - pgcopydb stream receive --to-stdout
	 *   - pgcopydb stream transform - -
	 *   - pgcopydb stream apply -
	 *   - pgcopydb stream replay
	 */
	if (streamSpecs->stdOut)
	{
		if (pipe(streamSpecs->pipe_rt) != 0)
		{
			log_fatal("Failed to create a pipe for streaming: %m");
			return false;
		}
	}

	if (streamSpecs->stdIn)
	{
		if (pipe(streamSpecs->pipe_ta) != 0)
		{
			log_fatal("Failed to create a pipe for streaming: %m");
			return false;
		}
	}

	FollowSubProcess *prefetch = &(streamSpecs->prefetch);
	FollowSubProcess *transform = &(streamSpecs->transform);
	FollowSubProcess *catchup = &(streamSpecs->catchup);

	/*
	 * When set to prefetch changes, we always also run the transform process
	 * to prepare the SQL files from the JSON files. Also upper modes (catchup,
	 * replay) does imply prefetching (and transform).
	 */
	if (streamSpecs->mode >= STREAM_MODE_PREFETCH)
	{
		if (!follow_start_subprocess(streamSpecs, prefetch))
		{
			log_error("Failed to start the %s process", prefetch->name);
			return false;
		}

		if (!follow_start_subprocess(streamSpecs, transform))
		{
			log_error("Failed to start the transform process");

			(void) follow_exit_early(streamSpecs);
			return false;
		}
	}

	/*
	 * When set to catchup or replay mode, we also start the catchup process.
	 */
	if (streamSpecs->mode >= STREAM_MODE_CATCHUP)
	{
		if (!follow_start_subprocess(streamSpecs, catchup))
		{
			log_error("Failed to start the %s process", catchup->name);

			(void) follow_exit_early(streamSpecs);
			return false;
		}
	}

	/*
	 * Close pipe ends which follow is not using. Otherwise the processes
	 * like transform and apply which reads from the pipe during replay
	 * will never see EOF.
	 */
	if (streamSpecs->stdOut)
	{
		close_fd_or_exit(streamSpecs->pipe_rt[1]);
		close_fd_or_exit(streamSpecs->pipe_rt[0]);
	}

	if (streamSpecs->stdIn)
	{
		close_fd_or_exit(streamSpecs->pipe_ta[0]);
		close_fd_or_exit(streamSpecs->pipe_ta[1]);
	}

	/*
	 * Finally wait until the process are finished.
	 *
	 * This happens when the sentinel endpos is set, typically using the
	 * command: pgcopydb stream sentinel set endpos --current.
	 */
	if (follow_wait_subprocesses(streamSpecs))
	{
		log_info("Subprocesses for %s, %s, and %s have now all exited",
				 prefetch->name,
				 transform->name,
				 catchup->name);
	}
	else
	{
		++errors;
		log_error("Some sub-process exited with errors, "
				  "see above for details");
	}

	return errors == 0;
}


/*
 * follow_start_prefetch starts a sub-process that prefetches changes from the
 * source database into local files.
 */
bool
follow_start_prefetch(StreamSpecs *specs)
{
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		/* arrange to write to the receive-transform pipe */
		specs->stdIn = false;
		specs->stdOut = true;

		specs->out = fdopen(specs->pipe_rt[1], "a");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_ta[0]);
		close_fd_or_exit(specs->pipe_ta[1]);

		/* switch out stream from block buffered to line buffered mode */
		if (setvbuf(specs->out, NULL, _IOLBF, 0) != 0)
		{
			log_error("Failed to set out stream to line buffered mode: %m");
			return false;
		}

		bool success = startLogicalStreaming(specs);

		close_fd_or_exit(specs->pipe_rt[1]);

		log_info("Prefetch process has terminated");

		return success;
	}
	else
	{
		specs->stdIn = false;
		specs->stdOut = false;

		bool success = startLogicalStreaming(specs);

		log_info("Prefetch process has terminated");

		return success;
	}

	return true;
}


/*
 * follow_start_transform creates a sub-process that transform JSON files into
 * SQL files as needed, consuming requests from a queue.
 */
bool
follow_start_transform(StreamSpecs *specs)
{
	/*
	 * In replay mode, the JSON messages are read from stdin, which we
	 * now setup to be a pipe between prefetch and transform processes;
	 * and the SQL commands are written to stdout which we setup to be
	 * a pipe between the transform and apply processes.
	 */
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		/*
		 * Arrange to read from receive-transform pipe and write to the
		 * transform-apply pipe.
		 */
		specs->stdIn = true;
		specs->stdOut = true;

		specs->in = fdopen(specs->pipe_rt[0], "r");
		specs->out = fdopen(specs->pipe_ta[1], "a");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[1]);
		close_fd_or_exit(specs->pipe_ta[0]);

		/* switch out stream from block buffered to line buffered mode */
		if (setvbuf(specs->out, NULL, _IOLBF, 0) != 0)
		{
			log_error("Failed to set out stream to line buffered mode: %m");
			return false;
		}

		bool success = stream_transform_stream(specs);

		log_info("Transform process has terminated");

		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_ta[1]);

		return success;
	}
	else
	{
		/*
		 * In other modes of operations (RECEIVE, CATCHUP) we start a
		 * transform worker process that reads LSN positions from an
		 * internal message queue and batch processes one file at a
		 * time.
		 */
		specs->stdIn = false;
		specs->stdOut = false;

		bool success = stream_transform_worker(specs);

		log_info("Transform process has terminated");

		return success;
	}

	return true;
}


/*
 * follow_start_catchup starts a sub-process that catches-up using the SQL
 * files that have been prepared by the prefetch process.
 */
bool
follow_start_catchup(StreamSpecs *specs)
{
	/*
	 * In replay mode, the SQL command are read from stdin.
	 */
	if (specs->mode == STREAM_MODE_REPLAY)
	{
		/* arrange to read from the transform-apply pipe */
		specs->stdIn = true;
		specs->stdOut = false;

		specs->in = fdopen(specs->pipe_ta[0], "r");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_rt[1]);
		close_fd_or_exit(specs->pipe_ta[1]);

		bool success = stream_apply_replay(specs);

		log_info("Apply process has terminated");

		close_fd_or_exit(specs->pipe_ta[0]);

		return success;
	}
	else
	{
		/*
		 * In other modes of operations (CATCHUP, really, here), we
		 * start the file based catchup mechanism, which follows the
		 * current LSN on the target database origin tracking system to
		 * open the right SQL file and apply statements from there.
		 */
		specs->stdIn = false;
		specs->stdOut = false;

		bool success = stream_apply_catchup(specs);

		log_info("Apply process has terminated");

		return success;
	}

	return true;
}


/*
 * follow_start_subprocess forks a subprocess and calls the given function.
 */
bool
follow_start_subprocess(StreamSpecs *specs, FollowSubProcess *subprocess)
{
	/* make sure to re-init the structure dynamic fields */
	subprocess->pid = -1;
	subprocess->exited = false;

	/*
	 * Flush stdio channels just before fork, to avoid double-output
	 * problems.
	 */
	fflush(stdout);
	fflush(stderr);

	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork %s subprocess: %m", subprocess->name);
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			char psTitle[BUFSIZE] = { 0 };

			sformat(psTitle, sizeof(psTitle), "pgcopydb: follow %s",
					subprocess->name);

			(void) set_ps_title(psTitle);

			log_notice("Starting the %s sub-process", subprocess->name);

			if (!(subprocess->command)(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			subprocess->pid = fpid;
			return true;
		}
	}

	return true;
}


/*
 * follow_exit_early exits early, typically used when a process fails to start
 * and other processes where started already.
 */
void
follow_exit_early(StreamSpecs *specs)
{
	log_debug("follow_exit_early");

	if (!follow_terminate_subprocesses(specs))
	{
		log_error("Failed to terminate other subprocesses, "
				  "see above for details");
	}

	if (!follow_wait_subprocesses(specs))
	{
		log_error("Some sub-process exited in error, "
				  "see above for details");
	}
}


/*
 * follow_wait_subprocesses waits until both sub-processes are finished.
 */
bool
follow_wait_subprocesses(StreamSpecs *specs)
{
	FollowSubProcess *processArray[] = {
		&(specs->prefetch),
		&(specs->transform),
		&(specs->catchup)
	};

	int count = sizeof(processArray) / sizeof(processArray[0]);

	bool success = true;
	int stillRunning = count;

	/* now the main loop, that waits until all given processes have exited */
	while (stillRunning > 0)
	{
		if (asked_to_quit)
		{
			if (!follow_terminate_subprocesses(specs))
			{
				log_error("Failed to terminate other subprocesses, "
						  "see above for details");
				return false;
			}
		}

		/* re-init stillRunning at each iteration */
		stillRunning = count;

		for (int i = 0; i < count; i++)
		{
			/* skip already exited sub-processes, and not started ones too */
			if (processArray[i]->pid <= 0 || processArray[i]->exited)
			{
				--stillRunning;
				continue;
			}

			/* follow_wait_pid is non-blocking: uses WNOHANG */
			if (!follow_wait_pid(processArray[i]->pid,
								 &(processArray[i]->exited),
								 &(processArray[i]->returnCode),
								 &(processArray[i]->sig)))
			{
				/* errors have already been logged */
				return false;
			}

			if (processArray[i]->exited)
			{
				--stillRunning;

				int logLevel = LOG_NOTICE;
				char details[BUFSIZE] = { 0 };

				if (processArray[i]->returnCode == 0)
				{
					if (processArray[i]->sig == 0)
					{
						sformat(details, sizeof(details), "successfully");
					}
					else
					{
						sformat(details, sizeof(details),
								"successfully after signal %s",
								signal_to_string(processArray[i]->sig));
					}
				}
				else
				{
					logLevel = LOG_ERROR;

					if (processArray[i]->sig == 0)
					{
						sformat(details, sizeof(details), "with error code %d",
								processArray[i]->returnCode);
					}
					else
					{
						sformat(details, sizeof(details),
								"with error code %d and signal %s",
								processArray[i]->returnCode,
								signal_to_string(processArray[i]->sig));
					}
				}

				log_level(logLevel,
						  "Subprocess %s with pid %d has exited %s",
						  processArray[i]->name,
						  processArray[i]->pid,
						  details);

				/*
				 * When one sub-process has exited abnormally, we terminate all
				 * the other sub-processes to handle the problem at the caller.
				 *
				 * When a sub-process exits with a successful returnCode, it
				 * might be because it has reached specs->endpos already: in
				 * that case let the other processes reach it too.
				 *
				 * Otherwise there is no reason for the other processes to
				 * stop, and we're missing one: terminate every one and handle
				 * at the caller.
				 *
				 * We need to first update current sentinel values (endpos).
				 */
				if (!follow_get_sentinel(specs, &(specs->sentinel), false))
				{
					/* continue without updated endpos */
					log_warn("Failed to get sentinel values");
				}

				if (processArray[i]->returnCode != 0 ||
					specs->endpos == InvalidXLogRecPtr)
				{
					char endposStatus[BUFSIZE] = { 0 };

					if (specs->endpos == InvalidXLogRecPtr)
					{
						sformat(endposStatus, sizeof(endposStatus), "unset");
					}
					else
					{
						sformat(endposStatus, sizeof(endposStatus),
								"set to %X/%X",
								LSN_FORMAT_ARGS(specs->endpos));
					}

					log_notice("Process %s has exited with return code %d, "
							   "and endpos is %s: "
							   "terminating other processes",
							   processArray[i]->name,
							   processArray[i]->returnCode,
							   endposStatus);

					if (!follow_terminate_subprocesses(specs))
					{
						log_error("Failed to terminate other subprocesses, "
								  "see above for details");
						return false;
					}
				}

				success = success && processArray[i]->returnCode == 0;
			}
		}

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
	}

	return success;
}


/*
 * follow_terminate_subprocesses is used in case of errors in one sub-process
 * to signal the other ones to quit early.
 */
bool
follow_terminate_subprocesses(StreamSpecs *specs)
{
	FollowSubProcess *processArray[] = {
		&(specs->prefetch),
		&(specs->transform),
		&(specs->catchup)
	};
	int count = sizeof(processArray) / sizeof(processArray[0]);

	/* signal the processes to exit */
	for (int i = 0; i < count; i++)
	{
		if (processArray[i]->pid <= 0 || processArray[i]->exited)
		{
			continue;
		}

		log_notice("kill -TERM %d (%s)",
				   processArray[i]->pid,
				   processArray[i]->name);

		if (kill(processArray[i]->pid, SIGTERM) != 0)
		{
			/* process might have exited on its own already */
			if (errno != ESRCH)
			{
				log_error("Failed to signal %s process %d: %m",
						  processArray[i]->name,
						  processArray[i]->pid);

				return false;
			}
		}
	}

	return true;
}


/*
 * follow_wait_pid waits for a given known sub-process.
 */
bool
follow_wait_pid(pid_t subprocess, bool *exited, int *returnCode, int *sig)
{
	int status = 0;

	if (subprocess <= 0)
	{
		log_error("BUG: follow_wait_pid called with subprocess %d", subprocess);
		return false;
	}

	int pid = waitpid(subprocess, &status, WNOHANG);

	switch (pid)
	{
		case -1:
		{
			if (errno == ECHILD)
			{
				/* no more childrens */
				*sig = 0;
				*exited = true;
				*returnCode = -1;
				return true;
			}
			else
			{
				log_warn("Failed to call waitpid(): %m");
				return false;
			}

			break;
		}

		case 0:
		{
			/*
			 * We're using WNOHANG, 0 means there are no stopped or
			 * exited children, it's all good.
			 */
			*sig = 0;
			*exited = false;
			*returnCode = -1;
			break;
		}

		default:
		{
			/* sub-process has finished now */
			if (pid != subprocess)
			{
				log_error("BUG: waitpid on %d returned %d", subprocess, pid);
				return false;
			}

			*sig = 0;
			*exited = true;
			*returnCode = WEXITSTATUS(status);

			if (WIFSIGNALED(status))
			{
				*sig = WTERMSIG(status);
			}

			break;
		}
	}

	return true;
}
