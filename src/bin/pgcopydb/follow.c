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
#include "signals.h"


/*
 * follow_export_snapshot opens a snapshot that we're going to re-use in all
 * our connections to the source database. When the --snapshot option has been
 * used, instead of exporting a new snapshot, we can just re-use it.
 */
bool
follow_export_snapshot(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	TransactionSnapshot *snapshot = &(copySpecs->sourceSnapshot);
	PGSQL *pgsql = &(snapshot->pgsql);

	if (!pgsql_init(pgsql, copySpecs->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * When using PostgreSQL 9.6 logical decoding, we need to create our
	 * replication slot and fetch the snapshot from that logical replication
	 * command, it's the only way.
	 */
	if (pgsql->pgversion_num < 100000)
	{
		if (!copydb_create_logical_replication_slot(copySpecs,
													streamSpecs->logrep_pguri,
													streamSpecs->plugin,
													streamSpecs->slotName))
		{
			/* errors have already been logged */
			return false;
		}
	}
	else
	{
		if (!copydb_prepare_snapshot(copySpecs))
		{
			/* errors have already been logged */
			return false;
		}
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
	if (!stream_setup_databases(&setupSpecs,
								streamSpecs->plugin,
								streamSpecs->slotName,
								streamSpecs->origin))
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


	/*
	 * Once the sub-processes have exited, it's time to clean-up the shared
	 * resources used for communication purposes.
	 */
	switch (streamSpecs->mode)
	{
		case STREAM_MODE_PREFETCH:
		case STREAM_MODE_CATCHUP:
		{
			/*
			 * Clean-up the message queue used to communicate between prefetch
			 * and catch-up sub-processes.
			 */
			if (!queue_unlink(&(streamSpecs->transformQueue)))
			{
				/* errors have already been logged */
				log_warn("HINT: use ipcrm -q %d to remove the queue",
						 streamSpecs->transformQueue.qId);
				return false;
			}
			break;
		}

		case STREAM_MODE_REPLAY:
		{
			/* no cleanup to do here */
			break;
		}

		case STREAM_MODE_UNKNOW:
		case STREAM_MODE_RECEIVE:
		default:
		{
			log_error("BUG: followDB cleanup section reached in mode %d",
					  streamSpecs->mode);
			return false;
		}
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
		specs->out = fdopen(specs->pipe_rt[1], "a");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_ta[0]);
		close_fd_or_exit(specs->pipe_ta[1]);
	}

	bool success = startLogicalStreaming(specs);

	close_fd_or_exit(specs->pipe_rt[1]);

	return success;
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
		specs->in = fdopen(specs->pipe_rt[0], "r");
		specs->out = fdopen(specs->pipe_ta[1], "a");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[1]);
		close_fd_or_exit(specs->pipe_ta[0]);

		bool success = stream_transform_stream(specs);

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
		return stream_transform_worker(specs);
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
		specs->in = fdopen(specs->pipe_ta[0], "r");

		/* close pipe ends we're not using */
		close_fd_or_exit(specs->pipe_rt[0]);
		close_fd_or_exit(specs->pipe_rt[1]);
		close_fd_or_exit(specs->pipe_ta[1]);

		bool success = stream_apply_replay(specs);

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
		return stream_apply_catchup(specs);
	}

	return true;
}


/*
 * follow_start_subprocess forks a subprocess and calls the given function.
 */
bool
follow_start_subprocess(StreamSpecs *specs, FollowSubProcess *subprocess)
{
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

	/* only count sub-processes for which we have a positive pid */
	for (int i = 0; i < count; i++)
	{
		if (processArray[i]->pid < 0)
		{
			--stillRunning;
		}
	}

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

		for (int i = 0; i < count; i++)
		{
			if (processArray[i]->pid <= 0 || processArray[i]->exited)
			{
				--stillRunning;
				continue;
			}

			/* follow_wait_pid is non-blocking: uses WNOHANG */
			if (!follow_wait_pid(processArray[i]->pid,
								 &(processArray[i]->exited),
								 &(processArray[i]->returnCode)))
			{
				/* errors have already been logged */
				return false;
			}

			if (processArray[i]->exited)
			{
				--stillRunning;

				int logLevel = LOG_INFO;
				char details[BUFSIZE] = { 0 };

				if (processArray[i]->returnCode == 0)
				{
					sformat(details, sizeof(details), "successfully");
				}
				else
				{
					logLevel = LOG_ERROR;
					sformat(details, sizeof(details), "with error code %d",
							processArray[i]->returnCode);
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
				 */
				if (processArray[i]->returnCode != 0 ||
					specs->endpos == InvalidXLogRecPtr)
				{
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
follow_wait_pid(pid_t subprocess, bool *exited, int *returnCode)
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
				*exited = true;
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
			*exited = false;
			break;
		}

		default:
		{
			/* sub-process has finished now */
			*exited = true;
			*returnCode = WEXITSTATUS(status);

			break;
		}
	}

	return true;
}
