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
	/*
	 * Remove the possibly still existing stream context files from
	 * previous round of operations (--resume, etc). We want to make sure
	 * that the catchup process reads the files created on this connection.
	 */
	if (!stream_cleanup_context(streamSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	if (streamSpecs->mode < STREAM_MODE_PREFETCH)
	{
		log_error("BUG: followDB with stream mode %d", streamSpecs->mode);
		return false;
	}

	pid_t prefetch = -1;
	pid_t transform = -1;
	pid_t catchup = -1;

	/*
	 * When set to prefetch changes, we always also run the transform process
	 * to prepare the SQL files from the JSON files. Also upper modes (catchup,
	 * replay) does imply prefetching (and transform).
	 */
	if (streamSpecs->mode >= STREAM_MODE_PREFETCH)
	{
		if (!follow_start_prefetch(streamSpecs, &prefetch))
		{
			/* errors have already been logged */
			return false;
		}

		if (!follow_start_transform(streamSpecs, &transform))
		{
			log_error("Failed to start the transform process");

			/*
			 * When we fail to start the transform process, we stop the
			 * prefetch process immediately and exit with error.
			 */
			if (!follow_terminate_subprocesses(prefetch, transform, catchup))
			{
				return false;
			}

			/* and return false anyways, we failed to start the process here */
			return false;
		}
	}

	/*
	 * When set to catchup or replay mode, we also start the catchup process.
	 */
	if (streamSpecs->mode >= STREAM_MODE_CATCHUP)
	{
		if (!follow_start_catchup(streamSpecs, &catchup))
		{
			log_error("Failed to start the catchup process");

			/*
			 * When we fail to start the transform process, we stop the
			 * prefetch process immediately and exit with error.
			 */
			if (!follow_terminate_subprocesses(prefetch, transform, catchup))
			{
				return false;
			}

			/* and return false anyways, we failed to start the process here */
			return false;
		}
	}

	/*
	 * Finally wait until the process are finished.
	 *
	 * This happens when the sentinel endpos is set, typically using the
	 * command: pgcopydb stream sentinel set endpos --current.
	 */
	if (!follow_wait_subprocesses(prefetch, transform, catchup))
	{
		log_error("Failed to wait for follow sub-processes, "
				  "see above for details");
		return false;
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

	return true;
}


/*
 * follow_start_prefetch starts a sub-process that prefetches changes from the
 * source database into local files.
 */
bool
follow_start_prefetch(StreamSpecs *specs, pid_t *pid)
{
	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to prefetch changes: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			log_notice("Starting the prefetch sub-process");

			if (!startLogicalStreaming(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_SOURCE);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			*pid = fpid;
			return true;
		}
	}

	return true;
}


/*
 * follow_start_transform creates a sub-process that transform JSON files into
 * SQL files as needed, consuming requests from a queue.
 */
bool
follow_start_transform(StreamSpecs *specs, pid_t *pid)
{
	/*
	 * Flush stdio channels just before fork, to avoid double-output
	 * problems.
	 */
	fflush(stdout);
	fflush(stderr);

	int fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to transform changes to SQL: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			log_notice("Starting the transform sub-process");
			if (!stream_transform_worker(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* fork succeeded, in parent */
			*pid = fpid;
			break;
		}
	}

	return true;
}


/*
 * follow_start_catchup starts a sub-process that catches-up using the SQL
 * files that have been prepared by the prefetch process.
 */
bool
follow_start_catchup(StreamSpecs *specs, pid_t *pid)
{
	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to prefetch changes: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			log_notice("Starting the catchup sub-process");
			if (!stream_apply_catchup(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_TARGET);
			}

			/* and we're done */
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			*pid = fpid;
			return true;
		}
	}

	return true;
}


struct subprocess
{
	char *name;
	pid_t pid;
	bool exited;
	int returnCode;
};


/*
 * follow_wait_subprocesses waits until both sub-processes are finished.
 */
bool
follow_wait_subprocesses(pid_t prefetch, pid_t transform, pid_t catchup)
{
	/*
	 * We might have started only a subset of these 3 processes, and in that
	 * case processes that have not been started are assigned a pid of -1.
	 */
	struct subprocess processArray[] = {
		{ "prefetch", prefetch, prefetch == -1, 0 },
		{ "transform", transform, transform == -1, 0 },
		{ "catchup", catchup, catchup == -1, 0 }
	};

	int count = sizeof(processArray) / sizeof(processArray[0]);

	bool success = true;
	int stillRunning = count;

	/* only count sub-processes for which we have a positive pid */
	for (int i = 0; i < count; i++)
	{
		if (processArray[i].pid < 0)
		{
			--stillRunning;
		}
	}

	/* now the main loop, that waits until all given processes have exited */
	while (stillRunning > 0)
	{
		for (int i = 0; i < count; i++)
		{
			if (processArray[i].pid <= 0 || processArray[i].exited)
			{
				continue;
			}

			/* follow_wait_pid is non-blocking: uses WNOHANG */
			if (!follow_wait_pid(processArray[i].pid,
								 &(processArray[i].exited),
								 &(processArray[i].returnCode)))
			{
				/* errors have already been logged */
				return false;
			}

			if (processArray[i].exited)
			{
				--stillRunning;

				log_level(processArray[i].returnCode == 0 ? LOG_INFO : LOG_ERROR,
						  "%s process %d has terminated [%d]",
						  processArray[i].name,
						  processArray[i].pid,
						  processArray[i].returnCode);
			}

			success = success || processArray[i].returnCode == 0;
		}

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
	}

	return true;
}


/*
 * follow_terminate_subprocesses is used in case of errors in one sub-process
 * to signal the other ones to quit early.
 */
bool
follow_terminate_subprocesses(pid_t prefetch, pid_t transform, pid_t catchup)
{
	struct subprocess processArray[] = {
		{ "prefetch", prefetch, prefetch == -1, 0 },
		{ "transform", transform, transform == -1, 0 },
		{ "catchup", catchup, catchup == -1, 0 }
	};

	int count = sizeof(processArray) / sizeof(processArray[0]);

	/* first, signal the processes to exit as soon as possible */
	for (int i = 0; i < count; i++)
	{
		if (processArray[i].pid <= 0 || processArray[i].exited)
		{
			continue;
		}

		if (kill(processArray[i].pid, SIGQUIT) != 0)
		{
			/* process might have exited on its own already */
			if (errno != ESRCH)
			{
				log_error("Failed to signal %s process %d: %m",
						  processArray[i].name,
						  processArray[i].pid);

				return false;
			}
		}
	}

	/* second, collect the processes exit statuses */
	return follow_wait_subprocesses(prefetch, transform, catchup);
}


/*
 * follow_wait_pid waits for a given known sub-process.
 */
bool
follow_wait_pid(pid_t subprocess, bool *exited, int *returnCode)
{
	int status = 0;
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
