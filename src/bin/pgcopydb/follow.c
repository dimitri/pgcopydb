/*
 * src/bin/pgcopydb/follow.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli_common.h"
#include "cli_root.h"
#include "log.h"
#include "signals.h"
#include "stream.h"


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
			log_error("Failed to fork a subprocess to prefetch changes");
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			log_debug("Starting the prefetch sub-process");
			if (!startLogicalStreaming(specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_SOURCE);
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
			log_error("Failed to fork a subprocess to prefetch changes");
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			log_debug("Starting the catchup sub-process");
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


/*
 * follow_wait_subprocesses waits until both sub-processes are finished.
 */
bool
follow_wait_subprocesses(StreamSpecs *specs, pid_t prefetch, pid_t catchup)
{
	bool prefetchExited = false;
	bool catchupExited = false;

	bool success = true;

	while (!prefetchExited || !catchupExited)
	{
		if (!prefetchExited)
		{
			int returnCode = -1;

			if (!follow_wait_pid(prefetch, &prefetchExited, &returnCode))
			{
				/* errors have already been logged */
				return false;
			}

			if (prefetchExited)
			{
				log_level(returnCode == 0 ? LOG_INFO : LOG_ERROR,
						  "Prefetch process %d has terminated [%d]",
						  prefetch,
						  returnCode);
			}

			success = success || returnCode == 0;
		}

		if (!catchupExited)
		{
			int returnCode = -1;

			if (!follow_wait_pid(catchup, &catchupExited, &returnCode))
			{
				/* errors have already been logged */
				return false;
			}

			if (catchupExited)
			{
				log_level(returnCode == 0 ? LOG_INFO : LOG_ERROR,
						  "Catch-up process %d has terminated [%d]",
						  catchup,
						  returnCode);
			}

			success = success || returnCode == 0;
		}

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
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
