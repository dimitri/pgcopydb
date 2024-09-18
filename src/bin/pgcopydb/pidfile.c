/*
 * src/bin/pgcopydb/pidfile.c
 *   Utilities to manage the pgcopydb pidfile.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */

#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "postgres.h"
#include "pqexpbuffer.h"

#include "cli_common.h"
#include "cli_root.h"
#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "pidfile.h"
#include "string_utils.h"

/*
 * create_pidfile writes our pid in a file.
 *
 * When running in a background loop, we need a pidFile to add a command line
 * tool that send signals to the process. The pidfile has a single line
 * containing our PID.
 */
bool
create_pidfile(const char *pidfile, pid_t pid)
{
	PQExpBuffer content = createPQExpBuffer();


	log_trace("create_pidfile(%d): \"%s\"", pid, pidfile);

	if (content == NULL)
	{
		log_fatal("Failed to allocate memory to update our PID file");
		return false;
	}

	if (!prepare_pidfile_buffer(content, pid))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(content);
		return false;
	}


	/* memory allocation could have failed while building string */
	if (PQExpBufferBroken(content))
	{
		log_error("Failed to create pidfile \"%s\": out of memory", pidfile);
		destroyPQExpBuffer(content);
		return false;
	}

	bool success = write_file(content->data, content->len, pidfile);
	destroyPQExpBuffer(content);

	return success;
}


/*
 * prepare_pidfile_buffer prepares a PQExpBuffer content with the information
 * expected to be found in a pidfile.
 */
bool
prepare_pidfile_buffer(PQExpBuffer content, pid_t pid)
{
	/*
	 * line #
	 *		1	supervisor PID
	 *		2	version number (PGCOPYDB_VERSION)
	 *		3	shared semaphore id (used to serialize log writes)
	 */
	appendPQExpBuffer(content, "%d\n", pid);
	appendPQExpBuffer(content, "%s\n", PGCOPYDB_VERSION);
	appendPQExpBuffer(content, "%d\n", log_semaphore.semId);

	return true;
}


/*
 * read_pidfile read pgcopydb pid from a file, and returns true when we could
 * read a PID that belongs to a currently running process.
 */
bool
read_pidfile(const char *pidfile, pid_t *pid)
{
	long fileSize = 0L;
	char *fileContents = NULL;
	int pidnum = 0;

	if (!file_exists(pidfile))
	{
		return false;
	}

	if (!read_file(pidfile, &fileContents, &fileSize))
	{
		log_debug("Failed to read the PID file \"%s\", removing it", pidfile);
		(void) remove_pidfile(pidfile);

		return false;
	}

	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, fileContents) || lbuf.count != 1)
	{
		/* errors have already been logged */
		return false;
	}

	stringToInt(lbuf.lines[0], &pidnum);

	*pid = pidnum;

	if (*pid <= 0)
	{
		log_debug("Read negative pid %d in file \"%s\", removing it",
				  *pid, pidfile);
		(void) remove_pidfile(pidfile);

		return false;
	}

	/* is it a stale file? */
	if (kill(*pid, 0) == 0)
	{
		return true;
	}
	else
	{
		log_debug("Failed to signal pid %d: %m", *pid);
		*pid = 0;

		log_debug("Found a stale pidfile at \"%s\"", pidfile);
		log_debug("Removing the stale pid file \"%s\"", pidfile);

		/*
		 * We must return false here, after having determined that the
		 * pidfile belongs to a process that doesn't exist anymore. So we
		 * remove the pidfile and don't take the return value into account
		 * at this point.
		 */
		(void) remove_pidfile(pidfile);

		/* we might have to cleanup a stale SysV semaphore, too */
		(void) semaphore_cleanup(pidfile);

		return false;
	}
}


/*
 * remove_pidfile removes pgcopydb pidfile.
 */
bool
remove_pidfile(const char *pidfile)
{
	if (!unlink_file(pidfile))
	{
		log_error("Failed to remove pid file \"%s\": %m", pidfile);
		return false;
	}
	return true;
}
