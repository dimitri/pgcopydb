/*
 * src/bin/pgcopydb/lock_utils.c
 *   Implementations of utility functions for inter-process locking
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <unistd.h>

#include "copydb.h"
#include "defaults.h"
#include "file_utils.h"
#include "env_utils.h"
#include "lock_utils.h"
#include "log.h"
#include "pidfile.h"
#include "string_utils.h"


/*
 * See man semctl(2)
 */
#if defined(__linux__)
union semun
{
	int val;
	struct semid_ds *buf;
	unsigned short *array;
};
#endif

/*
 * semaphore_init creates or opens a named semaphore for the current process.
 *
 * We use the environment variable PGCOPYDB_SERVICE to signal when a process
 * is a child process of the main pgcopydb supervisor so that we are able to
 * initialize our locking strategy before parsing the command line. After all,
 * we might have to log some output during the parsing itself.
 */
bool
semaphore_init(Semaphore *semaphore)
{
	if (env_exists(PGCOPYDB_LOG_SEMAPHORE))
	{
		return semaphore_open(semaphore);
	}
	else
	{
		bool success = semaphore_create(semaphore);

		/*
		 * Only the main process should unlink the semaphore at exit time.
		 *
		 * When we create a semaphore, ensure we put our semId in the expected
		 * environment variable (PGCOPYDB_LOG_SEMAPHORE), and we assign the
		 * current process' pid as the semaphore owner.
		 *
		 * When we open a pre-existing semaphore using PGCOPYDB_LOG_SEMAPHORE
		 * as the semId, the semaphore owner is left to zero.
		 *
		 * The atexit(3) function that removes the semaphores only acts when
		 * the owner is our current pid. That way, in case of an early failure
		 * in execv(), the semaphore is not dropped from under the main
		 * program.
		 *
		 * A typical way execv() would fail is when calling run_program() on a
		 * pathname that does not exists.
		 *
		 * Per atexit(3) manual page:
		 *
		 *   When a child process is created via fork(2), it inherits copies of
		 *   its parent's registrations. Upon a successful call to one of the
		 *   exec(3) functions, all registrations are removed.
		 *
		 * And that's why it's important that we don't remove the semaphore in
		 * the atexit() cleanup function when a call to run_command() fails
		 * early.
		 */
		if (success)
		{
			IntString semIdString = intToString(semaphore->semId);

			setenv(PGCOPYDB_LOG_SEMAPHORE, semIdString.strValue, 1);
		}

		return success;
	}
}


/*
 * semaphore_finish closes or unlinks given semaphore.
 */
bool
semaphore_finish(Semaphore *semaphore)
{
	/*
	 * At initialization time we either create a new semaphore and register
	 * getpid() as the owner, or we open a previously existing semaphore from
	 * its semId as found in our environment variable PGCOPYDB_LOG_SEMAPHORE.
	 *
	 * At finish time (called from the atexit(3) registry), we remove the
	 * semaphore only when we are the owner of it.
	 */
	if (semaphore->owner == getpid())
	{
		return semaphore_unlink(semaphore);
	}

	return true;
}


/*
 * semaphore_create creates a new semaphore with the value 1, or the value
 * semaphore->initValue when it's not zero.
 */
bool
semaphore_create(Semaphore *semaphore)
{
	union semun semun;

	semaphore->owner = getpid();
	semaphore->semId = semget(IPC_PRIVATE, 1, 0600);

	if (semaphore->semId < 0)
	{
		/* the semaphore_log_lock_function has not been set yet */
		log_fatal("Failed to create semaphore: %m\n");
		return false;
	}

	/* to see this log line, change the default log level in set_logger() */
	log_debug("Created semaphore %d (cleanup with ipcrm -s)", semaphore->semId);

	/* by default the Semaphore struct is initialized to { 0 }, fix it */
	semaphore->initValue = semaphore->initValue == 0 ? 1 : semaphore->initValue;

	semun.val = semaphore->initValue;
	if (semctl(semaphore->semId, 0, SETVAL, semun) < 0)
	{
		/* the semaphore_log_lock_function has not been set yet */
		log_fatal("Failed to set semaphore %d/%d to value %d : %m\n",
				  semaphore->semId, 0, semun.val);
		return false;
	}

	/* register the semaphore to the System V resources clean-up array */
	if (!copydb_register_sysv_semaphore(&system_res_array, semaphore))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * semaphore_open opens our IPC_PRIVATE semaphore.
 *
 * We don't have a key for it, because we asked the kernel to create a new
 * semaphore set with the guarantee that it would not exist already. So we
 * re-use the semaphore identifier directly.
 *
 * We don't even have to call semget(2) here at all, because we share our
 * semaphore identifier in the environment directly.
 */
bool
semaphore_open(Semaphore *semaphore)
{
	char semIdString[BUFSIZE] = { 0 };

	/* ensure the owner is set to zero when we re-open an existing semaphore */
	semaphore->owner = 0;

	if (!get_env_copy(PGCOPYDB_LOG_SEMAPHORE, semIdString, BUFSIZE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!stringToInt(semIdString, &semaphore->semId))
	{
		/* errors have already been logged */
		return false;
	}

	/* to see this log line, change the default log level in set_logger() */
	log_debug("Using semaphore %d", semaphore->semId);

	/* we have the semaphore identifier, no need to call semget(2), done */
	return true;
}


/*
 * semaphore_unlink removes an existing named semaphore.
 */
bool
semaphore_unlink(Semaphore *semaphore)
{
	union semun semun;

	semun.val = 0;              /* unused, but keep compiler quiet */

	log_debug("ipcrm -s %d", semaphore->semId);

	if (semctl(semaphore->semId, 0, IPC_RMID, semun) < 0)
	{
		fformat(stderr, "Failed to remove semaphore %d: %m", semaphore->semId);
		return false;
	}

	/* mark the queue as unlinekd to the System V resources clean-up array */
	if (!copydb_unlink_sysv_semaphore(&system_res_array, semaphore))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * semaphore_cleanup is used when we find a stale PID file, to remove a
 * possibly left behind semaphore. The user could also use ipcs and ipcrm to
 * figure that out, if the stale pidfile does not exist anymore.
 */
bool
semaphore_cleanup(const char *pidfile)
{
	Semaphore semaphore;

	long fileSize = 0L;
	char *fileContents = NULL;
	char *fileLines[BUFSIZE] = { 0 };

	if (!file_exists(pidfile))
	{
		return false;
	}

	if (!read_file(pidfile, &fileContents, &fileSize))
	{
		return false;
	}

	int lineCount = splitLines(fileContents, fileLines, BUFSIZE);

	if (lineCount < PIDFILE_LINE_SEM_ID)
	{
		log_debug("Failed to cleanup the semaphore from stale pid file \"%s\": "
				  "it contains %d lines, semaphore id is expected in line %d",
				  pidfile,
				  lineCount,
				  PIDFILE_LINE_SEM_ID);
		free(fileContents);
		return false;
	}

	if (!stringToInt(fileLines[PIDFILE_LINE_SEM_ID], &(semaphore.semId)))
	{
		/* errors have already been logged */
		free(fileContents);
		return false;
	}

	free(fileContents);

	log_trace("Read semaphore id %d from stale pidfile", semaphore.semId);

	return semaphore_unlink(&semaphore);
}


/*
 * semaphore_lock locks a semaphore (decrement count), blocking if count would
 * be < 0
 */
bool
semaphore_lock(Semaphore *semaphore)
{
	int errStatus;
	struct sembuf sops;

	sops.sem_op = -1;           /* decrement */
	sops.sem_flg = SEM_UNDO;
	sops.sem_num = 0;

	/*
	 * Note: if errStatus is -1 and errno == EINTR then it means we returned
	 * from the operation prematurely because we were sent a signal.  So we
	 * try and lock the semaphore again.
	 *
	 * We used to check interrupts here, but that required servicing
	 * interrupts directly from signal handlers. Which is hard to do safely
	 * and portably.
	 */
	do {
		errStatus = semop(semaphore->semId, &sops, 1);
	} while (errStatus < 0 && errno == EINTR);

	if (errStatus < 0)
	{
		fformat(stderr,
				"%d Failed to acquire a lock with semaphore %d: %m\n",
				getpid(),
				semaphore->semId);
		return false;
	}

	return true;
}


/*
 * semaphore_unlock unlocks a semaphore (increment count)
 */
bool
semaphore_unlock(Semaphore *semaphore)
{
	int errStatus;
	struct sembuf sops;

	sops.sem_op = 1;            /* increment */
	sops.sem_flg = SEM_UNDO;
	sops.sem_num = 0;

	/*
	 * Note: if errStatus is -1 and errno == EINTR then it means we returned
	 * from the operation prematurely because we were sent a signal.  So we
	 * try and unlock the semaphore again. Not clear this can really happen,
	 * but might as well cope.
	 */
	do {
		errStatus = semop(semaphore->semId, &sops, 1);
	} while (errStatus < 0 && errno == EINTR);

	if (errStatus < 0)
	{
		fformat(stderr,
				"Failed to release a lock with semaphore %d: %m\n",
				semaphore->semId);
		return false;
	}

	return true;
}


/*
 * semaphore_log_lock_function integrates our semaphore facility with the
 * logging tool in use in this project.
 */
void
semaphore_log_lock_function(void *udata, int mode)
{
	Semaphore *semaphore = (Semaphore *) udata;

	/*
	 * If locking/unlocking fails for some weird reason, we still want to log.
	 * It's not so bad that we want to completely quit the program.
	 * That's why we ignore the return values of semaphore_unlock and
	 * semaphore_lock.
	 */

	switch (mode)
	{
		/* unlock */
		case 0:
		{
			(void) semaphore_unlock(semaphore);
			break;
		}

		/* lock */
		case 1:
		{
			(void) semaphore_lock(semaphore);
			break;
		}

		default:
		{
			fformat(stderr,
					"BUG: semaphore_log_lock_function called with mode %d",
					mode);
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
}
