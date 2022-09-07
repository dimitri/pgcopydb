/*
 * src/bin/pgcopydb/main.c
 *    Main entry point for the pgcopydb command-line tool
 */

#include <getopt.h>
#include <unistd.h>

#include "postgres.h"

#if (PG_VERSION_NUM >= 120000)
#include "common/logging.h"
#endif

#include "cli_root.h"
#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "log.h"
#include "lock_utils.h"
#include "signals.h"
#include "string_utils.h"


char pgcopydb_argv0[MAXPGPATH];
char pgcopydb_program[MAXPGPATH];
int pgconnect_timeout = 10;     /* see also POSTGRES_CONNECT_TIMEOUT */

char *ps_buffer;                /* will point to argv area */
size_t ps_buffer_size;          /* space determined at run time */
size_t last_status_len;         /* use to minimize length of clobber */

Semaphore log_semaphore = { 0 }; /* allows inter-process locking */


static void set_logger(void);
static void log_semaphore_unlink_atexit(void);


/*
 * Main entry point for the binary.
 */
int
main(int argc, char **argv)
{
	CommandLine command = root;

	/* allows changing process title in ps/top/ptree etc */
	(void) init_ps_buffer(argc, argv);

	/* set our logging infrastructure */
	(void) set_logger();

	/*
	 * Since PG 12, we need to call pg_logging_init before any calls to pg_log_*
	 * otherwise, we get a segfault. Although we don't use pg_log_* directly,
	 * functions from the common library such as rmtree do use them.
	 * Logging change introduced in PG 12: https://git.postgresql.org/cgit/postgresql.git/commit/?id=cc8d41511721d25d557fc02a46c053c0a602fed0
	 */
	#if (PG_VERSION_NUM >= 120000)
	pg_logging_init(argv[0]);
	#endif

	/* register our logging clean-up atexit */
	atexit(log_semaphore_unlink_atexit);

	/*
	 * When PGCOPYDB_DEBUG is set in the environment, provide the user
	 * commands available to debug a pgcopydb instance.
	 */
	if (env_exists(PGCOPYDB_DEBUG))
	{
		command = root_with_debug;
	}

	/*
	 * When PGCONNECT_TIMEOUT is set in the environment, keep a copy of it in
	 * our own global variable pgconnect_timeout. We implement our own
	 * connection retry policy and will change change the environment variable
	 * setting when calling pg_basebackup and other tools anyway.
	 */
	if (env_exists("PGCONNECT_TIMEOUT"))
	{
		char env_pgtimeout[BUFSIZE] = { 0 };

		if (get_env_copy("PGCONNECT_TIMEOUT", env_pgtimeout, BUFSIZE) > 0)
		{
			if (!stringToInt(env_pgtimeout, &pgconnect_timeout))
			{
				log_warn("Failed to parse environment variable "
						 "PGCONNECT_TIMEOUT value \"%s\" as a "
						 "number of seconds (integer), "
						 "using our default %d seconds instead",
						 env_pgtimeout,
						 pgconnect_timeout);
			}
		}
	}

	/*
	 * We need to follow POSIX specifications for argument parsing, in
	 * particular we want getopt() to stop as soon as it reaches a non option
	 * in the command line.
	 *
	 * GNU and modern getopt() implementation will reorder the command
	 * arguments, making a mess of our nice subcommands facility.
	 *
	 * Note that we call unsetenv("POSIXLY_CORRECT"); before parsing options
	 * for commands that are the final sub-command of their chain and when we
	 * might mix options and arguments.
	 */
	setenv("POSIXLY_CORRECT", "1", 1);

	/*
	 * Stash away the argv[0] used to run this program and compute the realpath
	 * of the program invoked, which we need at several places including when
	 * preparing the systemd unit files.
	 *
	 * Note that we're using log_debug() in get_program_absolute_path and we
	 * have not set the log level from the command line option parsing yet. We
	 * hard-coded LOG_INFO as our log level. For now we won't see the log_debug
	 * output, but as a developer you could always change the LOG_INFO to
	 * LOG_DEBUG above and then see the message.
	 *
	 * When running pgcopydb using valgrind we also want the subprocesses to
	 * be run with valgrind. However, valgrind modifies the argv variables to
	 * be the pgcopydb binary, instead of the valgrind binary. So to make
	 * sure subprocesses are spawned using valgrind, we allow overriding To
	 * this program path detection using the PGCOPYDB_DEBUG_BIN_PATH
	 * environment variable.
	 */
	strlcpy(pgcopydb_argv0, argv[0], MAXPGPATH);
	if (env_exists("PGCOPYDB_DEBUG_BIN_PATH"))
	{
		if (!get_env_copy("PGCOPYDB_DEBUG_BIN_PATH", pgcopydb_program, MAXPGPATH))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else if (!set_program_absolute_path(pgcopydb_program, MAXPGPATH))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* Establish a handler for signals. */
	bool exitOnQuit = true;
	(void) set_signal_handlers(exitOnQuit);

	log_info("Running pgcopydb version %s from \"%s\"",
			 VERSION_STRING,
			 pgcopydb_program);

	if (!commandline_run(&command, argc, argv))
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	return 0;
}


/*
 * set_logger creates our log semaphore, sets the logging utility aspects such
 * as using colors in an interactive terminal and the default log level.
 */
static void
set_logger()
{
	log_set_level(LOG_INFO);

	/*
	 * Log messages go to stderr. We use colours when stderr is being shown
	 * directly to the user to make it easier to spot warnings and errors.
	 */
	log_use_colors(isatty(fileno(stderr)));

	/* initialize the semaphore used for locking log output */
	if (!semaphore_init(&log_semaphore))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* set our logging facility to use our semaphore as a lock mechanism */
	(void) log_set_udata(&log_semaphore);
	(void) log_set_lock(&semaphore_log_lock_function);
}


/*
 * log_semaphore_unlink_atexit calls semaphore_unlink() atexit.
 */
static void
log_semaphore_unlink_atexit(void)
{
	(void) semaphore_finish(&log_semaphore);
}
