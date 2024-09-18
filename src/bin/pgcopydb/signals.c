/*
 * src/bin/pgcopydb/signals.c
 *   Signal handlers for pgcopydb, used in loop.c and pgsetup.c
 */

#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "postgres_fe.h"        /* pqsignal, portable sigaction wrapper */

#include "defaults.h"
#include "lock_utils.h"
#include "log.h"
#include "signals.h"

/* This flag controls termination of the main loop. */
volatile sig_atomic_t asked_to_stop = 0;      /* SIGTERM */
volatile sig_atomic_t asked_to_stop_fast = 0; /* SIGINT */
volatile sig_atomic_t asked_to_reload = 0;    /* SIGHUP */
volatile sig_atomic_t asked_to_quit = 0;      /* SIGQUIT */

/*
 * set_signal_handlers sets our signal handlers for the 4 signals that we
 * specifically handle in pgcopydb.
 */
void
set_signal_handlers(bool exitOnQuit)
{
	/* Establish a handler for signals. */
	log_trace("set_signal_handlers%s", exitOnQuit ? " (exit on quit)" : "");

	pqsignal(SIGHUP, catch_reload);
	pqsignal(SIGINT, catch_int);
	pqsignal(SIGTERM, catch_term);

	/* ignore SIGPIPE so that EPIPE is returned instead */
	pqsignal(SIGPIPE, SIG_IGN);

	if (exitOnQuit)
	{
		pqsignal(SIGQUIT, catch_quit_and_exit);
	}
	else
	{
		pqsignal(SIGQUIT, catch_quit);
	}
}


/*
 * catch_reload receives the SIGHUP signal.
 */
void
catch_reload(int sig)
{
	asked_to_reload = 1;
	pqsignal(sig, catch_reload);
}


/*
 * catch_int receives the SIGINT signal.
 */
void
catch_int(int sig)
{
	asked_to_stop_fast = 1;
	pqsignal(sig, catch_int);
}


/*
 * catch_stop receives SIGTERM signal.
 */
void
catch_term(int sig)
{
	asked_to_stop = 1;
	pqsignal(sig, catch_term);
}


/*
 * catch_quit receives the SIGQUIT signal.
 */
void
catch_quit(int sig)
{
	/* default signal handler disposition is to core dump, we don't */
	asked_to_quit = 1;
	pqsignal(sig, catch_quit);
}


/*
 * quit_and_exit exit(EXIT_CODE_QUIT) upon receiving the SIGQUIT signal.
 */
void
catch_quit_and_exit(int sig)
{
	/* default signal handler disposition is to core dump, we don't */
	log_warn("SIGQUIT");
	exit(EXIT_CODE_QUIT);
}


/*
 * get_current_signal returns the current signal to process and gives a prioriy
 * towards SIGQUIT, then SIGINT, then SIGTERM.
 */
int
get_current_signal(int defaultSignal)
{
	if (asked_to_quit)
	{
		return SIGQUIT;
	}
	else if (asked_to_stop_fast)
	{
		return SIGINT;
	}
	else if (asked_to_stop)
	{
		return SIGTERM;
	}

	/* no termination signal to process at this time, return the default */
	return defaultSignal;
}


/*
 * unset_signal_flags assigns 0 to all our control flags. Use to avoid
 * re-processing an exit flag that is currently being processed already.
 */
void
unset_signal_flags()
{
	asked_to_stop = 0;
	asked_to_stop_fast = 0;
	asked_to_quit = 0;
	asked_to_reload = 0;
}


/*
 * signal_to_string is our own specialised function to display a signal. The
 * strsignal() output does not look like what we need.
 */
char *
signal_to_string(int signal)
{
	switch (signal)
	{
		case SIGQUIT:
		{
			return "SIGQUIT";
		}

		case SIGTERM:
		{
			return "SIGTERM";
		}

		case SIGINT:
		{
			return "SIGINT";
		}

		case SIGHUP:
		{
			return "SIGHUP";
		}

		default:
		{
			return strsignal(signal);
		}
	}
}


/*
 * signal_is_handled returns true when the given signal is handled/expected by
 * pgcopydb.
 */
bool
signal_is_handled(int signal)
{
	return

	    /* we add zero here for compliance with the waitpid() API */
		signal == 0 ||
		signal == SIGINT ||
		signal == SIGTERM ||
		signal == SIGQUIT ||
		signal == SIGHUP;
}
