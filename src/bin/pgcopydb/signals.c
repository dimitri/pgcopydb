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
 * mask_signals prepares a pselect() call by masking all the signals we handle
 * in this part of the code, to avoid race conditions with setting our atomic
 * variables at signal handling.
 */
bool
block_signals(sigset_t *mask, sigset_t *orig_mask)
{
	int signals[] = { SIGHUP, SIGINT, SIGTERM, SIGQUIT, -1 };

	if (sigemptyset(mask) == -1)
	{
		/* man sigemptyset sayth: No errors are defined. */
		log_error("sigemptyset: %m");
		return false;
	}

	for (int i = 0; signals[i] != -1; i++)
	{
		/*
		 * The sigaddset() function may fail if:
		 *
		 * EINVAL The value of the signo argument is an invalid or unsupported
		 *        signal number
		 *
		 * This should never happen given the manual set of signals we are
		 * processing here in this loop.
		 */
		if (sigaddset(mask, signals[i]) == -1)
		{
			log_error("sigaddset: %m");
			return false;
		}
	}

	if (sigprocmask(SIG_BLOCK, mask, orig_mask) == -1)
	{
		log_error("Failed to block signals: sigprocmask: %m");
		return false;
	}

	return true;
}


/*
 * unblock_signals calls sigprocmask to re-establish the normal signal mask, in
 * order to allow our code to handle signals again.
 *
 * If we fail to unblock signals, then we won't be able to react to any
 * interruption, reload, or shutdown sequence, and we'd rather exit now.
 */
void
unblock_signals(sigset_t *orig_mask)
{
	/* restore signal masks (un block them) now */
	if (sigprocmask(SIG_SETMASK, orig_mask, NULL) == -1)
	{
		log_fatal("Failed to restore signals: sigprocmask: %m");
		exit(EXIT_CODE_INTERNAL_ERROR);
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
 * pick_stronger_signal returns the "stronger" signal among the two given
 * arguments.
 *
 * Signal processing have a priority or hierarchy of their own. Once we have
 * received and processed SIGQUIT we want to stay at this signal level. Once we
 * have received SIGINT we may upgrade to SIGQUIT, but we won't downgrade to
 * SIGTERM.
 */
int
pick_stronger_signal(int sig1, int sig2)
{
	if (sig1 == SIGQUIT || sig2 == SIGQUIT)
	{
		return SIGQUIT;
	}
	else if (sig1 == SIGINT || sig2 == SIGINT)
	{
		return SIGINT;
	}
	else
	{
		return SIGTERM;
	}
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
			return "unknown signal";
	}
}
