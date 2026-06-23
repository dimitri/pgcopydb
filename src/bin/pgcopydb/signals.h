/*
 * src/bin/pgcopydb/signals.h
 *   Signal handlers for pgcopydb, used in loop.c and pgsetup.c
 */

#ifndef SIGNALS_H
#define SIGNALS_H

#include <inttypes.h>
#include <signal.h>

#include "postgres_fe.h"   /* SIGNAL_ARGS, pqsigfunc */

/* This flag controls termination of the main loop. */
extern volatile sig_atomic_t asked_to_stop;      /* SIGTERM */
extern volatile sig_atomic_t asked_to_stop_fast; /* SIGINT */
extern volatile sig_atomic_t asked_to_reload;    /* SIGHUP */
extern volatile sig_atomic_t asked_to_quit;      /* SIGQUIT */

#define CHECK_FOR_FAST_SHUTDOWN { if (asked_to_stop_fast) { break; } \
}

void set_signal_handlers(bool exitOnQuit);
void catch_reload(SIGNAL_ARGS);
void catch_int(SIGNAL_ARGS);
void catch_term(SIGNAL_ARGS);
void catch_quit(SIGNAL_ARGS);
void catch_quit_and_exit(SIGNAL_ARGS);
void unset_signal_flags(void);

int get_current_signal(int defaultSignal);
char * signal_to_string(int signal);
bool signal_is_handled(int signal);

#endif /* SIGNALS_H */
