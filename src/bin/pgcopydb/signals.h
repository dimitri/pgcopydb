/*
 * src/bin/pgcopydb/signals.h
 *   Signal handlers for pgcopydb, used in loop.c and pgsetup.c
 */

#ifndef SIGNALS_H
#define SIGNALS_H

#include <inttypes.h>
#include <signal.h>

/* This flag controls termination of the main loop. */
extern volatile sig_atomic_t asked_to_stop;      /* SIGTERM */
extern volatile sig_atomic_t asked_to_stop_fast; /* SIGINT */
extern volatile sig_atomic_t asked_to_reload;    /* SIGHUP */
extern volatile sig_atomic_t asked_to_quit;      /* SIGQUIT */

#define CHECK_FOR_FAST_SHUTDOWN { if (asked_to_stop_fast) { break; } \
}

void set_signal_handlers(bool exitOnQuit);
bool block_signals(sigset_t *mask, sigset_t *orig_mask);
void unblock_signals(sigset_t *orig_mask);
void catch_reload(int sig);
void catch_int(int sig);
void catch_term(int sig);
void catch_quit(int sig);
void catch_quit_and_exit(int sig);
void unset_signal_flags(void);

int get_current_signal(int defaultSignal);
int pick_stronger_signal(int sig1, int sig2);
char * signal_to_string(int signal);

#endif /* SIGNALS_H */
