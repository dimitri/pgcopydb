/*
 * src/bin/pgcopydb/pidfile.h
 *   Utilities to manage the pgcopydb pidfile.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */
#ifndef PIDFILE_H
#define PIDFILE_H

#include <inttypes.h>
#include <signal.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"

/*
 * As of pgcopydb 0.1, the contents of the pidfile is:
 *
 * line #
 *		1	PID
 *		2	version number (PGCOPYDB_VERSION)
 *		3	shared semaphore id (used to serialize log writes)
 *
 */
#define PIDFILE_LINE_PID 1
#define PIDFILE_LINE_VERSION_STRING 2
#define PIDFILE_LINE_SEM_ID 3

bool create_pidfile(const char *pidfile, pid_t pid);
bool prepare_pidfile_buffer(PQExpBuffer content, pid_t pid);
bool read_pidfile(const char *pidfile, pid_t *pid);
bool remove_pidfile(const char *pidfile);

#endif /* PIDFILE_H */
