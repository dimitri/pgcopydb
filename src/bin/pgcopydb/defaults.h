/*
 * src/bin/pgcopydb/defaults.h
 *     Default values for pgcopydb configuration settings
 */

#ifndef DEFAULTS_H
#define DEFAULTS_H

#include "git-version.h"

/* additional version information for printing version on CLI */
#define PGCOPYDB_VERSION "0.10"

#ifdef GIT_VERSION
#define VERSION_STRING GIT_VERSION
#else
#define VERSION_STRING PGCOPYDB_VERSION
#endif

/* environment variable to use to make DEBUG facilities available */
#define PGCOPYDB_DEBUG "PGCOPYDB_DEBUG"

/* environment variable for containing the id of the logging semaphore */
#define PGCOPYDB_LOG_SEMAPHORE "PGCOPYDB_LOG_SEMAPHORE"

/* environment variables for the command line options */
#define PGCOPYDB_SOURCE_PGURI "PGCOPYDB_SOURCE_PGURI"
#define PGCOPYDB_TARGET_PGURI "PGCOPYDB_TARGET_PGURI"
#define PGCOPYDB_TABLE_JOBS "PGCOPYDB_TABLE_JOBS"
#define PGCOPYDB_INDEX_JOBS "PGCOPYDB_INDEX_JOBS"
#define PGCOPYDB_SPLIT_TABLES_LARGER_THAN "PGCOPYDB_SPLIT_TABLES_LARGER_THAN"
#define PGCOPYDB_DROP_IF_EXISTS "PGCOPYDB_DROP_IF_EXISTS"
#define PGCOPYDB_SNAPSHOT "PGCOPYDB_SNAPSHOT"
#define PGCOPYDB_PGAPPNAME "pgcopydb"

/* default values for the command line options */
#define DEFAULT_TABLE_JOBS 4
#define DEFAULT_INDEX_JOBS 4
#define DEFAULT_SPLIT_TABLES_LARGER_THAN 0 /* no COPY partitioning by default */

#define POSTGRES_CONNECT_TIMEOUT "10"


/* retry PQping for a maximum of 15 mins, up to 2 secs between attemps */
#define POSTGRES_PING_RETRY_TIMEOUT 900               /* seconds */
#define POSTGRES_PING_RETRY_CAP_SLEEP_TIME (2 * 1000) /* milliseconds */
#define POSTGRES_PING_RETRY_BASE_SLEEP_TIME 5         /* milliseconds */

#define POSTGRES_PORT 5432

/* masqurade  passwords with that value in logs */
#define PASSWORD_MASK "****"

/* default replication slot and origin for logical replication */
#define REPLICATION_ORIGIN "pgcopydb"
#define REPLICATION_PLUGIN "wal2json"
#define REPLICATION_SLOT_NAME "pgcopydb"

#define CATCHINGUP_SLEEP_MS 10 * 1000 /* 10s */

/* internal default for allocating strings  */
#define BUFSIZE 1024

/*
 * 50kB seems enough to store the PATH environment variable if you have more,
 * simply set PATH to something smaller.
 * The limit on linux for environment variables is 128kB:
 * https://unix.stackexchange.com/questions/336934
 */
#define MAXPATHSIZE 50000


/* buffersize that is needed for results of ctime_r */
#define MAXCTIMESIZE 26

#define AWAIT_PROMOTION_SLEEP_TIME_MS 1000

/*
 * Error codes returned to the shell in case something goes wrong.
 */
#define EXIT_CODE_QUIT 0        /* it's ok, we were asked politely */
#define EXIT_CODE_BAD_ARGS 1
#define EXIT_CODE_BAD_CONFIG 2
#define EXIT_CODE_BAD_STATE 3
#define EXIT_CODE_PGSQL 4
#define EXIT_CODE_PGCTL 5
#define EXIT_CODE_SOURCE 6
#define EXIT_CODE_TARGET 7
#define EXIT_CODE_RELOAD 9
#define EXIT_CODE_INTERNAL_ERROR 12
#define EXIT_CODE_FATAL 122     /* error is fatal, no retry, quit now */

/*
 * This opens file write only and creates if it doesn't exist.
 */
#define FOPEN_FLAGS_W O_WRONLY | O_TRUNC | O_CREAT

/*
 * This opens the file in append mode and creates it if it doesn't exist.
 */
#define FOPEN_FLAGS_A O_APPEND | O_RDWR | O_CREAT


/* when malloc fails, what do we tell our users */
#define ALLOCATION_FAILED_ERROR "Failed to allocate memory: %m"

#endif /* DEFAULTS_H */
