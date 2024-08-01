/*
 * src/bin/pgcopydb/defaults.h
 *     Default values for pgcopydb configuration settings
 */

#ifndef DEFAULTS_H
#define DEFAULTS_H

/*
 * Setup Boehm-Demers-Weiser conservative garbage collector as a garbage
 * collecting replacement for malloc. See https://www.hboehm.info/gc/
 */
#include <gc/gc.h>

#define malloc(n) GC_malloc(n)
#define calloc(m, n) GC_malloc((m) * (n))
#define free(p) GC_free(p)
#define realloc(p, n) GC_realloc((p), (n))
#define strdup(p) GC_strdup(p)
#define strndup(p, n) GC_strndup(p, n)

/*
 * The GC API can also be used as leak detector, thanks to using the following
 * macro, as per https://www.hboehm.info/gc/leak.html
 */
#define CHECK_LEAKS() GC_gcollect()

/*
 * Now install the version string.
 */
#include "git-version.h"

/* additional version information for printing version on CLI */
#define PGCOPYDB_VERSION "0.16"

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
#define PGCOPYDB_RESTORE_JOBS "PGCOPYDB_RESTORE_JOBS"
#define PGCOPYDB_LARGE_OBJECTS_JOBS "PGCOPYDB_LARGE_OBJECTS_JOBS"
#define PGCOPYDB_SPLIT_TABLES_LARGER_THAN "PGCOPYDB_SPLIT_TABLES_LARGER_THAN"
#define PGCOPYDB_SPLIT_MAX_PARTS "PGCOPYDB_SPLIT_MAX_PARTS"
#define PGCOPYDB_ESTIMATE_TABLE_SIZES "PGCOPYDB_ESTIMATE_TABLE_SIZES"
#define PGCOPYDB_DROP_IF_EXISTS "PGCOPYDB_DROP_IF_EXISTS"
#define PGCOPYDB_SNAPSHOT "PGCOPYDB_SNAPSHOT"
#define PGCOPYDB_OUTPUT_PLUGIN "PGCOPYDB_OUTPUT_PLUGIN"
#define PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING "PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING"
#define PGCOPYDB_LOG_TIME_FORMAT "PGCOPYDB_LOG_TIME_FORMAT"
#define PGCOPYDB_LOG_JSON "PGCOPYDB_LOG_JSON"
#define PGCOPYDB_LOG_JSON_FILE "PGCOPYDB_LOG_JSON_FILE"
#define PGCOPYDB_LOG_FILENAME "PGCOPYDB_LOG_FILENAME"
#define PGCOPYDB_FAIL_FAST "PGCOPYDB_FAIL_FAST"
#define PGCOPYDB_SKIP_VACUUM "PGCOPYDB_SKIP_VACUUM"
#define PGCOPYDB_SKIP_ANALYZE "PGCOPYDB_SKIP_ANALYZE"
#define PGCOPYDB_SKIP_TABLESPACES "PGCOPYDB_SKIP_TABLESPACES"
#define PGCOPYDB_SKIP_DB_PROPERTIES "PGCOPYDB_SKIP_DB_PROPERTIES"
#define PGCOPYDB_SKIP_CTID_SPLIT "PGCOPYDB_SKIP_CTID_SPLIT"
#define PGCOPYDB_USE_COPY_BINARY "PGCOPYDB_USE_COPY_BINARY"

/* default values for the command line options */
#define DEFAULT_TABLE_JOBS 4
#define DEFAULT_INDEX_JOBS 4
#define DEFAULT_RESTORE_JOBS 0
#define DEFAULT_LARGE_OBJECTS_JOBS 4
#define DEFAULT_SPLIT_TABLES_LARGER_THAN 0 /* no COPY partitioning by default */

#define POSTGRES_CONNECT_TIMEOUT "10"

/* retry PQping for a maximum of 1 min, up to 2 secs between attemps */
#define POSTGRES_PING_RETRY_TIMEOUT 60               /* seconds */
#define POSTGRES_PING_RETRY_CAP_SLEEP_TIME (2 * 1000) /* milliseconds */
#define POSTGRES_PING_RETRY_BASE_SLEEP_TIME 5         /* milliseconds */

#define POSTGRES_PORT 5432

/* default replication slot and origin for logical replication */
#define REPLICATION_ORIGIN "pgcopydb"
#define REPLICATION_PLUGIN "test_decoding"
#define REPLICATION_SLOT_NAME "pgcopydb"

#define CATCHINGUP_SLEEP_MS 1 * 1000 /* 1s */
#define STREAM_EMPTY_TX_TIMEOUT 10   /* seconds */

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
