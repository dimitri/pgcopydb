/*
 * src/bin/pgcopydb/pg_utils.h
 *     Functions for interacting with a postgres server
 */

#ifndef PG_UTILS_H
#define PG_UTILS_H


#include <limits.h>
#include <stdbool.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "libpq-fe.h"
#include "portability/instr_time.h"

#include "datatype/timestamp.h"

#if PG_MAJORVERSION_NUM >= 15
#include "common/pg_prng.h"
#endif

#include "defaults.h"
#include "parsing_utils.h"


/*
 * OID values from PostgreSQL src/include/catalog/pg_type.h
 */
#define BOOLOID 16
#define BYTEAOID 17
#define NAMEOID 19
#define INT4OID 23
#define INT8OID 20
#define TEXTOID 25
#define OIDOID 26
#define LSNOID 3220
#define TIMESTAMPTZOID 1184

/*
 * Error codes that we use internally.
 */
#define STR_ERRCODE_DUPLICATE_OBJECT "42710"
#define STR_ERRCODE_DUPLICATE_DATABASE "42P04"

#define STR_ERRCODE_INVALID_OBJECT_DEFINITION "42P17"
#define STR_ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE "55000"
#define STR_ERRCODE_OBJECT_IN_USE "55006"
#define STR_ERRCODE_UNDEFINED_OBJECT "42704"

/*
 * From postgres/src/include/access/xlogdefs.h
 */
#ifdef LSN_FORMAT_ARGS
#undef LSN_FORMAT_ARGS
#endif
#define LSN_FORMAT_ARGS(lsn) ((uint32) ((lsn) >> 32)), ((uint32) (lsn))


/*
 * pg_stat_replication.sync_state is one if:
 *   sync, async, quorum, potential
 */
#define PGSR_SYNC_STATE_MAXLENGTH 10

/*
 * From postgres/src/bin/pg_basebackup/streamutil.h
 */

TimestampTz feGetCurrentTimestamp(void);

void feTimestampDifference(TimestampTz start_time,
						   TimestampTz stop_time,
						   long *secs,
						   int *microsecs);

bool feTimestampDifferenceExceeds(TimestampTz start_time,
								  TimestampTz stop_time,
								  int msec);

void fe_sendint64(int64 i, char *buf);
int64 fe_recvint64(char *buf);


#endif /* PG_UTILS_H */
