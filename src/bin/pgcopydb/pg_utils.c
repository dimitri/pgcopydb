/*
 * src/bin/pg_autoctl/pg_utils.c
 *	 API for sending SQL commands to a PostgreSQL server
 */
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "port/pg_bswap.h"
#include "libpq-fe.h"
#include "pqexpbuffer.h"
#include "portability/instr_time.h"

#if PG_MAJORVERSION_NUM >= 15
#include "common/pg_prng.h"
#endif

#include "cli_root.h"
#include "defaults.h"
#include "file_utils.h"
#include "log.h"
#include "parsing.h"
#include "pgsql.h"
#include "pg_utils.h"
#include "signals.h"
#include "string_utils.h"


/*
 * Frontend version of GetCurrentTimestamp(), since we are not linked with
 * backend code.
 */
TimestampTz
feGetCurrentTimestamp(void)
{
	TimestampTz result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (TimestampTz) tp.tv_sec -
			 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}


/*
 * Frontend version of TimestampDifference(), since we are not linked with
 * backend code.
 */
void
feTimestampDifference(TimestampTz start_time, TimestampTz stop_time,
					  long *secs, int *microsecs)
{
	TimestampTz diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}


/*
 * Frontend version of TimestampDifferenceExceeds(), since we are not
 * linked with backend code.
 */
bool
feTimestampDifferenceExceeds(TimestampTz start_time,
							 TimestampTz stop_time,
							 int msec)
{
	TimestampTz diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}


/*
 * Converts an int64 to network byte order.
 */
void
fe_sendint64(int64 i, char *buf)
{
	uint64 n64 = pg_hton64(i);

	memcpy(buf, &n64, sizeof(n64));
}


/*
 * Converts an int64 from network byte order to native format.
 */
int64
fe_recvint64(char *buf)
{
	uint64 n64;

	memcpy(&n64, buf, sizeof(n64));

	return pg_ntoh64(n64);
}
