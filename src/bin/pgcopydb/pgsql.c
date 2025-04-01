/*
 * src/bin/pgcopydb/pgsql.c
 *	 API for sending SQL commands to a PostgreSQL server
 */
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq/libpq-fs.h"
#include "pqexpbuffer.h"
#include "portability/instr_time.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#if PG_MAJORVERSION_NUM >= 15
#include "common/pg_prng.h"
#endif

#include "cli_root.h"
#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "pgsql_timeline.h"
#include "pgsql_utils.h"
#include "pg_utils.h"
#include "signals.h"
#include "string_utils.h"

#if defined(LIBPQ_HAS_PIPELINING) && LIBPQ_HAS_PIPELINING
#define pqPipelineModeEnabled(conn) (PQpipelineStatus(conn) == PQ_PIPELINE_ON)
#else
#warning Compiling pgcopydb without libpq pipeline mode, available from libpq 14 and later
#define pqPipelineModeEnabled(conn) (false)
#endif

static char * ConnectionTypeToString(ConnectionType connectionType);
static void log_connection_error(PGconn *connection, int logLevel);
static void pgAutoCtlDefaultNoticeProcessor(void *arg, const char *message);
static bool pgsql_retry_open_connection(PGSQL *pgsql);

static void pgsql_handle_notifications(PGSQL *pgsql);

static void pgsql_execute_log_error(PGSQL *pgsql,
									PGresult *result,
									const char *sql,
									PQExpBuffer debugParameters,
									void *context);

static bool build_parameters_list(PQExpBuffer buffer,
								  int paramCount,
								  const char **paramValues);

static bool pg_copy_data(PGSQL *src, PGSQL *dst,
						 CopyArgs *args, CopyStats *stats,
						 void *context, CopyStatsCallback *callback);

static bool pg_copy_send_query(PGSQL *pgsql, CopyArgs *args,
							   ExecStatusType status);

static void pgcopy_log_error(PGSQL *pgsql, PGresult *res, const char *context);

static void getSequenceValue(void *ctx, PGresult *result);

static void pgsql_stream_log_error(PGSQL *pgsql,
								   PGresult *res, const char *message);

static bool pgsqlSendFeedback(LogicalStreamClient *client,
							  LogicalStreamContext *context,
							  bool force,
							  bool replyRequested);

static bool flushAndSendFeedback(LogicalStreamClient *client,
								 LogicalStreamContext *context);

static void prepareToTerminate(LogicalStreamClient *client,
							   bool keepalive,
							   XLogRecPtr lsn);


/*
 * parseSingleValueResult is a ParsePostgresResultCB callback that reads the
 * first column of the first row of the resultset only, and parses the answer
 * into the expected C value, one of type QueryResultType.
 */
void
parseSingleValueResult(void *ctx, PGresult *result)
{
	SingleValueResultContext *context = (SingleValueResultContext *) ctx;

	context->ntuples = PQntuples(result);

	if (context->ntuples == 1)
	{
		char *value = PQgetvalue(result, 0, 0);

		/* this function is never used when we expect NULL values */
		if (PQgetisnull(result, 0, 0))
		{
			context->isNull = true;
			context->parsedOk = true;
			return;
		}

		switch (context->resultType)
		{
			case PGSQL_RESULT_BOOL:
			{
				context->boolVal = strcmp(value, "t") == 0;
				context->parsedOk = true;
				break;
			}

			case PGSQL_RESULT_INT:
			{
				if (!stringToInt(value, &context->intVal))
				{
					context->parsedOk = false;
					log_error("Failed to parse int result \"%s\"", value);
				}
				context->parsedOk = true;
				break;
			}

			case PGSQL_RESULT_BIGINT:
			{
				if (!stringToUInt64(value, &context->bigint))
				{
					context->parsedOk = false;
					log_error("Failed to parse uint64_t result \"%s\"", value);
				}
				context->parsedOk = true;
				break;
			}

			case PGSQL_RESULT_STRING:
			{
				context->strVal = strdup(value);
				context->parsedOk = true;
				break;
			}
		}
	}
}


/*
 * fetchedRows is a pgsql_execute_with_params callback function that sets a
 * SingleValueResultContext->intVal to PQntuples(result), that is how many rows
 * are fetched by the query.
 */
void
fetchedRows(void *ctx, PGresult *result)
{
	SingleValueResultContext *context = (SingleValueResultContext *) ctx;

	context->parsedOk = true;
	context->intVal = PQntuples(result);
}


/*
 * pgsql_init initializes a PGSQL struct to connect to the given database
 * URL or connection string.
 */
bool
pgsql_init(PGSQL *pgsql, char *url, ConnectionType connectionType)
{
	pgsql->connectionType = connectionType;
	pgsql->connection = NULL;

	/* set our default retry policy for interactive commands */
	(void) pgsql_set_interactive_retry_policy(&(pgsql->retryPolicy));
	if (validate_connection_string(url))
	{
		pgsql->connectionString = url;
	}
	else
	{
		return false;
	}

	/* by default we log all the SQL queries and their parameters */
	pgsql->logSQL = true;

	return true;
}


/*
 * pgsql_set_retry_policy sets the retry policy to the given maxT (maximum
 * total time spent retrying), maxR (maximum number of retries, zero when not
 * retrying at all, -1 for unbounded number of retries), and maxSleepTime to
 * cap our exponential backoff with decorrelated jitter computation.
 */
void
pgsql_set_retry_policy(ConnectionRetryPolicy *retryPolicy,
					   int maxT,
					   int maxR,
					   int maxSleepTime,
					   int baseSleepTime)
{
	retryPolicy->maxT = maxT;
	retryPolicy->maxR = maxR;
	retryPolicy->maxSleepTime = maxSleepTime;
	retryPolicy->baseSleepTime = baseSleepTime;

	/* initialize a seed for our random number generator */
#if PG_MAJORVERSION_NUM < 15
	pg_srand48(time(0));
#else
	pg_prng_seed(&(retryPolicy->prng_state), (uint64) (getpid() ^ time(NULL)));
#endif
}


/*
 * pgsql_set_interactive_retry_policy sets the retry policy to 1 minute of
 * total retrying time, unbounded number of attempts, and up to 2 seconds
 * of sleep time in between attempts.
 */
void
pgsql_set_interactive_retry_policy(ConnectionRetryPolicy *retryPolicy)
{
	(void) pgsql_set_retry_policy(retryPolicy,
								  POSTGRES_PING_RETRY_TIMEOUT,
								  -1, /* unbounded number of attempts */
								  POSTGRES_PING_RETRY_CAP_SLEEP_TIME,
								  POSTGRES_PING_RETRY_BASE_SLEEP_TIME);
}


#define min(a, b) (a < b ? a : b)

/*
 * http://c-faq.com/lib/randrange.html
 *
 * With additional protection against division-by-zero.
 */
#define random_between(R, M, N) \
	((((N) -(M) +1) == 0) \
	 ? ((M) + R / (RAND_MAX / ((N) -(M)) + 1)) \
	 : ((M) + R / (RAND_MAX / ((N) -(M) +1) + 1)))


/*
 * pick_random_sleep_time picks a random sleep time between the given policy
 * base sleep time and 3 times the previous sleep time. See below in
 * pgsql_compute_connection_retry_sleep_time for a deep dive into why we are
 * interested in this computation.
 */
static int
pick_random_sleep_time(ConnectionRetryPolicy *retryPolicy)
{
#if PG_MAJORVERSION_NUM < 15
	long random = pg_lrand48();
#else
	uint32_t random = pg_prng_uint32(&(retryPolicy->prng_state));
#endif

	return random_between(random,
						  retryPolicy->baseSleepTime,
						  retryPolicy->sleepTime * 3);
}


/*
 * pgsql_compute_connection_retry_sleep_time returns how much time to sleep
 * this time, in milliseconds.
 */
int
pgsql_compute_connection_retry_sleep_time(ConnectionRetryPolicy *retryPolicy)
{
	/*
	 * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	 *
	 * Adding jitter is a small change to the sleep function:
	 *
	 *  sleep = random_between(0, min(cap, base * 2^attempt))
	 *
	 * There are a few ways to implement these timed backoff loops. Let’s call
	 * the algorithm above “Full Jitter”, and consider two alternatives. The
	 * first alternative is “Equal Jitter”, where we always keep some of the
	 * backoff and jitter by a smaller amount:
	 *
	 *  temp = min(cap, base * 2^attempt)
	 *  sleep = temp/2 + random_between(0, temp/2)
	 *
	 * The intuition behind this one is that it prevents very short sleeps,
	 * always keeping some of the slow down from the backoff.
	 *
	 * A second alternative is “Decorrelated Jitter”, which is similar to “Full
	 * Jitter”, but we also increase the maximum jitter based on the last
	 * random value.
	 *
	 *  sleep = min(cap, random_between(base, sleep*3))
	 *
	 * Which approach do you think is best?
	 *
	 * The no-jitter exponential backoff approach is the clear loser. [...]
	 *
	 * Of the jittered approaches, “Equal Jitter” is the loser. It does
	 * slightly more work than “Full Jitter”, and takes much longer. The
	 * decision between “Decorrelated Jitter” and “Full Jitter” is less clear.
	 * The “Full Jitter” approach uses less work, but slightly more time. Both
	 * approaches, though, present a substantial decrease in client work and
	 * server load.
	 *
	 * Here we implement "Decorrelated Jitter", which is better in terms of
	 * time spent, something we care to optimize for even when it means more
	 * work on the monitor side.
	 */
	int sleepTime = pick_random_sleep_time(retryPolicy);

	retryPolicy->sleepTime = min(retryPolicy->maxSleepTime, sleepTime);

	++(retryPolicy->attempts);

	return retryPolicy->sleepTime;
}


/*
 * pgsql_retry_policy_expired returns true when we should stop retrying, either
 * per the policy (maxR / maxT) or because we received a signal that we have to
 * obey.
 */
bool
pgsql_retry_policy_expired(ConnectionRetryPolicy *retryPolicy)
{
	instr_time duration;

	/* Any signal is reason enough to break out from this retry loop. */
	if (asked_to_quit || asked_to_stop || asked_to_stop_fast || asked_to_reload)
	{
		return true;
	}

	/* set the first retry time when it's not been set previously */
	if (INSTR_TIME_IS_ZERO(retryPolicy->startTime))
	{
		INSTR_TIME_SET_CURRENT(retryPolicy->startTime);
	}

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, retryPolicy->startTime);

	/*
	 * We stop retrying as soon as we have spent all of our time budget or all
	 * of our attempts count budget, whichever comes first.
	 *
	 * maxR = 0 (zero) means no retry at all, checked before the loop
	 * maxR < 0 (zero) means unlimited number of retries
	 */
	if ((INSTR_TIME_GET_MILLISEC(duration) >= (retryPolicy->maxT * 1000)) ||
		(retryPolicy->maxR > 0 &&
		 retryPolicy->attempts >= retryPolicy->maxR))
	{
		return true;
	}

	return false;
}


/*
 * connectionTypeToString transforms a connectionType in a string to be used in
 * a user facing message.
 */
static char *
ConnectionTypeToString(ConnectionType connectionType)
{
	switch (connectionType)
	{
		case PGSQL_CONN_SOURCE:
		{
			return "source";
		}

		case PGSQL_CONN_TARGET:
		{
			return "target";
		}

		default:
		{
			return "unknown connection type";
		}
	}
}


/*
 * Finish a PGSQL client connection.
 */
void
pgsql_finish(PGSQL *pgsql)
{
	if (pgsql->connection != NULL)
	{
		if (pgsql->logSQL)
		{
			log_sql("Disconnecting from [%s] \"%s\"",
					ConnectionTypeToString(pgsql->connectionType),
					pgsql->safeURI.pguri);
		}

		PQfinish(pgsql->connection);
		pgsql->connection = NULL;

		/* cache invalidation for pgversion */
		pgsql->pgversion[0] = '\0';
		pgsql->pgversion_num = 0;

		/*
		 * When we fail to connect, on the way out we call pgsql_finish to
		 * reset the connection to NULL. We still want the callers to be able
		 * to inquire about our connection status, so refrain to reset the
		 * status.
		 */
	}

	pgsql->connectionStatementType = PGSQL_CONNECTION_SINGLE_STATEMENT;
}


/*
 * log_connection_error logs the PQerrorMessage from the given connection.
 */
static void
log_connection_error(PGconn *connection, int logLevel)
{
	/* PQerrorMessage is then "connection pointer is NULL", not helpful */
	if (connection == NULL)
	{
		return;
	}

	char *message = PQerrorMessage(connection);
	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, message))
	{
		/* errors have already been logged */
		return;
	}

	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		char *line = lbuf.lines[lineNumber];

		if (lineNumber == 0)
		{
			log_level(logLevel, "Connection to database failed: %s", line);
		}
		else
		{
			log_level(logLevel, "%s", line);
		}
	}
}


/*
 * pgsql_open_connection opens a PostgreSQL connection, given a PGSQL client
 * instance. If a connection is already open in the client (it's not NULL),
 * then this errors, unless we are inside a transaction opened by pgsql_begin.
 */
PGconn *
pgsql_open_connection(PGSQL *pgsql)
{
	/* we might be connected already */
	if (pgsql->connection != NULL)
	{
		if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT)
		{
			log_error("BUG: requested to open an already open connection in "
					  "non PGSQL_CONNECTION_MULTI_STATEMENT mode");
			pgsql_finish(pgsql);
			return NULL;
		}
		return pgsql->connection;
	}

	/* compute the URL without the password, we set it separately */
	if (pgsql->safeURI.pguri == NULL)
	{
		(void) parse_and_scrub_connection_string(pgsql->connectionString,
												 &(pgsql->safeURI));
	}

	if (pgsql->logSQL)
	{
		log_sql("Connecting to [%s] \"%s\"",
				ConnectionTypeToString(pgsql->connectionType),
				pgsql->safeURI.pguri);
	}

	/*
	 * Set application_name to contain the process title and pid, so that it is
	 * easier to identify our connections in pg_stat_activity.
	 *
	 * From Postgres docs: The application_name can be any string of less than
	 * NAMEDATALEN characters (64 characters in a standard build).
	 *
	 * See: https://www.postgresql.org/docs/current/runtime-config-logging.html
	 */
	const char *ps_buffer_prefix = "pgcopydb: ";
	int prefixLen = strlen(ps_buffer_prefix);

	char app_name[BUFSIZE] = { 0 };

	sformat(app_name, sizeof(app_name), "pgcopydb[%d]", getpid());

	if (strncmp(ps_buffer, ps_buffer_prefix, prefixLen) == 0)
	{
		sformat(app_name, sizeof(app_name), "%s %s",
				app_name,
				ps_buffer + prefixLen);
	}
	else
	{
		sformat(app_name, sizeof(app_name), "%s %s", app_name, ps_buffer);
	}

	/* make sure to truncate application name to NAMEDATALEN to avoid notices */
	app_name[NAMEDATALEN - 1] = '\0';

	setenv("PGAPPNAME", app_name, 1);

	/* we implement our own retry strategy */
	if (!env_exists("PGCONNECT_TIMEOUT"))
	{
		setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);
	}

	/* register our starting time */
	INSTR_TIME_SET_CURRENT(pgsql->retryPolicy.startTime);
	INSTR_TIME_SET_ZERO(pgsql->retryPolicy.connectTime);

	/* Make a connection to the database */
	pgsql->connection = PQconnectdb(pgsql->connectionString);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(pgsql->connection) != CONNECTION_OK)
	{
		/*
		 * Implement the retry policy:
		 *
		 * First observe the maxR property: maximum retries allowed. When set
		 * to zero, we don't retry at all.
		 */
		if (pgsql->retryPolicy.maxR == 0)
		{
			INSTR_TIME_SET_CURRENT(pgsql->retryPolicy.connectTime);

			(void) log_connection_error(pgsql->connection, LOG_ERROR);

			log_error("Failed to connect to %s database at \"%s\", "
					  "see above for details",
					  ConnectionTypeToString(pgsql->connectionType),
					  pgsql->safeURI.pguri);

			pgsql->status = PG_CONNECTION_BAD;

			pgsql_finish(pgsql);
			return NULL;
		}

		/*
		 * If we reach this part of the code, the connectionType is not LOCAL
		 * and the retryPolicy has a non-zero maximum retry count. Let's retry!
		 */
		if (!pgsql_retry_open_connection(pgsql))
		{
			/* errors have already been logged */
			return NULL;
		}
	}

	INSTR_TIME_SET_CURRENT(pgsql->retryPolicy.connectTime);
	pgsql->status = PG_CONNECTION_OK;
	pgsql->sqlstate[0] = '\0';

	/* set the libpq notice receiver to integrate notifications as warnings. */
	PQsetNoticeProcessor(pgsql->connection,
						 &pgAutoCtlDefaultNoticeProcessor,
						 NULL);

	return pgsql->connection;
}


/*
 * Refrain from warning too often. The user certainly wants to know that we are
 * still trying to connect, though warning several times a second is not going
 * to help anyone. A good trade-off seems to be a warning every 30s.
 */
#define SHOULD_WARN_AGAIN(duration) \
	(INSTR_TIME_GET_MILLISEC(duration) > 30000)

/*
 * pgsql_retry_open_connection loops over a PQping call until the remote server
 * is ready to accept connections, and then connects to it and returns true
 * when it could connect, false otherwise.
 */
static bool
pgsql_retry_open_connection(PGSQL *pgsql)
{
	bool connectionOk = false;

	PGPing lastWarningMessage = PQPING_OK;
	instr_time lastWarningTime;

	INSTR_TIME_SET_ZERO(lastWarningTime);

	log_warn("Failed to connect to \"%s\", retrying until "
			 "the server is ready", pgsql->safeURI.pguri);

	/* should not happen */
	if (pgsql->retryPolicy.maxR == 0)
	{
		return false;
	}

	/* reset our internal counter before entering the retry loop */
	pgsql->retryPolicy.attempts = 1;

	while (!connectionOk)
	{
		if (pgsql_retry_policy_expired(&(pgsql->retryPolicy)))
		{
			instr_time duration;

			INSTR_TIME_SET_CURRENT(duration);
			INSTR_TIME_SUBTRACT(duration, pgsql->retryPolicy.startTime);

			(void) log_connection_error(pgsql->connection, LOG_ERROR);

			log_error("Failed to connect to \"%s\" "
					  "after %d attempts in %d ms, "
					  "pgcopydb stops retrying now",
					  pgsql->safeURI.pguri,
					  pgsql->retryPolicy.attempts,
					  (int) INSTR_TIME_GET_MILLISEC(duration));

			pgsql->status = PG_CONNECTION_BAD;
			pgsql_finish(pgsql);

			return false;
		}

		/*
		 * Now compute how much time to wait for this round, and increment how
		 * many times we tried to connect already.
		 */
		int sleep =
			pgsql_compute_connection_retry_sleep_time(&(pgsql->retryPolicy));

		/* we have milliseconds, pg_usleep() wants microseconds */
		(void) pg_usleep(sleep * 1000);

		log_sql("PQping(%s): slept %d ms on attempt %d",
				pgsql->safeURI.pguri,
				pgsql->retryPolicy.sleepTime,
				pgsql->retryPolicy.attempts);

		switch (PQping(pgsql->connectionString))
		{
			/*
			 * https://www.postgresql.org/docs/current/libpq-connect.html
			 *
			 * The server is running and appears to be accepting connections.
			 */
			case PQPING_OK:
			{
				log_sql("PQping OK after %d attempts",
						pgsql->retryPolicy.attempts);

				/*
				 * Ping is now ok, and connection is still NULL because the
				 * first attempt to connect failed. Now is a good time to
				 * establish the connection.
				 *
				 * PQping does not check authentication, so we might still fail
				 * to connect to the server.
				 */
				pgsql->connection = PQconnectdb(pgsql->connectionString);

				if (PQstatus(pgsql->connection) == CONNECTION_OK)
				{
					instr_time duration;

					INSTR_TIME_SET_CURRENT(duration);

					connectionOk = true;
					pgsql->status = PG_CONNECTION_OK;
					pgsql->retryPolicy.connectTime = duration;

					INSTR_TIME_SUBTRACT(duration, pgsql->retryPolicy.startTime);

					log_info("Successfully connected to \"%s\" "
							 "after %d attempts in %d ms.",
							 pgsql->safeURI.pguri,
							 pgsql->retryPolicy.attempts,
							 (int) INSTR_TIME_GET_MILLISEC(duration));
				}
				else
				{
					instr_time durationSinceLastWarning;

					INSTR_TIME_SET_CURRENT(durationSinceLastWarning);
					INSTR_TIME_SUBTRACT(durationSinceLastWarning, lastWarningTime);

					if (lastWarningMessage != PQPING_OK ||
						SHOULD_WARN_AGAIN(durationSinceLastWarning))
					{
						lastWarningMessage = PQPING_OK;
						INSTR_TIME_SET_CURRENT(lastWarningTime);

						/*
						 * Only show details when that's the last attempt,
						 * otherwise accept that this may happen as a transient
						 * state.
						 */
						(void) log_connection_error(pgsql->connection, LOG_SQL);

						log_sql("Failed to connect after successful ping");
					}
				}
				break;
			}

			/*
			 * https://www.postgresql.org/docs/current/libpq-connect.html
			 *
			 * The server is running but is in a state that disallows
			 * connections (startup, shutdown, or crash recovery).
			 */
			case PQPING_REJECT:
			{
				instr_time durationSinceLastWarning;

				INSTR_TIME_SET_CURRENT(durationSinceLastWarning);
				INSTR_TIME_SUBTRACT(durationSinceLastWarning, lastWarningTime);

				if (lastWarningMessage != PQPING_REJECT ||
					SHOULD_WARN_AGAIN(durationSinceLastWarning))
				{
					lastWarningMessage = PQPING_REJECT;
					INSTR_TIME_SET_CURRENT(lastWarningTime);

					log_warn(
						"The server at \"%s\" is running but is in a state "
						"that disallows connections (startup, shutdown, or "
						"crash recovery).",
						pgsql->safeURI.pguri);
				}

				break;
			}

			/*
			 * https://www.postgresql.org/docs/current/libpq-connect.html
			 *
			 * The server could not be contacted. This might indicate that the
			 * server is not running, or that there is something wrong with the
			 * given connection parameters (for example, wrong port number), or
			 * that there is a network connectivity problem (for example, a
			 * firewall blocking the connection request).
			 */
			case PQPING_NO_RESPONSE:
			{
				instr_time durationSinceStart, durationSinceLastWarning;

				INSTR_TIME_SET_CURRENT(durationSinceStart);
				INSTR_TIME_SUBTRACT(durationSinceStart,
									pgsql->retryPolicy.startTime);

				INSTR_TIME_SET_CURRENT(durationSinceLastWarning);
				INSTR_TIME_SUBTRACT(durationSinceLastWarning, lastWarningTime);

				/* no message at all the first 30s: 30000ms */
				if (SHOULD_WARN_AGAIN(durationSinceStart) &&
					(lastWarningMessage != PQPING_NO_RESPONSE ||
					 SHOULD_WARN_AGAIN(durationSinceLastWarning)))
				{
					lastWarningMessage = PQPING_NO_RESPONSE;
					INSTR_TIME_SET_CURRENT(lastWarningTime);

					log_warn(
						"The server at \"%s\" could not be contacted "
						"after %d attempts in %d ms (milliseconds). "
						"This might indicate that the server is not running, "
						"or that there is something wrong with the given "
						"connection parameters (for example, wrong port "
						"number), or that there is a network connectivity "
						"problem (for example, a firewall blocking the "
						"connection request).",
						pgsql->safeURI.pguri,
						pgsql->retryPolicy.attempts,
						(int) INSTR_TIME_GET_MILLISEC(durationSinceStart));
				}

				break;
			}

			/*
			 * https://www.postgresql.org/docs/current/libpq-connect.html
			 *
			 * No attempt was made to contact the server, because the supplied
			 * parameters were obviously incorrect or there was some
			 * client-side problem (for example, out of memory).
			 */
			case PQPING_NO_ATTEMPT:
			{
				lastWarningMessage = PQPING_NO_ATTEMPT;
				log_sql("Failed to ping server \"%s\" because of "
						"client-side problems (no attempt were made)",
						pgsql->safeURI.pguri);
				break;
			}
		}
	}

	if (!connectionOk && pgsql->connection != NULL)
	{
		INSTR_TIME_SET_CURRENT(pgsql->retryPolicy.connectTime);

		(void) log_connection_error(pgsql->connection, LOG_ERROR);
		pgsql->status = PG_CONNECTION_BAD;
		pgsql_finish(pgsql);

		return false;
	}

	return true;
}


/*
 * pgAutoCtlDefaultNoticeProcessor is our default PostgreSQL libpq Notice
 * Processing: NOTICE, WARNING, HINT etc are processed as log_warn messages by
 * default.
 */
static void
pgAutoCtlDefaultNoticeProcessor(void *arg, const char *message)
{
	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, (char *) message))
	{
		/* errors have already been logged */
		return;
	}

	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		const char *line = lbuf.lines[lineNumber];

		/* Map WARNING to LOG_WARN, and others to LOG_NOTICE */
		if (regexp_first_match(line, "^(WARNING:)"))
		{
			log_warn("%s", line);
		}
		else
		{
			log_notice("%s", line);
		}
	}
}


/*
 * pgsql_begin is responsible for opening a mutli statement connection and
 * opening a transaction block by issuing a 'BEGIN' query.
 */
bool
pgsql_begin(PGSQL *pgsql)
{
	/*
	 * Indicate that we're running a transaction, so that the connection is not
	 * closed after each query automatically. It also allows us to detect bugs
	 * easily. We need to do this before executing BEGIN, because otherwise the
	 * connection is closed after the BEGIN statement automatically.
	 */
	pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	if (!pgsql_execute(pgsql, "BEGIN"))
	{
		/*
		 * We need to manually call pgsql_finish to clean up here in case of
		 * this failure, because we have set the statement type to MULTI.
		 */
		pgsql_finish(pgsql);
		return false;
	}

	return true;
}


/*
 * pgsql_rollback is responsible for issuing a 'ROLLBACK' query to an already
 * opened transaction, usually via a previous pgsql_begin() command.
 *
 * It closes the connection but leaves the error contents, if any, for the user
 * to examine should it is wished for.
 */
bool
pgsql_rollback(PGSQL *pgsql)
{
	bool result;

	if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT ||
		pgsql->connection == NULL)
	{
		log_error("BUG: call to pgsql_rollback without holding an open "
				  "multi statement connection");
		return false;
	}

	result = pgsql_execute(pgsql, "ROLLBACK");

	/*
	 * Connection might be be closed during the pgsql_execute(), notably in case
	 * of error. Be explicit and close it regardless though.
	 */
	if (pgsql->connection)
	{
		pgsql_finish(pgsql);
	}

	return result;
}


/*
 * pgsql_commit is responsible for issuing a 'COMMIT' query to an already
 * opened transaction, usually via a previous pgsql_begin() command.
 *
 * It closes the connection but leaves the error contents, if any, for the user
 * to examine should it is wished for.
 */
bool
pgsql_commit(PGSQL *pgsql)
{
	bool result;

	if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT ||
		pgsql->connection == NULL)
	{
		log_error("BUG: call to pgsql_commit() without holding an open "
				  "multi statement connection");
		if (pgsql->connection)
		{
			pgsql_finish(pgsql);
		}
		return false;
	}

	result = pgsql_execute(pgsql, "COMMIT");

	/*
	 * Connection might be be closed during the pgsql_execute(), notably in case
	 * of error. Be explicit and close it regardless though.
	 */
	if (pgsql->connection)
	{
		pgsql_finish(pgsql);
	}

	return result;
}


typedef struct PgVersionContext
{
	char sqlstate[SQLSTATE_LENGTH];
	char pgversion[PG_VERSION_STRING_MAX_LENGTH];
	int pgversion_num;
	bool parsedOk;
} PgVersionContext;


/*
 * parseVersionContext parses the result of the pgsql_server_version SQL query.
 */
static void
parseVersionContext(void *ctx, PGresult *result)
{
	PgVersionContext *context = (PgVersionContext *) ctx;
	int nTuples = PQntuples(result);
	int errors = 0;

	if (nTuples != 1)
	{
		log_error("Query returned %d rows, expected 1", nTuples);
		context->parsedOk = false;
		return;
	}

	if (PQnfields(result) != 2)
	{
		log_error("Query returned %d columns, expected 2", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	/* 1. server_version */
	char *value = PQgetvalue(result, 0, 0);
	int length = strlcpy(context->pgversion, value, sizeof(context->pgversion));

	if (length >= sizeof(context->pgversion))
	{
		log_error("Postgres version string \"%s\" is %d bytes long, "
				  "the maximum expected is %zu",
				  value, length, sizeof(context->pgversion) - 1);
		++errors;
	}

	/* 2. server_version_num */
	value = PQgetvalue(result, 0, 1);

	if (!stringToInt(value, &(context->pgversion_num)))
	{
		log_error("Failed to parse Postgres server_version_num \"%s\"", value);
		++errors;
	}

	context->parsedOk = errors == 0;
}


/*
 * pgsql_server_version_num sets pgversion in the given PGSQL instance.
 */
bool
pgsql_server_version(PGSQL *pgsql)
{
	PgVersionContext context = { { 0 }, { 0 }, 0, false };

	const char *sql =
		"select current_setting('server_version'), "
		"       current_setting('server_version_num')::integer";

	/* use the cache; invalidation happens in pgsql_finish() */
	if (pgsql->pgversion_num > 0)
	{
		return true;
	}

	if (!pgsql_execute_with_params(pgsql, sql,
								   0, NULL, NULL,
								   &context, &parseVersionContext))
	{
		log_error("Failed to get Postgres server_version_num");
		return false;
	}

	strlcpy(pgsql->pgversion, context.pgversion, sizeof(pgsql->pgversion));
	pgsql->pgversion_num = context.pgversion_num;

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	log_debug("[%s %d] Postgres version %s (%d)",
			  endpoint,
			  PQbackendPID(pgsql->connection),
			  pgsql->pgversion,
			  pgsql->pgversion_num);

	return true;
}


/*
 * pgsql_set_transaction calls SET ISOLATION LEVEl with the specific
 * transaction modes parameters.
 */
bool
pgsql_set_transaction(PGSQL *pgsql,
					  IsolationLevel level, bool readOnly, bool deferrable)
{
	char sql[BUFSIZE] = { 0 };

	char *isolationLevels[] = {
		"SERIALIZABLE",
		"REPEATABLE READ",
		"READ COMMITTED",
		"READ UNCOMMITTED"
	};

	int isolationLevelCount = sizeof(isolationLevels) / sizeof(char *);

	if (level < 0 || level >= isolationLevelCount)
	{
		log_error("BUG: unknown isolation level %d", level);
		return false;
	}

	sformat(sql, sizeof(sql),
			"SET TRANSACTION ISOLATION LEVEL %s, %s, %s",
			isolationLevels[level],
			readOnly ? "READ ONLY" : "READ WRITE",
			deferrable ? "DEFERRABLE" : "NOT DEFERRABLE");

	return pgsql_execute(pgsql, sql);
}


/*
 * pgsql_is_in_recovery connects to PostgreSQL and sets the is_in_recovery
 * boolean to the result of the SELECT pg_is_in_recovery() query. It returns
 * false when something went wrong doing that.
 */
bool
pgsql_is_in_recovery(PGSQL *pgsql, bool *is_in_recovery)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BOOL, false };
	char *sql = "SELECT pg_is_in_recovery()";

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &parseSingleValueResult))
	{
		/* errors have been logged already */
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to get result from pg_is_in_recovery()");
		return false;
	}

	*is_in_recovery = context.boolVal;

	return true;
}


/*
 * pgsql_has_database_privilege calls has_database_privilege() and copies the
 * result in the granted boolean pointer given.
 */
bool
pgsql_has_database_privilege(PGSQL *pgsql, const char *privilege, bool *granted)
{
	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_BOOL, false };

	char *sql = "select has_database_privilege(current_database(), $1);";

	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { privilege };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to query database privileges");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to query database privileges");
		return false;
	}

	*granted = parseContext.boolVal;

	return true;
}


/*
 * pgsql_has_sequence_privilege calls has_sequence_privilege() and copies the
 * result in the granted boolean pointer given.
 */
bool
pgsql_has_sequence_privilege(PGSQL *pgsql,
							 const char *seqname,
							 const char *privilege,
							 bool *granted)
{
	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_BOOL, false };

	char *sql = "select has_sequence_privilege($1, $2);";

	int paramCount = 2;
	Oid paramTypes[2] = { TEXTOID, TEXTOID };
	const char *paramValues[2] = { seqname, privilege };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to query privileges for sequence \"%s\"", seqname);
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to query privileges for sequence \"%s\"", seqname);
		return false;
	}

	*granted = parseContext.boolVal;

	return true;
}


/*
 * pgsql_has_table_privilege calls has_table_privilege() and copies the result
 * in the granted boolean pointer given.
 */
bool
pgsql_has_table_privilege(PGSQL *pgsql,
						  const char *tablename,
						  const char *privilege,
						  bool *granted)
{
	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_BOOL, false };

	char *sql = "select has_table_privilege($1, $2);";

	int paramCount = 2;
	Oid paramTypes[2] = { TEXTOID, TEXTOID };
	const char *paramValues[2] = { tablename, privilege };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to query privileges for table \"%s\"", tablename);
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to query privileges for table \"%s\"", tablename);
		return false;
	}

	*granted = parseContext.boolVal;

	return true;
}


/*
 * pgsql_get_search_path runs the query "show search_path" and copies the
 * result in the given pre-allocated string buffer.
 */
bool
pgsql_get_search_path(PGSQL *pgsql, char *search_path, size_t size)
{
	char *sql = "select current_setting('search_path')";

	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_STRING, false };

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to get current search_path");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to get current search_path");
		return false;
	}

	strlcpy(search_path, parseContext.strVal, size);

	return true;
}


/*
 * pgsql_set_search_path runs the query "set [ local ] search_path ..."
 */
bool
pgsql_set_search_path(PGSQL *pgsql, char *search_path, bool local)
{
	char sql[BUFSIZE] = { 0 };

	sformat(sql, sizeof(sql), "set %s search_path to %s",
			local ? "local" : "", search_path);

	if (!pgsql_execute(pgsql, sql))
	{
		log_error("Failed to set current search_path to: %s", search_path);
		return false;
	}

	return true;
}


/*
 * pgsql_prepend_search_path prepends Postgres search path with the given
 * namespace, only for the current transaction, using SET LOCAL.
 */
bool
pgsql_prepend_search_path(PGSQL *pgsql, const char *namespace)
{
	char search_path[BUFSIZE] = { 0 };

	if (!pgsql_get_search_path(pgsql, search_path, sizeof(search_path)))
	{
		/* errors have already been logged */
		return false;
	}

	if (IS_EMPTY_STRING_BUFFER(search_path))
	{
		return pgsql_set_search_path(pgsql, (char *) namespace, true);
	}
	else
	{
		char new_search_path[BUFSIZE] = { 0 };

		sformat(new_search_path, sizeof(new_search_path),
				"%s, %s",
				namespace,
				search_path);

		return pgsql_set_search_path(pgsql, new_search_path, true);
	}

	return false;
}


/*
 * pgsql_export_snapshot calls pg_export_snapshot() and copies the text into
 * the given string buffer, that must have been allocated by the caller.
 */
bool
pgsql_export_snapshot(PGSQL *pgsql, char *snapshot, size_t size)
{
	char *sql = "select pg_export_snapshot()";

	SingleValueResultContext parseContext = { { 0 }, PGSQL_RESULT_STRING, false };

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &parseContext, &parseSingleValueResult))
	{
		log_error("Failed to export snaphost");
		return false;
	}

	if (!parseContext.parsedOk)
	{
		log_error("Failed to export snaphost");
		return false;
	}

	strlcpy(snapshot, parseContext.strVal, size);

	return true;
}


/*
 * pgsql_set_snapshot calls SET TRANSACTION SNAPSHOT with the given snapshot.
 * Before we can set a transaction snapshot though, we must set the transaction
 * isolation level. Same as pg_dump, when sharing a snapshot between worker
 * processes then REPEATABLE READ is used in there.
 */
bool
pgsql_set_snapshot(PGSQL *pgsql, char *snapshot)
{
	char sql[BUFSIZE] = { 0 };

	sformat(sql, sizeof(sql), "SET TRANSACTION SNAPSHOT '%s'", snapshot);

	return pgsql_execute(pgsql, sql);
}


/*
 * pgsql_execute opens a connection, runs a given SQL command, and closes
 * the connection again.
 *
 * We avoid persisting connection across multiple commands to simplify error
 * handling.
 */
bool
pgsql_execute(PGSQL *pgsql, const char *sql)
{
	return pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL, NULL, NULL);
}


/*
 * pgsql_execute_with_params implements running a SQL query using the libpq
 * API. This API requires very careful handling of responses and return values,
 * so we have a single implementation of that client-side parts of the Postgres
 * protocol.
 *
 * Also to avoid connection leaks we automatically open and clone connection at
 * query time, unless when the connection type is
 * PGSQL_CONNECTION_MULTI_STATEMENT. See pgsql_begin() above for details.
 *
 * Finally, in some cases we want to avoid logging queries entirely: we might
 * be handling customer data so privacy rules apply.
 */
bool
pgsql_execute_with_params(PGSQL *pgsql, const char *sql, int paramCount,
						  const Oid *paramTypes, const char **paramValues,
						  void *context, ParsePostgresResultCB *parseFun)
{
	PQExpBuffer debugParameters = NULL;

	PGconn *connection = pgsql_open_connection(pgsql);

	if (connection == NULL)
	{
		return false;
	}

	bool pipelineMode = pqPipelineModeEnabled(connection);

	/* parseFun is not allowed in pipeline mode */
	if (pipelineMode && parseFun != NULL)
	{
		log_error("BUG: pgsql_execute_with_params called in pipeline mode "
				  "with a parseFun callback");
		return false;
	}

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	if (pgsql->logSQL)
	{
		log_sql("[%s %d] %s;", endpoint, PQbackendPID(connection), sql);

		debugParameters = createPQExpBuffer();

		if (!build_parameters_list(debugParameters, paramCount, paramValues))
		{
			/* errors have already been logged */
			destroyPQExpBuffer(debugParameters);
			return false;
		}

		if (paramCount > 0)
		{
			log_sql("[%s %d] %s",
					endpoint,
					PQbackendPID(connection),
					debugParameters->data);
		}
	}

	int sentQuery = 0;

	if (paramCount == 0 && !pipelineMode)
	{
		sentQuery = PQsendQuery(connection, sql);
	}
	else
	{
		sentQuery = PQsendQueryParams(connection, sql,
									  paramCount, paramTypes, paramValues,
									  NULL, NULL, 0);
	}

	/*
	 * Use PQsetSingleRowMode(connection) to switch to select single-row mode
	 * and fetch only one result at a time in memory. Works with query result
	 * handlers that do not expect PQntuples() to reflect the actual number of
	 * tuples returned by the query etc.
	 */
	if (pgsql->singleRowMode)
	{
		if (PQsetSingleRowMode(connection) != 1)
		{
			log_error("Failed to select single-row mode: %s",
					  PQerrorMessage(connection));
			return false;
		}
	}

	bool done = false;
	int errors = 0;

	/* Don't fetch results in pipeline mode */
	if (!pipelineMode)
	{
		while (!done)
		{
			if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
			{
				log_error("Postgres query was interrupted: %s", sql);

				destroyPQExpBuffer(debugParameters);
				(void) pgsql_finish(pgsql);

				return false;
			}

			/* this uses select() with a timeout: we're not busy looping */
			if (!pgsql_fetch_results(pgsql, &done, context, parseFun))
			{
				++errors;
				break;
			}
		}
	}

	/*
	 * 1 is returned if the command was successfully dispatched and 0 if not.
	 */
	if (sentQuery == 0 || errors > 0)
	{
		pgsql_execute_log_error(pgsql, NULL, sql, debugParameters, context);
		destroyPQExpBuffer(debugParameters);

		/*
		 * Multi statements might want to ROLLBACK and hold to the open
		 * connection for a retry step.
		 */
		if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
		{
			(void) pgsql_finish(pgsql);
		}

		return false;
	}

	destroyPQExpBuffer(debugParameters);

	/* Don't clear results in pipeline mode */
	if (!pipelineMode)
	{
		clear_results(pgsql);
	}

	if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
	{
		(void) pgsql_finish(pgsql);
	}

	return true;
}


/*
 * pgsql_send_with_params implements sending a SQL query using the libpq async
 * API. Use pgsql_fetch_results to see if results are available are fetch them.
 */
bool
pgsql_send_with_params(PGSQL *pgsql, const char *sql, int paramCount,
					   const Oid *paramTypes, const char **paramValues)
{
	PQExpBuffer debugParameters = NULL;

	/* we can't close the connection before we have fetched the result */
	if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT)
	{
		log_error("BUG: pgsql_send_with_params called in SINGLE statement mode");
		return false;
	}

	PGconn *connection = pgsql_open_connection(pgsql);

	if (connection == NULL)
	{
		return false;
	}

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	if (pgsql->logSQL)
	{
		debugParameters = createPQExpBuffer();

		if (!build_parameters_list(debugParameters, paramCount, paramValues))
		{
			/* errors have already been logged */
			destroyPQExpBuffer(debugParameters);
			return false;
		}

		log_sql("[%s %d] %s;", endpoint, PQbackendPID(pgsql->connection), sql);

		if (paramCount > 0)
		{
			log_sql("%s", debugParameters->data);
		}
	}

	int result;

	if (paramCount == 0)
	{
		result = PQsendQuery(connection, sql);
	}
	else
	{
		result = PQsendQueryParams(connection, sql,
								   paramCount, paramTypes, paramValues,
								   NULL, NULL, 0);
	}

	if (result == 0)
	{
		char *message = PQerrorMessage(connection);

		LinesBuffer lbuf = { 0 };

		if (!splitLines(&lbuf, message))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * PostgreSQL Error message might contain several lines. Log each of
		 * them as a separate ERROR line here.
		 */
		for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
		{
			log_error("[%s %d] %s",
					  endpoint,
					  PQbackendPID(pgsql->connection),
					  lbuf.lines[lineNumber]);
		}

		if (pgsql->logSQL)
		{
			log_error("SQL query: %s", sql);
			log_error("SQL params: %s", debugParameters->data);
		}

		destroyPQExpBuffer(debugParameters);
		clear_results(pgsql);

		return false;
	}

	destroyPQExpBuffer(debugParameters);

	return true;
}


/*
 * pgsql_fetch_results is used to fetch the results of a SQL query that was
 * sent using the libpq async protocol with the pgsql_send_with_params
 * function.
 *
 * When the result is ready, the parseFun is called to parse the results as
 * when using pgsql_execute_with_params.
 */
bool
pgsql_fetch_results(PGSQL *pgsql, bool *done,
					void *context, ParsePostgresResultCB *parseFun)
{
	int r;
	PGconn *conn = pgsql->connection;

	*done = false;

	if (PQsocket(conn) < 0)
	{
		(void) pgsql_stream_log_error(pgsql, NULL, "invalid socket");

		clear_results(pgsql);
		pgsql_finish(pgsql);

		return false;
	}

	fd_set input_mask;
	struct timeval timeout;
	struct timeval *timeoutptr = NULL;

	/* sleep for 1ms to wait for input on the Postgres socket */
	timeout.tv_sec = 0;
	timeout.tv_usec = 1000;
	timeoutptr = &timeout;

	FD_ZERO(&input_mask);
	FD_SET(PQsocket(conn), &input_mask);

	r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);

	if (r == 0 || (r < 0 && errno == EINTR))
	{
		/* got a timeout or signal. The caller will get back later. */
		return true;
	}
	else if (r < 0)
	{
		(void) pgsql_stream_log_error(pgsql, NULL, "select failed: %m");

		clear_results(pgsql);
		pgsql_finish(pgsql);

		return false;
	}

	/* Else there is actually data on the socket */
	if (PQconsumeInput(conn) == 0)
	{
		(void) pgsql_stream_log_error(
			pgsql,
			NULL,
			"Failed to get async query results");
		return false;
	}

	/* Only collect the results when we know the server is ready for it */
	if (PQisBusy(conn) == 0)
	{
		PGresult *result = NULL;

		bool firstResult = true;

		/*
		 * When we got clearance that libpq did fetch the Postgres query result
		 * in its internal buffers, we process the result without checking for
		 * interrupts.
		 *
		 * The reason is that pgcopydb relies internally on signaling sibling
		 * processes to terminate at several places, including logical
		 * replication client and operating mode management. It's better for
		 * the code that we process the already available query result now and
		 * let the callers check for interrupts (asked_to_stop and friends).
		 */
		while ((result = PQgetResult(conn)) != NULL)
		{
			/* remember to check PQnotifies after each PQgetResult or PQexec */
			(void) pgsql_handle_notifications(pgsql);

			if (!is_response_ok(result))
			{
				pgsql_execute_log_error(pgsql, result, NULL, NULL, context);
				return false;
			}

			/*
			 * From Postgres docs:
			 *
			 * If the query returns any rows, they are returned as individual
			 * PGresult objects, which look like normal query results except
			 * for having status code PGRES_SINGLE_TUPLE instead of
			 * PGRES_TUPLES_OK. After the last row, or immediately if the query
			 * returns zero rows, a zero-row object with status PGRES_TUPLES_OK
			 * is returned; this is the signal that no more rows will arrive.
			 */
			if (parseFun != NULL)
			{
				bool skipCallback =
					!firstResult &&
					pgsql->singleRowMode &&
					PQntuples(result) == 0 &&
					PQresultStatus(result) == PGRES_TUPLES_OK;

				if (!skipCallback)
				{
					(*parseFun)(context, result);
				}
			}

			PQclear(result);

			if (firstResult)
			{
				firstResult = false;
			}
		}

		*done = true;

		PQclear(result);
		clear_results(pgsql);
	}

	return true;
}


/*
 * pgsql_enable_pipeline_mode enables the pipeline mode in the given PGSQL
 * connection. It also sets the connection to non-blocking mode.
 */
bool
pgsql_enable_pipeline_mode(PGSQL *pgsql)
{
#if defined(LIBPQ_HAS_PIPELINING) && LIBPQ_HAS_PIPELINING
	PGconn *conn = pgsql_open_connection(pgsql);

	if (conn == NULL)
	{
		return false;
	}

	if (PQpipelineStatus(conn) == PQ_PIPELINE_ON)
	{
		log_error("BUG: pgsql_enable_pipeline_mode called with connection "
				  "already in pipeline mode");
		return false;
	}

	/* enter pipeline mode */
	if (PQenterPipelineMode(conn) != 1)
	{
		(void) pgcopy_log_error(pgsql, NULL, "Failed to enter pipeline");
		return false;
	}

	/* set non-blocking mode */
	if (PQsetnonblocking(conn, 1) != 0)
	{
		(void) pgcopy_log_error(pgsql, NULL, "Failed to set non-blocking mode");
		return false;
	}

	log_trace("Enabled pipeline mode");
	return true;
#else
	static bool warned = false;

	if (!warned)
	{
		log_warn("Skipping libpq pipeline mode optimisation because pgcopydb "
				 "was built with libpq " PG_MAJORVERSION
				 ", pipeline mode is available since libpq 14");

		warned = true;
	}

	return true;
#endif
}


/*
 * pgsql_sync_pipeline drains the pipeline by sending a SYNC message and
 * reads results until we get a PGRES_PIPELINE_SYNC result.
 */
bool
pgsql_sync_pipeline(PGSQL *pgsql)
{
#if defined(LIBPQ_HAS_PIPELINING) && LIBPQ_HAS_PIPELINING
	PGconn *conn = pgsql->connection;

	if (conn == NULL)
	{
		log_error("BUG: pgsql_sync_pipeline called with NULL connection");
		return false;
	}

	log_trace("Start pipeline sync");

	if (PQpipelineStatus(conn) != PQ_PIPELINE_ON)
	{
		log_error("BUG: Connection is not in pipeline mode");
		return false;
	}

	if (PQisnonblocking(conn) == 0)
	{
		log_error("BUG: Connection is not in non-blocking mode");
		return false;
	}

	if (PQpipelineSync(conn) != 1)
	{
		(void) pgcopy_log_error(pgsql, NULL, "Failed send sync pipeline");
		return false;
	}

	int sock = PQsocket(conn);

	if (sock < 0)
	{
		(void) pgcopy_log_error(pgsql, NULL, "Failed to get socket for pipeline sync");
		return false;
	}

	bool syncReceived = false;

	/*
	 * PQpipelineSync() might clear the select read readiness, so we need to
	 * consume the input on the first iteration.
	 *
	 * The next iterations will consume input only when select() returns read
	 * readiness.
	 */
	bool readyToConsume = true;

	int results = 0;

	/*
	 * This implementation is based on the following libpq pipeline test,
	 * https://github.com/postgres/postgres/blob/a68159ff2b32f290b1136e2940470d50b8491301/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1967-L2023
	 */
	while (!syncReceived)
	{
		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			log_error("Pipeline sync was interrupted");

			clear_results(pgsql);
			pgsql_finish(pgsql);

			return false;
		}

		/*
		 * PQisBusy will not itself attempt to read data from the server;
		 * therefore PQconsumeInput must be invoked first, or the busy state
		 * will never end.
		 * Reference: https://www.postgresql.org/docs/current/libpq-async.html
		 */
		if (readyToConsume || PQisBusy(conn) == 1)
		{
			if (PQconsumeInput(conn) == 0)
			{
				(void) pgsql_stream_log_error(pgsql, NULL, "Failed to consume input");
				return false;
			}

			/*
			 * On pipeline mode, we are not worried about the order of
			 * the notifications, we just want to consume them to avoid
			 * filling the notification buffer.
			 */
			(void) pgsql_handle_notifications(pgsql);
		}

		/*
		 * Read results when the command is not busy.
		 */
		while (PQisBusy(conn) == 0)
		{
			PGresult *res = PQgetResult(conn);

			if (res == NULL)
			{
				/*
				 * NULL represents a end of result for a single query, but we are
				 * in pipeline mode and we can have multiple results.
				 *
				 * We need to continue consuming until we get a SYNC message.
				 */

				continue;
			}

			results++;

			ExecStatusType resultStatus = PQresultStatus(res);

			if (resultStatus == PGRES_PIPELINE_SYNC)
			{
				syncReceived = true;

				log_trace("Received pipeline sync. Total results: %d", results);

				PQclear(res);

				break;
			}

			if (!is_response_ok(res))
			{
				(void) pgcopy_log_error(pgsql, res, "Failed to receive pipeline sync");
				return false;
			}

			PQclear(res);
		}

		/*
		 * We need to wait for the socket to be ready for reading, otherwise
		 * select() will return immediately and we will busy loop.
		 */
		fd_set input_mask;
		struct timeval timeout;
		struct timeval *timeoutptr = NULL;

		/* sleep for 10ms to wait for input on the Postgres socket */
		timeout.tv_sec = 0;
		timeout.tv_usec = 10000;
		timeoutptr = &timeout;

		FD_ZERO(&input_mask);
		FD_SET(sock, &input_mask);

		int r = select(sock + 1, &input_mask, NULL, NULL, timeoutptr);

		if (r == 0 || (r < 0 && errno == EINTR))
		{
			/* got a timeout or signal. The caller will get back later. */
			readyToConsume = false;

			continue;
		}
		else if (r < 0)
		{
			(void) pgcopy_log_error(pgsql, NULL, "select failed: %m");

			clear_results(pgsql);
			pgsql_finish(pgsql);

			return false;
		}

		/* Else there is actually data on the socket */
		readyToConsume = true;
	}

	/* update the last pipeline sync time */
	pgsql->pipelineSyncTime = time(NULL);

	log_trace("Endof pipeline sync");

	return true;
#else
	static bool warned = false;

	if (!warned)
	{
		log_warn("Skipping libpq pipeline mode optimisation because pgcopydb "
				 "was built with libpq " PG_MAJORVERSION
				 ", pipeline mode is available since libpq 14");

		warned = true;
	}

	return true;
#endif
}


/*
 * pgsql_prepare implements server-side prepared statements by using the
 * Postgres protocol prepare/bind/execute messages. Use with
 * pgsql_execute_prepared().
 */
bool
pgsql_prepare(PGSQL *pgsql, const char *name, const char *sql,
			  int paramCount, const Oid *paramTypes)
{
	PGconn *connection = pgsql_open_connection(pgsql);

	if (connection == NULL)
	{
		return false;
	}

	bool pipelineMode = pqPipelineModeEnabled(connection);

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	if (pgsql->logSQL)
	{
		log_sql("[%s %d] PREPARE %s AS %s;",
				endpoint, PQbackendPID(connection), name, sql);
	}

	PGresult *result = NULL;
	bool ok = false;

	if (pipelineMode)
	{
		ok = PQsendPrepare(connection, name, sql, paramCount, paramTypes) != 0;
	}
	else
	{
		result = PQprepare(connection, name, sql, paramCount, paramTypes);

		ok = is_response_ok(result);
	}

	if (!ok)
	{
		pgsql_execute_log_error(pgsql, result, sql, NULL, NULL);

		/*
		 * Multi statements might want to ROLLBACK and hold to the open
		 * connection for a retry step.
		 */
		if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
		{
			(void) pgsql_finish(pgsql);
		}

		return false;
	}

	/* Don't clear results in pipeline mode */
	if (!pipelineMode)
	{
		PQclear(result);
		clear_results(pgsql);
	}

	if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
	{
		(void) pgsql_finish(pgsql);
	}

	return true;
}


/*
 * pgsql_prepare implements server-side prepared statements by using the
 * Postgres protocol prepare/bind/execute messages. Use with
 * pgsql_prepare().
 */
bool
pgsql_execute_prepared(PGSQL *pgsql, const char *name,
					   int paramCount, const char **paramValues,
					   void *context, ParsePostgresResultCB *parseFun)
{
	PQExpBuffer debugParameters = NULL;

	PGconn *connection = pgsql_open_connection(pgsql);

	if (connection == NULL)
	{
		return false;
	}

	bool pipelineMode = pqPipelineModeEnabled(connection);

	/* parseFun is not allowed in pipeline mode */
	if (pipelineMode && parseFun != NULL)
	{
		log_error("BUG: pgsql_execute_prepared called in pipeline mode "
				  "with a parseFun callback");
		return false;
	}

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	if (pgsql->logSQL)
	{
		debugParameters = createPQExpBuffer();

		if (!build_parameters_list(debugParameters, paramCount, paramValues))
		{
			/* errors have already been logged */
			destroyPQExpBuffer(debugParameters);
			return false;
		}

		log_sql("[%s %d] EXECUTE %s;",
				endpoint, PQbackendPID(connection), name);

		if (paramCount > 0)
		{
			log_sql("[%s %d] %s",
					endpoint,
					PQbackendPID(connection),
					debugParameters->data);
		}
	}

	PGresult *result = NULL;
	bool ok = false;

	if (pipelineMode)
	{
		ok = PQsendQueryPrepared(connection, name,
								 paramCount, paramValues,
								 NULL, NULL, 0) != 0;
	}
	else
	{
		result = PQexecPrepared(connection, name,
								paramCount, paramValues,
								NULL, NULL, 0);

		ok = is_response_ok(result);
	}

	if (!ok)
	{
		char sql[BUFSIZE] = { 0 };
		sformat(sql, sizeof(sql), "EXECUTE %s;", name);

		pgsql_execute_log_error(pgsql, result, sql, debugParameters, context);
		destroyPQExpBuffer(debugParameters);

		/*
		 * Multi statements might want to ROLLBACK and hold to the open
		 * connection for a retry step.
		 */
		if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
		{
			(void) pgsql_finish(pgsql);
		}

		return false;
	}

	if (parseFun != NULL)
	{
		(*parseFun)(context, result);
	}

	destroyPQExpBuffer(debugParameters);

	/* Don't clear results in pipeline mode */
	if (!pipelineMode)
	{
		PQclear(result);
		clear_results(pgsql);
	}

	if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
	{
		(void) pgsql_finish(pgsql);
	}

	return true;
}


/*
 * pgsql_execute_log_error logs an error when !is_response_ok(result).
 */
static void
pgsql_execute_log_error(PGSQL *pgsql,
						PGresult *result,
						const char *sql,
						PQExpBuffer debugParameters,
						void *context)
{
	char *sqlstate = NULL;

	if (result != NULL)
	{
		sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);

		if (sqlstate)
		{
			strlcpy(pgsql->sqlstate, sqlstate, sizeof(pgsql->sqlstate));
		}
	}

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	/*
	 * PostgreSQL Error message might contain several lines. Log each of
	 * them as a separate ERROR line here.
	 */
	char *message = PQerrorMessage(pgsql->connection);

	LinesBuffer lbuf = { 0 };

	/*
	 * PostgreSQL error message could be a static memory area in the
	 * code, in which case we are not allowed to edit the text and inject
	 * newlines. Duplicate the memory area before manipulating it.
	 *
	 * Also, because we use the Boehm GC lib, refrain from manually calling
	 * free() on the newly allocated memory area.
	 */
	if (message != NULL)
	{
		/* make sure message is writable by splitLines */
		message = strdup(message);
	}

	if (!splitLines(&lbuf, message))
	{
		/* errors have already been logged */
		return;
	}

	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		log_error("[%s %d] %s",
				  endpoint,
				  PQbackendPID(pgsql->connection),
				  lbuf.lines[lineNumber]);
	}

	if (pgsql->logSQL)
	{
		/* when using send/fetch async queries, fetch doesn't have the sql */
		if (sql != NULL)
		{
			log_error("[%s %d] SQL query: %s",
					  endpoint,
					  PQbackendPID(pgsql->connection),
					  sql);
		}

		if (debugParameters != NULL)
		{
			log_error("[%s %d] SQL params: %s",
					  endpoint,
					  PQbackendPID(pgsql->connection),
					  debugParameters->data);
		}
	}

	/* now stash away the SQL STATE if any */
	if (context && sqlstate)
	{
		AbstractResultContext *ctx = (AbstractResultContext *) context;

		strlcpy(ctx->sqlstate, sqlstate, SQLSTATE_LENGTH);
	}

	/* if we get a connection exception, track that */
	if (sqlstate &&
		strncmp(sqlstate, STR_ERRCODE_CLASS_CONNECTION_EXCEPTION, 2) == 0)
	{
		pgsql->status = PG_CONNECTION_BAD;
	}

	if (result != NULL)
	{
		PQclear(result);
	}
	clear_results(pgsql);
}


/*
 * build_parameters_list builds a string representation of the SQL query
 * parameter list given.
 */
static bool
build_parameters_list(PQExpBuffer buffer,
					  int paramCount, const char **paramValues)
{
	if (buffer == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (paramCount > 0)
	{
		int paramIndex = 0;

		for (paramIndex = 0; paramIndex < paramCount; paramIndex++)
		{
			const char *value = paramValues[paramIndex];

			if (paramIndex > 0)
			{
				appendPQExpBuffer(buffer, ", ");
			}

			if (value == NULL)
			{
				appendPQExpBuffer(buffer, "NULL");
			}
			else
			{
				appendPQExpBuffer(buffer, "'%s'", value);
			}
		}

		if (PQExpBufferBroken(buffer))
		{
			log_error("Failed to create log message for SQL query parameters: "
					  "out of memory");
			destroyPQExpBuffer(buffer);
			return false;
		}
	}

	return true;
}


/*
 * is_response_ok returns whether the query result is a correct response
 * (not an error or failure).
 */
bool
is_response_ok(PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	bool ok =
		resultStatus == PGRES_SINGLE_TUPLE ||
		resultStatus == PGRES_TUPLES_OK ||
		resultStatus == PGRES_COPY_BOTH ||
		resultStatus == PGRES_COMMAND_OK;

	if (!ok)
	{
		log_debug("Postgres result status is %s", PQresStatus(resultStatus));
	}

	return ok;
}


/*
 * is_connection_error returns true if we have a client-side connection error
 * or a server-side reported connection issue, wherein the PGSQL sqlstate
 * belongs to:
 *
 *   Class 08 — Connection Exception.
 *
 * https://www.postgresql.org/docs/current/errcodes-appendix.html
 *
 * 08000	connection_exception
 * 08003	connection_does_not_exist
 * 08006	connection_failure
 * 08001	sqlclient_unable_to_establish_sqlconnection
 * 08004	sqlserver_rejected_establishment_of_sqlconnection
 * 08007	transaction_resolution_unknown
 * 08P01	protocol_violation
 */
#define SQLSTATE_IS_CONNECTION_EXCEPTION(pgsql) \
	(pgsql->sqlstate[0] == '0' && pgsql->sqlstate[1] == '8')

bool
pgsql_state_is_connection_error(PGSQL *pgsql)
{
	return pgsql->connection != NULL &&
		   (PQstatus(pgsql->connection) == CONNECTION_BAD ||
			SQLSTATE_IS_CONNECTION_EXCEPTION(pgsql));
}


/*
 * clear_results consumes results on a connection until NULL is returned.
 * If an error is returned it returns false.
 */
bool
clear_results(PGSQL *pgsql)
{
	PGconn *connection = pgsql->connection;

	if (PQstatus(connection) == CONNECTION_BAD)
	{
		pgsql_finish(pgsql);
		return false;
	}

	while (true)
	{
		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			pgsql_finish(pgsql);
			return false;
		}

		/*
		 * Per Postgres documentation: You should, however, remember to check
		 * PQnotifies after each PQgetResult or PQexec, to see if any
		 * notifications came in during the processing of the command.
		 *
		 * Before calling clear_results(), we called PQgetResult().
		 */
		(void) pgsql_handle_notifications(pgsql);

		PGresult *result = PQgetResult(connection);

		if (result == NULL)
		{
			/* one last time */
			(void) pgsql_handle_notifications(pgsql);
			break;
		}

		if (!is_response_ok(result))
		{
			LinesBuffer lbuf = { 0 };
			char *pqmessage = PQerrorMessage(connection);

			if (!splitLines(&lbuf, pqmessage))
			{
				/* errors have already been logged */
				return false;
			}

			for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
			{
				log_error("[Postgres] %s", lbuf.lines[lineNumber]);
			}

			PQclear(result);
			pgsql_finish(pgsql);
			return false;
		}

		PQclear(result);
	}

	return true;
}


/*
 * pgsql_handle_notifications check PQnotifies when a PGSQL notificationChannel
 * has been set. Then if the parsed notification is from the
 * notificationGroupId we set notificationReceived and also log the
 * notification.
 *
 * This allow another part of the code to later know that some notifications
 * have been received.
 */
static void
pgsql_handle_notifications(PGSQL *pgsql)
{
	PGconn *connection = pgsql->connection;
	PGnotify *notify;

	if (PQconsumeInput(connection) == 0)
	{
		char *message = PQerrorMessage(pgsql->connection);
		log_error("Failed to process Postgres notifications: %s", message);
		return;
	}

	/* consume all notifications, even when there is no function registered */
	while ((notify = PQnotifies(connection)) != NULL)
	{
		log_trace("pgsql_handle_notifications: \"%s\"", notify->extra);

		if (pgsql->notificationProcessFunction == NULL)
		{
			PQfreemem(notify);
			continue;
		}

		if ((*pgsql->notificationProcessFunction)(pgsql->notificationGroupId,
												  pgsql->notificationNodeId,
												  notify->relname,
												  notify->extra))
		{
			/* mark that we received some notifications */
			pgsql->notificationReceived = true;
		}

		PQfreemem(notify);
	}
}


/*
 * validate_connection_string takes a connection string and parses it with
 * libpq, varifying that it's well formed and usable.
 */
bool
validate_connection_string(const char *connectionString)
{
	char *errorMessage = NULL;

	PQconninfoOption *connInfo = PQconninfoParse(connectionString, &errorMessage);
	if (connInfo == NULL)
	{
		log_error("Failed to parse connection string \"%s\": %s ",
				  connectionString, errorMessage);
		PQfreemem(errorMessage);
		return false;
	}

	PQconninfoFree(connInfo);

	return true;
}


/*
 * pgsql_lock_table runs a LOCK command with given lockmode.
 */
bool
pgsql_lock_table(PGSQL *pgsql, const char *qname, const char *lockmode)
{
	char sql[BUFSIZE] = { 0 };

	sformat(sql, sizeof(sql), "LOCK TABLE ONLY %s IN %s MODE", qname, lockmode);

	/* this is an internal operation, not meaningful from the outside */
	log_sql("%s", sql);

	return pgsql_execute(pgsql, sql);
}


/*
 * pgsql_truncate executes the TRUNCATE command on the given quoted relation
 * name qname, in the given Postgres connection.
 */
bool
pgsql_truncate(PGSQL *pgsql, const char *qname)
{
	char sql[BUFSIZE] = { 0 };

	sformat(sql, sizeof(sql), "TRUNCATE ONLY %s", qname);

	/* this being more like a DDL operation, proper log level is NOTICE */
	log_notice("%s", sql);

	return pgsql_execute(pgsql, sql);
}


/*
 * pg_copy implements a COPY operation from a source Postgres instance (src) to
 * a target Postgres instance (dst), for the data found in the table referenced
 * by the qualified identifier name srcQname on the source, into the table
 * referenced by the qualified identifier name dstQname on the target.
 */
bool
pg_copy(PGSQL *src, PGSQL *dst,
		CopyArgs *args, CopyStats *stats,
		void *context, CopyStatsCallback *callback)
{
	bool srcConnIsOurs = src->connection == NULL;
	if (!pgsql_open_connection(src))
	{
		return false;
	}

	bool dstConnIsOurs = dst->connection == NULL;
	if (!pgsql_open_connection(dst))
	{
		/* errors have already been logged */
		if (srcConnIsOurs)
		{
			pgsql_finish(src);
		}
		return false;
	}

	bool result = pg_copy_data(src, dst, args, stats, context, callback);

	if (srcConnIsOurs)
	{
		pgsql_finish(src);
	}

	if (dstConnIsOurs)
	{
		pgsql_finish(dst);
	}

	return result;
}


/*
 * pg_copy_data implements the core of pg_copy. That is, COPY operation from a
 * source Postgres instance (src) to a target Postgres instance (dst). It
 * expects src and dst are opened connection and doesn't manage their lifetime.
 */
static bool
pg_copy_data(PGSQL *src, PGSQL *dst,
			 CopyArgs *args, CopyStats *stats,
			 void *context, CopyStatsCallback *callback)
{
	PGconn *srcConn = src->connection;
	PGconn *dstConn = dst->connection;

	if (!pgsql_begin(dst))
	{
		return false;
	}

	if (args->truncate)
	{
		if (!pgsql_truncate(dst, args->dstQname))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/*
	 * COPY FREEZE is only accepted by Postgres if the table was created or
	 * truncated in the current transaction.
	 */
	args->freeze &= args->truncate;

	/* make sure to log TRUNCATE before we log COPY, avoid confusion */
	log_notice("%s", args->logCommand);

	/* SRC: COPY schema.table TO STDOUT */
	if (!pg_copy_send_query(src, args, PGRES_COPY_OUT))
	{
		return false;
	}

	/* DST: COPY schema.table FROM STDIN WITH (FREEZE) */
	if (!pg_copy_send_query(dst, args, PGRES_COPY_IN))
	{
		return false;
	}

	/* now implement the copy loop */
	char *copybuf;
	bool failedOnSrc = false;
	bool failedOnDst = false;

	/* also init and maintain copy statistics */
	stats->startTime = time(NULL);
	stats->bytesTransmitted = 0;

	for (;;)
	{
		/* handle signals */
		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			log_debug("COPY was asked to stop");
			return false;
		}

		int bufsize = PQgetCopyData(srcConn, &copybuf, 1);

		/*
		 * A result of -2 indicates that an error occurred.
		 */
		if (bufsize == -2)
		{
			failedOnSrc = true;

			pgcopy_log_error(src, NULL, "Failed to fetch data from source");
			break;
		}

		/*
		 * PQgetCopyData returns -1 to indicate that the COPY is done. Call
		 * PQgetResult to obtain the final result status of the COPY command.
		 */
		else if (bufsize == -1)
		{
			PGresult *res = PQgetResult(srcConn);

			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				failedOnSrc = true;

				pgcopy_log_error(src, res, "Failed to fetch data from source");
				break;
			}

			/* we're done here */
			clear_results(src);

			/* make sure to pass through and send this last COPY buffer */
		}

		/*
		 * In async mode, and no data available.
		 */
		else if (bufsize == 0)
		{
			fd_set input_mask;
			int sock = PQsocket(srcConn);

			if (sock < 0)
			{
				failedOnSrc = true;

				pgcopy_log_error(src, NULL, "invalid socket");
				break;
			}

			struct timeval timeout;
			struct timeval *timeoutptr = NULL;

			/* sleep for 10ms to wait for input on the Postgres socket */
			timeout.tv_sec = 0;
			timeout.tv_usec = 10000;
			timeoutptr = &timeout;

			FD_ZERO(&input_mask);
			FD_SET(sock, &input_mask);

			int r = select(sock + 1, &input_mask, NULL, NULL, timeoutptr);

			if (r == 0 || (r < 0 && errno == EINTR))
			{
				/*
				 * Got a timeout or signal. Continue the loop and either
				 * deliver a status packet to the server or just go back into
				 * blocking.
				 */
				continue;
			}
			else if (r < 0)
			{
				failedOnSrc = true;

				pgcopy_log_error(src, NULL, "select failed: %m");
				break;
			}

			/* there is actually data on the socket */
			if (PQconsumeInput(srcConn) == 0)
			{
				failedOnSrc = true;

				pgcopy_log_error(src, NULL, "could not receive data");
				break;
			}
		}

		/*
		 * If successful PQgetCopyData returns the row length as a result.
		 */
		else if (bufsize > 0)
		{
			stats->bytesTransmitted += bufsize;

			if (callback != NULL)
			{
				/*
				 * Allow the Copy Stats user callback to fail, still continue
				 * with the copy.
				 */
				if (!(*callback)(context, stats))
				{
					log_debug("Copy Stats Callback failed, "
							  "see above for details");
				}
			}
		}

		/*
		 * We got a COPY buffer from the source database, send it over as-is to
		 * the target database, which speaks the same COPY protocol, after all.
		 */
		if (copybuf)
		{
			int ret = PQputCopyData(dstConn, copybuf, bufsize);
			PQfreemem(copybuf);

			if (ret == -1)
			{
				failedOnDst = true;

				pgcopy_log_error(dst, NULL, "Failed to copy data to target");

				clear_results(src);

				break;
			}
		}

		/* when we've reached the end of COPY from the source, stop here */
		if (bufsize == -1)
		{
			break;
		}
	}

	/*
	 * The COPY loop is over now.
	 *
	 * Time to send end-of-data indication to the server during COPY_IN state.
	 */
	if (!failedOnDst)
	{
		char *errormsg =
			failedOnSrc ? "Failed to get data from source" : NULL;

		int res = PQputCopyEnd(dstConn, errormsg);

		if (res > 0)
		{
			PGresult *res = PQgetResult(dstConn);

			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				failedOnDst = true;
				pgcopy_log_error(dst, res, "Failed to copy data to target");
			}
		}

		clear_results(dst);

		if (!failedOnDst)
		{
			if (!pgsql_execute(dst, "COMMIT"))
			{
				failedOnDst = true;
			}
		}
	}

	return !failedOnSrc && !failedOnDst;
}


/*
 * pg_copy_from_stdin prepares the SQL query to open a COPY streaming to upload
 * data to a Postgres table.
 */
bool
pg_copy_from_stdin(PGSQL *pgsql, const char *qname)
{
	char sql[BUFSIZE] = { 0 };

	sformat(sql, sizeof(sql), "COPY %s FROM stdin", qname);

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	log_sql("[%s %d] %s;", endpoint, PQbackendPID(pgsql->connection), sql);

	PGresult *res = PQexec(pgsql->connection, sql);

	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		pgcopy_log_error(pgsql, res, sql);

		return false;
	}

	return true;
}


/*
 * pg_copy_row_from_stdin streams a row of data at a time into the already
 * opened COPY protocol stream. Only default text mode is supported.
 *
 * The fmt string is a list of data type selectors, from the following list:
 *
 *  - 's' for a text column (string, as in %s)
 */
bool
pg_copy_row_from_stdin(PGSQL *pgsql, char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);

	for (int i = 0; fmt[i] != '\0'; i++)
	{
		if (i > 0)
		{
			if (PQputCopyData(pgsql->connection, "\t", 1) == -1)
			{
				va_end(args);

				pgcopy_log_error(pgsql, NULL, "Failed to copy row from stdin");
				pgsql_finish(pgsql);

				return false;
			}
		}

		switch (fmt[i])
		{
			case 's':
			{
				char *str = va_arg(args, char *);
				int len = strlen(str);

				if (PQputCopyData(pgsql->connection, str, len) == -1)
				{
					va_end(args);

					pgcopy_log_error(pgsql, NULL, "Failed to copy row from stdin");
					pgsql_finish(pgsql);

					return false;
				}

				break;
			}

			/* at the moment we don't need to support numeric fields etc */
			default:
			{
				va_end(args);

				log_error("BUG: COPY data type %c is not supported", fmt[i]);
				pgsql_finish(pgsql);

				return false;
			}
		}
	}

	if (PQputCopyData(pgsql->connection, "\n", 1) == -1)
	{
		va_end(args);

		pgcopy_log_error(pgsql, NULL, "Failed to copy row from stdin");
		pgsql_finish(pgsql);

		return false;
	}

	va_end(args);

	return true;
}


/*
 * pg_copy_end calls PQputCopyEnd and clears pending notifications and results
 * from the connection.
 */
bool
pg_copy_end(PGSQL *pgsql)
{
	if (PQputCopyEnd(pgsql->connection, NULL) == -1)
	{
		pgcopy_log_error(pgsql, NULL, "Failed to copy row from stdin");
		pgsql_finish(pgsql);
		return false;
	}

	clear_results(pgsql);

	return true;
}


/*
 * pg_copy_send_query prepares the SQL query that opens a COPY protocol from or
 * to a Postgres instance, and checks that the server's result is as expected.
 */
static bool
pg_copy_send_query(PGSQL *pgsql, CopyArgs *args, ExecStatusType status)
{
	PQExpBuffer sql = createPQExpBuffer();

	if (status == PGRES_COPY_OUT)
	{
		/* There is no COPY TO with FREEZE */
		if (args->srcWhereClause != NULL)
		{
			appendPQExpBuffer(sql, "copy (SELECT %s FROM ONLY %s %s) ",
							  args->srcAttrList,
							  args->srcQname,
							  args->srcWhereClause);
		}
		else
		{
			appendPQExpBuffer(sql, "copy (SELECT %s FROM ONLY %s) ",
							  args->srcAttrList,
							  args->srcQname);
		}

		appendPQExpBuffer(sql, "to stdout");

		if (args->useCopyBinary)
		{
			appendPQExpBuffer(sql, " with (format binary)");
		}
	}
	else if (status == PGRES_COPY_IN)
	{
		if (args->dstAttrList != NULL && !streq(args->dstAttrList, ""))
		{
			appendPQExpBuffer(sql, "copy %s(%s) from stdin",
							  args->dstQname,
							  args->dstAttrList);
		}
		else
		{
			appendPQExpBuffer(sql, "copy %s from stdin", args->dstQname);
		}

		if (args->freeze && args->useCopyBinary)
		{
			appendPQExpBuffer(sql, " with (freeze, format binary)");
		}
		else if (args->freeze)
		{
			appendPQExpBuffer(sql, " with (freeze)");
		}
		else if (args->useCopyBinary)
		{
			appendPQExpBuffer(sql, " with (format binary)");
		}
	}
	else
	{
		log_error("BUG: pg_copy_send_query: unknown ExecStatusType %d", status);
		return false;
	}

	if (PQExpBufferBroken(sql))
	{
		log_error("Failed to create COPY query for %s: out of memory",
				  args->srcQname);
		return false;
	}

	log_sql("%s;", sql->data);

	PGresult *res = PQexec(pgsql->connection, sql->data);

	if (PQresultStatus(res) != status)
	{
		pgcopy_log_error(pgsql, res, sql->data);

		destroyPQExpBuffer(sql);
		return false;
	}

	destroyPQExpBuffer(sql);
	return true;
}


/*
 * pgcopy_log_error logs an error message when the PGresult obtained during
 * COPY is not as expected.
 */
static void
pgcopy_log_error(PGSQL *pgsql, PGresult *res, const char *context)
{
	LinesBuffer lbuf = { 0 };
	char *message = PQerrorMessage(pgsql->connection);

	if (!splitLines(&lbuf, message))
	{
		/* errors have already been logged */
		return;
	}

	if (res != NULL)
	{
		char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (sqlstate == NULL)
		{
			/* PQresultErrorField returned NULL! */
			pgsql->sqlstate[0] = '\0';  /* Set to an empty string to avoid segfault */
		}
		else
		{
			strlcpy(pgsql->sqlstate, sqlstate, sizeof(pgsql->sqlstate));
		}
	}

	char *endpoint =
		pgsql->connectionType == PGSQL_CONN_SOURCE ? "SOURCE" : "TARGET";

	/*
	 * PostgreSQL Error message might contain several lines. Log each of
	 * them as a separate ERROR line here.
	 */
	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		if (lineNumber == 0 && res != NULL)
		{
			log_error("[%s %d] [%s] %s",
					  endpoint,
					  PQbackendPID(pgsql->connection),
					  pgsql->sqlstate,
					  lbuf.lines[lineNumber]);
		}
		else
		{
			log_error("[%s %d] %s",
					  endpoint,
					  PQbackendPID(pgsql->connection),
					  lbuf.lines[lineNumber]);
		}
	}

	log_error("[%s %d] Context: %s",
			  endpoint,
			  PQbackendPID(pgsql->connection),
			  context);

	if (res != NULL)
	{
		PQclear(res);
	}

	clear_results(pgsql);
	pgsql_finish(pgsql);
}


/* Context used when fetching metadata for a given sequence */
typedef struct SourceSequenceContext
{
	char sqlstate[SQLSTATE_LENGTH];
	int64_t lastValue;
	bool isCalled;
	bool parsedOk;
} SourceSequenceContext;


/*
 * pgsql_get_sequence queries the Postgres catalog object for the sequence to
 * get the last_value and is_called columns.
 *
 * The connection is expected to be opened and closed from the caller.
 */
bool
pgsql_get_sequence(PGSQL *pgsql, const char *qname,
				   int64_t *lastValue,
				   bool *isCalled)
{
	char sql[BUFSIZE] = { 0 };
	SourceSequenceContext context = { 0 };

	/* identifiers have already been escaped thanks to format('%I', ...) */
	sformat(sql, sizeof(sql), "select last_value, is_called from %s",
			qname);

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &getSequenceValue))
	{
		log_error("Failed to retrieve metadata for sequence %s",
				  qname);
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to retrieve metadata for sequence %s",
				  qname);
		return false;
	}

	/* publish values */
	*lastValue = context.lastValue;
	*isCalled = context.isCalled;

	return true;
}


/*
 * getSequenceValue parses a single row of the table listing query
 * result.
 */
static void
getSequenceValue(void *ctx, PGresult *result)
{
	SourceSequenceContext *context = (SourceSequenceContext *) ctx;

	if (PQntuples(result) != 1)
	{
		log_error("Query returned %d rows, expected 1", PQntuples(result));
		context->parsedOk = false;
		return;
	}

	if (PQnfields(result) != 2)
	{
		log_error("Query returned %d columns, expected 2", PQnfields(result));
		context->parsedOk = false;
		return;
	}

	int errors = 0;

	/* 1. last_value */
	char *value = PQgetvalue(result, 0, 0);

	if (!stringToInt64(value, &(context->lastValue)))
	{
		log_error("Invalid sequence last_value \"%s\"", value);
		++errors;
	}

	/* 2. is_called */
	if (PQgetisnull(result, 0, 1))
	{
		log_error("Invalid sequence is_called value: NULL");
		++errors;
	}
	else
	{
		value = PQgetvalue(result, 0, 1);
		context->isCalled = (*value) == 't';
	}

	if (errors > 0)
	{
		context->parsedOk = false;
		return;
	}

	/* if we reach this line, then we're good. */
	context->parsedOk = true;
}


/*
 * pgsql_set_gucs sets the given GUC array in the current session attached to
 * the pgsql client.
 */
bool
pgsql_set_gucs(PGSQL *pgsql, GUC *settings)
{
	/*
	 * This only works for already opened connections set-up for multiple
	 * statements, otherwise after the SET command is done, the setting changes
	 * would be lost already.
	 */
	if (pgsql->connection == NULL)
	{
		/* open a multi-statements connection then */
		pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;
	}
	else if (pgsql->connectionStatementType != PGSQL_CONNECTION_MULTI_STATEMENT)
	{
		log_error("BUG: calling pgsql_set_gucs with a "
				  "non PGSQL_CONNECTION_MULTI_STATEMENT connection");
		pgsql_finish(pgsql);
		return false;
	}

	for (int i = 0; settings[i].name != NULL; i++)
	{
		char sql[BUFSIZE] = { 0 };

		sformat(sql, sizeof(sql), "SET %s TO %s",
				settings[i].name, settings[i].value);

		if (!pgsql_execute(pgsql, sql))
		{
			return false;
		}
	}

	return true;
}


/*
 * pg_copy_large_object copies given large object found on the src database
 * into the dst database. The copy includes re-using the same OID for the large
 * objects on both sides.
 */
bool
pg_copy_large_object(PGSQL *src,
					 PGSQL *dst,
					 bool dropIfExists,
					 uint32_t blobOid,
					 uint64_t *bytesTransmitted)
{
	log_debug("Copying large object %u", blobOid);

	/*
	 * 1. Open the blob on the source database
	 */
	int srcfd = lo_open(src->connection, blobOid, INV_READ);

	if (srcfd == -1)
	{
		char context[BUFSIZE] = { 0 };

		sformat(context, sizeof(context),
				"Failed to open large object %u", blobOid);

		(void) pgcopy_log_error(src, NULL, context);

		pgsql_finish(src);
		pgsql_finish(dst);

		return false;
	}

	/*
	 * 2. Drop/Create the blob on the target database.
	 *
	 *    When using --drop-if-exists, we first try to unlink the
	 *    target large object, then copy the data all over again.
	 *
	 *    In normal cases `pg_dump --section=pre-data` outputs the
	 *    large object metadata and we only have to take care of the
	 *    contents of the large objects.
	 */
	if (dropIfExists)
	{
		if (!lo_unlink(dst->connection, blobOid))
		{
			/* ignore errors, the object might not exists */
			log_debug("Failed to delete large object %u", blobOid);
		}

		Oid dstBlobOid = lo_create(dst->connection, blobOid);

		if (dstBlobOid != blobOid)
		{
			char context[BUFSIZE] = { 0 };

			sformat(context, sizeof(context),
					"Failed to create large object %u", blobOid);

			(void) pgcopy_log_error(dst, NULL, context);

			lo_close(src->connection, srcfd);

			pgsql_finish(src);
			pgsql_finish(dst);

			return false;
		}
	}

	/*
	 * 3. Open the blob on the target database
	 */
	int dstfd = lo_open(dst->connection, blobOid, INV_WRITE);

	if (dstfd == -1)
	{
		char context[BUFSIZE] = { 0 };

		sformat(context, sizeof(context),
				"Failed to open new large object %u", blobOid);

		(void) pgcopy_log_error(dst, NULL, context);

		lo_close(src->connection, srcfd);

		pgsql_finish(src);
		pgsql_finish(dst);

		return false;
	}

	/*
	 * 4. Read the large object content in chunks on the source
	 *    database, and write them on the target database.
	 */
	int bytesRead = 0;
	int bytesWritten = 0;

	do {
		char *buffer = (char *) calloc(LOBBUFSIZE, sizeof(char));

		if (buffer == NULL)
		{
			char context[BUFSIZE] = { 0 };

			sformat(context, sizeof(context),
					"Out of Memory for reading large object %u", blobOid);

			(void) pgcopy_log_error(dst, NULL, context);

			lo_close(src->connection, srcfd);
			lo_close(dst->connection, dstfd);

			pgsql_finish(src);
			pgsql_finish(dst);

			return false;
		}

		bytesRead =
			lo_read(src->connection, srcfd, buffer, LOBBUFSIZE);

		if (bytesRead < 0)
		{
			char context[BUFSIZE] = { 0 };

			sformat(context, sizeof(context),
					"Failed to read large object %u", blobOid);

			(void) pgcopy_log_error(src, NULL, context);

			lo_close(src->connection, srcfd);
			lo_close(dst->connection, dstfd);

			pgsql_finish(src);
			pgsql_finish(dst);

			return false;
		}

		bytesWritten =
			lo_write(dst->connection, dstfd, buffer, (size_t) bytesRead);

		if (bytesWritten != bytesRead)
		{
			char context[BUFSIZE] = { 0 };

			sformat(context, sizeof(context),
					"Failed to write large object %u", blobOid);

			(void) pgcopy_log_error(dst, NULL, context);

			lo_close(src->connection, srcfd);
			lo_close(dst->connection, dstfd);

			pgsql_finish(src);
			pgsql_finish(dst);

			return false;
		}

		*bytesTransmitted += bytesRead;
	} while (bytesRead > 0);

	lo_close(src->connection, srcfd);
	lo_close(dst->connection, dstfd);

	return true;
}


/*
 * pgsql_init_stream initializes the logical decoding streaming client with the
 * given parameters.
 */
bool
pgsql_init_stream(LogicalStreamClient *client,
				  const char *pguri,
				  StreamOutputPlugin plugin,
				  const char *slotName,
				  XLogRecPtr startpos,
				  XLogRecPtr endpos)
{
	PGSQL *pgsql = &(client->pgsql);

	if (!pgsql_init(pgsql, (char *) pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		return false;
	}

	/* we're going to send several replication commands */
	pgsql->connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	client->plugin = plugin;

	strlcpy(client->slotName, slotName, sizeof(client->slotName));

	client->startpos = startpos;
	client->endpos = endpos;

	client->fsync_interval = 10 * 1000;          /* 10 sec = default */
	client->standby_message_timeout = 10 * 1000; /* 10 sec = default */

	client->current.written_lsn = startpos;
	client->current.flushed_lsn = startpos;
	client->current.applied_lsn = InvalidXLogRecPtr;

	client->feedback.written_lsn = startpos;
	client->feedback.flushed_lsn = startpos;
	client->feedback.applied_lsn = InvalidXLogRecPtr;

	return true;
}


/*
 * OutputPluginFromString returns an enum value from its string representation.
 */
StreamOutputPlugin
OutputPluginFromString(char *plugin)
{
	if (strcmp(plugin, "test_decoding") == 0)
	{
		return STREAM_PLUGIN_TEST_DECODING;
	}
	else if (strcmp(plugin, "wal2json") == 0)
	{
		return STREAM_PLUGIN_WAL2JSON;
	}

	return STREAM_PLUGIN_UNKNOWN;
}


/*
 * OutputPluginToString converts a StreamOutputPlugin enum to string.
 */
char *
OutputPluginToString(StreamOutputPlugin plugin)
{
	switch (plugin)
	{
		case STREAM_PLUGIN_UNKNOWN:
		{
			return "unknon output plugin";
		}

		case STREAM_PLUGIN_TEST_DECODING:
		{
			return "test_decoding";
		}

		case STREAM_PLUGIN_WAL2JSON:
		{
			return "wal2json";
		}

		default:
		{
			log_error("Unknown logical decoding output plugin %d", plugin);
			return NULL;
		}
	}

	return NULL;
}


/*
 * Send the CREATE_REPLICATION_SLOT logical replication command.
 *
 * This is a Postgres 9.6 compatibility function.
 *
 * There is a deadlock situation when calling
 * pg_create_logical_replication_slot() within a transaction that uses an
 * already exported snapshot in Postgres 9.6.
 *
 * So when the source server is running 9.6 we need to export the snapshot from
 * the logical replication command CREATE_REPLICATION_SLOT,
 */
bool
pgsql_create_logical_replication_slot(LogicalStreamClient *client,
									  ReplicationSlot *slot)
{
	PGSQL *pgsql = &(client->pgsql);

	/* Initiate the replication stream at specified location */
	char query[BUFSIZE] = { 0 };

	sformat(query, sizeof(query),
			"CREATE_REPLICATION_SLOT \"%s\" LOGICAL \"%s\"",
			client->slotName,
			OutputPluginToString(client->plugin));

	if (!pgsql_open_connection(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	PGresult *result = PQexec(pgsql->connection, query);

	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		log_error("Failed to send CREATE_REPLICATION_SLOT command:");

		(void) pgcopy_log_error(pgsql, result, query);

		return false;
	}

	int nTuples = PQntuples(result);

	if (nTuples != 1)
	{
		log_error("Logical replication command CREATE_REPLICATION_SLOT "
				  "returned %d rows, expected 1",
				  nTuples);
		pgsql_finish(pgsql);
		return false;
	}

	if (PQnfields(result) != 4)
	{
		log_error("Logical replication command CREATE_REPLICATION_SLOT "
				  "returned %d columns, expected 4",
				  PQnfields(result));
		pgsql_finish(pgsql);
		return false;
	}

	/* 1. slot_name */
	char *value = PQgetvalue(result, 0, 0);

	if (strcmp(value, client->slotName) != 0)
	{
		log_error("Logical replication command CREATE_REPLICATION_SLOT "
				  "returned slot_name \"%s\", expected \"%s\"",
				  value,
				  client->slotName);
		pgsql_finish(pgsql);
		return false;
	}

	strlcpy(value, slot->slotName, sizeof(slot->slotName));

	/* 2. consistent_point */
	value = PQgetvalue(result, 0, 1);

	if (!parseLSN(value, &(slot->lsn)))
	{
		log_error("Failed to parse consistent_point LSN \"%s\" returned by "
				  " logical replication command CREATE_REPLICATION_SLOT",
				  value);
		pgsql_finish(pgsql);
		return false;
	}

	/* 3. snapshot_name */
	if (PQgetisnull(result, 0, 2))
	{
		log_error("Logical replication command CREATE_REPLICATION_SLOT "
				  "returned snapshot_name NULL");
		pgsql_finish(pgsql);
		return false;
	}
	else
	{
		value = PQgetvalue(result, 0, 2);
		int length = strlcpy(slot->snapshot, value, sizeof(slot->snapshot));

		if (length >= sizeof(slot->snapshot))
		{
			log_error("Snapshot \"%s\" is %d bytes long, the maximum is %zu",
					  value, length, sizeof(slot->snapshot) - 1);
			pgsql_finish(pgsql);
			return false;
		}
	}

	/* 4. output_plugin */
	if (PQgetisnull(result, 0, 3))
	{
		log_error("Logical replication command CREATE_REPLICATION_SLOT "
				  "returned output_plugin is NULL, expected \"%s\"",
				  OutputPluginToString(client->plugin));
		pgsql_finish(pgsql);
		return false;
	}
	else
	{
		value = PQgetvalue(result, 0, 3);

		if (OutputPluginFromString(value) != client->plugin)
		{
			log_error("Logical replication command CREATE_REPLICATION_SLOT "
					  "returned output_plugin \"%s\", expected \"%s\"",
					  value,
					  OutputPluginToString(client->plugin));
			pgsql_finish(pgsql);
			return false;
		}

		slot->plugin = client->plugin;
	}

	log_info("Created logical replication slot \"%s\" with plugin \"%s\" "
			 "at %X/%X and exported snapshot %s",
			 slot->slotName,
			 OutputPluginToString(slot->plugin),
			 LSN_FORMAT_ARGS(slot->lsn),
			 slot->snapshot);

	return true;
}


/*
 * Convert a Postgres TimestampTz value to an ISO date time string.
 */
bool
pgsql_timestamptz_to_string(TimestampTz ts, char *str, size_t size)
{
	/* Postgres Epoch is 2000-01-01, Unix Epoch usually is 1970-01-01 */
	static time_t pgepoch = 0;

	if (pgepoch == 0)
	{
		char *pgepoch_str = "2000-01-01";
		struct tm pgepochtm = { 0 };

		if (strptime(pgepoch_str, "%Y-%m-%d", &pgepochtm) == NULL)
		{
			log_error("Failed to parse Postgres epoch \"%s\": %m", pgepoch_str);
			return false;
		}

		pgepoch = mktime(&pgepochtm);

		if (pgepoch == (time_t) -1)
		{
			log_error("Failed to compute Postgres epoch: %m");
			return false;
		}

		log_trace("pgsql_timestamptz_to_string: pgepoch == %lld",
				  (long long) pgepoch);
	}

	/*
	 * Postgres Timestamps are stored as int64 values with units of
	 * microseconds. time_t are the number of seconds since the Epoch.
	 */
	time_t ts_secs = (time_t) (ts / 1000000);
	uint64_t ts_us = ts - (((uint64_t) ts_secs) * 1000000);

	time_t t = ts_secs + pgepoch;
	struct tm lt = { 0 };

	if (localtime_r(&t, &lt) == NULL)
	{
		log_error("Failed to format timestamptz value %lld: %m", (long long) ts);
		return false;
	}

	char tmpl[BUFSIZE] = { 0 };
	strftime(tmpl, sizeof(tmpl), "%Y-%m-%d %H:%M:%S.%%d%z", &lt);

	/* add our microseconds back to the formatted string */
	sformat(str, size, tmpl, (long long) ts_us);

	return true;
}


/*
 * Send the START_REPLICATION logical replication command.
 */
bool
pgsql_start_replication(LogicalStreamClient *client)
{
	PGSQL *pgsql = &(client->pgsql);

	/*
	 * Start the replication, build the START_REPLICATION query.
	 */
	log_sql("starting log streaming at %X/%X (slot %s)",
			LSN_FORMAT_ARGS(client->startpos),
			client->slotName);

	/* Initiate the replication stream at specified location */
	PQExpBuffer query = createPQExpBuffer();

	appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
					  client->slotName, LSN_FORMAT_ARGS(client->startpos));

	/* print options if there are any */
	if (client->pluginOptions.count > 0)
	{
		appendPQExpBufferStr(query, " (");
	}

	for (int i = 0; i < client->pluginOptions.count; i++)
	{
		/* separator */
		if (i > 0)
		{
			appendPQExpBufferStr(query, ", ");
		}

		/* write option name */
		appendPQExpBuffer(query, "\"%s\"", client->pluginOptions.keywords[i]);

		/* write option value if specified */
		if (client->pluginOptions.values[i] != NULL)
		{
			appendPQExpBuffer(query, " '%s'", client->pluginOptions.values[i]);
		}
	}

	if (client->pluginOptions.count > 0)
	{
		appendPQExpBufferChar(query, ')');
	}

	if (!pgsql_open_connection(pgsql))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(query);
		return false;
	}

	/* fetch the source timeline */
	if (!pgsql_identify_system(pgsql, &(client->system), client->cdcPathDir))
	{
		/* errors have already been logged */
		destroyPQExpBuffer(query);
		return false;
	}

	/* determine remote server's xlog segment size */
	if (!RetrieveWalSegSize(client))
	{
		destroyPQExpBuffer(query);
		return false;
	}

	log_sql("%s", query->data);

	PGresult *res = PQexec(pgsql->connection, query->data);

	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		log_error("Failed to send replication command:");

		(void) pgcopy_log_error(pgsql, res, query->data);

		destroyPQExpBuffer(query);

		return false;
	}

	log_sql("streaming initiated");

	destroyPQExpBuffer(query);

	return true;
}


/*
 * pgsql_stream_logical streams replication information from the given
 * pre-established source connection.
 *
 * From postgres/src/bin/pg_basebackup/pg_recvlogical.c
 */
bool
pgsql_stream_logical(LogicalStreamClient *client, LogicalStreamContext *context)
{
	PGSQL *pgsql = &(client->pgsql);
	PGconn *conn = client->pgsql.connection;

	PGresult *res = NULL;
	char *copybuf = NULL;

	bool time_to_abort = false;

	client->last_fsync = -1;
	client->last_status = -1;

	context->plugin = client->plugin;

	context->timeline = client->system.timeline;
	context->WalSegSz = client->WalSegSz;
	context->tracking = &(client->current);

	client->now = feGetCurrentTimestamp();

	while (!time_to_abort)
	{
		int r;
		int hdr_len;
		XLogRecPtr cur_record_lsn = InvalidXLogRecPtr;

		/*
		 * When receiving a signal to stop operations, cleanly terminate the
		 * streaming connection, flushing the current position on the way out.
		 */
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			if (!flushAndSendFeedback(client, context))
			{
				goto error;
			}

			prepareToTerminate(client, false, cur_record_lsn);
			time_to_abort = true;
			break;
		}

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		/*
		 * Is it time to ask the logical decoding client to flush?
		 */
		if (client->fsync_interval > 0 &&
			feTimestampDifferenceExceeds(client->last_fsync,
										 client->now,
										 client->fsync_interval))
		{
			/* the flushFunction manages the LogicalTrackLSN tracking */
			if (!(*client->flushFunction)(context))
			{
				/* errors have already been logged */
				goto error;
			}

			client->last_fsync = client->now;
		}

		/*
		 * Potentially send a status message to the primary.
		 */
		client->now = feGetCurrentTimestamp();

		if (client->standby_message_timeout > 0 &&
			feTimestampDifferenceExceeds(client->last_status,
										 client->now,
										 client->standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!pgsqlSendFeedback(client, context, true, false))
			{
				goto error;
			}

			client->last_status = client->now;

			/* the endpos target might have been updated in the past */
			if (context->endpos != InvalidXLogRecPtr &&
				context->endpos <= cur_record_lsn)
			{
				log_warn("New endpos %X/%X is in the past, current "
						 "record LSN is %X/%X",
						 LSN_FORMAT_ARGS(context->endpos),
						 LSN_FORMAT_ARGS(cur_record_lsn));
			}
		}

		r = PQgetCopyData(conn, &copybuf, 1);
		if (r == 0)
		{
			/*
			 * In async mode, and no data available. We block on reading but
			 * not more than the specified timeout, so that we can send a
			 * response back to the client.
			 */
			fd_set input_mask;
			TimestampTz message_target = 0;
			TimestampTz fsync_target = 0;
			struct timeval timeout;
			struct timeval *timeoutptr = NULL;

			if (PQsocket(conn) < 0)
			{
				(void) pgsql_stream_log_error(pgsql, NULL, "invalid socket");
				goto error;
			}

			FD_ZERO(&input_mask);
			FD_SET(PQsocket(conn), &input_mask);

			/* Compute when we need to wakeup to send a keepalive message. */
			if (client->standby_message_timeout)
			{
				message_target =
					client->last_status +
					(client->standby_message_timeout - 1) * ((int64) 1000);
			}

			/* Now compute when to wakeup. */
			if (message_target > 0 || fsync_target > 0)
			{
				TimestampTz targettime;
				long secs;
				int usecs;

				targettime = message_target;

				feTimestampDifference(client->now,
									  targettime,
									  &secs,
									  &usecs);
				if (secs <= 0)
				{
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				}
				else
				{
					timeout.tv_sec = secs;
				}
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;
			}

			r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
			if (r == 0 || (r < 0 && errno == EINTR))
			{
				/*
				 * Got a timeout or signal. Continue the loop and either
				 * deliver a status packet to the server or just go back into
				 * blocking.
				 */
				continue;
			}
			else if (r < 0)
			{
				(void) pgsql_stream_log_error(pgsql, NULL, "select failed: %m");
				goto error;
			}

			/* Else there is actually data on the socket */
			if (PQconsumeInput(conn) == 0)
			{
				(void) pgsql_stream_log_error(
					pgsql,
					NULL,
					"could not receive data from WAL stream");
				goto error;
			}
			continue;
		}

		/* End of copy stream */
		if (r == -1)
		{
			break;
		}

		/* Failure while reading the copy stream */
		if (r == -2)
		{
			(void) pgsql_stream_log_error(pgsql, NULL, "could not read COPY data");
			goto error;
		}

		/* Check the message type. */
		if (copybuf[0] == 'k')
		{
			int pos;
			bool replyRequested;
			bool endposReached = false;

			/*
			 * Parse the keepalive message, enclosed in the CopyData message.
			 * We just check if the server requested a reply, and ignore the
			 * rest.
			 */
			pos = 1;            /* skip msgtype 'k' */
			cur_record_lsn = fe_recvint64(&copybuf[pos]);

			/*
			 * Extract WAL location for keepalive messages in case we call
			 * keepaliveFunction (directly or via flushAndSendFeedback)
			 */
			context->cur_record_lsn = cur_record_lsn;

			client->current.written_lsn =
				Max(cur_record_lsn, client->current.written_lsn);

			pos += 8;           /* read WAL location */

			/* Extract server's system clock at the time of transmission */
			context->sendTime = fe_recvint64(&copybuf[pos]);

			pos += 8;           /* skip sendTime */

			if (r < pos + 1)
			{
				log_error("streaming header too small: %d", r);
				goto error;
			}
			replyRequested = copybuf[pos];

			if (client->endpos != InvalidXLogRecPtr && cur_record_lsn >= client->endpos)
			{
				/*
				 * If there's nothing to read on the socket until a keepalive
				 * we know that the server has nothing to send us; and if
				 * cur_record_lsn has passed endpos, we know nothing else can have
				 * committed before endpos.  So we can bail out now.
				 */
				endposReached = true;

				log_debug("pgsql_stream_logical: endpos reached on keepalive: "
						  "%X/%X",
						  LSN_FORMAT_ARGS(cur_record_lsn));
			}

			/* call the keepaliveFunction callback now, ignore errors */
			if (replyRequested)
			{
				context->now = client->now;

				(void) (*client->keepaliveFunction)(context);

				/* the keepalive function may advance written_lsn, update */
				client->startpos = client->current.written_lsn;
				client->feedback.written_lsn = client->current.written_lsn;
			}

			/* Send a reply, if necessary */
			if (replyRequested || endposReached)
			{
				if (!flushAndSendFeedback(client, context))
				{
					goto error;
				}
				client->last_status = client->now;
			}

			if (endposReached)
			{
				prepareToTerminate(client, true, InvalidXLogRecPtr);
				time_to_abort = true;
				break;
			}

			continue;
		}
		else if (copybuf[0] != 'w')
		{
			log_error("unrecognized streaming header: \"%c\"",
					  copybuf[0]);
			goto error;
		}

		/*
		 * Read the header of the XLogData message, enclosed in the CopyData
		 * message. We only need the WAL location field (dataStart), the rest
		 * of the header is ignored.
		 */
		hdr_len = 1;            /* msgtype 'w' */
		hdr_len += 8;           /* dataStart */
		hdr_len += 8;           /* walEnd */
		hdr_len += 8;           /* sendTime */
		if (r < hdr_len + 1)
		{
			log_error("streaming header too small: %d", r);
			goto error;
		}

		/* Extract WAL location for this block */
		cur_record_lsn = fe_recvint64(&copybuf[1]);

		/* Extract server's system clock at the time of transmission */
		context->sendTime = fe_recvint64(&copybuf[1 + 8 + 8]);

		if (client->endpos != InvalidXLogRecPtr &&
			cur_record_lsn > client->endpos)
		{
			/*
			 * We've read past our endpoint, so prepare to go away being
			 * cautious about what happens to our output data.
			 */
			log_debug("pgsql_stream_logical: endpos reached at %X/%X",
					  LSN_FORMAT_ARGS(cur_record_lsn));

			if (!flushAndSendFeedback(client, context))
			{
				goto error;
			}
			prepareToTerminate(client, false, cur_record_lsn);
			time_to_abort = true;
			break;
		}

		/* call the consumer function */
		context->cur_record_lsn = cur_record_lsn;
		context->buffer = copybuf + hdr_len;
		context->now = client->now;

		/* the tracking LSN information is updated in the writeFunction */
		if (!(*client->writeFunction)(context))
		{
			log_error("Failed to consume from the stream at pos %X/%X",
					  LSN_FORMAT_ARGS(cur_record_lsn));
			goto error;
		}

		if (client->endpos != InvalidXLogRecPtr &&
			cur_record_lsn > client->endpos)
		{
			/* endpos was exactly the record we just processed, we're done */
			log_debug("pgsql_stream_logical: endpos reached at %X/%X",
					  LSN_FORMAT_ARGS(cur_record_lsn));

			if (!flushAndSendFeedback(client, context))
			{
				goto error;
			}
			prepareToTerminate(client, false, cur_record_lsn);
			time_to_abort = true;
			break;
		}
	}
	res = PQgetResult(conn);
	if (PQresultStatus(res) == PGRES_COPY_OUT)
	{
		PQclear(res);

		/*
		 * We're doing a client-initiated clean exit and have sent CopyDone to
		 * the server. Drain any messages, so we don't miss a last-minute
		 * ErrorResponse. The walsender stops generating XLogData records once
		 * it sees CopyDone, so expect this to finish quickly. After CopyDone,
		 * it's too late for sendFeedback(), even if this were to take a long
		 * time. Hence, use synchronous-mode PQgetCopyData().
		 */
		while (1)
		{
			int r;

			if (copybuf != NULL)
			{
				PQfreemem(copybuf);
				copybuf = NULL;
			}
			r = PQgetCopyData(conn, &copybuf, 0);
			if (r == -1)
			{
				break;
			}
			if (r == -2)
			{
				log_error("could not read COPY data: %s",
						  PQerrorMessage(conn));
				time_to_abort = false;  /* unclean exit */
				goto error;
			}
		}

		res = PQgetResult(conn);
	}
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		(void) pgsql_stream_log_error(
			pgsql,
			NULL,
			"unexpected termination of replication stream");

		goto error;
	}

	clear_results(pgsql);
	pgsql_finish(pgsql);

	/* unset the signals which have been processed correctly now */
	(void) unset_signal_flags();

	/* call the closeFunction callback now */
	if (!(*client->closeFunction)(context))
	{
		/* errors have already been logged */
		return false;
	}

	return true;

error:
	if (copybuf != NULL)
	{
		PQfreemem(copybuf);
		copybuf = NULL;
	}

	/* do not attempt to clear_results() on protocol failure */
	PQclear(res);
	pgsql_finish(pgsql);

	return false;
}


/*
 * pgsql_stream_log_error logs an error message when something wrong happens
 * within a logical streaming connection.
 */
static void
pgsql_stream_log_error(PGSQL *pgsql, PGresult *res, const char *message)
{
	char *pqmessage = PQerrorMessage(pgsql->connection);

	if (strcmp(pqmessage, "") == 0)
	{
		log_error("%s", message);
	}
	else
	{
		LinesBuffer lbuf = { 0 };

		if (!splitLines(&lbuf, pqmessage))
		{
			/* errors have already been logged */
			return;
		}

		if (lbuf.count == 1)
		{
			log_error("%s: %s", message, lbuf.lines[0]);
		}
		else
		{
			/*
			 * PostgreSQL Error message contains several lines. Log each of
			 * them as a separate ERROR line here.
			 */
			log_error("%s:", message);

			for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
			{
				log_error("%s", lbuf.lines[lineNumber]);
			}
		}
	}

	if (res != NULL)
	{
		PQclear(res);
	}

	clear_results(pgsql);
	pgsql_finish(pgsql);
}


/*
 * pgsqlSendFeedback sends feedback to a logical replication connection.
 *
 * From postgres/src/bin/pg_basebackup/pg_recvlogical.c
 */
static bool
pgsqlSendFeedback(LogicalStreamClient *client,
				  LogicalStreamContext *context,
				  bool force,
				  bool replyRequested)
{
	PGconn *conn = client->pgsql.connection;

	char replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int len = 0;

	/*
	 * we normally don't want to send superfluous feedback, but if it's
	 * because of a timeout we need to, otherwise wal_sender_timeout will kill
	 * us.
	 */
	if (!force &&
		client->feedback.written_lsn == client->current.written_lsn &&
		client->feedback.flushed_lsn == client->current.flushed_lsn)
	{
		return true;
	}

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(client->current.written_lsn, &replybuf[len]);   /* write */
	len += 8;
	fe_sendint64(client->current.flushed_lsn, &replybuf[len]); /* flush */
	len += 8;
	fe_sendint64(client->current.applied_lsn, &replybuf[len]);    /* apply */
	len += 8;
	fe_sendint64(client->now, &replybuf[len]);  /* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0; /* replyRequested */
	len += 1;

	client->startpos = client->current.written_lsn;
	client->feedback.written_lsn = client->current.written_lsn;
	client->feedback.flushed_lsn = client->current.flushed_lsn;
	client->feedback.applied_lsn = client->current.applied_lsn;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		log_error("could not send feedback packet: %s",
				  PQerrorMessage(conn));
		return false;
	}

	/* call the callback function from the streaming client first */
	context->forceFeedback = force;

	if ((*client->feedbackFunction)(context))
	{
		/* we might have a new endpos from the client callback */
		if (context->endpos != InvalidXLogRecPtr &&
			context->endpos != client->endpos)
		{
			client->endpos = context->endpos;
			log_notice("endpos is now set to %X/%X",
					   LSN_FORMAT_ARGS(client->endpos));
		}
	}

	if (client->current.written_lsn != InvalidXLogRecPtr ||
		client->current.flushed_lsn != InvalidXLogRecPtr)
	{
		/* use same terms as in pg_stat_replication view */
		log_info("Reported write_lsn %X/%X, flush_lsn %X/%X, replay_lsn %X/%X",
				 LSN_FORMAT_ARGS(client->current.written_lsn),
				 LSN_FORMAT_ARGS(client->current.flushed_lsn),
				 LSN_FORMAT_ARGS(client->current.applied_lsn));
	}

	return true;
}


/*
 * If successful, *now is updated to the current timestamp just before sending
 * feedback.
 */
static bool
flushAndSendFeedback(LogicalStreamClient *client, LogicalStreamContext *context)
{
	/* call the flushFunction callback now */
	if (!(*client->flushFunction)(context))
	{
		/* errors have already been logged */
		return false;
	}

	client->now = feGetCurrentTimestamp();

	if (!pgsqlSendFeedback(client, context, true, false))
	{
		return false;
	}

	return true;
}


/*
 * Try to inform the server about our upcoming demise, but don't wait around or
 * retry on failure.
 */
static void
prepareToTerminate(LogicalStreamClient *client, bool keepalive, XLogRecPtr lsn)
{
	PGconn *conn = client->pgsql.connection;

	(void) PQputCopyEnd(conn, NULL);
	(void) PQflush(conn);

	if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
	{
		log_debug("received signal to stop streaming, currently at %X/%X",
				  LSN_FORMAT_ARGS(client->current.written_lsn));
	}
	else if (keepalive)
	{
		log_debug("end position %X/%X reached by keepalive",
				  LSN_FORMAT_ARGS(client->endpos));
	}
	else
	{
		log_debug("end position %X/%X reached by WAL record at %X/%X",
				  LSN_FORMAT_ARGS(client->endpos),
				  LSN_FORMAT_ARGS(client->current.written_lsn));
	}
}


/*
 * From version 10, explicitly set wal segment size using SHOW wal_segment_size
 * since ControlFile is not accessible here.
 */
bool
RetrieveWalSegSize(LogicalStreamClient *client)
{
	PGconn *conn = client->pgsql.connection;

	PGresult *res;
	char xlog_unit[3];
	int xlog_val,
		multiplier = 1;

	/* check connection existence */
	if (conn == NULL)
	{
		log_error("BUG: RetrieveWalSegSize called with a NULL client connection");
		return false;
	}

	/* for previous versions set the default xlog seg size */
	if (PQserverVersion(conn) < MINIMUM_VERSION_FOR_SHOW_CMD)
	{
		client->WalSegSz = DEFAULT_XLOG_SEG_SIZE;
		return true;
	}

	res = PQexec(conn, "SHOW wal_segment_size");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		log_error("could not send replication command \"%s\": %s",
				  "SHOW wal_segment_size", PQerrorMessage(conn));

		PQclear(res);
		return false;
	}
	if (PQntuples(res) != 1 || PQnfields(res) < 1)
	{
		log_error("could not fetch WAL segment size: got %d rows and %d fields, "
				  "expected %d rows and %d or more fields",
				  PQntuples(res), PQnfields(res), 1, 1);

		PQclear(res);
		return false;
	}

	/* fetch xlog value and unit from the result */
	if (sscanf(PQgetvalue(res, 0, 0), "%d%2s", &xlog_val, xlog_unit) != 2) /* IGNORE-BANNED */
	{
		log_error("WAL segment size could not be parsed");
		PQclear(res);
		return false;
	}

	PQclear(res);

	/* set the multiplier based on unit to convert xlog_val to bytes */
	if (strcmp(xlog_unit, "MB") == 0)
	{
		multiplier = 1024 * 1024;
	}
	else if (strcmp(xlog_unit, "GB") == 0)
	{
		multiplier = 1024 * 1024 * 1024;
	}

	/* convert and set WalSegSz */
	client->WalSegSz = xlog_val * multiplier;

	if (!IsValidWalSegSize(client->WalSegSz))
	{
		log_error("WAL segment size must be a power of two between 1 MB and 1 GB, "
				  "but the remote server reported a value of %d bytes",
				  client->WalSegSz);
		return false;
	}

	log_sql("RetrieveWalSegSize: %d", client->WalSegSz);

	return true;
}


/*
 * Get block size from the connected Postgres instance.
 */
bool
pgsql_get_block_size(PGSQL *pgsql, int *blockSize)
{
	PGconn *conn = pgsql->connection;

	/* check connection existence */
	if (conn == NULL)
	{
		log_error("BUG: pgsql_get_block_size called with a NULL client connection");
		return false;
	}

	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BIGINT, false };
	const char *query = "SELECT current_setting('block_size')";

	if (!pgsql_execute_with_params(pgsql, query, 0, NULL, NULL,
								   &context, &parseSingleValueResult))
	{
		/* errors have been logged already */
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to get result from current_setting('block_size')");
		return false;
	}

	*blockSize = context.bigint;

	log_sql("pgsql_get_block_size: %d", *blockSize);
	return true;
}


/*
 * pgsql_replication_origin_oid calls pg_replication_origin_oid().
 */
bool
pgsql_replication_origin_oid(PGSQL *pgsql, char *nodeName, uint32_t *oid)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BIGINT, false };

	const char *sql = "select pg_replication_origin_oid($1)";
	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { nodeName };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to get replication origin oid for \"%s\"", nodeName);
		return false;
	}

	*oid = context.isNull ? 0 : context.bigint;

	return true;
}


/*
 * pgsql_replication_origin_create calls pg_replication_origin_create() on the
 * given connection. The returned oid is ignored.
 */
bool
pgsql_replication_origin_create(PGSQL *pgsql, char *nodeName)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BIGINT, false };

	const char *sql = "select pg_replication_origin_create($1)";
	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { nodeName };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to create replication origin \"%s\"", nodeName);
		return false;
	}

	return true;
}


/*
 * pgsql_replication_origin_drop calls pg_replication_origin_drop.
 */
bool
pgsql_replication_origin_drop(PGSQL *pgsql, char *nodeName)
{
	char *sql =
		"SELECT pg_replication_origin_drop(roname) "
		"  FROM pg_replication_origin "
		" WHERE roname = $1";
	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { nodeName };

	log_info("Dropping replication origin \"%s\"", nodeName);

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   NULL, NULL))
	{
		log_error("Failed to drop replication origin \"%s\"", nodeName);
		return false;
	}

	return true;
}


/*
 * pgsql_replication_origin_session_setup calls the function
 * pg_replication_origin_session_setup().
 */
bool
pgsql_replication_origin_session_setup(PGSQL *pgsql, char *nodeName)
{
	const char *sql = "select pg_replication_origin_session_setup($1)";
	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { nodeName };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   NULL, NULL))
	{
		log_error("Failed to setup replication origin session for node \"%s\"",
				  nodeName);
		return false;
	}

	return true;
}


/*
 * pgsql_replication_origin_xact_setup calls pg_replication_origin_xact_setup().
 */
bool
pgsql_replication_origin_xact_setup(PGSQL *pgsql,
									char *origin_lsn,
									char *origin_timestamp)
{
	const char *sql = "select pg_replication_origin_xact_setup($1, $2)";
	int paramCount = 2;
	Oid paramTypes[2] = { LSNOID, TIMESTAMPTZOID };
	const char *paramValues[2] = { origin_lsn, origin_timestamp };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   NULL, NULL))
	{
		log_error("Failed to setup replication origin transaction at "
				  "origin LSN %s and origin timestamp \"%s\"",
				  origin_lsn,
				  origin_timestamp);
		return false;
	}

	return true;
}


/*
 * pgsql_replication_origin_advance calls pg_replication_origin_advance().
 */
bool
pgsql_replication_origin_advance(PGSQL *pgsql, char *nodeName, char *lsn)
{
	const char *sql = "select pg_replication_origin_advance($1, $2)";
	int paramCount = 2;
	Oid paramTypes[2] = { TEXTOID, LSNOID };
	const char *paramValues[2] = { nodeName, lsn };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   NULL, NULL))
	{
		log_error("Failed to advance replication origin for \"%s\" at LSN %s",
				  nodeName, lsn);
		return false;
	}

	return true;
}


/*
 * pgsql_replication_origin_progress calls pg_replication_origin_progress().
 */
bool
pgsql_replication_origin_progress(PGSQL *pgsql,
								  char *nodeName,
								  bool flush,
								  uint64_t *lsn)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_STRING, false };

	const char *sql = "select pg_replication_origin_progress($1, $2)";
	int paramCount = 2;
	Oid paramTypes[2] = { TEXTOID, BOOLOID };
	const char *paramValues[2] = { nodeName, flush ? "t" : "f" };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to fetch progress of replication origin for \"%s\"",
				  nodeName);
		return false;
	}

	if (context.isNull)
	{
		/* when we get a NULL, return 0/0 instead */
		*lsn = InvalidXLogRecPtr;
	}
	else
	{
		if (!parseLSN(context.strVal, lsn))
		{
			log_error("Failed to parse LSN \"%s\" returned from "
					  "pg_replication_origin_progress('%s', %s)",
					  context.strVal,
					  nodeName,
					  flush ? "true" : "false");

			return false;
		}
	}

	return true;
}


/*
 * pgsql_replication_slot_exists checks that a replication slot with the given
 * slotName exists on the Postgres server.
 */
bool
pgsql_replication_slot_exists(PGSQL *pgsql, const char *slotName,
							  bool *slotExists, uint64_t *lsn)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_STRING, false };
	char *sql =
		pgsql->pgversion_num < 90600
		?

		/* Postgres 9.5 does not have confirmed_flush_lsn */
		"SELECT restart_lsn "
		"FROM pg_replication_slots WHERE slot_name = $1"
		:
		"SELECT confirmed_flush_lsn "
		"FROM pg_replication_slots WHERE slot_name = $1";

	int paramCount = 1;
	Oid paramTypes[1] = { NAMEOID };
	const char *paramValues[1] = { slotName };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		/* errors have already been logged */
		return false;
	}

	if (context.ntuples == 0)
	{
		/* we receive 0 rows in the result when the slot does not exist yet */
		*slotExists = false;
		return true;
	}

	/* the parsedOk status is only updated when ntuples == 1 */
	if (!context.parsedOk)
	{
		log_error("Failed to check if the replication slot \"%s\" exists",
				  slotName);
		return false;
	}

	*slotExists = context.ntuples == 1;

	if (*slotExists)
	{
		if (context.isNull)
		{
			/* when we get a NULL, return 0/0 instead */
			*lsn = InvalidXLogRecPtr;
		}
		else
		{
			if (!parseLSN(context.strVal, lsn))
			{
				log_error("Failed to parse LSN \"%s\" returned from "
						  "confirmed_flush_lsn for slot \"%s\"",
						  context.strVal,
						  slotName);

				return false;
			}
		}
	}

	return true;
}


/*
 * pgsql_drop_replication_slot drops a given replication slot.
 */
bool
pgsql_drop_replication_slot(PGSQL *pgsql, const char *slotName)
{
	char *sql =
		"SELECT pg_drop_replication_slot(slot_name) "
		"  FROM pg_replication_slots "
		" WHERE slot_name = $1";
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { slotName };

	log_info("Dropping replication slot \"%s\"", slotName);

	return pgsql_execute_with_params(pgsql, sql,
									 1, paramTypes, paramValues,
									 NULL, NULL);
}


/*
 * pgsql_table_exists checks that a table with the given name exists on the
 * Postgres server.
 */
bool
pgsql_table_exists(PGSQL *pgsql,
				   uint32_t oid,
				   const char *nspname,
				   const char *relname,
				   bool *exists)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BOOL, false };

	char *existsQuery =
		"select exists( "
		"         select 1 "
		"           from pg_class c "
		"                join pg_namespace n on n.oid = c.relnamespace "
		"          where c.oid = $1 "
		"            and format('%I', n.nspname) = $2 "
		"            and format('%I', c.relname) = $3"
		"       )";

	int paramCount = 3;
	const Oid paramTypes[3] = { OIDOID, TEXTOID, TEXTOID };
	const char *paramValues[3] = { 0 };

	paramValues[0] = intToString(oid).strValue;
	paramValues[1] = nspname;
	paramValues[2] = relname;

	if (!pgsql_execute_with_params(pgsql, existsQuery,
								   paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to check if \"%s\".\"%s\" exists", nspname, relname);
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to check if \"%s\".\"%s\" exists", nspname, relname);
		return false;
	}

	*exists = context.boolVal;

	return true;
}


/*
 * pgsql_role_exists checks that a role with the given roleName exists on the
 * Postgres server.
 */
bool
pgsql_role_exists(PGSQL *pgsql, const char *roleName, bool *exists)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BOOL, false };
	char *sql = "SELECT 1 FROM pg_roles WHERE rolname = $1";
	int paramCount = 1;
	Oid paramTypes[1] = { NAMEOID };
	const char *paramValues[1] = { roleName };

	if (!pgsql_execute_with_params(pgsql, sql,
								   paramCount, paramTypes, paramValues,
								   &context, &fetchedRows))
	{
		/* errors have already been logged */
		return false;
	}

	if (!context.parsedOk)
	{
		log_error("Failed to check if the role \"%s\" already exists",
				  roleName);
		return false;
	}

	/* we receive 0 rows in the result when the role does not exist yet */
	*exists = context.intVal == 1;

	return true;
}


/*
 * pgsql_configuration_exists checks that a configuration exists on the
 * Postgres server.
 */
bool
pgsql_configuration_exists(PGSQL *pgsql, const char *setconfig, bool *exists)
{
	char *sql = "select exists(select name from pg_settings WHERE name = $1)";

	char *configName = strdup(setconfig);
	char *equalsSign = strchr(configName, '=');
	if (equalsSign != NULL)
	{
		*equalsSign = '\0';
	}

	int paramCount = 1;
	Oid paramTypes[1] = { TEXTOID };
	const char *paramValues[1] = { configName };

	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_BOOL, false };

	if (!pgsql_execute_with_params(pgsql, sql, paramCount, paramTypes, paramValues,
								   &context, &parseSingleValueResult))
	{
		return false;
	}

	if (!context.parsedOk)
	{
		log_error(
			"Failed to check if target database contains the configuration, see above for details");
		return false;
	}

	*exists = context.boolVal;

	return true;
}


/*
 * pgsql_current_wal_flush_lsn calls pg_current_wal_flush_lsn().
 */
bool
pgsql_current_wal_flush_lsn(PGSQL *pgsql, uint64_t *lsn)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_STRING, false };

	const char *sql = "select pg_current_wal_flush_lsn()";

	/*
	 * Postgres function pg_current_wal_flush_lsn() has had different names.
	 */
	if (pgsql->pgversion_num < 90600)
	{
		/* Postgres 9.5 only had that one */
		sql = "select pg_current_xlog_location()";
	}
	else if (pgsql->pgversion_num < 100000)
	{
		/* Postgres 9.6 then had that new one */
		sql = "select pg_current_xlog_flush_location()";
	}

	if (!pgsql_execute_with_params(pgsql, sql, 0, NULL, NULL,
								   &context, &parseSingleValueResult))
	{
		log_error("Failed to call pg_current_wal_flush_lsn()");
		return false;
	}

	if (context.isNull)
	{
		/* when we get a NULL, return 0/0 instead */
		*lsn = InvalidXLogRecPtr;
	}
	else
	{
		if (!parseLSN(context.strVal, lsn))
		{
			log_error("Failed to parse LSN \"%s\" returned from "
					  "pg_current_wal_flush_lsn()",
					  context.strVal);

			return false;
		}
	}

	return true;
}


/*
 * pgsql_escape_identifier escapes PostgreSQL identifiers and always encloses
 * the resulting string in quotes. It utilizes the PQescapeIdentifier function
 * (https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQESCAPEIDENTIFIER).
 */
char *
pgsql_escape_identifier(PGSQL *pgsql, char *src)
{
	PGconn *conn = pgsql->connection;
	if (conn == NULL)
	{
		return NULL;
	}

	char *escapedIdentifier = PQescapeIdentifier(conn, src, strlen(src));

	if (escapedIdentifier == NULL)
	{
		log_error("Failed to escape identifier %s", src);
		return NULL;
	}

	/*
	 * The PQescapeIdentifier function returns a string that is allocated
	 * using C's malloc. However, we use libgc to manage memory with in
	 * pgcopydb, so we need to copy the string into a new memory location
	 * that is managed by libgc.
	 *
	 * Please note that the below strdup is a macro that uses libgc's
	 * GC_strdup function which is defined in defaults.h.
	 */
	char *escapedIdentifierCopy = strdup(escapedIdentifier);

	if (escapedIdentifierCopy == NULL)
	{
		log_error("Failed to allocate memory for escaped identifier %s",
				  escapedIdentifier);
		return NULL;
	}

	/*
	 * Free escapedIdentifier as we already copied the string into a memory
	 * location managed by libgc.
	 */
	PQfreemem(escapedIdentifier);

	return escapedIdentifierCopy;
}
