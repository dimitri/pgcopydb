/*
 * src/bin/pg_autoctl/pgsql.c
 *	 API for sending SQL commands to a PostgreSQL server
 */
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
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
#include "signals.h"
#include "string_utils.h"


#define STR_ERRCODE_DUPLICATE_OBJECT "42710"
#define STR_ERRCODE_DUPLICATE_DATABASE "42P04"

#define STR_ERRCODE_INVALID_OBJECT_DEFINITION "42P17"
#define STR_ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE "55000"
#define STR_ERRCODE_OBJECT_IN_USE "55006"
#define STR_ERRCODE_UNDEFINED_OBJECT "42704"

static char * ConnectionTypeToString(ConnectionType connectionType);
static void log_connection_error(PGconn *connection, int logLevel);
static void pgAutoCtlDefaultNoticeProcessor(void *arg, const char *message);
static PGconn * pgsql_open_connection(PGSQL *pgsql);
static bool pgsql_retry_open_connection(PGSQL *pgsql);
static bool is_response_ok(PGresult *result);
static bool clear_results(PGSQL *pgsql);
static void pgsql_handle_notifications(PGSQL *pgsql);


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
			context->parsedOk = false;
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
		/* size of url has already been validated. */
		strlcpy(pgsql->connectionString, url, MAXCONNINFO);
	}
	else
	{
		return false;
	}
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
 * pgsql_set_default_retry_policy sets the default retry policy: no retry. We
 * use the other default parameters but with a maxR of zero they don't get
 * used.
 *
 * This is the retry policy that prevails in the main keeper loop.
 */
void
pgsql_set_main_loop_retry_policy(ConnectionRetryPolicy *retryPolicy)
{
	(void) pgsql_set_retry_policy(retryPolicy,
								  POSTGRES_PING_RETRY_TIMEOUT,
								  0, /* do not retry by default */
								  POSTGRES_PING_RETRY_CAP_SLEEP_TIME,
								  POSTGRES_PING_RETRY_BASE_SLEEP_TIME);
}


/*
 * pgsql_set_interactive_retry_policy sets the retry policy to 2 seconds of
 * total retrying time (or PGCONNECT_TIMEOUT when that's set), unbounded number
 * of attempts, and up to 2 seconds of sleep time in between attempts.
 */
void
pgsql_set_interactive_retry_policy(ConnectionRetryPolicy *retryPolicy)
{
	(void) pgsql_set_retry_policy(retryPolicy,
								  pgconnect_timeout,
								  -1, /* unbounded number of attempts */
								  POSTGRES_PING_RETRY_CAP_SLEEP_TIME,
								  POSTGRES_PING_RETRY_BASE_SLEEP_TIME);
}


#define min(a, b) (a < b ? a : b)

/*
 * http://c-faq.com/lib/randrange.html
 */
#define random_between(R, M, N) ((M) + R / (RAND_MAX / ((N) -(M) +1) + 1))

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
		char scrubbedConnectionString[MAXCONNINFO] = { 0 };

		if (!parse_and_scrub_connection_string(pgsql->connectionString,
											   scrubbedConnectionString))
		{
			log_debug("Failed to scrub password from connection string");

			strlcpy(scrubbedConnectionString,
					pgsql->connectionString,
					sizeof(scrubbedConnectionString));
		}

		log_debug("Disconnecting from [%s] \"%s\"",
				  ConnectionTypeToString(pgsql->connectionType),
				  scrubbedConnectionString);
		PQfinish(pgsql->connection);
		pgsql->connection = NULL;

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
	char *message = connection != NULL ? PQerrorMessage(connection) : NULL;
	char *errorLines[BUFSIZE] = { 0 };
	int lineCount = splitLines(message, errorLines, BUFSIZE);
	int lineNumber = 0;

	/* PQerrorMessage is then "connection pointer is NULL", not helpful */
	if (connection == NULL)
	{
		return;
	}

	for (lineNumber = 0; lineNumber < lineCount; lineNumber++)
	{
		char *line = errorLines[lineNumber];

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
static PGconn *
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

	char scrubbedConnectionString[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(pgsql->connectionString,
											 scrubbedConnectionString);

	log_debug("Connecting to [%s] \"%s\"",
			  ConnectionTypeToString(pgsql->connectionType),
			  scrubbedConnectionString);

	/* we implement our own retry strategy */
	setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);

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
					  pgsql->connectionString);

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

	char scrubbedConnectionString[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(pgsql->connectionString,
											 scrubbedConnectionString);

	log_warn("Failed to connect to \"%s\", retrying until "
			 "the server is ready", scrubbedConnectionString);

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
			pgsql->status = PG_CONNECTION_BAD;
			pgsql_finish(pgsql);

			log_error("Failed to connect to \"%s\" "
					  "after %d attempts in %d ms, "
					  "pg_autoctl stops retrying now",
					  scrubbedConnectionString,
					  pgsql->retryPolicy.attempts,
					  (int) INSTR_TIME_GET_MILLISEC(duration));

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

		log_debug("PQping(%s): slept %d ms on attempt %d",
				  scrubbedConnectionString,
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
				log_debug("PQping OK after %d attempts",
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
							 scrubbedConnectionString,
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
						(void) log_connection_error(pgsql->connection, LOG_DEBUG);

						log_debug("Failed to connect after successful ping");
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
						scrubbedConnectionString);
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
						scrubbedConnectionString,
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
				log_debug("Failed to ping server \"%s\" because of "
						  "client-side problems (no attempt were made)",
						  scrubbedConnectionString);
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
	char *m = strdup(message);
	char *lines[BUFSIZE];
	int lineCount = splitLines(m, lines, BUFSIZE);
	int lineNumber = 0;

	for (lineNumber = 0; lineNumber < lineCount; lineNumber++)
	{
		log_warn("%s", lines[lineNumber]);
	}

	free(m);
}


/*
 * pgAutoCtlDebugNoticeProcessor is our PostgreSQL libpq Notice Processing to
 * use when wanting to send NOTICE, WARNING, HINT as log_debug messages.
 */
void
pgAutoCtlDebugNoticeProcessor(void *arg, const char *message)
{
	char *m = strdup(message);
	char *lines[BUFSIZE];
	int lineCount = splitLines(m, lines, BUFSIZE);
	int lineNumber = 0;

	for (lineNumber = 0; lineNumber < lineCount; lineNumber++)
	{
		log_debug("%s", lines[lineNumber]);
	}

	free(m);
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
 * pgsql_execute_with_params opens a connection, runs a given SQL command,
 * and closes the connection again.
 *
 * We avoid persisting connection across multiple commands to simplify error
 * handling.
 */
bool
pgsql_execute_with_params(PGSQL *pgsql, const char *sql, int paramCount,
						  const Oid *paramTypes, const char **paramValues,
						  void *context, ParsePostgresResultCB *parseFun)
{
	char debugParameters[BUFSIZE] = { 0 };
	PGresult *result = NULL;

	PGconn *connection = pgsql_open_connection(pgsql);

	if (connection == NULL)
	{
		return false;
	}

	log_debug("%s;", sql);

	if (paramCount > 0)
	{
		int paramIndex = 0;
		int remainingBytes = BUFSIZE;
		char *writePointer = (char *) debugParameters;

		for (paramIndex = 0; paramIndex < paramCount; paramIndex++)
		{
			int bytesWritten = 0;
			const char *value = paramValues[paramIndex];

			if (paramIndex > 0)
			{
				bytesWritten = sformat(writePointer, remainingBytes, ", ");
				remainingBytes -= bytesWritten;
				writePointer += bytesWritten;
			}

			if (value == NULL)
			{
				bytesWritten = sformat(writePointer, remainingBytes, "NULL");
			}
			else
			{
				bytesWritten =
					sformat(writePointer, remainingBytes, "'%s'", value);
			}
			remainingBytes -= bytesWritten;
			writePointer += bytesWritten;
		}
		log_debug("%s", debugParameters);
	}

	if (paramCount == 0)
	{
		result = PQexec(connection, sql);
	}
	else
	{
		result = PQexecParams(connection, sql,
							  paramCount, paramTypes, paramValues,
							  NULL, NULL, 0);
	}

	if (!is_response_ok(result))
	{
		char *sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		char *message = PQerrorMessage(connection);
		char *errorLines[BUFSIZE];
		int lineCount = splitLines(message, errorLines, BUFSIZE);
		int lineNumber = 0;

		char *prefix =
			pgsql->connectionType == PGSQL_CONN_SOURCE ? "[SOURCE]" : "[TARGET]";

		/*
		 * PostgreSQL Error message might contain several lines. Log each of
		 * them as a separate ERROR line here.
		 */
		for (lineNumber = 0; lineNumber < lineCount; lineNumber++)
		{
			log_error("%s %s", prefix, errorLines[lineNumber]);
		}

		log_error("SQL query: %s", sql);
		log_error("SQL params: %s", debugParameters);

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

		PQclear(result);
		clear_results(pgsql);

		/*
		 * Multi statements might want to ROLLBACK and hold to the open
		 * connection for a retry step.
		 */
		if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
		{
			PQfinish(pgsql->connection);
			pgsql->connection = NULL;
		}

		return false;
	}

	if (parseFun != NULL)
	{
		(*parseFun)(context, result);
	}

	PQclear(result);
	clear_results(pgsql);
	if (pgsql->connectionStatementType == PGSQL_CONNECTION_SINGLE_STATEMENT)
	{
		PQfinish(pgsql->connection);
		pgsql->connection = NULL;
	}

	return true;
}


/*
 * is_response_ok returns whether the query result is a correct response
 * (not an error or failure).
 */
static bool
is_response_ok(PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	return resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
		   resultStatus == PGRES_COMMAND_OK;
}


/*
 * clear_results consumes results on a connection until NULL is returned.
 * If an error is returned it returns false.
 */
static bool
clear_results(PGSQL *pgsql)
{
	PGconn *connection = pgsql->connection;

	/*
	 * Per Postgres documentation: You should, however, remember to check
	 * PQnotifies after each PQgetResult or PQexec, to see if any
	 * notifications came in during the processing of the command.
	 *
	 * Before calling clear_results(), we called PQexecParams().
	 */
	(void) pgsql_handle_notifications(pgsql);

	while (true)
	{
		PGresult *result = PQgetResult(connection);

		/*
		 * Per Postgres documentation: You should, however, remember to check
		 * PQnotifies after each PQgetResult or PQexec, to see if any
		 * notifications came in during the processing of the command.
		 *
		 * Here, we just called PQgetResult().
		 */
		(void) pgsql_handle_notifications(pgsql);

		if (result == NULL)
		{
			break;
		}

		if (!is_response_ok(result))
		{
			log_error("Failure from Postgres: %s", PQerrorMessage(connection));

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

	if (pgsql->notificationProcessFunction == NULL)
	{
		return;
	}

	PQconsumeInput(connection);
	while ((notify = PQnotifies(connection)) != NULL)
	{
		log_trace("pgsql_handle_notifications: \"%s\"", notify->extra);

		if ((*pgsql->notificationProcessFunction)(pgsql->notificationGroupId,
												  pgsql->notificationNodeId,
												  notify->relname,
												  notify->extra))
		{
			/* mark that we received some notifications */
			pgsql->notificationReceived = true;
		}

		PQfreemem(notify);
		PQconsumeInput(connection);
	}
}


/*
 * hostname_from_uri parses a PostgreSQL connection string URI and returns
 * whether the URL was successfully parsed.
 */
bool
hostname_from_uri(const char *pguri,
				  char *hostname, int maxHostLength, int *port)
{
	int found = 0;
	char *errmsg;
	PQconninfoOption *conninfo, *option;

	conninfo = PQconninfoParse(pguri, &errmsg);
	if (conninfo == NULL)
	{
		log_error("Failed to parse pguri \"%s\": %s", pguri, errmsg);
		PQfreemem(errmsg);
		return false;
	}

	for (option = conninfo; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, "host") == 0 ||
			strcmp(option->keyword, "hostaddr") == 0)
		{
			if (option->val)
			{
				int hostNameLength = strlcpy(hostname, option->val, maxHostLength);

				if (hostNameLength >= maxHostLength)
				{
					log_error(
						"The URL \"%s\" contains a hostname of %d characters, "
						"the maximum supported by pg_autoctl is %d characters",
						option->val, hostNameLength, maxHostLength);
					PQconninfoFree(conninfo);
					return false;
				}

				++found;
			}
		}

		if (strcmp(option->keyword, "port") == 0)
		{
			if (option->val)
			{
				/* we expect a single port number in a monitor's URI */
				if (!stringToInt(option->val, port))
				{
					log_error("Failed to parse port number : %s", option->val);

					PQconninfoFree(conninfo);
					return false;
				}
				++found;
			}
			else
			{
				*port = POSTGRES_PORT;
			}
		}

		if (found == 2)
		{
			break;
		}
	}
	PQconninfoFree(conninfo);

	return true;
}


/*
 * validate_connection_string takes a connection string and parses it with
 * libpq, varifying that it's well formed and usable.
 */
bool
validate_connection_string(const char *connectionString)
{
	char *errorMessage = NULL;

	int length = strlen(connectionString);
	if (length >= MAXCONNINFO)
	{
		log_error("Connection string \"%s\" is %d "
				  "characters, the maximum supported by pg_autoctl is %d",
				  connectionString, length, MAXCONNINFO);
		return false;
	}

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
