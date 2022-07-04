/*
 * src/bin/pgcopydb/pgsql.h
 *     Functions for interacting with a postgres server
 */

#ifndef PGSQL_H
#define PGSQL_H


#include <limits.h>
#include <stdbool.h>

#include "postgres.h"
#include "libpq-fe.h"
#include "portability/instr_time.h"

#include "access/xlogdefs.h"

#if PG_MAJORVERSION_NUM >= 15
#include "common/pg_prng.h"
#endif

#include "defaults.h"
#include "parsing.h"
#include "pg_utils.h"


/*
 * OID values from PostgreSQL src/include/catalog/pg_type.h
 */
#define BOOLOID 16
#define NAMEOID 19
#define INT4OID 23
#define INT8OID 20
#define TEXTOID 25
#define LSNOID 3220

#define FLOAT4OID 700
#define FLOAT8OID 701

/*
 * Maximum connection info length as used in walreceiver.h
 */
#define MAXCONNINFO 1024

/*
 * Chunk size for reading and writting large objects
 */
#define LOBBUFSIZE 16384


/*
 * pg_stat_replication.sync_state is one if:
 *   sync, async, quorum, potential
 */
#define PGSR_SYNC_STATE_MAXLENGTH 10

/*
 * We receive a list of "other nodes" from the monitor, and we store that list
 * in local memory. We pre-allocate the memory storage, and limit how many node
 * addresses we can handle because of the pre-allocation strategy.
 */
#define NODE_ARRAY_MAX_COUNT 12


/* abstract representation of a Postgres server that we can connect to */
typedef enum
{
	PGSQL_CONN_SOURCE = 0,
	PGSQL_CONN_TARGET,
} ConnectionType;


/*
 * Retry policy to follow when we fail to connect to a Postgres URI.
 *
 * In almost all the code base the retry mechanism is implemented in the main
 * loop so we want to fail fast and let the main loop handle the connection
 * retry and the different network timeouts that we have, including the network
 * partition detection timeout.
 *
 * When we do retry connecting, we implement an Exponential Backoff with
 * Decorrelated Jitter algorithm as proven useful in the following article:
 *
 *  https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
 */
typedef struct ConnectionRetryPolicy
{
	int maxT;                   /* maximum time spent retrying (seconds) */
	int maxR;                   /* maximum number of retries, might be zero */
	int maxSleepTime;           /* in millisecond, used to cap sleepTime */
	int baseSleepTime;          /* in millisecond, base time to sleep for */
	int sleepTime;              /* in millisecond, time waited for last round */

	instr_time startTime;       /* time of the first attempt */
	instr_time connectTime;     /* time of successful connection */
	int attempts;               /* how many attempts have been made so far */

#if PG_MAJORVERSION_NUM >= 15
	pg_prng_state prng_state;
#endif
} ConnectionRetryPolicy;

/*
 * Denote if the connetion is going to be used for one, or multiple statements.
 * This is used by psql_* functions to know if a connection is to be closed
 * after successful completion, or if the the connection is to be maintained
 * open for further queries.
 *
 * A common use case for maintaining a connection open, is while wishing to open
 * and maintain a transaction block. Another, is while listening for events.
 */
typedef enum
{
	PGSQL_CONNECTION_SINGLE_STATEMENT = 0,
	PGSQL_CONNECTION_MULTI_STATEMENT
} ConnectionStatementType;

/*
 * Allow higher level code to distinguish between failure to connect to the
 * target Postgres service and failure to run a query or obtain the expected
 * result. To that end we expose PQstatus() of the connection.
 *
 * We don't use the same enum values as in libpq because we want to have the
 * unknown value when we didn't try to connect yet.
 */
typedef enum
{
	PG_CONNECTION_UNKNOWN = 0,
	PG_CONNECTION_OK,
	PG_CONNECTION_BAD
} PGConnStatus;

/*
 * Support for ISOLATION LEVEL in pgsql_set_transaction transaction modes.
 *
 * ISOLATION LEVEL
 * { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
 */
typedef enum
{
	ISOLATION_SERIALIZABLE = 0,
	ISOLATION_REPEATABLE_READ,
	ISOLATION_READ_COMMITTED,
	ISOLATION_READ_UNCOMMITTED,
} IsolationLevel;

/* notification processing */
typedef bool (*ProcessNotificationFunction)(int notificationGroupId,
											int64_t notificationNodeId,
											char *channel, char *payload);

typedef struct PGSQL
{
	ConnectionType connectionType;
	ConnectionStatementType connectionStatementType;
	char connectionString[MAXCONNINFO];
	PGconn *connection;
	ConnectionRetryPolicy retryPolicy;
	PGConnStatus status;

	ProcessNotificationFunction notificationProcessFunction;
	int notificationGroupId;
	int64_t notificationNodeId;
	bool notificationReceived;
} PGSQL;


/*
 * Arrange a generic way to parse PostgreSQL result from a query. Most of the
 * queries we need here return a single row of a single column, so that's what
 * the default context and parsing allows for.
 */

/* callback for parsing query results */
typedef void (ParsePostgresResultCB)(void *context, PGresult *result);

typedef enum
{
	PGSQL_RESULT_BOOL = 1,
	PGSQL_RESULT_INT,
	PGSQL_RESULT_BIGINT,
	PGSQL_RESULT_STRING
} QueryResultType;

/*
 * As a way to communicate the SQL STATE when an error occurs, every
 * pgsql_execute_with_params context structure must have the same first field,
 * an array of 5 characters (plus '\0' at the end).
 */
#define SQLSTATE_LENGTH 6

#define STR_ERRCODE_CLASS_CONNECTION_EXCEPTION "08"

typedef struct AbstractResultContext
{
	char sqlstate[SQLSTATE_LENGTH];
} AbstractResultContext;

/* data structure for keeping a single-value query result */
typedef struct SingleValueResultContext
{
	char sqlstate[SQLSTATE_LENGTH];
	QueryResultType resultType;
	bool parsedOk;
	bool isNull;
	int ntuples;
	bool boolVal;
	int intVal;
	uint64_t bigint;
	char *strVal;
} SingleValueResultContext;


/* PostgreSQL ("Grand Unified Configuration") setting */
typedef struct GUC
{
	char *name;
	char *value;
} GUC;


bool pgsql_init(PGSQL *pgsql, char *url, ConnectionType connectionType);

void pgsql_set_retry_policy(ConnectionRetryPolicy *retryPolicy,
							int maxT,
							int maxR,
							int maxSleepTime,
							int baseSleepTime);
void pgsql_set_main_loop_retry_policy(ConnectionRetryPolicy *retryPolicy);
void pgsql_set_interactive_retry_policy(ConnectionRetryPolicy *retryPolicy);
int pgsql_compute_connection_retry_sleep_time(ConnectionRetryPolicy *retryPolicy);
bool pgsql_retry_policy_expired(ConnectionRetryPolicy *retryPolicy);

void pgsql_finish(PGSQL *pgsql);
void parseSingleValueResult(void *ctx, PGresult *result);
void fetchedRows(void *ctx, PGresult *result);

bool pgsql_begin(PGSQL *pgsql);
bool pgsql_commit(PGSQL *pgsql);
bool pgsql_rollback(PGSQL *pgsql);
bool pgsql_set_transaction(PGSQL *pgsql,
						   IsolationLevel level, bool readOnly, bool deferrable);
bool pgsql_export_snapshot(PGSQL *pgsql, char *snapshot, size_t size);
bool pgsql_set_snapshot(PGSQL *pgsql, char *snapshot);
bool pgsql_execute(PGSQL *pgsql, const char *sql);
bool pgsql_execute_with_params(PGSQL *pgsql, const char *sql, int paramCount,
							   const Oid *paramTypes, const char **paramValues,
							   void *parseContext, ParsePostgresResultCB *parseFun);

void pgAutoCtlDebugNoticeProcessor(void *arg, const char *message);

bool hostname_from_uri(const char *pguri,
					   char *hostname, int maxHostLength, int *port);
bool validate_connection_string(const char *connectionString);

bool pgsql_truncate(PGSQL *pgsql, const char *qname);

bool pg_copy(PGSQL *src, PGSQL *dst,
			 const char *srcQname, const char *dstQname, bool truncate);

bool pg_copy_from_stdin(PGSQL *pgsql, const char *qname);
bool pg_copy_row_from_stdin(PGSQL *pgsql, char *fmt, ...);
bool pg_copy_end(PGSQL *pgsql);

bool pgsql_get_sequence(PGSQL *pgsql, const char *nspname, const char *relname,
						int64_t *lastValue,
						bool *isCalled);

bool pgsql_set_gucs(PGSQL *pgsql, GUC *settings);

bool pg_copy_large_objects(PGSQL *src, PGSQL *dst,
						   bool dropIfExists, uint32_t *count);

/*
 * Maximum length of serialized pg_lsn value
 * It is taken from postgres file pg_lsn.c.
 * It defines MAXPG_LSNLEN to be 17 and
 * allocates a buffer 1 byte larger. We
 * went for 18 to make buffer allocation simpler.
 */
#define PG_LSN_MAXLENGTH 18

/*
 * The IdentifySystem contains information that is parsed from the
 * IDENTIFY_SYSTEM replication command, and then the TIMELINE_HISTORY result.
 */
typedef struct IdentifySystem
{
	uint64_t identifier;
	uint32_t timeline;
	char xlogpos[PG_LSN_MAXLENGTH];
	char dbname[NAMEDATALEN];
} IdentifySystem;

bool pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system);

/*
 * Logical Decoding support.
 */
typedef struct LogicalTrackLSN
{
	XLogRecPtr written_lsn;
	XLogRecPtr flushed_lsn;
	XLogRecPtr applied_lsn;
} LogicalTrackLSN;


typedef struct LogicalStreamContext
{
	void *private;

	XLogRecPtr cur_record_lsn;
	int timeline;
	uint32_t WalSegSz;
	const char *buffer;

	TimestampTz now;
	LogicalTrackLSN *tracking;
} LogicalStreamContext;


typedef bool (*LogicalStreamReceiver) (LogicalStreamContext *context);

typedef struct LogicalStreamClient
{
	PGSQL pgsql;
	IdentifySystem system;

	char slotName[NAMEDATALEN];
	KeyVal pluginOptions;
	uint32_t WalSegSz;

	XLogRecPtr startpos;
	XLogRecPtr endpos;

	TimestampTz now;
	TimestampTz last_status;
	TimestampTz last_fsync;

	LogicalTrackLSN current;    /* updated at receive time */
	LogicalTrackLSN feedback;   /* updated at feedback sending time */

	LogicalStreamReceiver writeFunction;
	LogicalStreamReceiver flushFunction;
	LogicalStreamReceiver closeFunction;

	int fsync_interval;
	int standby_message_timeout;
} LogicalStreamClient;


bool pgsql_init_stream(LogicalStreamClient *client,
					   const char *pguri,
					   const char *slotName,
					   XLogRecPtr startpos,
					   XLogRecPtr endpos);

bool pgsql_start_replication(LogicalStreamClient *client);
bool pgsql_stream_logical(LogicalStreamClient *client,
						  LogicalStreamContext *context);

/* SHOW command for replication connection was introduced in version 10 */
#define MINIMUM_VERSION_FOR_SHOW_CMD 100000

bool RetrieveWalSegSize(LogicalStreamClient *client);

bool pgsql_replication_origin_oid(PGSQL *pgsql, char *nodeName, uint32_t *oid);
bool pgsql_replication_origin_create(PGSQL *pgsql, char *nodeName);
bool pgsql_replication_origin_drop(PGSQL *pgsql, char *nodeName);
bool pgsql_replication_origin_session_setup(PGSQL *pgsql, char *nodeName);

bool pgsql_replication_origin_xact_setup(PGSQL *pgsql,
										 char *origin_lsn,
										 char *origin_timestamp);

bool pgsql_replication_origin_advance(PGSQL *pgsql, char *nodeName, char *lsn);

bool pgsql_replication_origin_progress(PGSQL *pgsql,
									   char *nodeName,
									   bool flush,
									   char *lsn);

bool pgsql_replication_slot_exists(PGSQL *pgsql,
								   const char *slotName,
								   bool *slotExists);

bool pgsql_create_replication_slot(PGSQL *pgsql,
								   const char *slotName,
								   const char *plugin,
								   uint64_t *lsn);

bool pgsql_drop_replication_slot(PGSQL *pgsql, const char *slotName);

#endif /* PGSQL_H */
