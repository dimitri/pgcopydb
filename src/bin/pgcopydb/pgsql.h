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
#include "parsing_utils.h"
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
#define LOBBUFSIZE 16 * 1024 * 1024 /* 16 MB */


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

/*
 * As a way to communicate the SQL STATE when an error occurs, every
 * pgsql_execute_with_params context structure must have the same first field,
 * an array of 5 characters (plus '\0' at the end).
 */
#define SQLSTATE_LENGTH 6

/*
 * That's "x.yy.zz" or "xx.zz" or maybe a debian style version string such as:
 *  "13.8 (Debian 13.8-1.pgdg110+1)"
 *  "16beta1 (Debian 16~beta1-2.pgdg+~20230605.2256.g3f1aaaa)"
 */
#define PG_VERSION_STRING_MAX_LENGTH 128

/* notification processing */
typedef bool (*ProcessNotificationFunction)(int notificationGroupId,
											int64_t notificationNodeId,
											char *channel, char *payload);

typedef struct PGSQL
{
	ConnectionType connectionType;
	ConnectionStatementType connectionStatementType;

	char *connectionString;
	SafeURI safeURI;

	PGconn *connection;
	ConnectionRetryPolicy retryPolicy;
	PGConnStatus status;
	char sqlstate[SQLSTATE_LENGTH];

	char pgversion[PG_VERSION_STRING_MAX_LENGTH];
	int pgversion_num;

	ProcessNotificationFunction notificationProcessFunction;
	int notificationGroupId;
	int64_t notificationNodeId;
	bool notificationReceived;

	bool logSQL;
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
void pgsql_set_interactive_retry_policy(ConnectionRetryPolicy *retryPolicy);
int pgsql_compute_connection_retry_sleep_time(ConnectionRetryPolicy *retryPolicy);
bool pgsql_retry_policy_expired(ConnectionRetryPolicy *retryPolicy);

bool pgsql_state_is_connection_error(PGSQL *pgsql);

void pgsql_finish(PGSQL *pgsql);
void parseSingleValueResult(void *ctx, PGresult *result);
void fetchedRows(void *ctx, PGresult *result);

bool pgsql_begin(PGSQL *pgsql);
bool pgsql_commit(PGSQL *pgsql);
bool pgsql_rollback(PGSQL *pgsql);
bool pgsql_savepoint(PGSQL *pgsql, char *name);
bool pgsql_release_savepoint(PGSQL *pgsql, char *name);
bool pgsql_rollback_to_savepoint(PGSQL *pgsql, char *name);

bool pgsql_server_version(PGSQL *pgsql);

bool pgsql_set_transaction(PGSQL *pgsql,
						   IsolationLevel level,
						   bool readOnly,
						   bool deferrable);

bool pgsql_is_in_recovery(PGSQL *pgsql, bool *is_in_recovery);

bool pgsql_has_database_privilege(PGSQL *pgsql, const char *privilege,
								  bool *granted);

bool pgsql_has_sequence_privilege(PGSQL *pgsql,
								  const char *seqname,
								  const char *privilege,
								  bool *granted);

bool pgsql_get_search_path(PGSQL *pgsql, char *search_path, size_t size);
bool pgsql_set_search_path(PGSQL *pgsql, char *search_path, bool local);
bool pgsql_prepend_search_path(PGSQL *pgsql, const char *namespace);

bool pgsql_export_snapshot(PGSQL *pgsql, char *snapshot, size_t size);
bool pgsql_set_snapshot(PGSQL *pgsql, char *snapshot);

bool pgsql_execute(PGSQL *pgsql, const char *sql);
bool pgsql_execute_with_params(PGSQL *pgsql, const char *sql, int paramCount,
							   const Oid *paramTypes, const char **paramValues,
							   void *parseContext, ParsePostgresResultCB *parseFun);

bool pgsql_send_with_params(PGSQL *pgsql, const char *sql, int paramCount,
							const Oid *paramTypes, const char **paramValues);

bool pgsql_fetch_results(PGSQL *pgsql, bool *done,
						 void *context, ParsePostgresResultCB *parseFun);

bool pgsql_prepare(PGSQL *pgsql, const char *name, const char *sql,
				   int paramCount, const Oid *paramTypes);

bool pgsql_execute_prepared(PGSQL *pgsql, const char *name,
							int paramCount, const char **paramValues,
							void *context, ParsePostgresResultCB *parseFun);

void pgAutoCtlDebugNoticeProcessor(void *arg, const char *message);

bool validate_connection_string(const char *connectionString);

bool pgsql_truncate(PGSQL *pgsql, const char *qname);

bool pg_copy(PGSQL *src, PGSQL *dst,
			 const char *srcQname, const char *dstQname,
			 bool truncate, uint64_t *bytesTransmitted);

bool pg_copy_from_stdin(PGSQL *pgsql, const char *qname);
bool pg_copy_row_from_stdin(PGSQL *pgsql, char *fmt, ...);
bool pg_copy_end(PGSQL *pgsql);

bool pgsql_get_sequence(PGSQL *pgsql, const char *nspname, const char *relname,
						int64_t *lastValue,
						bool *isCalled);

bool pgsql_set_gucs(PGSQL *pgsql, GUC *settings);

bool pg_copy_large_object(PGSQL *src,
						  PGSQL *dst,
						  bool dropIfExists,
						  uint32_t oid);

/*
 * Maximum length of serialized pg_lsn value
 * It is taken from postgres file pg_lsn.c.
 * It defines MAXPG_LSNLEN to be 17 and
 * allocates a buffer 1 byte larger. We
 * went for 18 to make buffer allocation simpler.
 */
#define PG_LSN_MAXLENGTH 18

/*
 * TimeLineHistoryEntry is taken from Postgres definitions and adapted to
 * client-size code where we don't have all the necessary infrastruture. In
 * particular we don't define a XLogRecPtr data type nor do we define a
 * TimeLineID data type.
 *
 * Zero is used indicate an invalid pointer. Bootstrap skips the first possible
 * WAL segment, initializing the first WAL page at WAL segment size, so no XLOG
 * record can begin at zero.
 */
#define InvalidXLogRecPtr 0
#define XLogRecPtrIsInvalid(r) ((r) == InvalidXLogRecPtr)

#define PGCOPYDB_MAX_TIMELINES 1024
#define PGCOPYDB_MAX_TIMELINE_CONTENT (1024 * 1024)

typedef struct TimeLineHistoryEntry
{
	uint32_t tli;
	uint64_t begin;         /* inclusive */
	uint64_t end;           /* exclusive, InvalidXLogRecPtr means infinity */
} TimeLineHistoryEntry;


typedef struct TimeLineHistory
{
	int count;
	TimeLineHistoryEntry history[PGCOPYDB_MAX_TIMELINES];

	char filename[MAXPGPATH];
	char content[PGCOPYDB_MAX_TIMELINE_CONTENT];
} TimeLineHistory;

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
	TimeLineHistory timelines;
} IdentifySystem;

bool pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system);
bool parseTimeLineHistory(const char *filename, const char *content,
						  IdentifySystem *system);

/*
 * Logical Decoding support.
 */
typedef enum
{
	STREAM_PLUGIN_UNKNOWN = 0,
	STREAM_PLUGIN_TEST_DECODING,
	STREAM_PLUGIN_WAL2JSON
} StreamOutputPlugin;

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

	const char *buffer;         /* expose internal buffer */
	StreamOutputPlugin plugin;

	bool forceFeedback;

	TimestampTz now;
	TimestampTz lastFeedbackSync;
	TimestampTz sendTime;
	XLogRecPtr endpos;          /* might be update at runtime */

	LogicalTrackLSN *tracking;  /* expose LogicalStreamClient.current */
} LogicalStreamContext;


typedef bool (*LogicalStreamReceiver) (LogicalStreamContext *context);

typedef struct LogicalStreamClient
{
	PGSQL pgsql;
	IdentifySystem system;

	char slotName[NAMEDATALEN];

	StreamOutputPlugin plugin;
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
	LogicalStreamReceiver feedbackFunction;
	LogicalStreamReceiver keepaliveFunction;

	int fsync_interval;
	int standby_message_timeout;
} LogicalStreamClient;


bool pgsql_init_stream(LogicalStreamClient *client,
					   const char *pguri,
					   StreamOutputPlugin plugin,
					   const char *slotName,
					   XLogRecPtr startpos,
					   XLogRecPtr endpos);

StreamOutputPlugin OutputPluginFromString(char *plugin);
char * OutputPluginToString(StreamOutputPlugin plugin);

typedef struct ReplicationSlot
{
	char slotName[BUFSIZE];
	uint64_t lsn;
	char snapshot[BUFSIZE];
	StreamOutputPlugin plugin;
} ReplicationSlot;

bool pgsql_create_logical_replication_slot(LogicalStreamClient *client,
										   ReplicationSlot *slot);

bool pgsql_timestamptz_to_string(TimestampTz ts, char *str, size_t size);

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
									   uint64_t *lsn);

bool pgsql_replication_slot_exists(PGSQL *pgsql,
								   const char *slotName,
								   bool *slotExists,
								   uint64_t *lsn);

bool pgsql_create_replication_slot(PGSQL *pgsql,
								   const char *slotName,
								   StreamOutputPlugin plugin,
								   uint64_t *lsn);

bool pgsql_drop_replication_slot(PGSQL *pgsql, const char *slotName);

bool pgsql_role_exists(PGSQL *pgsql, const char *roleName, bool *exists);

bool pgsql_table_exists(PGSQL *pgsql,
						const char *relname,
						const char *nspname,
						bool *exists);

bool pgsql_current_wal_flush_lsn(PGSQL *pgsql, uint64_t *lsn);
bool pgsql_current_wal_insert_lsn(PGSQL *pgsql, uint64_t *lsn);

/*
 * pgcopydb sentinel is a table that's created on the source database and
 * allows communicating elements from the outside, and in between the receive
 * and apply processes.
 */
typedef struct CopyDBSentinel
{
	bool apply;
	uint64_t startpos;
	uint64_t endpos;
	uint64_t write_lsn;
	uint64_t flush_lsn;
	uint64_t replay_lsn;
} CopyDBSentinel;

bool pgsql_update_sentinel_startpos(PGSQL *pgsql, uint64_t startpos);
bool pgsql_update_sentinel_endpos(PGSQL *pgsql, bool current, uint64_t endpos);
bool pgsql_update_sentinel_apply(PGSQL *pgsql, bool apply);

bool pgsql_get_sentinel(PGSQL *pgsql, CopyDBSentinel *sentinel);

bool pgsql_sync_sentinel_recv(PGSQL *pgsql,
							  uint64_t write_lsn,
							  uint64_t flush_lsn,
							  CopyDBSentinel *sentinel);

bool pgsql_sync_sentinel_apply(PGSQL *pgsql,
							   uint64_t replay_lsn,
							   CopyDBSentinel *sentinel);

bool pgsql_send_sync_sentinel_apply(PGSQL *pgsql, uint64_t replay_lsn);
bool pgsql_fetch_sync_sentinel_apply(PGSQL *pgsql,
									 bool *retry,
									 CopyDBSentinel *sentinel);


#endif /* PGSQL_H */
