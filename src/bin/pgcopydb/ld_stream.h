/*
 * src/bin/pgcopydb/ld_stream.h
 *	 SQL queries to discover the source database stream
 */

#ifndef LD_STREAM_H
#define LD_STREAM_H

#include <stdbool.h>

#include "parson.h"

#include "copydb.h"
#include "queue_utils.h"
#include "pgsql.h"
#include "schema.h"
#include "string_utils.h"

#define OUTPUT_BEGIN "BEGIN; -- "
#define OUTPUT_COMMIT "COMMIT; -- "
#define OUTPUT_ROLLBACK "ROLLBACK; -- "
#define OUTPUT_SWITCHWAL "-- SWITCH WAL "
#define OUTPUT_KEEPALIVE "-- KEEPALIVE "
#define OUTPUT_ENDPOS "-- ENDPOS "

#define PREPARE "PREPARE "
#define EXECUTE "EXECUTE "
#define TRUNCATE "TRUNCATE "

#define INSERT "AS INSERT INTO "
#define UPDATE "AS UPDATE "
#define DELETE "AS DELETE "

typedef enum
{
	STREAM_ACTION_UNKNOWN = 0,
	STREAM_ACTION_BEGIN = 'B',
	STREAM_ACTION_COMMIT = 'C',
	STREAM_ACTION_INSERT = 'I',
	STREAM_ACTION_UPDATE = 'U',
	STREAM_ACTION_DELETE = 'D',
	STREAM_ACTION_EXECUTE = 'x',
	STREAM_ACTION_TRUNCATE = 'T',
	STREAM_ACTION_MESSAGE = 'M',
	STREAM_ACTION_SWITCH = 'X',
	STREAM_ACTION_KEEPALIVE = 'K',
	STREAM_ACTION_ENDPOS = 'E',
	STREAM_ACTION_ROLLBACK = 'R'
} StreamAction;

typedef struct InternalMessage
{
	StreamAction action;
	uint64_t lsn;
	uint64_t time;
	char timeStr[BUFSIZE];
} InternalMessage;

typedef struct StreamCounters
{
	uint64_t total;
	uint64_t begin;
	uint64_t commit;
	uint64_t insert;
	uint64_t update;
	uint64_t delete;
	uint64_t truncate;
} StreamCounters;


#define PG_MAX_TIMESTAMP 36     /* "2022-06-27 14:42:21.795714+00" */

typedef struct LogicalMessageMetadata
{
	uint64_t recvTime;         /* time(NULL) at message receive time */

	/* from parsing the message itself */
	StreamAction action;
	uint32_t hash;              /* PREPARE/EXECUTE statement name is a hash */
	uint32_t xid;
	uint64_t lsn;
	uint64_t txnCommitLSN;      /* COMMIT LSN of the transaction */
	char timestamp[PG_MAX_TIMESTAMP];

	/* our own internal decision making */
	bool filterOut;
	bool skipping;

	/* the statement part of a PREPARE dseadbeef AS ... */
	char *stmt;

	/* the raw message in our internal JSON format */
	char *jsonBuffer;           /* malloc'ed area */
} LogicalMessageMetadata;


/* data types to support here are limited to what JSON/wal2json offers */
typedef struct LogicalMessageValue
{
	int oid;                    /* BOOLOID, INT8OID, FLOAT8OID, TEXTOID */
	bool isNull;
	bool isQuoted;

	union value
	{
		bool boolean;
		uint64_t int8;
		double float8;
		char *str;              /* malloc'ed area (strdup) */
	} val;
} LogicalMessageValue;

typedef struct LogicalMessageValues
{
	int cols;
	LogicalMessageValue *array; /* malloc'ed area */
} LogicalMessageValues;


typedef struct LogicalMessageValuesArray
{
	int count;
	int capacity;
	LogicalMessageValues *array; /* malloc'ed area */
} LogicalMessageValuesArray;

typedef struct LogicalMessageAttribute
{
	char *attname; /* malloc'ed area */

	bool isgenerated;
} LogicalMessageAttribute;

typedef struct LogicalMessageAttributeArray
{
	int count;
	LogicalMessageAttribute *array; /* malloc'ed area */
} LogicalMessageAttributeArray;

typedef struct LogicalMessageTuple
{
	LogicalMessageAttributeArray attributes;
	LogicalMessageValuesArray values;
} LogicalMessageTuple;

typedef struct LogicalMessageTupleArray
{
	int count;
	LogicalMessageTuple *array; /* malloc'ed area */
} LogicalMessageTupleArray;

typedef struct LogicalMessageRelation
{
	char *nspname;  /* malloc'ed area */
	char *relname;  /* malloc'ed area */
} LogicalMessageRelation;

typedef struct LogicalMessageInsert
{
	LogicalMessageRelation table;
	LogicalMessageTupleArray new;   /* {"columns": ...} */
} LogicalMessageInsert;

typedef struct LogicalMessageUpdate
{
	LogicalMessageRelation table;
	LogicalMessageTupleArray old;   /* {"identity": ...} */
	LogicalMessageTupleArray new;   /* {"columns": ...} */
} LogicalMessageUpdate;

typedef struct LogicalMessageDelete
{
	LogicalMessageRelation table;
	LogicalMessageTupleArray old;   /* {"identity": ...} */
} LogicalMessageDelete;

typedef struct LogicalMessageTruncate
{
	LogicalMessageRelation table;
} LogicalMessageTruncate;

typedef struct LogicalMessageSwitchWAL
{
	uint64_t lsn;
} LogicalMessageSwitchWAL;

typedef struct LogicalMessageKeepalive
{
	uint64_t lsn;
	char timestamp[PG_MAX_TIMESTAMP];
} LogicalMessageKeepalive;

typedef struct LogicalMessageEndpos
{
	uint64_t lsn;
} LogicalMessageEndpos;

/*
 * The JSON-lines logical decoding stream is then parsed into transactions that
 * contains a series of insert/update/delete/truncate commands.
 */
typedef struct LogicalTransactionStatement
{
	StreamAction action;

	union stmt
	{
		LogicalMessageInsert insert;
		LogicalMessageUpdate update;
		LogicalMessageDelete delete;
		LogicalMessageTruncate truncate;
		LogicalMessageSwitchWAL switchwal;
		LogicalMessageKeepalive keepalive;
		LogicalMessageEndpos endpos;
	} stmt;

	struct LogicalTransactionStatement *prev; /* double linked-list */
	struct LogicalTransactionStatement *next; /* double linked-list */
} LogicalTransactionStatement;


typedef struct LogicalTransaction
{
	uint32_t xid;
	uint64_t beginLSN;
	uint64_t commitLSN;
	uint64_t rollbackLSN;
	char timestamp[PG_MAX_TIMESTAMP];
	bool continued;
	bool commit;
	bool rollback;

	uint32_t count;                     /* number of statements */
	LogicalTransactionStatement *first;
	LogicalTransactionStatement *last;
} LogicalTransaction;

typedef struct LogicalTransactionArray
{
	int count;
	LogicalTransaction *array; /* malloc'ed area */
} LogicalTransactionArray;


/*
 * The logical decoding client produces messages that can be either:
 *
 *  - part of a transaction (BEGIN/COMMIT, then INSERT/UPDATE/DELETE/TRUNCATE)
 *  - a keepalive message
 *  - a pgcopydb constructed SWITCH WAL message
 *
 * The keepalive and switch wal messages could also appear within a
 * transaction.
 */
typedef struct LogicalMessage
{
	bool isTransaction;
	StreamAction action;

	union command
	{
		LogicalTransaction tx;
		LogicalMessageSwitchWAL switchwal;
		LogicalMessageKeepalive keepalive;
		LogicalMessageEndpos endpos;
	} command;
} LogicalMessage;


typedef struct LogicalMessageArray
{
	int count;
	LogicalMessage *array; /* malloc'ed area */
} LogicalMessageArray;


/*
 * The detailed behavior of the LogicalStreamClient is implemented in the
 * callback functions writeFunction, flushFunction, and closeFunction.
 */
typedef enum
{
	STREAM_MODE_UNKNOW = 0,
	STREAM_MODE_RECEIVE,        /* pgcopydb receive */
	STREAM_MODE_PREFETCH,       /* pgcopydb fetch */
	STREAM_MODE_CATCHUP,        /* pgcopydb catchup */
	STREAM_MODE_REPLAY          /* pgcopydb replay */
} LogicalStreamMode;


/*
 * Lookup key for the hash table GeneratedColumnsCache.
 */
typedef struct GeneratedColumnsCache_Lookup
{
	/* The table which has generated columns */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
} GeneratedColumnsCache_Lookup;


typedef struct GeneratedColumnSet
{
	char attname[PG_NAMEDATALEN];

	UT_hash_handle hh;           /* makes this structure hashable */
} GeneratedColumnSet;


/*
 * This is a multi-level hash table. The first level is the table
 * (nspname.relname) with generated columns. The second level is the
 * column name (attname).
 *
 * This design quickly eliminates tables without generated columns and
 * finds generated column names efficiently.
 *
 * Another option is a single hash table with a composite key of
 * nspname.relname.attname, but it requires a lookup for each column
 * while processing.
 */
typedef struct GeneratedColumnsCache
{
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];

	/* set of generated columns implemented as a hash table */
	GeneratedColumnSet *columns;

	UT_hash_handle hh;           /* makes this structure hashable */
} GeneratedColumnsCache;


/*
 * StreamContext allows tracking the progress of the ld_stream module and is
 * shared also with the ld_transform module, which has its own instance of a
 * StreamContext to track its own progress.
 */
typedef struct StreamContext
{
	CDCPaths paths;
	LogicalStreamMode mode;

	ConnStrings *connStrings;

	uint64_t startpos;
	uint64_t endpos;
	bool apply;

	bool stdIn;
	bool stdOut;

	FILE *in;
	FILE *out;

	LogicalMessage currentMsg;
	LogicalMessageMetadata metadata;
	LogicalMessageMetadata previous;
	LogicalTransactionStatement *stmt;

	uint64_t maxWrittenLSN;     /* max LSN written so far to the JSON files */

	uint64_t lastWriteTime;

	/* transform needs some catalog lookups (pkey, type oid) */
	DatabaseCatalog *sourceDB;

	/* hash table acts as a cache for tables with generated columns */
	GeneratedColumnsCache *generatedColumnsCache;

	Queue *transformQueue;
	PGSQL *transformPGSQL;

	uint32_t WalSegSz;
	uint32_t timeline;

	uint64_t firstLSN;
	char partialFileName[MAXPGPATH];
	char walFileName[MAXPGPATH];
	char sqlFileName[MAXPGPATH];
	FILE *jsonFile;
	FILE *sqlFile;

	StreamCounters counters;

	bool transactionInProgress;
} StreamContext;


/*
 * Keep track of the statements that have already been prepared in this
 * session.
 */
typedef struct PreparedStmt
{
	uint32_t hash;
	bool prepared;

	UT_hash_handle hh;          /* makes this structure hashable */
} PreparedStmt;


/*
 * As we're using synchronous_commit = off to speed-up things on the apply
 * side, we need to track durability in the client-side.
 */
typedef struct LSNTracking
{
	uint64_t sourceLSN;         /* source system: replication origin */
	uint64_t insertLSN;         /* target pgsql_current_wal_insert_lsn() */

	/* that's a linked list */
	struct LSNTracking *previous;
} LSNTracking;

/*
 * StreamApplyContext allows tracking the apply progress.
 */
typedef struct StreamApplyContext
{
	CDCPaths paths;

	/* target connection to find current wal_lsn for replay_lsn mapping */
	PGSQL controlPgConn;

	/* target connection created in pipeline mode responsible for apply */
	PGSQL applyPgConn;

	/* apply needs access to the catalogs to register sentinel replay_lsn */
	DatabaseCatalog *sourceDB;
	uint64_t sentinelSyncTime;

	ConnStrings *connStrings;
	char origin[BUFSIZE];

	IdentifySystem system;      /* information about source database */
	uint32_t WalSegSz;          /* information about source database */

	uint64_t previousLSN;       /* register COMMIT LSN progress */
	uint64_t switchLSN;         /* helps to find the next .sql file to apply */

	LSNTracking *lsnTrackingList;

	bool apply;                 /* from the pgcopydb sentinel */
	uint64_t startpos;          /* from the pgcopydb sentinel */
	uint64_t endpos;            /* finish applying when endpos is reached */
	uint64_t replay_lsn;        /* from the pgcopydb sentinel */

	bool reachedStartPos;
	bool continuedTxn;
	bool reachedEndPos;
	bool reachedEOF;
	bool transactionInProgress;
	bool logSQL;

	char wal[MAXPGPATH];
	char sqlFileName[MAXPGPATH];

	PreparedStmt *preparedStmt;
} StreamApplyContext;


typedef struct StreamContent
{
	char filename[MAXPGPATH];
	LinesBuffer lbuf;
	LogicalMessageMetadata *messages; /* malloc'ed area */
} StreamContent;


/*
 * SubProcess management utils.
 */
typedef struct StreamSpecs StreamSpecs;

typedef bool (*FollowSubCommand) (StreamSpecs *specs);

typedef struct FollowSubProcess
{
	char *name;
	FollowSubCommand command;
	pid_t pid;
	bool exited;
	int returnCode;
	int sig;
} FollowSubProcess;


/*
 * StreamSpecs is the streaming specifications used by the client-side of the
 * logical decoding implementation, where we keep track of progress etc.
 */
struct StreamSpecs
{
	CDCPaths paths;

	ConnStrings *connStrings;

	uint32_t WalSegSz;
	IdentifySystem system;

	ReplicationSlot slot;
	KeyVal pluginOptions;

	char origin[NAMEDATALEN];

	uint64_t startpos;
	uint64_t endpos;
	CopyDBSentinel sentinel;

	LogicalStreamMode mode;

	bool restart;
	bool resume;
	bool logSQL;

	/* subprocess management */
	FollowSubProcess prefetch;
	FollowSubProcess transform;
	FollowSubProcess catchup;

	/* transform needs some catalog lookups (pkey, type oid) */
	DatabaseCatalog *sourceDB;

	/* receive push json filenames to a queue for transform */
	Queue transformQueue;
	PGSQL transformPGSQL;

	/* ld_stream and ld_transform needs their own StreamContext instance */
	StreamContext private;

	bool stdIn;                 /* read from stdin? */
	bool stdOut;                /* (also) write to stdout? */

	/* STREAM_MODE_REPLAY (and other operations) requires two unix pipes */
	int pipe_rt[2];     /* receive-transform pipe */
	int pipe_ta[2];     /* transform-apply pipe */

	/* The previous pipe ends are connected to in/out for the sub-processes */
	FILE *in;
	FILE *out;
};


bool stream_init_specs(StreamSpecs *specs,
					   CDCPaths *paths,
					   ConnStrings *connStrings,
					   ReplicationSlot *slot,
					   char *origin,
					   uint64_t endpos,
					   LogicalStreamMode mode,
					   DatabaseCatalog *sourceDB,
					   bool stdIn,
					   bool stdOut,
					   bool logSQL);

bool stream_init_for_mode(StreamSpecs *specs, LogicalStreamMode mode);

char * LogicalStreamModeToString(LogicalStreamMode mode);

bool stream_check_in_out(StreamSpecs *specs);
bool stream_init_context(StreamSpecs *specs);

bool startLogicalStreaming(StreamSpecs *specs);
bool streamCheckResumePosition(StreamSpecs *specs);

bool streamWrite(LogicalStreamContext *context);
bool streamFlush(LogicalStreamContext *context);
bool streamKeepalive(LogicalStreamContext *context);
bool streamClose(LogicalStreamContext *context);
bool streamFeedback(LogicalStreamContext *context);

bool streamRotateFile(LogicalStreamContext *context);
bool streamCloseFile(LogicalStreamContext *context, bool time_to_abort);

bool prepareMessageMetadataFromContext(LogicalStreamContext *context);
bool prepareMessageJSONbuffer(LogicalStreamContext *context);

bool parseMessageActionAndXid(LogicalStreamContext *context);

bool parseMessageMetadata(LogicalMessageMetadata *metadata,
						  const char *buffer,
						  JSON_Value *json,
						  bool skipAction);

bool LogicalMessageValueEq(LogicalMessageValue *a, LogicalMessageValue *b);

bool stream_write_json(LogicalStreamContext *context, bool previous);

bool stream_write_internal_message(LogicalStreamContext *context,
								   InternalMessage *message);

bool stream_read_file(StreamContent *content);
bool stream_read_latest(StreamSpecs *specs, StreamContent *content);
bool stream_update_latest_symlink(StreamContext *privateContext,
								  const char *filename);

bool stream_sync_sentinel(LogicalStreamContext *context);

bool buildReplicationURI(const char *pguri, char **repl_pguri);

bool stream_setup_databases(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);

bool stream_cleanup_databases(CopyDataSpec *copySpecs,
							  char *slotName,
							  char *origin);

bool stream_create_origin(CopyDataSpec *copySpecs,
						  char *nodeName, uint64_t startpos);

bool stream_create_sentinel(CopyDataSpec *copySpecs,
							uint64_t startpos,
							uint64_t endpos);

bool stream_fetch_current_lsn(uint64_t *lsn,
							  const char *pguri,
							  ConnectionType connectionType);

bool stream_write_context(StreamSpecs *specs, LogicalStreamClient *stream);
bool stream_cleanup_context(StreamSpecs *specs);
bool stream_read_context(StreamSpecs *specs);

StreamAction StreamActionFromChar(char action);
char * StreamActionToString(StreamAction action);

/* ld_transform.c */
bool stream_transform_worker(StreamSpecs *specs);
bool stream_transform_from_queue(StreamSpecs *specs);
bool stream_transform_add_file(Queue *queue, uint64_t firstLSN);
bool stream_transform_send_stop(Queue *queue);

bool stream_compute_pathnames(uint32_t WalSegSz,
							  uint32_t timeline,
							  uint64_t lsn,
							  char *dir,
							  char *walFileName,
							  char *sqlFileName);

bool stream_transform_context_init(StreamSpecs *specs);
bool stream_transform_stream(StreamSpecs *specs);
bool stream_transform_resume(StreamSpecs *specs);
bool stream_transform_line(void *ctx, const char *line, bool *stop);

bool stream_transform_write_message(StreamContext *privateContext,
									uint64_t *currentMsgIndex);

bool stream_transform_message(StreamContext *privateContext,
							  char *message);

bool stream_transform_rotate(StreamContext *privateContext);

bool stream_transform_file(StreamSpecs *specs,
						   char *jsonfilename,
						   char *sqlfilename);

bool stream_transform_file_at_lsn(StreamSpecs *specs, uint64_t lsn);

bool stream_write_message(FILE *out, LogicalMessage *msg);
bool stream_write_transaction(FILE *out, LogicalTransaction *tx);

bool stream_write_switchwal(FILE *out, LogicalMessageSwitchWAL *switchwal);
bool stream_write_keepalive(FILE *out, LogicalMessageKeepalive *keepalive);
bool stream_write_endpos(FILE *out, LogicalMessageEndpos *endpos);

bool stream_write_begin(FILE *out, LogicalTransaction *tx);
bool stream_write_commit(FILE *out, LogicalTransaction *tx);
bool stream_write_rollback(FILE *out, LogicalTransaction *tx);

bool stream_write_insert(FILE *out, LogicalMessageInsert *insert);
bool stream_write_truncate(FILE *out, LogicalMessageTruncate *truncate);
bool stream_write_update(FILE *out, LogicalMessageUpdate *update);
bool stream_write_delete(FILE * out, LogicalMessageDelete *delete);

bool stream_add_value_in_json_array(LogicalMessageValue *value,
									JSON_Array *jsArray);


bool parseMessage(StreamContext *privateContext, char *message, JSON_Value *json);

bool streamLogicalTransactionAppendStatement(LogicalTransaction *txn,
											 LogicalTransactionStatement *stmt);

bool AllocateLogicalMessageTuple(LogicalMessageTuple *tuple, int count);

/* ld_test_decoding.c */
bool prepareTestDecodingMessage(LogicalStreamContext *context);

bool parseTestDecodingMessageActionAndXid(LogicalStreamContext *context);

bool parseTestDecodingMessage(StreamContext *privateContext,
							  char *message,
							  JSON_Value *json);

/* ld_wal2json.c */
bool prepareWal2jsonMessage(LogicalStreamContext *context);

bool parseWal2jsonMessageActionAndXid(LogicalStreamContext *context);

bool parseWal2jsonMessage(StreamContext *privateContext,
						  char *message,
						  JSON_Value *json);

/* ld_apply.c */
bool stream_apply_catchup(StreamSpecs *specs);

bool stream_apply_setup(StreamSpecs *specs, StreamApplyContext *context);

bool stream_apply_cleanup(StreamApplyContext *context);

bool stream_apply_wait_for_sentinel(StreamSpecs *specs,
									StreamApplyContext *context);

bool stream_apply_sync_sentinel(StreamApplyContext *context,
								bool findDurableLSN);

bool stream_apply_file(StreamApplyContext *context);

bool stream_apply_sql(StreamApplyContext *context,
					  LogicalMessageMetadata *metadata,
					  const char *sql);

bool stream_apply_init_context(StreamApplyContext *context,
							   DatabaseCatalog *sourceDB,
							   CDCPaths *paths,
							   ConnStrings *connStrings,
							   char *origin,
							   uint64_t endpos);

bool setupReplicationOrigin(StreamApplyContext *context);

bool computeSQLFileName(StreamApplyContext *context);

bool parseSQLAction(const char *query, LogicalMessageMetadata *metadata);

bool stream_apply_find_durable_lsn(StreamApplyContext *context,
								   uint64_t *durableLSN);


/* ld_replay */
bool stream_apply_replay(StreamSpecs *specs);
bool stream_replay_line(void *ctx, const char *line, bool *stop);
bool stream_replay_reached_endpos(StreamSpecs *specs,
								  StreamApplyContext *context,
								  bool stop);

/* follow.c */
bool follow_export_snapshot(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);
bool follow_setup_databases(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);
bool follow_reset_sequences(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);

bool follow_init_sentinel(StreamSpecs *specs, CopyDBSentinel *sentinel);
bool follow_get_sentinel(StreamSpecs *specs,
						 CopyDBSentinel *sentinel,
						 bool verbose);
bool follow_main_loop(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);

bool followDB(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);

bool follow_reached_endpos(StreamSpecs *streamSpecs, bool *done);
bool follow_prepare_mode_switch(StreamSpecs *streamSpecs,
								LogicalStreamMode previousMode,
								LogicalStreamMode currentMode);

bool follow_start_subprocess(StreamSpecs *specs, FollowSubProcess *subprocess);

bool follow_start_prefetch(StreamSpecs *specs);
bool follow_start_transform(StreamSpecs *specs);
bool follow_start_catchup(StreamSpecs *specs);

void follow_exit_early(StreamSpecs *specs);
bool follow_wait_subprocesses(StreamSpecs *specs);
bool follow_terminate_subprocesses(StreamSpecs *specs);

bool follow_wait_pid(pid_t subprocess, bool *exited, int *returnCode, int *sig);

#endif /* LD_STREAM_H */
