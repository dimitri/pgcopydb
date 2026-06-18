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
#include "ld_pgoutput.h"

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
	uint32_t xid;              /* current transaction XID (0 for non-txn messages) */
	uint64_t time;
	char timeStr[BUFSIZE];
} InternalMessage;


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

	/* the statement part of a PREPARE deadbeef AS ... */
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
	uint32_t xid;
	uint64_t lsn;
	char timestamp[PG_MAX_TIMESTAMP];

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
	uint32_t xid;
	uint64_t lsn;
	char timestamp[PG_MAX_TIMESTAMP];

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
 * TestDecodingAttrCache caches per-column attributes needed by the
 * test_decoding parser hot path (replica-identity classification of each
 * column on UPDATE messages).
 */
typedef struct TestDecodingAttrCache
{
	char attname[PG_NAMEDATALEN];

	int attnum;
	bool attisprimary;
	bool attisreplident;

	UT_hash_handle hh;           /* hashable by attname */
} TestDecodingAttrCache;


/*
 * TestDecodingTableCache caches the SourceTable + per-attribute lookups for
 * each table seen by the test_decoding parser. Without it, every UPDATE
 * message (for tables that lack an old-key section, i.e. REPLICA IDENTITY
 * DEFAULT) costs one SQL query per row to look up the table, one to fetch
 * its attributes, plus one SQL query per column to fetch each attribute --
 * which dominates transform CPU on high-throughput workloads.
 *
 * The (nspname, relname) key is stored as-is without the normalization
 * applied by GeneratedColumnsCache: test_decoding emits identifiers
 * already escaped by Postgres' deterministic quote_identifier (see the
 * comment in parseTestDecodingMessageHeader), so the same real table
 * always produces the same byte sequence -- no normalization needed.
 */
typedef struct TestDecodingTableCache
{
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];

	uint32_t oid;

	/* per-column attribute info, keyed by attname */
	TestDecodingAttrCache *attrs;

	UT_hash_handle hh;           /* hashable by (nspname, relname) */
} TestDecodingTableCache;


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
	DatabaseCatalog *outputDB;   /* output.db — receive writes, apply reads */
	DatabaseCatalog *replayDB;   /* replay.db — apply writes exclusively */

	/* hash table acts as a cache for tables with generated columns */
	GeneratedColumnsCache *generatedColumnsCache;

	/* per-table cache for the test_decoding parser hot path */
	TestDecodingTableCache *testDecodingTableCache;

	/* relation cache for pgoutput binary protocol */
	PgoutputRelationCache *pgoutputRelationCache;

	/* current decoded pgoutput message (receive step) */
	PgoutputMessage pgoutputMsg;

	PGSQL *transformPGSQL;

	uint32_t WalSegSz;
	uint32_t timeline;

	uint64_t firstLSN;
	char walFileName[MAXPGPATH];
	char sqlFileName[MAXPGPATH];
	FILE *sqlFile;

	bool transactionInProgress;

	/*
	 * Output plugin identity, copied from specs->slot.plugin at context init.
	 * Used by the transform step to dispatch DML messages to the correct
	 * parser (parseTestDecodingMessage vs parseWal2jsonMessage) without
	 * inspecting the JSON shape of each individual message.
	 */
	StreamOutputPlugin plugin;

	/*
	 * pgcopydb normalisation: the current transaction XID.
	 *
	 * test_decoding only stamps XID on BEGIN and COMMIT messages; DML rows
	 * carry no XID.  pgoutput follows the same convention.  wal2json
	 * includes "xid" on every message.
	 *
	 * To produce a uniform output table (matching wal2json behaviour and
	 * making WHERE xid = $1 queries reliable for all plugins), we track the
	 * XID from the BEGIN message and stamp it onto every DML message before
	 * inserting into the SQLite output table.
	 */
	uint32_t currentXid;

	/*
	 * Back-pointer to the owning StreamSpecs so that streamCloseFile can
	 * send the lifecycle pipe signal when endpos is reached.
	 */
	struct StreamSpecs *specs;

	/*
	 * Set by ld_store_iter_output when the pending_xid path detects that
	 * receive has finished but the open transaction has no COMMIT in the
	 * output table — i.e. endpos fell mid-transaction.
	 *
	 * stream_transform_messages checks this flag after each ld_store_iter_output
	 * call and exits cleanly without advancing transform_lsn to endpos.
	 */
	bool midTxnEndpos;

	/*
	 * In-memory apply pipeline state owned by the apply driver loop
	 * (stream_apply_replaydb).  The inline-transform hook updates these fields
	 * as it writes each complete transaction to replayDB, so the driver can
	 * detect transform-stage progress by snapshotting and comparing.  NULL
	 * when there is no driver (e.g. the stdout/file apply path).
	 */
	struct PipelineStateEntry *applyState;
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
	DatabaseCatalog *replayDB;
	uint64_t sentinelSyncTime;

	ConnStrings *connStrings;
	char origin[BUFSIZE];

	uint64_t previousLSN;       /* register COMMIT LSN progress */
	uint64_t switchLSN;         /* LSN of the most recent SWITCH WAL message */

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
	uint64_t maxReplayDBSize;   /* rotate replayDB file at this size (bytes) */
	CopyDBSentinel sentinel;

	LogicalStreamMode mode;

	bool restart;
	bool resume;
	bool logSQL;

	/* subprocess management: receive (prefetch) and apply (catchup) */
	FollowSubProcess prefetch;
	FollowSubProcess catchup;

	/* catalog handles: outputDB owned by receive, replayDB owned by apply */
	DatabaseCatalog *sourceDB;
	DatabaseCatalog *outputDB;   /* output.db — receive writes, apply reads */
	DatabaseCatalog *replayDB;   /* replay.db — apply writes exclusively */

	PGSQL transformPGSQL;

	/* ld_stream and ld_transform needs their own StreamContext instance */
	StreamContext private;

	bool stdIn;                 /* read from stdin? */
	bool stdOut;                /* (also) write to stdout? */

	/* STREAM_MODE_REPLAY (and other operations) requires one unix pipe */
	int pipe_rt[2];     /* receive→apply lifecycle signal pipe */

	/* The previous pipe ends are connected to in/out for the sub-processes */
	FILE *in;
	FILE *out;

	/*
	 * Out-of-band lifecycle signal: upstream process writes its final LSN as
	 * an 8-byte big-endian value to the pipe and then closes the write end.
	 * Downstream reads it once; EOF (n==0) means upstream crashed.
	 *
	 * upstream_done_lsn == InvalidXLogRecPtr means "not yet received".
	 * upstream_done     == true once the signal has been read.
	 */
	uint64_t upstream_done_lsn;   /* final LSN signalled by upstream */
	bool upstream_done;           /* have we received the upstream signal? */

	/*
	 * Optional follow coordinator TCP endpoint (--host/--port).  When coordHost
	 * is non-empty the follow supervisor starts a TCP coordinator listening on
	 * coordHost:coordPort, letting "pgcopydb stream sentinel" CLI clients
	 * read/write the sentinel over TCP instead of opening the SQLite catalog.
	 */
	char coordHost[256];
	int coordPort;

	/*
	 * User-specified source filters.  For pgoutput the publication is created
	 * with a filter-aware FOR TABLE list; for wal2json the filter-tables (and
	 * optionally add-tables) plugin option is extended accordingly.
	 */
	SourceFilters filters;

	/* Stable buffer for the dynamically-built wal2json filter-tables value */
	char wal2jsonFilterTables[4096];

	/* Stable buffer for the dynamically-built wal2json add-tables value */
	char wal2jsonAddTables[4096];
};

/* ld_stream.c */
bool stream_init_specs(StreamSpecs *specs,
					   CDCPaths *paths,
					   ConnStrings *connStrings,
					   ReplicationSlot *slot,
					   char *origin,
					   uint64_t endpos,
					   LogicalStreamMode mode,
					   DatabaseCatalog *sourceDB,
					   DatabaseCatalog *outputDB,
					   DatabaseCatalog *replayDB,
					   bool stdIn,
					   bool stdOut,
					   bool logSQL,
					   SourceFilters *filters);

bool stream_init_for_mode(StreamSpecs *specs, LogicalStreamMode mode);

char * LogicalStreamModeToString(LogicalStreamMode mode);

bool stream_check_in_out(StreamSpecs *specs);

/* Lifecycle pipe signals (out-of-band, 8-byte big-endian LSN) */
bool stream_signal_upstream_done(int pipe_write_fd, uint64_t done_lsn);
bool stream_recv_upstream_done(StreamSpecs *specs, int pipe_read_fd);
bool stream_init_context(StreamSpecs *specs);
bool stream_init_timeline(StreamSpecs *specs, LogicalStreamClient *stream);

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

StreamAction StreamActionFromChar(char action);
char * StreamActionToString(StreamAction action);
bool StreamActionIsTCL(StreamAction action);
bool StreamActionIsDML(StreamAction action);
bool StreamActionIsInternal(StreamAction action);


/* ld_transform.c */
/* stream_transform_messages: removed (inline transform now done by apply) */
/* stream_transform_cdc_file: removed (was outer loop driver)             */
/* stream_transform_stream:   removed (REPLAY mode pipe path removed)     */
/* stream_transform_resume:   removed                                     */

bool stream_transform_write_transaction(StreamSpecs *specs);
bool stream_transform_write_replay_stmt(StreamSpecs *specs);
bool stream_transform_write_replay_txn(StreamSpecs *specs);

bool stream_transform_context_init(StreamSpecs *specs);
bool stream_transform_from_outputdb(StreamSpecs *specs, uint64_t previousLSN);

bool stream_transform_message(StreamContext *privateContext,
							  char *message);

bool stream_transform_rotate(StreamContext *privateContext);

bool stream_transform_file(StreamSpecs *specs,
						   char *jsonfilename,
						   char *sqlfilename);

bool stream_compute_pathnames(uint32_t WalSegSz,
							  uint32_t timeline,
							  uint64_t lsn,
							  char *dir,
							  char *walFileName,
							  char *sqlFileName);

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
bool stream_apply_replaydb(StreamSpecs *specs, StreamApplyContext *context);
bool stream_apply_stdin(StreamSpecs *specs, StreamApplyContext *context);
bool stream_apply_to_stdout(StreamSpecs *specs, FILE *out);

bool stream_apply_setup(StreamSpecs *specs, StreamApplyContext *context);

bool stream_apply_cleanup(StreamApplyContext *context);

bool stream_apply_wait_for_sentinel(StreamSpecs *specs,
									StreamApplyContext *context);

bool stream_apply_sync_sentinel(StreamApplyContext *context,
								bool findDurableLSN);

bool stream_apply_sql(StreamApplyContext *context,
					  LogicalMessageMetadata *metadata,
					  const char *sql);

bool stream_apply_init_context(StreamApplyContext *context,
							   DatabaseCatalog *sourceDB,
							   DatabaseCatalog *replayDB,
							   CDCPaths *paths,
							   ConnStrings *connStrings,
							   char *origin,
							   uint64_t endpos);

bool setupReplicationOrigin(StreamApplyContext *context);


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
bool follow_start_catchup(StreamSpecs *specs);

void follow_exit_early(StreamSpecs *specs);
bool follow_wait_subprocesses(StreamSpecs *specs);
bool follow_terminate_subprocesses(StreamSpecs *specs);

bool follow_wait_pid(pid_t subprocess, bool *exited, int *returnCode, int *sig);

#endif /* LD_STREAM_H */
