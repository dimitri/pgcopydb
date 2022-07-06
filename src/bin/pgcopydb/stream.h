/*
 * src/bin/pgcopydb/stream.h
 *	 SQL queries to discover the source database stream
 */

#ifndef STREAM_H
#define STREAM_H

#include <stdbool.h>

#include "pgsql.h"

#define OUTPUT_BEGIN "BEGIN; -- "
#define OUTPUT_COMMIT "COMMIT; -- "

typedef enum
{
	STREAM_ACTION_UNKNOWN = 0,
	STREAM_ACTION_BEGIN = 'B',
	STREAM_ACTION_COMMIT = 'C',
	STREAM_ACTION_INSERT = 'I',
	STREAM_ACTION_UPDATE = 'U',
	STREAM_ACTION_DELETE = 'D',
	STREAM_ACTION_TRUNCATE = 'T',
	STREAM_ACTION_MESSAGE = 'M'
} StreamAction;

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


/*
 * The detailed behavior of the LogicalStreamClient is implemented in the
 * callback functions writeFunction, flushFunction, and closeFunction.
 *
 */
typedef enum
{
	STREAM_MODE_UNKNOW = 0,
	STREAM_MODE_RECEIVE,        /* pgcopydb receive */
	STREAM_MODE_PREFETCH,       /* pgcopydb fetch */
	STREAM_MODE_APPLY           /* pgcopydb replay */
} LogicalStreamMode;


typedef struct StreamContext
{
	char *cdcdir;

	LogicalStreamMode mode;

	uint64_t startLSN;
	char walFileName[MAXPGPATH];
	char sqlFileName[MAXPGPATH];
	FILE *jsonFile;

	pid_t subprocess;

	StreamCounters counters;
} StreamContext;

typedef struct StreamApplyContext
{
	PGSQL pgsql;
	char target_pguri[MAXCONNINFO];
	char origin[BUFSIZE];
	uint64_t previousLSN;
} StreamApplyContext;

#define PG_MAX_TIMESTAMP 36     /* "2022-06-27 14:42:21.795714+00" */

typedef struct LogicalMessageMetadata
{
	StreamAction action;
	uint32_t xid;
	uint64_t lsn;
	uint64_t nextlsn;
	char timestamp[PG_MAX_TIMESTAMP];
} LogicalMessageMetadata;

/* data types to support here are limited to what JSON/wal2json offers */
typedef struct LogicalMessageValue
{
	int oid;                    /* BOOLOID, INT8OID, FLOAT8OID, TEXTOID */
	bool isNull;

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
	LogicalMessageValues *array; /* malloc'ed area */
} LogicalMessageValuesArray;

typedef struct LogicalMessageTuple
{
	int cols;
	char **columns;                  /* malloc'ed area */
	LogicalMessageValuesArray values;
} LogicalMessageTuple;

typedef struct LogicalMessageTupleArray
{
	int count;
	LogicalMessageTuple *array; /* malloc'ed area */
} LogicalMessageTupleArray;

typedef struct LogicalMessageInsert
{
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	LogicalMessageTupleArray new;   /* {"columns": ...} */
} LogicalMessageInsert;

typedef struct LogicalMessageUpdate
{
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	LogicalMessageTupleArray old;   /* {"identity": ...} */
	LogicalMessageTupleArray new;   /* {"columns": ...} */
} LogicalMessageUpdate;

typedef struct LogicalMessageDelete
{
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
	LogicalMessageTupleArray old;   /* {"identity": ...} */
} LogicalMessageDelete;

typedef struct LogicalMessageTruncate
{
	char nspname[NAMEDATALEN];
	char relname[NAMEDATALEN];
} LogicalMessageTruncate;


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
	} stmt;

	struct LogicalTransactionStatement *prev; /* double linked-list */
	struct LogicalTransactionStatement *next; /* double linked-list */
} LogicalTransactionStatement;


typedef struct LogicalTransaction
{
	uint32_t xid;
	uint64_t beginLSN;
	uint64_t commitLSN;
	char timestamp[PG_MAX_TIMESTAMP];

	uint32_t count;                     /* number of statements */
	LogicalTransactionStatement *first;
	LogicalTransactionStatement *last;
} LogicalTransaction;

typedef struct LogicalTransactionArray
{
	int count;
	LogicalTransaction *array; /* malloc'ed area */
} LogicalTransactionArray;


typedef struct StreamSpecs
{
	char cdcdir[MAXPGPATH];

	char source_pguri[MAXCONNINFO];
	char logrep_pguri[MAXCONNINFO];
	char target_pguri[MAXCONNINFO];

	char slotName[NAMEDATALEN];
	uint64_t startLSN;
	uint64_t endpos;

	LogicalStreamMode mode;

	bool restart;
	bool resume;
} StreamSpecs;

#define MAX_STREAM_CONTENT_COUNT 16 * 1024

typedef struct StreamContent
{
	char filename[MAXPGPATH];
	int count;
	char *buffer;
	char *lines[MAX_STREAM_CONTENT_COUNT];
	LogicalMessageMetadata messages[MAX_STREAM_CONTENT_COUNT];
} StreamContent;

bool stream_init_specs(StreamSpecs *specs,
					   char *cdcdir,
					   char *source_pguri,
					   char *target_pguri,
					   char *slotName,
					   uint64_t endpos,
					   LogicalStreamMode mode);

bool startLogicalStreaming(StreamSpecs *specs);

bool streamWrite(LogicalStreamContext *context);
bool streamFlush(LogicalStreamContext *context);
bool streamClose(LogicalStreamContext *context);

bool streamRotateFile(LogicalStreamContext *context);
bool streamCloseFile(LogicalStreamContext *context, bool time_to_abort);

bool streamTransformFileInSubprocess(LogicalStreamContext *context);
bool streamWaitForSubprocess(LogicalStreamContext *context);

bool parseMessageMetadata(LogicalMessageMetadata *metadata,
						  const char *buffer,
						  JSON_Value *json,
						  bool skipAction);

bool stream_read_file(StreamContent *content);
bool stream_read_latest(StreamSpecs *specs, StreamContent *content);

bool buildReplicationURI(const char *pguri, char *repl_pguri);

bool stream_create_repl_slot(CopyDataSpec *copySpecs,
							 char *slotName, uint64_t *lsn);

bool stream_create_origin(CopyDataSpec *copySpecs,
						  char *nodeName, uint64_t startpos);

StreamAction StreamActionFromChar(char action);

/* ld_transform.c */
bool stream_transform_file(char *jsonfilename, char *sqlfilename);
bool stream_write_transaction(FILE *out, LogicalTransaction *tx);
bool stream_write_insert(FILE *out, LogicalMessageInsert *insert);
bool stream_write_truncate(FILE *out, LogicalMessageTruncate *truncate);
bool stream_write_update(FILE *out, LogicalMessageUpdate *update);
bool stream_write_delete(FILE * out, LogicalMessageDelete *delete);
bool stream_write_value(FILE *out, LogicalMessageValue *value);

void FreeLogicalTransaction(LogicalTransaction *tx);
void FreeLogicalMessageTupleArray(LogicalMessageTupleArray *tupleArray);

bool parseMessage(LogicalTransaction *txn,
				  LogicalMessageMetadata *metadata,
				  char *message,
				  JSON_Value *json);

/* ld_apply.c */
bool stream_apply_file(StreamApplyContext *context, char *sqlfilename);
bool setupReplicationOrigin(StreamApplyContext *context,
							char *target_pguri,
							char *origin);

StreamAction parseSQLAction(const char *query, LogicalMessageMetadata *metadata);


#endif /* STREAM_H */
