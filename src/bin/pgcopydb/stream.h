/*
 * src/bin/pgcopydb/stream.h
 *	 SQL queries to discover the source database stream
 */

#ifndef STREAM_H
#define STREAM_H

#include <stdbool.h>

#include "pgsql.h"

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

typedef struct StreamContext
{
	char *cdcdir;

	uint64_t startLSN;
	char walFileName[MAXPGPATH];
	FILE *jsonFile;

	StreamCounters counters;
} StreamContext;

typedef struct LogicalMessageMetadata
{
	StreamAction action;
	uint32_t xid;
	uint64_t lsn;
	uint64_t nextlsn;
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
					   uint64_t endpos);

bool startLogicalStreaming(StreamSpecs *specs);

bool streamWrite(LogicalStreamContext *context);
bool streamFlush(LogicalStreamContext *context);
bool streamClose(LogicalStreamContext *context);

bool parseMessageMetadata(LogicalMessageMetadata *metadata,
						  const char *buffer,
						  JSON_Value *json);

bool stream_read_file(StreamContent *content);
bool stream_read_latest(StreamSpecs *specs, StreamContent *content);

bool buildReplicationURI(const char *pguri, char *repl_pguri);

StreamAction StreamActionFromChar(char action);

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


#endif /* STREAM_H */
