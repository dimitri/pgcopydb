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
	STREAM_ACTION_BEGIN = 'B',
	STREAM_ACTION_COMMIT = 'C',
	STREAM_ACTION_INSERT = 'I',
	STREAM_ACTION_UPDATE = 'U',
	STREAM_ACTION_DELETE = 'D',
	STREAM_ACTION_TRUNCATE = 'T'
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
	char lsn[PG_LSN_MAXLENGTH];
	char nextlsn[PG_LSN_MAXLENGTH];
} LogicalMessageMetadata;


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

bool parseMessageMetadata(LogicalMessageMetadata *metadata, const char *buffer);

bool stream_read_file(StreamContent *content);
bool stream_read_latest(StreamSpecs *specs, StreamContent *content);

bool buildReplicationURI(const char *pguri, char *repl_pguri);

#endif /* STREAM_H */
