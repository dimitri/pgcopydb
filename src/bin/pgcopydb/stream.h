/*
 * src/bin/pgcopydb/stream.h
 *	 SQL queries to discover the source database stream
 */

#ifndef STREAM_H
#define STREAM_H

#include <stdbool.h>

#include "pgsql.h"

typedef struct StreamContext
{
	uint64_t startLSN;
	char walFileName[MAXPGPATH];
	FILE *jsonFile;
} StreamContext;

bool startLogicalStreaming(const char *pguri,
						   const char *slotName,
						   uint64_t startLSN);

bool startLogicalStreaming(const char *pguri,
						   const char *slotName,
						   uint64_t startLSN);

bool streamToFiles(LogicalStreamContext *context);

bool buildReplicationURI(const char *pguri, char *repl_pguri);

#endif /* STREAM_H */
