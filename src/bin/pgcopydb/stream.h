/*
 * src/bin/pgcopydb/stream.h
 *	 SQL queries to discover the source database stream
 */

#ifndef STREAM_H
#define STREAM_H

#include <stdbool.h>

#include "pgsql.h"

bool startLogicalStreaming(const char *pguri,
						   const char *slotName,
						   uint64_t startLSN);

bool buildReplicationURI(const char *pguri, char *repl_pguri);

bool startLogicalStreaming(const char *pguri,
						   const char *slotName,
						   uint64_t startLSN);

#endif /* STREAM_H */
