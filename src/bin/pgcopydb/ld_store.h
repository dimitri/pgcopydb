/*
 * src/bin/pgcopydb/ld_store.h
 *	 CDC implementation for Postgres Logical Decoding in pgcopydb.
 */

#ifndef LD_STORE_H
#define LD_STORE_H

#include <stdbool.h>

#include "ld_stream.h"


typedef struct ReplayDBOutputMessage
{
	uint64_t id;
	StreamAction action;
	uint32_t xid;
	uint64_t lsn;

	char timestamp[PG_MAX_TIMESTAMP];

	char *jsonBuffer;           /* malloc'ed area */

	PQExpBuffer stmt;
	PQExpBuffer data;
} ReplayDBOutputMessage;


/*
 * This structure reprensents both the stmt and replay tables of the replayDB
 * schema, as these are meant to be used together, via a JOIN on the statement
 * hash.
 */
typedef struct ReplayDBStmt
{
	uint64_t id;
	StreamAction action;
	uint32_t xid;
	uint64_t lsn;

	char timestamp[PG_MAX_TIMESTAMP];

	uint32_t hash;
	char *stmt;                 /* malloc'ed area */
	char *data;                 /* malloc'ed area */
} ReplayDBStmt;


bool ld_store_open_replaydb(StreamSpecs *specs);

bool ld_store_set_current_cdc_filename(StreamSpecs *specs);
bool ld_store_set_first_cdc_filename(StreamSpecs *specs);
bool ld_store_set_cdc_filename_at_lsn(StreamSpecs *specs, uint64_t lsn);

bool ld_store_cdc_filename_fetch(SQLiteQuery *query);

bool ld_store_lookup_lsn(DatabaseCatalog *catalog, uint64_t lsn);

bool ld_store_insert_cdc_filename(StreamSpecs *specs);

bool ld_store_insert_timeline_history(DatabaseCatalog *catalog,
									  uint32_t tli,
									  uint64_t startpos,
									  uint64_t endpos);

bool ld_store_insert_message(DatabaseCatalog *catalog,
							 LogicalMessageMetadata *metadata);

bool ld_store_insert_internal_message(DatabaseCatalog *catalog,
									  InternalMessage *message);

bool ld_store_insert_replay_stmt(DatabaseCatalog *catalog,
								 ReplayDBStmt *replayStmt);

typedef bool (ReplayDBOutputIterFun)(StreamSpecs *specs,
									 ReplayDBOutputMessage *output,
									 bool *stop);

bool ld_store_iter_output(StreamSpecs *specs, ReplayDBOutputIterFun *callback);

typedef struct ReplayDBOutputIterator
{
	DatabaseCatalog *catalog;
	ReplayDBOutputMessage *output;
	SQLiteQuery query;

	uint64_t transform_lsn;
	uint64_t endpos;
} ReplayDBOutputIterator;

bool ld_store_iter_output_init(ReplayDBOutputIterator *iter);
bool ld_store_iter_output_next(ReplayDBOutputIterator *iter);
bool ld_store_output_fetch(SQLiteQuery *query);
bool ld_store_iter_output_finish(ReplayDBOutputIterator *iter);

#endif /* LD_STORE_H */
