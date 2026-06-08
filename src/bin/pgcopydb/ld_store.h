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
	uint64_t endlsn;

	char timestamp[PG_MAX_TIMESTAMP];

	uint32_t hash;
	char *stmt;                 /* malloc'ed area */
	char *data;                 /* malloc'ed area */
} ReplayDBStmt;


bool ld_store_open_outputdb(StreamSpecs *specs);
bool ld_store_open_replaydb(StreamSpecs *specs);

bool ld_store_set_current_cdc_filename(StreamSpecs *specs);
bool ld_store_set_cdc_filename_at_lsn(StreamSpecs *specs, uint64_t lsn);

bool ld_store_cdc_filename_fetch(SQLiteQuery *query);

bool ld_store_lookup_output_at_lsn(DatabaseCatalog *catalog,
								   uint64_t lsn,
								   ReplayDBOutputMessage *output);

bool ld_store_lookup_output_after_lsn(DatabaseCatalog *catalog,
									  uint64_t lsn,
									  ReplayDBOutputMessage *output);

bool ld_store_lookup_output_xid_end(DatabaseCatalog *catalog,
									uint32_t xid,
									ReplayDBOutputMessage *output);

bool ld_store_output_fetch(SQLiteQuery *query);

bool ld_store_insert_cdc_filename(StreamSpecs *specs);

bool ld_store_insert_timeline_history(DatabaseCatalog *catalog,
									  uint32_t tli,
									  uint64_t startpos,
									  uint64_t endpos);

bool ld_store_insert_message(DatabaseCatalog *catalog,
							 LogicalMessageMetadata *metadata);

bool ld_store_insert_internal_message(DatabaseCatalog *catalog,
									  InternalMessage *message);

bool ld_store_delete_output_xid(DatabaseCatalog *catalog, uint32_t xid);

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

	/*
	 * pending_xid is set when the init step found a BEGIN for xid N in the
	 * output table but no matching COMMIT/ROLLBACK yet.  The outer function
	 * (ld_store_iter_output) checks this to decide between two situations:
	 *
	 *   pending_xid == 0  — no rows at all; upstream is still producing.
	 *   pending_xid != 0  — a BEGIN exists but COMMIT has not arrived yet.
	 *     → receive still running: outer loop select() will wait.
	 *     → receive finished (pipeline_state/pipe): advance transform_lsn
	 *       to the receive run_end_lsn so the outer loop exits cleanly.
	 */
	uint32_t pending_xid;
} ReplayDBOutputIterator;

bool ld_store_iter_output_init(ReplayDBOutputIterator *iter);
bool ld_store_iter_output_next(ReplayDBOutputIterator *iter);
bool ld_store_iter_output_finish(ReplayDBOutputIterator *iter);


/*
 * ReplayDBReplayIterator iterates over the replay table of a replayDB,
 * returning one ReplayDBStmt per step. Used by the apply process to read
 * transformed SQL statements from SQLite instead of reading .sql files.
 *
 * The query joins replay with stmt so that each row carries the full SQL
 * template alongside its bound parameters.
 */
typedef struct ReplayDBReplayIterator
{
	DatabaseCatalog *catalog;
	ReplayDBStmt *current;
	SQLiteQuery query;

	uint64_t previousLSN;       /* start after this LSN */
	uint64_t endpos;            /* stop at this LSN (0 = no limit) */
} ReplayDBReplayIterator;

bool ld_store_replay_fetch(SQLiteQuery *query);

bool ld_store_iter_replay_init(ReplayDBReplayIterator *iter);
bool ld_store_iter_replay_next(ReplayDBReplayIterator *iter);
bool ld_store_iter_replay_finish(ReplayDBReplayIterator *iter);


/*
 * ld_store_replay_next_event returns the next event to apply after
 * previousLSN.  For transactions it returns the BEGIN row only when the
 * full transaction has been written (endlsn > previousLSN).  For
 * non-transactional events (KEEPALIVE/SWITCH/ENDPOS) it returns the first
 * row at lsn >= previousLSN.
 *
 * s->action is set to STREAM_ACTION_UNKNOWN when no rows are available.
 */
bool ld_store_replay_event_fetch(SQLiteQuery *query);

bool ld_store_replay_next_event(DatabaseCatalog *catalog,
								uint64_t previousLSN,
								ReplayDBStmt *s);


/*
 * ReplayDBReplayTxnIterator iterates over all rows of a single transaction
 * in the replay table (BEGIN + DML rows + COMMIT/ROLLBACK), starting from
 * the given begin_id, ordered by id.
 */
typedef struct ReplayDBReplayTxnIterator
{
	DatabaseCatalog *catalog;
	ReplayDBStmt *current;
	SQLiteQuery query;

	uint32_t xid;
	uint64_t begin_id;
} ReplayDBReplayTxnIterator;

bool ld_store_iter_replay_txn_init(ReplayDBReplayTxnIterator *iter);
bool ld_store_iter_replay_txn_next(ReplayDBReplayTxnIterator *iter);
bool ld_store_iter_replay_txn_finish(ReplayDBReplayTxnIterator *iter);

#endif /* LD_STORE_H */
