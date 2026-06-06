/*
 * src/bin/pgcopydb/summary.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <unistd.h>

#include "catalog.h"
#include "copydb.h"
#include "ld_stream.h"
#include "log.h"
#include "parsing_utils.h"
#include "schema.h"
#include "string_utils.h"


/*
 * sentinel_setup registers the sentinel data.
 */
bool
sentinel_setup(DatabaseCatalog *catalog, uint64_t startpos, uint64_t endpos)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: catalog_register_setup: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into sentinel("
		"  id, startpos, endpos, apply, "
		"  write_lsn, flush_lsn, replay_lsn) "
		"values($1, $2, $3, $4, '0/0', '0/0', '0/0')";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	char startLSN[PG_LSN_MAXLENGTH] = { 0 };
	char endLSN[PG_LSN_MAXLENGTH] = { 0 };

	sformat(startLSN, sizeof(startLSN), "%X/%X", LSN_FORMAT_ARGS(startpos));
	sformat(endLSN, sizeof(endLSN), "%X/%X", LSN_FORMAT_ARGS(endpos));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "id", 1, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, (char *) startLSN },
		{ BIND_PARAMETER_TYPE_TEXT, "endpos", 0, (char *) endLSN },
		{ BIND_PARAMETER_TYPE_INT, "apply", 0, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * sentinel_update_startpos updates our pgcopydb sentinel table start pos.
 */
bool
sentinel_update_startpos(DatabaseCatalog *catalog, uint64_t startpos)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_update_startpos: db is NULL");
		return false;
	}

	char *sql = "update sentinel set startpos = $1 where id = 1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { .errorOnZeroRows = true };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	char startLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(startLSN, sizeof(startLSN), "%X/%X", LSN_FORMAT_ARGS(startpos));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, (char *) startLSN }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * sentinel_update_endpos updates our pgcopydb sentinel table end pos.
 */
bool
sentinel_update_endpos(DatabaseCatalog *catalog, uint64_t endpos)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_update_endpos: db is NULL");
		return false;
	}

	char *sql = "update sentinel set endpos = $1 where id = 1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { .errorOnZeroRows = true };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	char endLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(endLSN, sizeof(endLSN), "%X/%X", LSN_FORMAT_ARGS(endpos));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "endpos", 0, (char *) endLSN }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * sentinel_update_endpos updates our pgcopydb sentinel table end pos.
 */
bool
sentinel_update_apply(DatabaseCatalog *catalog, bool apply)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_update_apply: db is NULL");
		return false;
	}

	char *sql = "update sentinel set apply = $1 where id = 1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { .errorOnZeroRows = true };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT, "apply", apply ? 1 : 0, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * sentinel_update_write_flush_lsn updates the current sentinel values for
 * write_lsn and flush_lsn, and startpos too.
 */
bool
sentinel_update_write_flush_lsn(DatabaseCatalog *catalog,
								uint64_t write_lsn,
								uint64_t flush_lsn)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_update_write_flush_lsn: db is NULL");
		return false;
	}

	/*
	 * In the SQLite-based CDC pipeline, startpos is the replication slot's
	 * initial LSN and must not be overwritten with the current flush_lsn.
	 * flush_lsn here is replay_lsn (last applied commit) — updating startpos
	 * to flush_lsn would cause apply to skip remaining uncommitted rows on
	 * restart.
	 */
	char *sql =
		"update sentinel set write_lsn = $1, flush_lsn = $2 "
		"where id = 1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { .errorOnZeroRows = true };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	char writeLSN[PG_LSN_MAXLENGTH] = { 0 };
	char flushLSN[PG_LSN_MAXLENGTH] = { 0 };

	sformat(writeLSN, sizeof(writeLSN), "%X/%X", LSN_FORMAT_ARGS(write_lsn));
	sformat(flushLSN, sizeof(flushLSN), "%X/%X", LSN_FORMAT_ARGS(flush_lsn));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "write_lsn", 0, (char *) writeLSN },
		{ BIND_PARAMETER_TYPE_TEXT, "flush_lsn", 0, (char *) flushLSN }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}



/*
 * sentinel_update_replay_lsn updates our pgcopydb sentinel table end pos.
 */
bool
sentinel_update_replay_lsn(DatabaseCatalog *catalog, uint64_t replay_lsn)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_update_replay_lsn: db is NULL");
		return false;
	}

	char *sql = "update sentinel set replay_lsn = $1 where id = 1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { .errorOnZeroRows = true };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	char replayLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(replayLSN, sizeof(replayLSN), "%X/%X", LSN_FORMAT_ARGS(replay_lsn));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "replay_lsn", 0, (char *) replayLSN }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * sentinel_get fetches the current sentinel values
 */
bool
sentinel_get(DatabaseCatalog *catalog, CopyDBSentinel *sentinel)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_get: db is NULL");
		return false;
	}

	char *sql =
		"select startpos, endpos, apply, "
		"       write_lsn, flush_lsn, replay_lsn "
		"  from sentinel "
		" where id = 1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.errorOnZeroRows = true,
		.context = sentinel,
		.fetchFunction = &sentinel_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * sentinel_fetch fetches a CopyDBSentinel value from a SQLiteQuery result.
 */
bool
sentinel_fetch(SQLiteQuery *query)
{
	CopyDBSentinel *sentinel = (CopyDBSentinel *) query->context;

	bzero(sentinel, sizeof(CopyDBSentinel));

	if (sqlite3_column_type(query->ppStmt, 0) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 0);

		if (!parseLSN(lsn, &(sentinel->startpos)))
		{
			log_error("Failed to parse sentinel startpos LSN \"%s\"", lsn);
			return false;
		}
	}

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 1);

		if (!parseLSN(lsn, &(sentinel->endpos)))
		{
			log_error("Failed to parse sentinel endpos LSN \"%s\"", lsn);
			return false;
		}
	}

	sentinel->apply = sqlite3_column_int(query->ppStmt, 2) == 1;

	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 3);

		if (!parseLSN(lsn, &(sentinel->write_lsn)))
		{
			log_error("Failed to parse sentinel write_lsn LSN \"%s\"", lsn);
			return false;
		}
	}

	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 4);

		if (!parseLSN(lsn, &(sentinel->flush_lsn)))
		{
			log_error("Failed to parse sentinel flush_lsn LSN \"%s\"", lsn);
			return false;
		}
	}

	if (sqlite3_column_type(query->ppStmt, 5) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 5);

		if (!parseLSN(lsn, &(sentinel->replay_lsn)))
		{
			log_error("Failed to parse sentinel replay_lsn LSN \"%s\"", lsn);
			return false;
		}
	}

	return true;
}


/*
 * sentinel_sync_recv updates the current sentinel values for write_lsn and
 * flush_lsn, and fetches the current value for replay_lsn, endpos, and apply.
 */
bool
sentinel_sync_recv(DatabaseCatalog *catalog,
				   uint64_t write_lsn,
				   uint64_t flush_lsn,
				   CopyDBSentinel *sentinel)
{
	if (!sentinel_update_write_flush_lsn(catalog, write_lsn, flush_lsn))
	{
		/* errors have already been logged */
		return false;
	}

	if (!sentinel_get(catalog, sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("sentinel_sync_recv: write_lsn %X/%X flush_lsn %X/%X",
			  LSN_FORMAT_ARGS(sentinel->write_lsn),
			  LSN_FORMAT_ARGS(sentinel->flush_lsn));

	return true;
}



/*
 * sentinel_sync_apply updates the current sentinel values for replay_lsn, and
 * fetches the current value for endpos and apply.
 */
bool
sentinel_sync_apply(DatabaseCatalog *catalog,
					uint64_t replay_lsn,
					CopyDBSentinel *sentinel)
{
	if (!sentinel_update_replay_lsn(catalog, replay_lsn))
	{
		/* errors have already been logged */
		return false;
	}

	if (!sentinel_get(catalog, sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("sentinel_sync_apply: sentinel.replay_lsn %X/%X",
			  LSN_FORMAT_ARGS(sentinel->replay_lsn));

	return true;
}

/*
 * ────────────────────────────────────────────────────────────────────────────
 * pipeline_state — per-process lifecycle tracking
 *
 * One row per process ('receive', 'apply') in the same sourceDB
 * file as the sentinel table.  Rows are UPSERT'd so the first run creates them
 * and subsequent runs update in place.
 * ────────────────────────────────────────────────────────────────────────────
 */

/*
 * pipeline_state_start — called at the beginning of each process run.
 *
 * Writes run_state='running', captures start LSN and PID, clears any previous
 * run_end_lsn / ended_at.
 */
bool
pipeline_state_start(DatabaseCatalog *catalog,
					 const char *process_name,
					 uint64_t    run_start_lsn)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: pipeline_state_start: db is NULL");
		return false;
	}

	char startLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(startLSN, sizeof(startLSN), "%X/%X", LSN_FORMAT_ARGS(run_start_lsn));

	char *sql =
		"insert or replace into pipeline_state"
		"  (process_name, pid, run_state, run_start_lsn, run_end_lsn,"
		"   started_at, ended_at,"
		"   last_xid, last_txn_begin_lsn, last_txn_end_lsn,"
		"   last_txn_complete, last_txn_processed)"
		"values($1, $2, 'running', $3, null,"
		"       strftime('%s','now'), null,"
		"       null, null, null, 0, 0)";

	if (!semaphore_lock(&(catalog->sema)))
		return false;

	SQLiteQuery q = { 0 };

	if (!catalog_sql_prepare(db, sql, &q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT,  "process_name",  0,          (char *) process_name },
		{ BIND_PARAMETER_TYPE_INT64, "pid",           getpid(),   NULL },
		{ BIND_PARAMETER_TYPE_TEXT,  "run_start_lsn", 0,          startLSN }
	};

	if (!catalog_sql_bind(&q, params, 3) || !catalog_sql_execute_once(&q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	log_debug("pipeline_state_start: %s at %s", process_name, startLSN);
	return true;
}


/*
 * pipeline_state_txn_begin — called when a process opens a new transaction.
 *
 * Sets last_txn_begin_lsn, clears last_txn_end_lsn (NULL = still open),
 * marks complete=0, processed=0.
 */
bool
pipeline_state_txn_begin(DatabaseCatalog *catalog,
						 const char      *process_name,
						 uint32_t         xid,
						 uint64_t         begin_lsn)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: pipeline_state_txn_begin: db is NULL");
		return false;
	}

	char beginLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(beginLSN, sizeof(beginLSN), "%X/%X", LSN_FORMAT_ARGS(begin_lsn));

	char *sql =
		"update pipeline_state set"
		"  last_xid             = $1,"
		"  last_txn_begin_lsn   = $2,"
		"  last_txn_end_lsn     = null,"
		"  last_txn_complete    = 0,"
		"  last_txn_processed   = 0"
		" where process_name = $3";

	if (!semaphore_lock(&(catalog->sema)))
		return false;

	SQLiteQuery q = { 0 };

	if (!catalog_sql_prepare(db, sql, &q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid",          xid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT,  "begin_lsn",    0,   beginLSN },
		{ BIND_PARAMETER_TYPE_TEXT,  "process_name", 0,   (char *) process_name }
	};

	if (!catalog_sql_bind(&q, params, 3) || !catalog_sql_execute_once(&q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));
	return true;
}


/*
 * pipeline_state_txn_done — called after a transaction has been fully
 * processed (written to replay table or applied to target).
 *
 * Sets last_txn_end_lsn, complete=1, processed=1.
 */
bool
pipeline_state_txn_done(DatabaseCatalog *catalog,
						const char      *process_name,
						uint32_t         xid,
						uint64_t         end_lsn)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: pipeline_state_txn_done: db is NULL");
		return false;
	}

	char endLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(endLSN, sizeof(endLSN), "%X/%X", LSN_FORMAT_ARGS(end_lsn));

	char *sql =
		"update pipeline_state set"
		"  last_xid            = $1,"
		"  last_txn_end_lsn    = $2,"
		"  last_txn_complete   = 1,"
		"  last_txn_processed  = 1"
		" where process_name = $3";

	if (!semaphore_lock(&(catalog->sema)))
		return false;

	SQLiteQuery q = { 0 };

	if (!catalog_sql_prepare(db, sql, &q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid",          xid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT,  "end_lsn",      0,   endLSN },
		{ BIND_PARAMETER_TYPE_TEXT,  "process_name", 0,   (char *) process_name }
	};

	if (!catalog_sql_bind(&q, params, 3) || !catalog_sql_execute_once(&q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));
	return true;
}


/*
 * pipeline_state_end — called when a process finishes a run (cleanly or with
 * error).
 */
bool
pipeline_state_end(DatabaseCatalog *catalog,
				   const char      *process_name,
				   uint64_t         run_end_lsn,
				   bool             success)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: pipeline_state_end: db is NULL");
		return false;
	}

	char endLSN[PG_LSN_MAXLENGTH] = { 0 };
	sformat(endLSN, sizeof(endLSN), "%X/%X", LSN_FORMAT_ARGS(run_end_lsn));

	const char *state = success ? "done" : "error";

	char *sql =
		"update pipeline_state set"
		"  run_state   = $1,"
		"  run_end_lsn = $2,"
		"  ended_at    = strftime('%s','now')"
		" where process_name = $3";

	if (!semaphore_lock(&(catalog->sema)))
		return false;

	SQLiteQuery q = { 0 };

	if (!catalog_sql_prepare(db, sql, &q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "run_state",    0, (char *) state },
		{ BIND_PARAMETER_TYPE_TEXT, "run_end_lsn",  0, endLSN },
		{ BIND_PARAMETER_TYPE_TEXT, "process_name", 0, (char *) process_name }
	};

	if (!catalog_sql_bind(&q, params, 3) || !catalog_sql_execute_once(&q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));

	log_info("pipeline_state_end: %s %s at %s", process_name, state, endLSN);
	return true;
}


/*
 * pipeline_state_sync — persist the in-memory "work" fields (last_xid,
 * last_txn_*, run_end_lsn) of a process's pipeline_state row in a single
 * UPDATE.  The apply driver loop keeps these values in memory and calls this
 * once in a while (and once at end of processing) instead of doing a SQLite
 * round-trip per transaction.  run_state / pid / started_at are owned by
 * pipeline_state_start/_end and are intentionally left untouched here.
 */
bool
pipeline_state_sync(DatabaseCatalog        *catalog,
					const PipelineStateEntry *state)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: pipeline_state_sync: db is NULL");
		return false;
	}

	char beginLSN[PG_LSN_MAXLENGTH] = { 0 };
	char endLSN[PG_LSN_MAXLENGTH] = { 0 };
	char runEndLSN[PG_LSN_MAXLENGTH] = { 0 };

	sformat(beginLSN, sizeof(beginLSN), "%X/%X",
			LSN_FORMAT_ARGS(state->last_txn_begin_lsn));
	sformat(endLSN, sizeof(endLSN), "%X/%X",
			LSN_FORMAT_ARGS(state->last_txn_end_lsn));
	sformat(runEndLSN, sizeof(runEndLSN), "%X/%X",
			LSN_FORMAT_ARGS(state->run_end_lsn));

	char *sql =
		"update pipeline_state set"
		"  last_xid            = $1,"
		"  last_txn_begin_lsn  = $2,"
		"  last_txn_end_lsn    = $3,"
		"  last_txn_complete   = $4,"
		"  last_txn_processed  = $5,"
		"  run_end_lsn         = $6"
		" where process_name = $7";

	if (!semaphore_lock(&(catalog->sema)))
		return false;

	SQLiteQuery q = { 0 };

	if (!catalog_sql_prepare(db, sql, &q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "last_xid",     state->last_xid,                  NULL },
		{ BIND_PARAMETER_TYPE_TEXT,  "begin_lsn",    0,                                beginLSN },
		{ BIND_PARAMETER_TYPE_TEXT,  "end_lsn",      0,                                endLSN },
		{ BIND_PARAMETER_TYPE_INT64, "complete",     state->last_txn_complete ? 1 : 0, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "processed",    state->last_txn_processed ? 1 : 0, NULL },
		{ BIND_PARAMETER_TYPE_TEXT,  "run_end_lsn",  0,                                runEndLSN },
		{ BIND_PARAMETER_TYPE_TEXT,  "process_name", 0,                                (char *) state->process_name }
	};

	if (!catalog_sql_bind(&q, params, 7) || !catalog_sql_execute_once(&q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));
	return true;
}


static bool pipeline_state_fetch(SQLiteQuery *query);

/*
 * pipeline_state_get — read the current state for a named process.
 *
 * Returns false on SQL error; sets state->process_name[0] = '\0' if no row.
 */
bool
pipeline_state_get(DatabaseCatalog   *catalog,
				   const char        *process_name,
				   PipelineStateEntry *state)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: pipeline_state_get: db is NULL");
		return false;
	}

	memset(state, 0, sizeof(PipelineStateEntry));

	char *sql =
		"select process_name, pid, run_state,"
		"       run_start_lsn, run_end_lsn,"
		"       started_at, ended_at,"
		"       last_xid, last_txn_begin_lsn, last_txn_end_lsn,"
		"       last_txn_complete, last_txn_processed"
		"  from pipeline_state"
		" where process_name = $1";

	if (!semaphore_lock(&(catalog->sema)))
		return false;

	SQLiteQuery q = {
		.errorOnZeroRows = false,
		.context         = state,
		.fetchFunction   = &pipeline_state_fetch
	};

	if (!catalog_sql_prepare(db, sql, &q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "process_name", 0, (char *) process_name }
	};

	if (!catalog_sql_bind(&q, params, 1) || !catalog_sql_execute_once(&q))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));
	return true;
}


static bool
pipeline_state_fetch(SQLiteQuery *query)
{
	PipelineStateEntry *state = (PipelineStateEntry *) query->context;

	/* column 0: process_name */
	if (sqlite3_column_type(query->ppStmt, 0) != SQLITE_NULL)
		strlcpy(state->process_name,
				(char *) sqlite3_column_text(query->ppStmt, 0),
				sizeof(state->process_name));

	/* column 1: pid */
	state->pid = (int) sqlite3_column_int64(query->ppStmt, 1);

	/* column 2: run_state */
	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
		strlcpy(state->run_state,
				(char *) sqlite3_column_text(query->ppStmt, 2),
				sizeof(state->run_state));

	/* columns 3,4: run_start_lsn, run_end_lsn */
	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
		(void) parseLSN((char *) sqlite3_column_text(query->ppStmt, 3),
						&state->run_start_lsn);

	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
		(void) parseLSN((char *) sqlite3_column_text(query->ppStmt, 4),
						&state->run_end_lsn);

	/* columns 5,6: started_at, ended_at */
	state->started_at = sqlite3_column_int64(query->ppStmt, 5);
	state->ended_at   = sqlite3_column_int64(query->ppStmt, 6);

	/* column 7: last_xid */
	state->last_xid = (uint32_t) sqlite3_column_int64(query->ppStmt, 7);

	/* columns 8,9: last_txn_begin_lsn, last_txn_end_lsn */
	if (sqlite3_column_type(query->ppStmt, 8) != SQLITE_NULL)
		(void) parseLSN((char *) sqlite3_column_text(query->ppStmt, 8),
						&state->last_txn_begin_lsn);

	if (sqlite3_column_type(query->ppStmt, 9) != SQLITE_NULL)
		(void) parseLSN((char *) sqlite3_column_text(query->ppStmt, 9),
						&state->last_txn_end_lsn);

	/* columns 10,11: last_txn_complete, last_txn_processed */
	state->last_txn_complete   = (bool) sqlite3_column_int(query->ppStmt, 10);
	state->last_txn_processed  = (bool) sqlite3_column_int(query->ppStmt, 11);

	return true;
}
