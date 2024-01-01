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
#include "log.h"
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
		"  id, startpos, endpos, apply, write_lsn, flush_lsn, replay_lsn) "
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
		log_error("BUG: sentinel_update_endpos: db is NULL");
		return false;
	}

	char *sql =
		"update sentinel set startpos = $1, write_lsn = $2, flush_lsn = $3 "
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

	/*
	 * Update startpos to flush_lsn, which is our safe restart point.
	 */
	char writeLSN[PG_LSN_MAXLENGTH] = { 0 };
	char flushLSN[PG_LSN_MAXLENGTH] = { 0 };

	sformat(writeLSN, sizeof(writeLSN), "%X/%X", LSN_FORMAT_ARGS(write_lsn));
	sformat(flushLSN, sizeof(flushLSN), "%X/%X", LSN_FORMAT_ARGS(flush_lsn));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, (char *) flushLSN },
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
		"select startpos, endpos, apply, write_lsn, flush_lsn, replay_lsn "
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
