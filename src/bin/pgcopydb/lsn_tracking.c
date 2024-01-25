/*
 * src/bin/pgcopydb/lsn_tracking.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>

#include "catalog.h"
#include "copydb.h"
#include "ld_stream.h"
#include "log.h"
#include "lsn_tracking.h"
#include "string_utils.h"


static bool lsn_tracking_read_hook(void *ctx, LSNTracking *lsn_tracking);


/*
 * lsn_tracking_write writes the context->LSNTracking linked-list to our SQLite
 * catalog, on-disk. This function replaces whatever was stored before with the
 * new content.
 */
bool
lsn_tracking_write(DatabaseCatalog *catalog, LSNTracking *lsnTrackingList)
{
	if (!lsn_tracking_delete_all(catalog))
	{
		/* errors have already been logged */
		return false;
	}

	LSNTracking *current = lsnTrackingList;

	for (; current != NULL; current = current->previous)
	{
		if (!lsn_tracking_add(catalog, current))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * lsn_tracking_delete_all DELETEs the lsn_tracking SQLite table contents.
 */
bool
lsn_tracking_delete_all(DatabaseCatalog *catalog)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_delete_all_lsn_tracking: db is NULL");
		return false;
	}

	char *sql = "delete from lsn_tracking";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * lsn_tracking_add INSERTs an LSNTracking point to our catalogs.
 */
bool
lsn_tracking_add(DatabaseCatalog *catalog, LSNTracking *current)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: sentinel_add_lsn_tracking: db is NULL");
		return false;
	}

	char sourceLSN[PG_LSN_MAXLENGTH] = { 0 };
	char insertLSN[PG_LSN_MAXLENGTH] = { 0 };

	sformat(sourceLSN, sizeof(sourceLSN), "%X/%X",
			LSN_FORMAT_ARGS(current->sourceLSN));

	sformat(insertLSN, sizeof(insertLSN), "%X/%X",
			LSN_FORMAT_ARGS(current->insertLSN));

	char *sql = "insert into lsn_tracking(source, target) values($1, $2)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "source", 0, (char *) sourceLSN },
		{ BIND_PARAMETER_TYPE_TEXT, "target", 0, (char *) insertLSN }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which does not return any row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * lsn_tracking_read reads the LSN Tracking information from the SQLite
 * catalogs.
 */
bool
lsn_tracking_read(StreamApplyContext *context)
{
	DatabaseCatalog *sourceDB = context->sourceDB;

	if (!lsn_tracking_iter(sourceDB, context, lsn_tracking_read_hook))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * lsn_tracking_read_hook is an iterator callback function.
 */
static bool
lsn_tracking_read_hook(void *ctx, LSNTracking *lsn_tracking)
{
	StreamApplyContext *context = (StreamApplyContext *) ctx;

	/*
	 * The iterator API re-uses the same memory area for each SQLite row that
	 * is being iterated over, but here we want to accumulate a linked-list of
	 * lsn tracking values. So create a new memory area item each time.
	 */
	LSNTracking *entry = (LSNTracking *) calloc(1, sizeof(LSNTracking));

	if (entry == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	entry->sourceLSN = lsn_tracking->sourceLSN;
	entry->insertLSN = lsn_tracking->insertLSN;
	entry->previous = context->lsnTrackingList;

	context->lsnTrackingList = entry;

	return true;
}


/*
 * lsn_tracking_iter iterates over the list of sequences in our catalogs.
 */
bool
lsn_tracking_iter(DatabaseCatalog *catalog,
				  void *context,
				  LSNTrackingIterFun *callback)
{
	LSNTrackingIterator *iter =
		(LSNTrackingIterator *) calloc(1, sizeof(LSNTrackingIterator));

	iter->catalog = catalog;

	if (!lsn_tracking_iter_init(iter))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!lsn_tracking_iter_next(iter))
		{
			/* errors have already been logged */
			return false;
		}

		LSNTracking *lsnTracking = iter->lsnTracking;

		if (lsnTracking == NULL)
		{
			if (!lsn_tracking_iter_finish(iter))
			{
				/* errors have already been logged */
				return false;
			}

			break;
		}

		/* now call the provided callback */
		if (!(*callback)(context, lsnTracking))
		{
			log_error("Failed to iterate over list of lsnTrackings, "
					  "see above for details");
			return false;
		}
	}


	return true;
}


/*
 * lsn_tracking_iter_init initializes an Interator over our catalog of
 * LSNTracking entries.
 */
bool
lsn_tracking_iter_init(LSNTrackingIterator *iter)
{
	sqlite3 *db = iter->catalog->db;

	if (db == NULL)
	{
		log_error("BUG: Failed to initialize lsnTracking iterator: db is NULL");
		return false;
	}

	iter->lsnTracking = (LSNTracking *) calloc(1, sizeof(LSNTracking));

	if (iter->lsnTracking == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql = "select source, target from lsn_tracking order by source desc";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->lsnTracking;
	query->fetchFunction = &lsn_tracking_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * sentinel_iter_lsn_tracking_next fetches the next LSNTracking entry in our catalogs.
 */
bool
lsn_tracking_iter_next(LSNTrackingIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->lsnTracking = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);
		log_error("[SQLite] %s", sqlite3_errmsg(query->db));
		return false;
	}

	return lsn_tracking_fetch(query);
}


/*
 * lsn_tracking_fetch fetches a LSNTracking entry from a SQLite ppStmt result
 * set.
 */
bool
lsn_tracking_fetch(SQLiteQuery *query)
{
	LSNTracking *lsnTracking = (LSNTracking *) query->context;

	/* cleanup the memory area before re-use */
	bzero(lsnTracking, sizeof(LSNTracking));

	if (sqlite3_column_type(query->ppStmt, 0) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 0);

		if (!parseLSN(lsn, &(lsnTracking->sourceLSN)))
		{
			log_error("Failed to parse source LSN \"%s\"", lsn);
			return false;
		}
	}

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		char *lsn = (char *) sqlite3_column_text(query->ppStmt, 1);

		if (!parseLSN(lsn, &(lsnTracking->insertLSN)))
		{
			log_error("Failed to parse target insert LSN \"%s\"", lsn);
			return false;
		}
	}

	return true;
}


/*
 * sentinel_iter_lsn_tracking_finish cleans-up the internal memory used for the
 * iteration.
 */
bool
lsn_tracking_iter_finish(LSNTrackingIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	/* in case we finish before reaching the DONE step */
	if (iter->lsnTracking != NULL)
	{
		iter->lsnTracking = NULL;
	}

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}
