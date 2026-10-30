/*
 * src/bin/pgcopydb/ld_store.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parson.h"

#include "copydb.h"
#include "ld_pgoutput.h"
#include "ld_store.h"
#include "ld_stream.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "file_utils.h"
#include "string_utils.h"


/*
 * ld_store_replaydb_filename_from_outputdb derives the replay.db path from the
 * output.db path by suffix substitution ("-output.db" -> "-replay.db").
 */
static void
ld_store_replaydb_filename_from_outputdb(const char *outputFile,
										 char *replayFile,
										 size_t replayFileSize)
{
	/*
	 * outputFile ends with "-output.db"; replace that suffix with "-replay.db".
	 * If the suffix is absent (legacy file without suffix), just append "-replay".
	 */
	strlcpy(replayFile, outputFile, replayFileSize);

	char *suffix = strstr(replayFile, "-output.db");

	if (suffix != NULL)
	{
		strlcpy(suffix, "-replay.db", replayFileSize - (suffix - replayFile));
	}
	else
	{
		/* fallback: strip .db and append -replay.db */
		char *dot = strrchr(replayFile, '.');
		if (dot != NULL)
		{
			strlcpy(dot, "-replay.db", replayFileSize - (dot - replayFile));
		}
		else
		{
			strlcat(replayFile, "-replay.db", replayFileSize);
		}
	}
}


/*
 * ld_store_open_outputdb opens (or creates) the output.db file for receive.
 *
 * output.db contains only the "output" table and is written exclusively by
 * the receive process.  Apply opens it read-only via its own catalog handle.
 */
bool
ld_store_open_outputdb(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);
	DatabaseCatalog *outputDB = specs->outputDB;

	/*
	 * Only query cdc_files when the caller has not already pre-populated the
	 * filename.  The apply path sets the filename via ld_store_set_first_cdc_filename
	 * or ld_store_set_cdc_filename_at_lsn before calling here, so we must not
	 * overwrite it with the (potentially different) newest-open-file result.
	 */
	if (IS_EMPTY_STRING_BUFFER(outputDB->dbfile))
	{
		if (!ld_store_set_current_cdc_filename(specs))
		{
			/* errors have already been logged */
			return false;
		}
	}

	bool createOutputDB = IS_EMPTY_STRING_BUFFER(outputDB->dbfile);

	/* if we don't have a filename yet, derive one from timeline + startpos */
	if (IS_EMPTY_STRING_BUFFER(outputDB->dbfile))
	{
		if (privateContext->timeline == 0)
		{
			log_error("BUG: ld_store_open_outputdb: timeline is zero");
			return false;
		}

		sformat(outputDB->dbfile, MAXPGPATH, "%s/%08d-%08X-%08X-output.db",
				specs->paths.dir,
				privateContext->timeline,
				LSN_FORMAT_ARGS(privateContext->startpos));
	}

	log_info("%s output CDC file \"%s\"",
			 createOutputDB ? "Creating" : "Opening",
			 outputDB->dbfile);

	outputDB->type = DATABASE_CATALOG_TYPE_OUTPUT;

	if (!catalog_init(outputDB))
	{
		log_error("Failed to open output database \"%s\", "
				  "see above for details",
				  outputDB->dbfile);
		return false;
	}

	if (createOutputDB)
	{
		if (!ld_store_insert_cdc_filename(specs))
		{
			log_error("Failed to register output CDC file \"%s\", "
					  "see above for details",
					  outputDB->dbfile);
			return false;
		}
	}

	return true;
}


/*
 * ld_store_open_replaydb opens (or creates) the replay.db file for apply.
 *
 * replay.db contains stmt and replay tables and is written exclusively by
 * the apply process.  Its filename is derived from the matching output.db
 * filename by replacing the "-output.db" suffix with "-replay.db".
 */
bool
ld_store_open_replaydb(StreamSpecs *specs)
{
	DatabaseCatalog *outputDB = specs->outputDB;
	DatabaseCatalog *replayDB = specs->replayDB;

	if (IS_EMPTY_STRING_BUFFER(outputDB->dbfile))
	{
		log_error("BUG: ld_store_open_replaydb called before outputDB filename is set");
		return false;
	}

	/* derive replay.db path from output.db path */
	ld_store_replaydb_filename_from_outputdb(outputDB->dbfile,
											 replayDB->dbfile,
											 sizeof(replayDB->dbfile));

	bool createReplayDB = (access(replayDB->dbfile, F_OK) != 0);

	log_info("%s replay CDC file \"%s\"",
			 createReplayDB ? "Creating" : "Opening",
			 replayDB->dbfile);

	replayDB->type = DATABASE_CATALOG_TYPE_REPLAY;

	if (!catalog_init(replayDB))
	{
		log_error("Failed to open replay database \"%s\", "
				  "see above for details",
				  replayDB->dbfile);
		return false;
	}

	/* a file from an earlier version may lack the indexes the queries name */
	if (!createReplayDB && !catalog_create_indexes(replayDB))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_set_first_cdc_filename selects the CDC file with the smallest id
 * in cdc_files (regardless of done_time_epoch) and stores its path in
 * specs->outputDB->dbfile.
 *
 * Used by the apply process when previousLSN == 0 (fresh start): in the
 * prefetch+catchup workflow with file rotation there may be several closed
 * files before the single open one, and the apply must start from file 1.
 */
bool
ld_store_set_first_cdc_filename(StreamSpecs *specs)
{
	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_set_first_cdc_filename: db is NULL");
		return false;
	}

	char *sql =
		"select filename "
		"  from cdc_files "
		"order by id asc "
		"   limit 1";

	SQLiteQuery query = {
		.context = specs->outputDB->dbfile,
		.fetchFunction = &ld_store_cdc_filename_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* no parameters: the query is unconditional */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_set_current_cdc_filename finds the most recently created open
 * replayDB file in cdc_files and stores its path in specs->outputDB->dbfile.
 *
 * "Open" means done_time_epoch IS NULL.  We sort by id DESC to get the most
 * recently inserted row, which corresponds to the current streaming epoch.
 *
 * This is only used by the RECEIVE path.  The APPLY path should call
 * ld_store_set_first_cdc_filename (previousLSN == 0) or
 * ld_store_set_cdc_filename_at_lsn (previousLSN > 0) to land on the right
 * starting file after file rotation.
 */
bool
ld_store_set_current_cdc_filename(StreamSpecs *specs)
{
	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_set_current_cdc_filename: db is NULL");
		return false;
	}

	char *sql =
		"  select filename "
		"    from cdc_files "
		"   where done_time_epoch is null "
		"order by id desc "
		"   limit 1";

	SQLiteQuery query = {
		.context = specs->outputDB->dbfile,
		.fetchFunction = &ld_store_cdc_filename_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* no parameters: the query is unconditional */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_set_cdc_filename_at_lsn finds the CDC file that contains the given
 * LSN.
 *
 * Because of the Postgres Logical Decoding default transaction ordering, where
 * a transaction is streamed after COMMIT, we could have interleaving
 * transactions, meaning several file candidates for the same LSN.
 *
 * In that case open the files until we find the one that actually contains the
 * given LSN.
 *
 * When the given LSN is "0/0", which is InvalidXLogRecPtr, then open the first
 * file that we have.
 */
bool
ld_store_set_cdc_filename_at_lsn(StreamSpecs *specs, uint64_t lsn)
{
	if (lsn == InvalidXLogRecPtr)
	{
		log_debug("ld_store_set_cdc_filename_at_lsn: 0/0");
		return true;
	}

	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_set_cdc_filename_at_lsn: db is NULL");
		return false;
	}

	/*
	 * Find the CDC file whose LSN range covers the given position.
	 * A file covers an LSN when its startpos <= lsn AND its endpos is
	 * either NULL (still open / not yet closed) or >= lsn.
	 *
	 * The old predicate "startpos >= $1 AND endpos <= $2" was inverted and
	 * also broke on NULL endpos (SQLite evaluates NULL <= x as NULL, not
	 * TRUE), so the query returned zero rows for every open file.
	 */
	char *sql =
		"  select filename "
		"    from cdc_files "
		"   where startpos <= $1 "
		"     and (endpos is null or endpos >= $1) "
		"order by id, filename ";

	char candidate[MAXPGPATH] = { 0 };

	SQLiteQuery query = {
		.context = candidate,
		.fetchFunction = &ld_store_cdc_filename_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	char pg_lsn[PG_LSN_MAXLENGTH] = { 0 };
	sformat(pg_lsn, sizeof(pg_lsn), "%08X/%08X", LSN_FORMAT_ARGS(lsn));

	/* single parameter: the target LSN used for both <= and >= comparisons */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "lsn", 0, (char *) pg_lsn }
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	int rc;

	while ((rc = catalog_sql_step(&query)) != SQLITE_DONE)
	{
		if (rc != SQLITE_ROW)
		{
			log_error("Failed to fetch cdc_file for LSN %X/%X, "
					  "see above for details",
					  LSN_FORMAT_ARGS(lsn));
			return false;
		}

		if (!ld_store_cdc_filename_fetch(&query))
		{
			/* errors have already been logged */
			return false;
		}

		/*
		 * If we already have this file open, skip the probe/promotion.
		 * Every ~1.5s the apply loop calls this function and the same
		 * file is returned; recreating the outputDB each time would leak a
		 * SysV semaphore into the global array (max 16 slots).
		 */
		if (specs->outputDB != NULL &&
			specs->outputDB->db != NULL &&
			strcmp(specs->outputDB->dbfile, candidate) == 0)
		{
			break;
		}

		DatabaseCatalog *candidateDB =
			(DatabaseCatalog *) calloc(1, sizeof(DatabaseCatalog));

		if (candidateDB == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(candidateDB->dbfile, candidate, sizeof(candidateDB->dbfile));

		/*
		 * Probe the database file with a raw read-only SQLite open.
		 * We avoid catalog_open/catalog_init here because that would create a
		 * new SysV semaphore and register it in the global system_res_array,
		 * which has only 16 slots.  The apply loop calls this function
		 * every ~1.5s, so the array would fill up within seconds.
		 */
		sqlite3 *probeDB = NULL;

		int sqlite_rc = sqlite3_open_v2(candidateDB->dbfile,
										&probeDB,
										SQLITE_OPEN_READONLY,
										NULL);

		if (sqlite_rc != SQLITE_OK)
		{
			log_error("Failed to open \"%s\": %s",
					  candidateDB->dbfile,
					  sqlite3_errmsg(probeDB));
			if (probeDB != NULL)
			{
				sqlite3_close(probeDB);
			}
			free(candidateDB);
			return false;
		}

		/*
		 * Temporarily attach the raw handle so ld_store_lookup_output_after_lsn
		 * can run its query through the usual catalog_sql_* helpers.
		 */
		candidateDB->db = probeDB;

		/*
		 * Verify the file contains at least one output row at or after the
		 * given LSN.  Use errorOnZeroRows = false: the apply_lsn might
		 * sit between two message LSNs (e.g. when it equals the stream
		 * startpos which precedes the first written message).
		 */
		ReplayDBOutputMessage output = { 0 };

		/* the probe asks "any row from here on", so both cursors are lsn */
		if (!ld_store_lookup_output_after_lsn(candidateDB, lsn, lsn, &output))
		{
			/* errors have already been logged */
			sqlite3_close(probeDB);
			candidateDB->db = NULL;
			free(candidateDB);
			return false;
		}

		sqlite3_close(probeDB);
		candidateDB->db = NULL;

		if (output.lsn != InvalidXLogRecPtr)
		{
			/*
			 * This file contains messages at or after the given LSN.
			 * Fully initialize it (semaphore, WAL mode, schema) so it
			 * becomes the active outputDB.
			 */
			candidateDB->type = DATABASE_CATALOG_TYPE_OUTPUT;

			if (!catalog_init(candidateDB))
			{
				/* errors have already been logged */
				free(candidateDB);
				return false;
			}

			/* close the old outputDB SQLite handle before replacing */
			if (specs->outputDB != NULL && specs->outputDB->db != NULL)
			{
				(void) catalog_close(specs->outputDB);
			}

			specs->outputDB = candidateDB;

			/* derive the matching replay.db filename */
			if (specs->replayDB != NULL)
			{
				ld_store_replaydb_filename_from_outputdb(
					candidateDB->dbfile,
					specs->replayDB->dbfile,
					sizeof(specs->replayDB->dbfile));
			}

			break;
		}

		free(candidateDB);
	}

	if (!catalog_sql_finalize(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_cdc_filename_fetch is a SQLiteQuery callback.
 */
bool
ld_store_cdc_filename_fetch(SQLiteQuery *query)
{
	char *filename = (char *) query->context;

	if (sqlite3_column_type(query->ppStmt, 0) == SQLITE_NULL)
	{
		query->context = NULL;
	}
	else
	{
		strlcpy(filename,
				(char *) sqlite3_column_text(query->ppStmt, 0),
				MAXPGPATH);
	}

	return true;
}


/*
 * ld_store_lookup_output_at_lsn searches the given LSN in the given replayDB
 * database.
 */
bool
ld_store_lookup_output_at_lsn(DatabaseCatalog *catalog, uint64_t lsn,
							  ReplayDBOutputMessage *output)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_lookup_output_at_lsn: db is NULL");
		return false;
	}

	char *sql =
		"  select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
		"    from output "
		"   where lsn = $1 "
		"order by id "
		"   limit 1";

	log_debug("ld_store_lookup_output_at_lsn: %X/%X", LSN_FORMAT_ARGS(lsn));

	SQLiteQuery query = {
		.errorOnZeroRows = true,
		.context = output,
		.fetchFunction = &ld_store_output_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "lsn", lsn },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_lookup_output_one runs a query that returns at most one row of the
 * output table, with the usual columns, and fetches it into output: left
 * zeroed (its lsn is InvalidXLogRecPtr) when there is none.
 */
static bool
ld_store_lookup_output_one(DatabaseCatalog *catalog,
						   const char *sql,
						   BindParam *params,
						   int count,
						   ReplayDBOutputMessage *output)
{
	SQLiteQuery query = {
		.errorOnZeroRows = false,
		.context = output,
		.fetchFunction = &ld_store_output_fetch
	};

	if (!catalog_sql_prepare_cached(catalog, sql, &query) ||
		!catalog_sql_bind(&query, params, count) ||
		!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_lookup_output_after_lsn searches the first message following the
 * given LSN in the replayDB database.
 *
 * The same LSN would typically be used in Postgres for a COMMIT message and
 * the BEGIN message of the following transaction, so we search for a message
 * with an lsn greater than or equal to the given one, and a message that's
 * neither a COMMIT nor a ROLLBACK.
 *
 * {"action":"C","xid":"499","lsn":"0/24E1B08"}
 * {"action":"B","xid":"500","lsn":"0/24E1B08"}
 */
bool
ld_store_lookup_output_after_lsn(DatabaseCatalog *catalog,
								 uint64_t lsn,
								 uint64_t keepaliveLSN,
								 ReplayDBOutputMessage *output)
{
	sqlite3 *db = catalog->db;


	if (db == NULL)
	{
		log_error("BUG: ld_store_lookup_output_after_lsn: db is NULL");
		return false;
	}


	/*
	 * Find the next thing to process starting from transform_lsn.
	 *
	 * Valid "first row" types are: BEGIN (start of a new transaction),
	 * KEEPALIVE, ENDPOS, SWITCH (non-transactional markers), or a DML row
	 * (when transform_lsn was advanced past a partial boundary and the slot
	 * re-delivered the complete transaction — the "resuming" case).
	 *
	 * COMMIT (action='C') and ROLLBACK (action='R') are NEVER valid starting
	 * points: they are the END of a transaction, never the start.  If we
	 * returned them here we would re-iterate the same transaction forever:
	 *
	 *   transform_lsn = COMMIT_LSN
	 *   → query returns COMMIT (lsn = COMMIT_LSN, lowest id that is not B)
	 *   → default: case resumes from BEGIN, processes txn, sets transform_lsn
	 *     back to COMMIT_LSN  → infinite loop
	 *
	 * By excluding C and R, the query instead returns the ENDPOS or next
	 * BEGIN sitting at COMMIT_LSN, which breaks the loop.
	 *
	 * Note: ld_store_lookup_output_xid_end uses a separate query that still
	 * looks for C/R — it is not affected by this change.
	 */

	/*
	 * Outer query: find the next unit of work for transform.
	 *
	 * There are exactly two kinds of row that the outer loop should see:
	 *
	 *   1. BEGIN ('B') — the start of a transaction.  Transform iterates
	 *      the full transaction (B … DML … C/R) via the inner iterator.
	 *      BEGIN may legitimately share a LSN with the previous COMMIT
	 *      (wal2json emits them at the same WAL position), so use >=.
	 *
	 *   2. KEEPALIVE ('K') or SWITCH ('X') — non-transactional markers
	 *      that appear between committed transactions.  They carry the
	 *      server's walEnd, which sits above transactions the server has
	 *      not decoded yet, so they get their own cursor ($2) instead of
	 *      riding on the transaction cursor ($1), which would hide those
	 *      transactions.  Strict > so that the marker the apply just
	 *      consumed is not handed back.
	 *
	 * DML rows (I/U/D/T) are NEVER returned here: they are always inside
	 * a transaction and are consumed exclusively by the inner iterator
	 * once the BEGIN has been found.  COMMIT and ROLLBACK are also
	 * excluded — they are end-of-transaction anchors, not starting points.
	 */

	/*
	 * Picking the next BEGIN by ascending LSN is wrong once transactions
	 * overlap: a transaction's own BEGIN LSN reflects only when that
	 * session issued BEGIN, which has no reliable relationship to commit
	 * order under concurrency.  A slow transaction that began early but
	 * commits late can have a lower BEGIN LSN than many fast, fully
	 * concurrent transactions that both began and committed inside its
	 * range.  Once that slow transaction is finally applied, transform_lsn
	 * jumps past every one of those transactions' LSN ranges, and
	 * "lsn >= $1" then permanently hides them -- silent data loss, not a
	 * stall: apply keeps going, just over a shrinking set of transactions.
	 *
	 * `id` is autoincrement insertion order, and receive always writes a
	 * transaction's full BEGIN..COMMIT to this table at the moment its
	 * COMMIT is encountered while scanning WAL sequentially, so id is a
	 * reliable proxy for true commit order -- unlike each transaction's own
	 * BEGIN LSN. COMMIT rows themselves stay consistent in id/lsn order
	 * (commits are always encountered, and written, in strictly increasing
	 * LSN order), so the COMMIT row matching transform_lsn reliably gives
	 * the id boundary of everything already applied.
	 *
	 * That row is the highest-LSN commit at or below transform_lsn, one seek
	 * on o_a_lsn: max(id) would read every commit up to it.
	 */

	/*
	 * Three single-row lookups rather than one UNION: a UNION ALL with an
	 * ORDER BY is sorted through a temporary b-tree even when each branch
	 * comes out of an index in order, and building one means allocating a
	 * pager and its page cache, for every call.  Each lookup below is a
	 * single seek.
	 *
	 * The next BEGIN is the first one after the commit row at transform_lsn
	 * (see above).  The next KEEPALIVE and the next SWITCH are each the
	 * first above their cursor, in LSN order, which is the order they were
	 * inserted in.  The lowest id of the three is what comes next.
	 */
	static const char *beginSQL =
		"select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
		"  from output "
		" where action = 'B' "
		"   and id > coalesce((select id from output "
		"                       where action = 'C' and lsn <= $1 "
		"                    order by lsn desc limit 1), 0) "
		"order by id "
		"limit 1";

	static const char *markerSQL =
		"select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
		"  from output "
		" where action = $1 and lsn > $2 "
		"order by lsn, id "
		"limit 1";

	log_debug("ld_store_lookup_output_after_lsn: %X/%X (keepalive %X/%X)",
			  LSN_FORMAT_ARGS(lsn),
			  LSN_FORMAT_ARGS(keepaliveLSN));

	ReplayDBOutputMessage next = { 0 };

	BindParam beginParams[] = {
		{ BIND_PARAMETER_TYPE_INT64, "lsn", lsn, NULL }
	};

	if (!ld_store_lookup_output_one(catalog, beginSQL, beginParams, 1, &next))
	{
		/* errors have already been logged */
		return false;
	}

	char *markers[] = { "K", "X" };

	for (int i = 0; i < 2; i++)
	{
		ReplayDBOutputMessage marker = { 0 };

		BindParam markerParams[] = {
			{ BIND_PARAMETER_TYPE_TEXT, "action", 0, markers[i] },
			{ BIND_PARAMETER_TYPE_INT64, "keepaliveLSN", keepaliveLSN, NULL }
		};

		if (!ld_store_lookup_output_one(catalog, markerSQL, markerParams, 2,
										&marker))
		{
			/* errors have already been logged */
			return false;
		}

		if (marker.lsn != InvalidXLogRecPtr &&
			(next.lsn == InvalidXLogRecPtr || marker.id < next.id))
		{
			next = marker;
		}
	}

	*output = next;

	return true;
}


/*
 * ld_store_lookup_output_xid_end searches the last message for the given
 * transaction (xid) in the replayDB database.
 */
bool
ld_store_lookup_output_xid_end(DatabaseCatalog *catalog,
							   uint32_t xid,
							   ReplayDBOutputMessage *output)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_lookup_output_xid_end: db is NULL");
		return false;
	}

	/*
	 * Prefer COMMIT (action='C') over ROLLBACK (action='R') when both exist.
	 *
	 * In the mid-transaction endpos scenario, the prefetch inserts an
	 * artificial ROLLBACK so the transform can advance past the partial
	 * transaction boundary.  When pgcopydb follow reconnects and the slot
	 * re-delivers the complete transaction, the real COMMIT is inserted with
	 * a higher id than the stale ROLLBACK.  Ordering by COMMIT first ensures
	 * we find the real COMMIT rather than the stale ROLLBACK.
	 */
	char *sql =
		"  select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
		"    from output "
		"   where xid = $1 and (action = 'C' or action = 'R') "
		"order by case when action = 'C' then 0 else 1 end, id "
		"   limit 1";

	log_debug("ld_store_lookup_output_xid_end: %u", xid);

	/*
	 * errorOnZeroRows = false: the COMMIT for the given xid may not have
	 * been written by the receive subprocess yet (concurrent pipeline).
	 * The caller (ld_store_iter_output_init) checks last.lsn ==
	 * InvalidXLogRecPtr and treats zero rows as "not yet available".
	 */
	SQLiteQuery query = {
		.errorOnZeroRows = false,
		.context = output,
		.fetchFunction = &ld_store_output_fetch
	};

	if (!catalog_sql_prepare_cached(catalog, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* bind our parameters now */
	BindParam params[1] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid", xid },
	};

	if (!catalog_sql_bind(&query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_lookup_output_complete_txn returns the oldest complete transaction
 * in the output table that commits after previousLSN and at or before endpos,
 * with the commit LSN in output->lsn.  output->lsn is InvalidXLogRecPtr when
 * there is none.
 *
 * The join to the COMMIT row is what makes a transaction count as complete: an
 * endpos that falls inside a transaction still leaves nothing to drain here,
 * which is how the apply loop terminates in that case.
 *
 * Only a COMMIT terminates a transaction for this purpose.  A ROLLBACK leaves
 * previousLSN where it was, by design, so counting one as work left to drain
 * would hold the window open on a transaction the apply is never going to
 * move past.  ld_store_lookup_output_xid_end documents the reconnect case
 * where one xid carries both a stale artificial ROLLBACK and the re-delivered
 * real COMMIT; correlating on c.id > b.id keeps the stale row from pairing
 * with the later BEGIN.
 */
bool
ld_store_lookup_output_complete_txn(DatabaseCatalog *catalog,
									uint64_t previousLSN,
									uint64_t endpos,
									ReplayDBOutputMessage *output)
{
	/* the apply closes and re-opens this handle on CDC file rotation */
	if (catalog == NULL || catalog->db == NULL)
	{
		log_error("BUG: ld_store_lookup_output_complete_txn: db is NULL");
		return false;
	}

	sqlite3 *db = catalog->db;

	/*
	 * The BEGIN row carries the transaction, the C/R row carries the position
	 * the apply compares against: report c.lsn as the row lsn.  The message
	 * body is never read here, so select null for it and skip its malloc.
	 */
	char *sql =
		"  select b.id, b.action, b.xid, c.lsn, b.timestamp, null, "
		"         b.nspname, b.relname, b.old_type "
		"    from output b "
		"    join output c on c.xid = b.xid and c.action = 'C' and c.id > b.id "
		"   where b.action = 'B' and c.lsn > $1 and c.lsn <= $2 "
		"order by c.lsn asc "
		"   limit 1";

	log_debug("ld_store_lookup_output_complete_txn: %X/%X .. %X/%X",
			  LSN_FORMAT_ARGS(previousLSN),
			  LSN_FORMAT_ARGS(endpos));

	/* errorOnZeroRows = false: an empty drain is the expected answer */
	SQLiteQuery query = {
		.errorOnZeroRows = false,
		.context = output,
		.fetchFunction = &ld_store_output_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "previousLSN", previousLSN, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "endpos", endpos, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_output_fetch fetches a ReplayDBOutputMessage entry from a SQLite
 * ppStmt result set.
 */
bool
ld_store_output_fetch(SQLiteQuery *query)
{
	ReplayDBOutputMessage *output = (ReplayDBOutputMessage *) query->context;

	/* cleanup the memory area before re-use */
	bzero(output, sizeof(ReplayDBOutputMessage));

	output->id = sqlite3_column_int64(query->ppStmt, 0);

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		char *action = (char *) sqlite3_column_text(query->ppStmt, 1);
		output->action = action[0];
	}
	else
	{
		log_error("Failed to fetch action for output id %lld",
				  (long long) output->id);
		return false;
	}

	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		output->xid = sqlite3_column_int64(query->ppStmt, 2);
	}

	/* lsn could be NULL */
	output->lsn = InvalidXLogRecPtr;

	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		output->lsn = sqlite3_column_int64(query->ppStmt, 3);
	}

	log_debug("ld_store_output_fetch: %lld %c %u %X/%X",
			  (long long) output->id,
			  output->action,
			  output->xid,
			  LSN_FORMAT_ARGS(output->lsn));

	/* timestamp */
	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
	{
		strlcpy(output->timestamp,
				(char *) sqlite3_column_text(query->ppStmt, 4),
				sizeof(output->timestamp));
	}

	/* message */
	if (sqlite3_column_type(query->ppStmt, 5) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 5);
		int bytes = len + 1;

		output->jsonBuffer = (char *) calloc(bytes, sizeof(char));

		if (output->jsonBuffer == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(output->jsonBuffer,
				(char *) sqlite3_column_text(query->ppStmt, 5),
				bytes);
	}

	/* nspname (col 6) — pgoutput only, NULL for other plugins */
	if (sqlite3_column_type(query->ppStmt, 6) != SQLITE_NULL)
	{
		strlcpy(output->nspname,
				(char *) sqlite3_column_text(query->ppStmt, 6),
				sizeof(output->nspname));
	}

	/* relname (col 7) */
	if (sqlite3_column_type(query->ppStmt, 7) != SQLITE_NULL)
	{
		strlcpy(output->relname,
				(char *) sqlite3_column_text(query->ppStmt, 7),
				sizeof(output->relname));
	}

	/* old_type (col 8) */
	if (sqlite3_column_type(query->ppStmt, 8) != SQLITE_NULL)
	{
		const char *ot = (char *) sqlite3_column_text(query->ppStmt, 8);
		output->old_type = ot ? ot[0] : 0;
	}

	return true;
}


/*
 * ld_store_insert_cdc_filename inserts a new entry to the streaming
 * table of replaydb filename with metadata.
 */
bool
ld_store_insert_cdc_filename(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_insert_cdc_filename: db is NULL");
		return false;
	}

	char *sql =
		"insert into cdc_files(filename, timeline, startpos, start_time_epoch)"
		"values($1, $2, $3, $4)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	char lsn[PG_LSN_MAXLENGTH] = { 0 };
	sformat(lsn, sizeof(lsn), "%08X/%08X", LSN_FORMAT_ARGS(privateContext->startpos));

	uint64_t startTime = time(NULL);

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "filename", 0, specs->outputDB->dbfile },
		{ BIND_PARAMETER_TYPE_INT, "timeline", privateContext->timeline, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, lsn },
		{ BIND_PARAMETER_TYPE_INT64, "start_time_epoch", startTime, NULL }
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
 * ld_store_fetch_int64 is a generic SQLiteQuery fetchFunction that reads
 * column 0 as an int64 into a (int64_t *) context.  Used by the count-based
 * queries in this file.
 */
static bool
ld_store_fetch_int64(SQLiteQuery *query)
{
	int64_t *result = (int64_t *) query->context;

	*result = sqlite3_column_int64(query->ppStmt, 0);
	return true;
}


/*
 * ld_store_close_outputdb_cdc marks the current output.db file as done in
 * cdc_files by setting endpos and done_time_epoch.  This is called by
 * ld_store_rotate_outputdb before the receive process opens a new output.db.
 */
bool
ld_store_close_outputdb_cdc(StreamSpecs *specs, uint64_t endpos_lsn)
{
	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_close_outputdb_cdc: db is NULL");
		return false;
	}

	char *sql =
		"update cdc_files "
		"   set endpos = $1, done_time_epoch = $2 "
		" where filename = $3";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	char lsn[PG_LSN_MAXLENGTH] = { 0 };
	sformat(lsn, sizeof(lsn), "%08X/%08X", LSN_FORMAT_ARGS(endpos_lsn));

	uint64_t doneTime = time(NULL);

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "endpos", 0, lsn },
		{ BIND_PARAMETER_TYPE_INT64, "done_time_epoch", doneTime, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "filename", 0, specs->outputDB->dbfile }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("ld_store_close_outputdb_cdc: closed \"%s\" at %X/%X",
			  specs->outputDB->dbfile,
			  LSN_FORMAT_ARGS(endpos_lsn));

	return true;
}


/*
 * ld_store_rotate_outputdb closes the current output.db at commit_lsn, then
 * opens a new output.db file for the receive process to continue writing into.
 *
 * Called from streamWrite() after a COMMIT when the current output.db has
 * reached or exceeded specs->maxReplayDBSize bytes.
 */
bool
ld_store_rotate_outputdb(StreamSpecs *specs, uint64_t commit_lsn)
{
	StreamContext *privateContext = &(specs->private);

	log_info("Rotating outputDB \"%s\" at LSN %X/%X",
			 specs->outputDB->dbfile,
			 LSN_FORMAT_ARGS(commit_lsn));

	/* 1. mark current output.db as done in cdc_files */
	if (!ld_store_close_outputdb_cdc(specs, commit_lsn))
	{
		/* errors have already been logged */
		return false;
	}

	/* 2. close the SQLite handle and release the semaphore */
	if (!catalog_close(specs->outputDB))
	{
		/* errors have already been logged */
		return false;
	}

	/* clear so ld_store_open_outputdb creates a fresh file */
	memset(specs->outputDB->dbfile, 0, sizeof(specs->outputDB->dbfile));
	specs->outputDB->db = NULL;

	/* 3. advance startpos — new file starts at the commit boundary */
	privateContext->startpos = commit_lsn;

	/* 4. open the new output.db (creates file + inserts cdc_files row) */
	if (!ld_store_open_outputdb(specs))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("Rotated to new outputDB \"%s\"", specs->outputDB->dbfile);

	return true;
}


/*
 * ld_store_cleanup_cdc_files removes output.db/replay.db file pairs that
 * have been fully applied to the target.  A pair is safe to delete when its
 * endpos is strictly less than sentinel.replay_lsn AND done_time_epoch is set
 * (i.e. the receive process has closed it).
 *
 * The sentinel.replay_lsn is the LSN through which every transaction has been
 * committed on the target and acknowledged back to the source slot as
 * flush_lsn (confirmed_flush).  The slot will never re-deliver transactions
 * before that point, so the corresponding CDC files are no longer needed for
 * resume/restart.
 *
 * dry_run=true logs what would be deleted without touching the filesystem.
 * On completion, *filesDeleted and *bytesFreed are updated with counts.
 *
 * No VACUUM is issued: source.db contains the full schema catalog and can be
 * tens of megabytes; vacuuming it periodically for the sake of ~150 bytes per
 * deleted cdc_files row is poor trade.  SQLite recycles the freed freelist
 * pages automatically when the receive process inserts new cdc_files rows.
 */
bool
ld_store_cleanup_cdc_files(StreamSpecs *specs,
						   bool dry_run,
						   uint64_t *filesDeleted,
						   uint64_t *bytesFreed)
{
	DatabaseCatalog *sourceDB = specs->sourceDB;
	sqlite3 *db = sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_cleanup_cdc_files: sourceDB is NULL");
		return false;
	}

	/* read the current apply progress */
	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_get(sourceDB, &sentinel))
	{
		log_error("Failed to read sentinel for CDC file cleanup");
		return false;
	}

	if (sentinel.replay_lsn == InvalidXLogRecPtr)
	{
		log_debug("ld_store_cleanup_cdc_files: replay_lsn is not set, skipping");
		return true;
	}

	/* query cdc_files for file pairs whose endpos < replay_lsn */
	char safe_lsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(safe_lsn, sizeof(safe_lsn),
			"%08X/%08X", LSN_FORMAT_ARGS(sentinel.replay_lsn));

	char *sql =
		"select id, filename "
		"  from cdc_files "
		" where done_time_epoch is not null "
		"   and endpos < $1 "
		" order by id";

	typedef struct
	{
		int id;
		char filename[MAXPGPATH];
	} CDCFileRow;

	/* collect rows first so we can delete without cursor iteration issues */
	int capacity = 64;
	int count = 0;
	CDCFileRow *rows = (CDCFileRow *) calloc(capacity, sizeof(CDCFileRow));

	if (rows == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Use a direct SQLite query rather than the catalog iterator framework,
	 * because we need to collect all rows before issuing DELETE statements
	 * on the same table.
	 */
	sqlite3_stmt *ppStmt = NULL;

	if (sqlite3_prepare_v2(db, sql, -1, &ppStmt, NULL) != SQLITE_OK)
	{
		log_error("Failed to prepare CDC file query: %s", sqlite3_errmsg(db));
		free(rows);
		return false;
	}

	sqlite3_bind_text(ppStmt, 1, safe_lsn, -1, SQLITE_STATIC);

	int rc;

	while ((rc = sqlite3_step(ppStmt)) == SQLITE_ROW)
	{
		if (count >= capacity)
		{
			capacity *= 2;
			CDCFileRow *tmp = realloc(rows, capacity * sizeof(CDCFileRow));

			if (tmp == NULL)
			{
				log_fatal(ALLOCATION_FAILED_ERROR);
				sqlite3_finalize(ppStmt);
				free(rows);
				return false;
			}
			rows = tmp;
		}

		rows[count].id = sqlite3_column_int(ppStmt, 0);
		strlcpy(rows[count].filename,
				(const char *) sqlite3_column_text(ppStmt, 1),
				MAXPGPATH);
		++count;
	}

	sqlite3_finalize(ppStmt);

	if (count == 0)
	{
		log_debug("ld_store_cleanup_cdc_files: no files to clean up "
				  "(replay_lsn %X/%X)",
				  LSN_FORMAT_ARGS(sentinel.replay_lsn));
		free(rows);
		return true;
	}

	bool success = true;

	for (int i = 0; i < count; i++)
	{
		char replayFile[MAXPGPATH] = { 0 };

		ld_store_replaydb_filename_from_outputdb(rows[i].filename,
												 replayFile,
												 sizeof(replayFile));

		/* tally sizes before deletion */
		struct stat st = { 0 };
		uint64_t pairBytes = 0;

		if (stat(rows[i].filename, &st) == 0)
		{
			pairBytes += st.st_size;
		}

		struct stat stReplay = { 0 };

		if (stat(replayFile, &stReplay) == 0)
		{
			pairBytes += stReplay.st_size;
		}

		if (dry_run)
		{
			char pairPretty[BUFSIZE] = { 0 };

			pretty_print_bytes(pairPretty, sizeof(pairPretty), pairBytes);
			log_info("Would remove CDC file pair: %s + %s (%s)",
					 rows[i].filename,
					 replayFile,
					 pairPretty);
			*bytesFreed += pairBytes;
			++(*filesDeleted);
			continue;
		}

		/* remove output.db */
		if (unlink_file(rows[i].filename))
		{
			log_info("Removed CDC output file: %s", rows[i].filename);
		}
		else
		{
			/* file missing on disk is not fatal — catalog entry still needs removal */
			log_warn("CDC output file not found on disk, removing catalog entry: %s",
					 rows[i].filename);
		}

		/* remove replay.db */
		if (unlink_file(replayFile))
		{
			log_info("Removed CDC replay file: %s", replayFile);
		}
		else
		{
			log_warn("CDC replay file not found on disk, removing catalog entry: %s",
					 replayFile);
		}

		/* remove the catalog entry */
		char *del = "delete from cdc_files where id = $1";
		sqlite3_stmt *delStmt = NULL;

		if (sqlite3_prepare_v2(db, del, -1, &delStmt, NULL) != SQLITE_OK)
		{
			log_error("Failed to prepare cdc_files delete: %s",
					  sqlite3_errmsg(db));
			success = false;
			continue;
		}

		sqlite3_bind_int(delStmt, 1, rows[i].id);

		if (sqlite3_step(delStmt) != SQLITE_DONE)
		{
			log_error("Failed to delete cdc_files row %d: %s",
					  rows[i].id, sqlite3_errmsg(db));
			success = false;
		}

		sqlite3_finalize(delStmt);

		*bytesFreed += pairBytes;
		++(*filesDeleted);
	}

	free(rows);

	if (!dry_run && *filesDeleted > 0)
	{
		char freedPretty[BUFSIZE] = { 0 };

		pretty_print_bytes(freedPretty, sizeof(freedPretty), *bytesFreed);
		log_info("CDC cleanup: removed %llu file pair(s), freed %s "
				 "(replay_lsn %X/%X)",
				 (unsigned long long) *filesDeleted,
				 freedPretty,
				 LSN_FORMAT_ARGS(sentinel.replay_lsn));
	}

	return success;
}


/*
 * ld_store_advance_cdc_files is called by the apply process when
 * stream_transform_from_outputdb() returns no new rows and no progress was
 * made.  It checks whether the current output.db has been closed by the
 * receive process (done_time_epoch IS NOT NULL) and, if so, opens the next
 * output.db / replay.db file pair.
 *
 * Sets *advanced to true when a new file pair was successfully opened;
 * false when the current pair is still the latest (caller should wait).
 */
bool
ld_store_advance_cdc_files(StreamSpecs *specs, bool *advanced)
{
	*advanced = false;

	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_advance_cdc_files: db is NULL");
		return false;
	}

	if (specs->outputDB == NULL || IS_EMPTY_STRING_BUFFER(specs->outputDB->dbfile))
	{
		log_error("BUG: ld_store_advance_cdc_files: outputDB has no filename");
		return false;
	}

	/*
	 * Step 1: check whether the current output.db is closed (done_time_epoch
	 * IS NOT NULL in cdc_files).  We count matching rows with a non-null
	 * done_time_epoch; a count of 1 means the file is closed.
	 */
	char *closedSQL =
		"select count(*) "
		"  from cdc_files "
		" where filename = $1 "
		"   and done_time_epoch is not null";

	int64_t closedCount = 0;

	SQLiteQuery closedQuery = {
		.context = &closedCount,
		.fetchFunction = &ld_store_fetch_int64
	};

	if (!catalog_sql_prepare(db, closedSQL, &closedQuery))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam closedParams[1] = {
		{ BIND_PARAMETER_TYPE_TEXT, "filename", 0, specs->outputDB->dbfile }
	};

	if (!catalog_sql_bind(&closedQuery, closedParams, 1))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_execute_once(&closedQuery))
	{
		/* errors have already been logged */
		return false;
	}

	if (closedCount == 0)
	{
		/* current file is still open — nothing to advance to */
		return true;
	}

	/*
	 * Step 2: find the next file (id > current), open or closed.
	 *
	 * In the prefetch+catchup workflow receive may have already rotated
	 * through several files before catchup starts; those intermediate files
	 * have done_time_epoch IS NOT NULL.  We must visit every file in id
	 * order regardless of its close status, so we do NOT filter on
	 * done_time_epoch here.
	 */
	char *nextSQL =
		"  select c2.filename "
		"    from cdc_files c2 "
		"   where c2.id > (select c1.id from cdc_files c1 "
		"                   where c1.filename = $1) "
		"order by c2.id asc "
		"   limit 1";

	char nextFile[MAXPGPATH] = { 0 };

	SQLiteQuery nextQuery = {
		.context = nextFile,
		.fetchFunction = &ld_store_cdc_filename_fetch
	};

	if (!catalog_sql_prepare(db, nextSQL, &nextQuery))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam nextParams[1] = {
		{ BIND_PARAMETER_TYPE_TEXT, "filename", 0, specs->outputDB->dbfile }
	};

	if (!catalog_sql_bind(&nextQuery, nextParams, 1))
	{
		/* errors have already been logged */
		return false;
	}

	if (!catalog_sql_execute_once(&nextQuery))
	{
		/* errors have already been logged */
		return false;
	}

	if (IS_EMPTY_STRING_BUFFER(nextFile))
	{
		/* next file not yet created — caller should wait */
		log_debug("ld_store_advance_cdc_files: current file closed, "
				  "waiting for receive to create next file");
		return true;
	}

	/*
	 * Step 3: close old outputDB and replayDB handles, open the new pair.
	 */
	log_info("Apply advancing from outputDB \"%s\" to \"%s\"",
			 specs->outputDB->dbfile, nextFile);

	(void) catalog_close(specs->outputDB);

	if (specs->replayDB != NULL && specs->replayDB->db != NULL)
	{
		(void) catalog_close(specs->replayDB);
	}

	/* open new outputDB */
	strlcpy(specs->outputDB->dbfile, nextFile, sizeof(specs->outputDB->dbfile));
	specs->outputDB->db = NULL;
	specs->outputDB->type = DATABASE_CATALOG_TYPE_OUTPUT;

	if (!catalog_init(specs->outputDB))
	{
		log_error("Failed to open new outputDB \"%s\"", nextFile);
		return false;
	}

	/* derive and open the matching replayDB */
	if (specs->replayDB != NULL)
	{
		ld_store_replaydb_filename_from_outputdb(specs->outputDB->dbfile,
												 specs->replayDB->dbfile,
												 sizeof(specs->replayDB->dbfile));

		specs->replayDB->db = NULL;
		specs->replayDB->type = DATABASE_CATALOG_TYPE_REPLAY;

		if (!catalog_init(specs->replayDB))
		{
			log_error("Failed to open new replayDB \"%s\"",
					  specs->replayDB->dbfile);
			return false;
		}

		log_info("Apply advanced to outputDB \"%s\" / replayDB \"%s\"",
				 specs->outputDB->dbfile, specs->replayDB->dbfile);
	}

	*advanced = true;

	return true;
}


/*
 * ld_store_insert_timeline_history inserts a timeline history entry to our
 * SQLite catalogs.
 */
bool
ld_store_insert_timeline_history(DatabaseCatalog *catalog,
								 uint32_t tli,
								 uint64_t startpos,
								 uint64_t endpos)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_insert_timeline_history: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into timeline_history(tli, startpos, endpos)"
		"values($1, $2, $3)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	char slsn[PG_LSN_MAXLENGTH] = { 0 };
	char elsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(slsn, sizeof(slsn), "%08X/%08X", LSN_FORMAT_ARGS(startpos));
	sformat(elsn, sizeof(elsn), "%08X/%08X", LSN_FORMAT_ARGS(endpos));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT, "tli", tli, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, slsn },
		{ BIND_PARAMETER_TYPE_TEXT, "endpos", 0, elsn }
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
 * ld_store_insert_message inserts a logical decoding output plugin message
 * into our SQLite file format.
 */
bool
ld_store_insert_message(DatabaseCatalog *catalog,
						LogicalMessageMetadata *metadata)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_insert_message: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into output(action, xid, lsn, timestamp, message)"
		"values($1, $2, $3, $4, $5) ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare_cached(catalog, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/*
	 * Store xid as NULL only for messages that genuinely have no transaction
	 * context (KEEPALIVE, SWITCH, ENDPOS).  After pgcopydb normalisation,
	 * test_decoding DML rows carry the current transaction XID (propagated
	 * from BEGIN by parseTestDecodingMessageActionAndXid), so xid > 0 for
	 * all transactional actions (BEGIN, DML, COMMIT).
	 */
	BindParameterType xidParamType =
		metadata->xid == 0 ? BIND_PARAMETER_TYPE_NULL : BIND_PARAMETER_TYPE_INT64;

	char action[2] = { metadata->action, '\0' };

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ xidParamType, "xid", metadata->xid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "lsn", metadata->lsn, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "timestamp", 0, metadata->timestamp },
		{ BIND_PARAMETER_TYPE_TEXT, "message", 0, metadata->jsonBuffer }
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
 * ld_store_insert_pgoutput_message stores a decoded pgoutput message as one
 * row of `output`, with nspname, relname and old_type, and the columns of a
 * row change in the message column (see pgoutputEncodeColumns).
 *
 * The pgoutput_col table is no longer written: an output.db created by an
 * earlier version has its columns there, and is still read from it.
 */
bool
ld_store_insert_pgoutput_message(DatabaseCatalog *catalog,
								 LogicalMessageMetadata *metadata,
								 PgoutputMessage *pgmsg)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_insert_pgoutput_message: db is NULL");
		return false;
	}

	if (!semaphore_lock(&(catalog->sema)))
	{
		return false;
	}

	/* --- INSERT into output --- */
	char *output_sql =
		"insert or replace into output"
		"  (action, xid, lsn, timestamp, message, nspname, relname, old_type)"
		"  values($1, $2, $3, $4, $5, $6, $7, $8)";

	SQLiteQuery oq = { 0 };
	if (!catalog_sql_prepare_cached(catalog, output_sql, &oq))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/*
	 * The columns of a row change go into the message column, where a MESSAGE
	 * carries its envelope: one row per message, see pgoutputEncodeColumns.
	 */
	char *message = pgmsg->message;

	if (message == NULL && (pgmsg->old_cols != NULL || pgmsg->new_cols != NULL))
	{
		message = pgoutputEncodeColumns(pgmsg);

		if (message == NULL)
		{
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}
	}

	BindParameterType xidType =
		metadata->xid == 0 ? BIND_PARAMETER_TYPE_NULL : BIND_PARAMETER_TYPE_INT64;

	char action[2] = { metadata->action, '\0' };
	char old_type_str[2] = { pgmsg->oldType, '\0' };

	BindParam oparams[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ xidType, "xid", metadata->xid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "lsn", metadata->lsn, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "timestamp", 0, metadata->timestamp },
		{
			message != NULL ? BIND_PARAMETER_TYPE_TEXT : BIND_PARAMETER_TYPE_NULL,
			"message", 0, message
		},
		{ BIND_PARAMETER_TYPE_TEXT, "nspname", 0, pgmsg->nspname },
		{ BIND_PARAMETER_TYPE_TEXT, "relname", 0, pgmsg->relname },
		{
			pgmsg->oldType != 0 ? BIND_PARAMETER_TYPE_TEXT : BIND_PARAMETER_TYPE_NULL,
			"old_type", 0, pgmsg->oldType != 0 ? old_type_str : NULL
		}
	};

	if (!catalog_sql_bind(&oq, oparams, lengthof(oparams)))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	if (!catalog_sql_execute_once(&oq))
	{
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	(void) semaphore_unlock(&(catalog->sema));
	return true;
}


/*
 * ld_store_insert_message inserts a logical decoding output plugin message
 * into our SQLite file format.
 */
bool
ld_store_insert_internal_message(DatabaseCatalog *catalog,
								 InternalMessage *message)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_insert_internal_message: db is NULL");
		return false;
	}

	char *sql =
		"insert or replace into output(action, xid, lsn, timestamp)"
		"values($1, $2, $3, $4)";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare_cached(catalog, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* not all internal messages have a time entry */
	BindParameterType timeParamType =
		message->time > 0 ? BIND_PARAMETER_TYPE_TEXT : BIND_PARAMETER_TYPE_NULL;

	if (message->time > 0)
	{
		/* add the server sendTime to the LogicalMessageMetadata */
		if (!pgsql_timestamptz_to_string(message->time,
										 message->timeStr,
										 sizeof(message->timeStr)))
		{
			log_error("Failed to format server send time %lld to time string",
					  (long long) message->time);
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}
	}

	char action[2] = { message->action, '\0' };

	BindParameterType xidParamType =
		message->xid > 0 ? BIND_PARAMETER_TYPE_INT64 : BIND_PARAMETER_TYPE_NULL;

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ xidParamType, "xid", message->xid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "lsn", message->lsn, NULL },
		{ timeParamType, "timestamp", 0, message->timeStr }
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
 * ld_store_delete_output_xid removes all output rows for the given xid.
 *
 * Called when streaming stops mid-transaction (endpos reached or connection
 * retry).  Removing the partial rows lets the reconnect re-deliver the full
 * transaction cleanly without leaving a phantom ROLLBACK row in the table.
 *
 * We do NOT insert a synthetic ROLLBACK instead, because PostgreSQL logical
 * decoding never emits a ROLLBACK message.  The unique index on (action, lsn)
 * means a synthetic ROLLBACK row (action='R') would never be overwritten by
 * the re-delivered COMMIT (action='C') on reconnect.
 */
bool
ld_store_delete_output_xid(DatabaseCatalog *catalog, uint32_t xid)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_delete_output_xid: db is NULL");
		return false;
	}

	char *sql = "DELETE FROM output WHERE xid = $1";

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

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid", xid, NULL },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	log_debug("ld_store_delete_output_xid: deleted partial rows for xid %u", xid);

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * ld_store_insert_replay_stmt inserts a replay statement in the stmt and
 * replay tables of the replayDB.
 */
bool
ld_store_insert_replay_stmt(DatabaseCatalog *catalog,
							ReplayDBStmt *replayStmt)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_insert_internal_message: db is NULL");
		return false;
	}

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	/* compute the hash as a string, needed in both stmt and replay tables */
	char hash[9] = { 0 };
	sformat(hash, sizeof(hash), "%x", replayStmt->hash);

	if (replayStmt->stmt != NULL)
	{
		char *sql = "insert or ignore into stmt(hash, sql) values($1, $2)";

		SQLiteQuery query = { 0 };

		if (!catalog_sql_prepare_cached(catalog, sql, &query))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}

		/* bind our parameters now */
		BindParam params[] = {
			{ BIND_PARAMETER_TYPE_TEXT, "hash", 0, hash },
			{ BIND_PARAMETER_TYPE_TEXT, "sql", 0, replayStmt->stmt }
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
	}

	char *sql =
		"insert into replay"
		"(action, xid, lsn, endlsn, timestamp, nspname, relname, stmt_hash, stmt_args)"
		"values($1, $2, $3, $4, $5, $6, $7, $8, $9)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare_cached(catalog, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	char action[2] = { replayStmt->action, '\0' };

	/* not all messages have an xid */
	BindParameterType xidParamType =
		replayStmt->xid > 0 ?
		BIND_PARAMETER_TYPE_INT64 :
		BIND_PARAMETER_TYPE_NULL;

	/* not all messages have an lsn */
	BindParameterType lsnParamType =
		replayStmt->lsn == InvalidXLogRecPtr ?
		BIND_PARAMETER_TYPE_NULL :
		BIND_PARAMETER_TYPE_INT64;

	/* not all messages have an end lsn */
	BindParameterType endlsnParamType =
		replayStmt->endlsn == InvalidXLogRecPtr ?
		BIND_PARAMETER_TYPE_NULL :
		BIND_PARAMETER_TYPE_INT64;

	/* not all messages have a time entry */
	char *timestr = NULL;
	BindParameterType timeParamType = BIND_PARAMETER_TYPE_NULL;

	if (!IS_EMPTY_STRING_BUFFER(replayStmt->timestamp))
	{
		timeParamType = BIND_PARAMETER_TYPE_TEXT;
		timestr = replayStmt->timestamp;
	}

	/* not all messages have a statement (hash, data) */
	BindParameterType hashParamType =
		replayStmt->hash > 0 ?
		BIND_PARAMETER_TYPE_TEXT :
		BIND_PARAMETER_TYPE_NULL;

	BindParameterType dataParamType =
		replayStmt->data != NULL ?
		BIND_PARAMETER_TYPE_TEXT :
		BIND_PARAMETER_TYPE_NULL;

	BindParameterType nspnameParamType =
		IS_EMPTY_STRING_BUFFER(replayStmt->nspname) ?
		BIND_PARAMETER_TYPE_NULL :
		BIND_PARAMETER_TYPE_TEXT;

	BindParameterType relnameParamType =
		IS_EMPTY_STRING_BUFFER(replayStmt->relname) ?
		BIND_PARAMETER_TYPE_NULL :
		BIND_PARAMETER_TYPE_TEXT;

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ xidParamType, "xid", replayStmt->xid, NULL },
		{ lsnParamType, "lsn", replayStmt->lsn, NULL },
		{ endlsnParamType, "endlsn", replayStmt->endlsn, NULL },
		{ timeParamType, "timestamp", 0, timestr },
		{ nspnameParamType, "nspname", 0, replayStmt->nspname },
		{ relnameParamType, "relname", 0, replayStmt->relname },
		{ hashParamType, "stmt_hash", 0, hash },
		{ dataParamType, "stmt_args", 0, replayStmt->data },
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
 * ld_store_iter_output iterates over the output table of the replayDB.
 *
 * It reads without the catalog semaphore, which serializes writers: the file
 * is in WAL mode, so a reader does not wait for receive and receive does not
 * wait for it.  What the reads see together is up to the caller: a read
 * transaction around several calls gives them one snapshot (see
 * stream_transform_from_outputdb).
 */
bool
ld_store_iter_output(StreamSpecs *specs, ReplayDBOutputIterFun *callback)
{
	ReplayDBOutputIterator *iter =
		(ReplayDBOutputIterator *) calloc(1, sizeof(ReplayDBOutputIterator));


	if (iter == NULL)
	{
		log_error("ld_store_iter_output: calloc failed for ReplayDBOutputIterator");
		return false;
	}

	iter->catalog = specs->outputDB;
	iter->transform_lsn = specs->sentinel.replay_lsn;  /* apply reads from replay_lsn */
	iter->keepalive_lsn = specs->private.keepaliveLSN;
	iter->endpos = specs->endpos;

	/* a transaction that an earlier pass has started on continues */
	iter->resume_begin_id = specs->private.openBeginId;
	iter->resume_after_id = specs->private.openLastId;

	specs->private.pendingXid = 0;
	specs->private.pendingBeginId = 0;


	if (!ld_store_iter_output_init(iter))
	{
		/* errors have already been logged */
		return false;
	}


	/*
	 * The transaction found complete is not the one the transform has started
	 * on: receive went back to the server for it and replaced it. What the
	 * transform wrote of the earlier copy would be written again with this
	 * one, so it goes, and what apply sent of it to the target with it.
	 */
	if (iter->output != NULL &&
		iter->output->action == STREAM_ACTION_BEGIN &&
		specs->private.openXid != 0 &&
		specs->private.openBeginId != (int64_t) iter->output->id)
	{
		bool dropped = false;

		if (!ld_store_replay_drop_open(specs->replayDB, &dropped))
		{
			/* errors have already been logged */
			return false;
		}

		if (dropped)
		{
			specs->private.openGeneration++;
		}

		specs->private.openXid = 0;
		specs->private.openBeginId = 0;
		specs->private.openLastId = 0;
		specs->private.openReplayBeginId = 0;
	}

	if (iter->output == NULL ||
		iter->output->action == STREAM_ACTION_UNKNOWN)
	{
		if (iter->pending_xid != 0)
		{
			specs->private.pendingXid = iter->pending_xid;
			specs->private.pendingBeginId = iter->pending_begin_id;

			/*
			 * A BEGIN for xid N is in the output table but its COMMIT has
			 * not been written yet.  Decide whether to wait or exit cleanly:
			 *
			 *   Pipe signal   (specs->upstream_done):  receive sent lifecycle
			 *                                          signal — fast path.
			 *   pipeline_state['receive'].run_state:   durable end-of-receive
			 *                                          record — reliable check.
			 *   sentinel.endpos:                       configured stop position.
			 *
			 * If any source confirms receive has finished, endpos fell inside
			 * this transaction.  Advance transform_lsn to the receive run_end_lsn
			 * so the outer loop's exit condition (transform_lsn >= endpos) fires.
			 *
			 * If receive is still running: return with no progress — the outer
			 * loop's select() on pipe_rt sleeps up to 100 ms before retrying.
			 */
			bool receive_done = specs->upstream_done;
			uint64_t end_lsn = specs->upstream_done_lsn;

			/*
			 * Cross-check with the pipeline_state table.  This catches the
			 * race where the pipe was closed by EOF (receive crash) without
			 * the 8-byte lifecycle payload, giving upstream_done_lsn == 0,
			 * and also gives the canonical run_end_lsn when the in-memory
			 * value is zero.
			 */
			PipelineStateEntry recv_state = { 0 };

			if (pipeline_state_get(specs->sourceDB, "receive", &recv_state) &&
				recv_state.process_name[0] != '\0')
			{
				if (strcmp(recv_state.run_state, "done") == 0)
				{
					receive_done = true;
				}

				if (recv_state.run_end_lsn != InvalidXLogRecPtr &&
					(end_lsn == InvalidXLogRecPtr || end_lsn == 0))
				{
					end_lsn = recv_state.run_end_lsn;
				}
			}

			if (receive_done && end_lsn != InvalidXLogRecPtr && end_lsn != 0)
			{
				/*
				 * Do NOT call sentinel_sync_transform(end_lsn) here.
				 *
				 * end_lsn is the receive run_end_lsn, which equals the
				 * user-supplied endpos.  That position is INSIDE an
				 * uncommitted transaction (the BEGIN was seen but COMMIT
				 * has not arrived).  Setting transform_lsn = end_lsn would
				 * advance confirmed_flush_lsn past the transaction's BEGIN,
				 * causing the PostgreSQL slot to skip that XID on the next
				 * run — data loss.
				 *
				 * Instead, set the midTxnEndpos flag on the owning specs.
				 * stream_transform_messages() checks this flag after each
				 * ld_store_iter_output() call and exits cleanly, leaving
				 * transform_lsn at the last committed transaction boundary.
				 */
				specs->private.midTxnEndpos = true;
			}
			else
			{
				log_debug("ld_store_iter_output: xid %u has no COMMIT yet "
						  "and receive is still running — outer loop will wait",
						  iter->pending_xid);
			}
		}

		log_debug("ld_store_iter_output: no rows at LSN %X/%X (upstream_done=%s)",
				  LSN_FORMAT_ARGS(iter->transform_lsn),
				  specs->upstream_done ? "true" : "false");

		return true;
	}

	/* single message, call the callback function and finish */
	if (iter->output->action != STREAM_ACTION_BEGIN)
	{
		bool stop = false;
		ReplayDBOutputMessage *output = iter->output;

		log_debug("ld_store_iter_output: single message %c %s at LSN %X/%X",
				  output->action,
				  StreamActionToString(output->action),
				  LSN_FORMAT_ARGS(output->lsn));

		/* now call the provided callback */
		if (!(*callback)(specs, output, &stop))
		{
			log_error("Failed to iterate over CDC output messages, "
					  "see above for details");
			return false;
		}

		if (!ld_store_iter_output_finish(iter))
		{
			/* errors have already been logged */
			return false;
		}


		return true;
	}
	else
	{
		/* iterate over a transaction */
		for (;;)
		{
			if (!ld_store_iter_output_next(iter))
			{
				/* errors have already been logged */
				return false;
			}

			ReplayDBOutputMessage *output = iter->output;

			if (output == NULL)
			{
				if (!ld_store_iter_output_finish(iter))
				{
					/* errors have already been logged */
					return false;
				}

				break;
			}

			bool stop = false;

			/* now call the provided callback */
			if (!(*callback)(specs, output, &stop))
			{
				log_error("Failed to iterate over CDC output messages, "
						  "see above for details");
				return false;
			}

			if (stop)
			{
				if (!ld_store_iter_output_finish(iter))
				{
					/* errors have already been logged */
					return false;
				}
				break;
			}
		}
	}


	return true;
}


/*
 * ld_store_replay_set_endlsn sets the end LSN of a BEGIN row of the replay
 * table: the transaction can then be applied.
 */
bool
ld_store_replay_set_endlsn(DatabaseCatalog *catalog, int64_t id, uint64_t endlsn)
{
	char *sql = "update replay set endlsn = $1 where id = $2";

	SQLiteQuery query = { 0 };

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "endlsn", endlsn, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "id", id, NULL }
	};

	return catalog_sql_prepare_cached(catalog, sql, &query) &&
		   catalog_sql_bind(&query, params, 2) &&
		   catalog_sql_execute_once(&query);
}


/*
 * ld_store_replay_drop_open removes from the replay table a transaction that
 * the transform wrote part of and never completed (its BEGIN row has no end
 * LSN), and everything after it, which is part of it.
 */
bool
ld_store_replay_drop_open(DatabaseCatalog *catalog, bool *dropped)
{
	char *sql =
		"delete from replay "
		" where id >= (select min(id) from replay "
		"               where action = 'B' and endlsn is null)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare_cached(catalog, sql, &query) ||
		!catalog_sql_execute_once(&query))
	{
		return false;
	}

	*dropped = sqlite3_changes(catalog->db) > 0;

	return true;
}


/*
 * ld_store_fetch_count is a SQLiteQuery callback that stores the first column
 * of the row, a count, in the int64_t that query->context points to.
 */
static bool
ld_store_fetch_count(SQLiteQuery *query)
{
	int64_t *count = (int64_t *) query->context;

	*count = sqlite3_column_int64(query->ppStmt, 0);

	return true;
}


/*
 * ld_store_replay_txn_is_complete sets complete when the replay table has the
 * BEGIN row of that id and transaction, and it has an end LSN: the transform
 * has written the transaction, up to its COMMIT row.
 */
bool
ld_store_replay_txn_is_complete(DatabaseCatalog *catalog,
								uint64_t id,
								uint32_t xid,
								bool *complete)
{
	char *sql =
		"select count(*) from replay "
		" where id = $1 and action = 'B' and xid = $2 and endlsn is not null";

	int64_t count = 0;

	SQLiteQuery query = {
		.context = &count,
		.fetchFunction = &ld_store_fetch_count
	};

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "id", id, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "xid", xid, NULL }
	};

	if (!catalog_sql_prepare_cached(catalog, sql, &query) ||
		!catalog_sql_bind(&query, params, 2) ||
		!catalog_sql_execute_once(&query))
	{
		return false;
	}

	*complete = count > 0;

	return true;
}


/*
 * ld_store_iter_output_open calls the callback for the rows of the open
 * transaction (specs->private.openXid) that the output table holds after
 * the last one consumed, in insertion order, and keeps openLastId up to
 * date.  The transaction has no COMMIT row yet: it is what receive has
 * committed of it so far.
 */
bool
ld_store_iter_output_open(StreamSpecs *specs, ReplayDBOutputIterFun *callback)
{
	StreamContext *privateContext = &(specs->private);

	ReplayDBOutputMessage output = { 0 };

	ReplayDBOutputIterator iter = {
		.catalog = specs->outputDB,
		.output = &output
	};

	char *sql =
		"   select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
		"     from output "
		"    where xid = $1 and id > $2 "
		" order by id";

	SQLiteQuery *query = &(iter.query);

	query->context = &output;
	query->fetchFunction = &ld_store_output_fetch;

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid", privateContext->openXid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "afterId", privateContext->openLastId, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_prepare_cached(iter.catalog, sql, query) ||
		!catalog_sql_bind(query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	for (;;)
	{
		if (!ld_store_iter_output_next(&iter))
		{
			/* errors have already been logged */
			(void) ld_store_iter_output_finish(&iter);
			return false;
		}

		if (iter.output == NULL)
		{
			break;
		}

		bool stop = false;

		privateContext->openLastId = output.id;

		if (!(*callback)(specs, &output, &stop))
		{
			log_error("Failed to iterate over CDC output messages, "
					  "see above for details");
			(void) ld_store_iter_output_finish(&iter);
			return false;
		}

		if (stop)
		{
			break;
		}
	}

	return ld_store_iter_output_finish(&iter);
}


/*
 * ld_store_iter_output_init initializes an Iterator over our SQLite replayDB
 * output messages.
 */
bool
ld_store_iter_output_init(ReplayDBOutputIterator *iter)
{
	DatabaseCatalog *catalog = iter->catalog;
	sqlite3 *db = catalog->db;


	if (db == NULL)
	{
		log_error("BUG: Failed to initialize output iterator: db is NULL");
		return false;
	}

	iter->output =
		(ReplayDBOutputMessage *) calloc(1, sizeof(ReplayDBOutputMessage));


	if (iter->output == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	ReplayDBOutputMessage first = { 0 };
	ReplayDBOutputMessage last = { 0 };


	/*
	 * Grab the output row for the given LSN, and then if it's a single message
	 * (KEEPALIVE or SWITCH) return it directly.  If it's a BEGIN, look up the
	 * matching COMMIT so the full transaction can be iterated.
	 *
	 * ENDPOS rows no longer appear in the output table: receive signals
	 * transform via the pipe_rt lifecycle pipe instead.
	 */
	if (!ld_store_lookup_output_after_lsn(catalog,
										  iter->transform_lsn,
										  iter->keepalive_lsn,
										  &first))
	{
		/* errors have already been logged */
		iter->output = NULL;
		return false;
	}


	if (first.lsn == InvalidXLogRecPtr)
	{
		/* no rows available yet */
		iter->output = NULL;
		return true;
	}

	switch (first.action)
	{
		case STREAM_ACTION_KEEPALIVE:
		{
			/* single message, just return it */
			log_debug("ld_store_iter_output_init: single message %c at %X/%X",
					  first.action, LSN_FORMAT_ARGS(first.lsn));
			*(iter->output) = first;
			return true;
		}

		case STREAM_ACTION_BEGIN:
		{
			/* greab the COMMIT or ROLLBACK output entry if there is one */

			if (!ld_store_lookup_output_xid_end(catalog, first.xid, &last))
			{
				/* errors have already been logged */
				iter->output = NULL;
				return false;
			}


			/* the COMMIT/ROLLBACK message is not available yet */
			if (last.lsn == InvalidXLogRecPtr)
			{
				/*
				 * Transaction is incomplete (no COMMIT/ROLLBACK yet).
				 *
				 * Two possible situations:
				 *   1. Receive is still running — the COMMIT will arrive
				 *      soon.  The caller should wait (outer loop select()).
				 *   2. Receive has finished — endpos fell inside this
				 *      transaction; the COMMIT will never arrive.  The
				 *      caller should advance transform_lsn to receive's
				 *      run_end_lsn so the outer loop exits cleanly.
				 *
				 * This function does not have access to specs (pipe fd,
				 * sourceDB) so it cannot make that determination here.
				 * Signal the situation to the caller via pending_xid; the
				 * caller (ld_store_iter_output) will consult the pipe,
				 * the pipeline_state table, and the sentinel to decide.
				 */
				log_debug("ld_store_iter_output_init: "
						  "xid %u (begin %X/%X) has no COMMIT yet — "
						  "signalling caller via pending_xid",
						  first.xid,
						  LSN_FORMAT_ARGS(first.lsn));

				iter->pending_xid = first.xid;
				iter->pending_begin_id = first.id;
				iter->output = NULL;
				return true;
			}

			/* return the first message we iterate over */
			*(iter->output) = first;

			break;
		}

		default:
		{
			/*
			 * A DML message (INSERT/UPDATE/DELETE) appeared as the first
			 * entry after transform_lsn.  This happens when transform_lsn
			 * was advanced past a partial transaction boundary (via the
			 * artificial ROLLBACK in streamCloseFile) and the slot then
			 * re-delivered the complete transaction on reconnect.
			 *
			 * The BEGIN for this XID is at a lower LSN — look it up and
			 * treat it just like the BEGIN case above.
			 */
			if (first.xid == 0)
			{
				log_error("Failed to start iterating over output at LSN %X/%X "
						  "with unexpected action %s (no xid)",
						  LSN_FORMAT_ARGS(iter->transform_lsn),
						  StreamActionToString(first.action));
				iter->output = NULL;
				return false;
			}

			/* check the transaction has a COMMIT (prefer over ROLLBACK) */
			if (!ld_store_lookup_output_xid_end(catalog, first.xid, &last))
			{
				iter->output = NULL;
				return false;
			}

			/*
			 * ld_store_lookup_output_after_lsn returns BEGIN rows and markers
			 * only, and the markers ride their own strict cursor, so a row at
			 * exactly transform_lsn cannot come back as a starting point.
			 */
			if (last.lsn == InvalidXLogRecPtr)
			{
				/*
				 * Incomplete transaction — same situation as the BEGIN case.
				 * Signal the caller via pending_xid; it will consult the
				 * pipe, pipeline_state, and sentinel to decide whether to
				 * wait or advance transform_lsn.
				 */
				log_debug("ld_store_iter_output_init: "
						  "xid %u (DML at %X/%X) has no COMMIT yet — "
						  "signalling caller via pending_xid",
						  first.xid,
						  LSN_FORMAT_ARGS(first.lsn));

				iter->pending_xid = first.xid;
				iter->output = NULL;
				return true;
			}

			/* find the BEGIN for this XID */
			{
				char *begin_sql =
					"  select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
					"    from output "
					"   where xid = $1 and action = 'B' "
					"order by id limit 1";

				ReplayDBOutputMessage begin = { 0 };
				SQLiteQuery bq = {
					.errorOnZeroRows = false,
					.context = &begin,
					.fetchFunction = &ld_store_output_fetch
				};

				if (!catalog_sql_prepare_cached(catalog, begin_sql, &bq))
				{
					iter->output = NULL;
					return false;
				}

				BindParam bp[1] = {
					{ BIND_PARAMETER_TYPE_INT64, "xid", first.xid },
				};

				if (!catalog_sql_bind(&bq, bp, 1) ||
					!catalog_sql_execute_once(&bq))
				{
					iter->output = NULL;
					return false;
				}

				if (begin.lsn == InvalidXLogRecPtr)
				{
					/*
					 * The BEGIN for this transaction is missing from the
					 * output table.  This happens when the onRetry cleanup
					 * path in the prefetch process deleted the BEGIN row
					 * (because confirmed_flush had already advanced past it)
					 * while the DML rows were written on a subsequent
					 * reconnect.  The transaction cannot be properly
					 * transformed without its BEGIN.
					 *
					 * Signal the caller (ld_store_iter_output) to advance
					 * sentinel.transform_lsn to the COMMIT lsn so these
					 * orphaned rows are skipped cleanly.
					 */
					log_error("No BEGIN found for xid %u "
							  "while resuming after transform_lsn %X/%X",
							  first.xid,
							  LSN_FORMAT_ARGS(iter->transform_lsn));
					iter->output = NULL;
					return false;
				}


				*(iter->output) = begin;
			}

			break;
		}
	}

	/*
	 * Select all output rows for this transaction in insertion order.
	 *
	 * We start at id >= iter->output->id (the BEGIN's row-id) to skip any
	 * artificial ROLLBACK rows that were inserted during an earlier partial
	 * run and now have a lower id than the re-delivered BEGIN (because
	 * INSERT OR REPLACE renewed the BEGIN's id).  For a first-time run the
	 * BEGIN has the lowest id so this filter is a no-op.
	 *
	 * The upper bound is the id of the COMMIT (or ROLLBACK) row found above.
	 * Without it, SQLite walks the rowid range from the BEGIN to the end of
	 * the table, filtering on xid, which reads every transaction that
	 * receive has spooled since: the cost of transforming one transaction
	 * then grows with the backlog.  When the end row was written before the
	 * BEGIN (the artificial ROLLBACK case just described), there is no usable
	 * bound and the range stays open.
	 */
	uint64_t endId = last.id > iter->output->id ? last.id : INT64_MAX;

	/*
	 * A transaction the transform has started on (see
	 * stream_transform_open_transaction) continues from the row after the
	 * last one it consumed, when it is the same BEGIN: receive can replace it.
	 */
	uint64_t fromId = iter->output->id;

	if (iter->resume_after_id > 0 &&
		iter->resume_begin_id == iter->output->id)
	{
		fromId = iter->resume_after_id + 1;
	}

	char *sql =
		"   select id, action, xid, lsn, timestamp, message, nspname, relname, old_type "
		"     from output "
		"    where xid = $1 and id between $2 and $3 "
		" order by id";


	SQLiteQuery *query = &(iter->query);

	query->context = iter->output;
	query->fetchFunction = &ld_store_output_fetch;

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid", first.xid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "id", fromId, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "endId", endId, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);


	if (!catalog_sql_prepare_cached(catalog, sql, query))
	{
		/* errors have already been logged */
		return false;
	}


	/* re-use params, hard code the count */
	if (!catalog_sql_bind(query, params, count))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("ld_store_iter_output_init: iterating xid=%u (begin id=%lld)",
			  first.xid,
			  (long long) first.id);

	return true;
}


/*
 * ld_store_iter_output_next fetches the next ReplayDBOutputMessage
 */
bool
ld_store_iter_output_next(ReplayDBOutputIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->output = NULL;

		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through statement: %s", query->sql);

		int offset = sqlite3_error_offset(query->db);

		if (offset != -1)
		{
			/* "Failed to step through statement: %s" is 34 chars of prefix */
			log_error("%34s%*s^", " ", offset, " ");
		}

		int errcode = sqlite3_extended_errcode(query->db);

		log_error("[SQLite] %s: %s",
				  sqlite3_errmsg(query->db),
				  sqlite3_errstr(errcode));

		return false;
	}

	return ld_store_output_fetch(query);
}


/*
 * ld_store_iter_output_finish cleans-up the SQLite query used in the iterator.
 */
bool
ld_store_iter_output_finish(ReplayDBOutputIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	iter->output = NULL;

	return true;
}


/*
 * ld_store_replay_fetch fetches one row from the replay+stmt join query into
 * the ReplayDBStmt pointed to by query->context.
 */
bool
ld_store_replay_fetch(SQLiteQuery *query)
{
	ReplayDBStmt *s = (ReplayDBStmt *) query->context;

	bzero(s, sizeof(ReplayDBStmt));

	s->id = sqlite3_column_int64(query->ppStmt, 0);

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		char *action = (char *) sqlite3_column_text(query->ppStmt, 1);
		s->action = action[0];
	}

	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		s->xid = sqlite3_column_int64(query->ppStmt, 2);
	}

	s->lsn = InvalidXLogRecPtr;
	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		s->lsn = sqlite3_column_int64(query->ppStmt, 3);
	}

	s->endlsn = InvalidXLogRecPtr;
	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
	{
		s->endlsn = sqlite3_column_int64(query->ppStmt, 4);
	}

	if (sqlite3_column_type(query->ppStmt, 5) != SQLITE_NULL)
	{
		strlcpy(s->timestamp,
				(char *) sqlite3_column_text(query->ppStmt, 5),
				sizeof(s->timestamp));
	}

	/* stmt.sql — the prepared statement template */
	if (sqlite3_column_type(query->ppStmt, 6) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 6);

		s->stmt = (char *) calloc(len + 1, sizeof(char));
		if (s->stmt == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}
		strlcpy(s->stmt,
				(char *) sqlite3_column_text(query->ppStmt, 6),
				len + 1);
	}

	/* replay.stmt_args — JSON-encoded parameter array */
	if (sqlite3_column_type(query->ppStmt, 7) != SQLITE_NULL)
	{
		int len = sqlite3_column_bytes(query->ppStmt, 7);

		s->data = (char *) calloc(len + 1, sizeof(char));
		if (s->data == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}
		strlcpy(s->data,
				(char *) sqlite3_column_text(query->ppStmt, 7),
				len + 1);
	}

	/* r.stmt_hash — stored as 8-char hex text, e.g. "a14fc954" */
	if (sqlite3_column_type(query->ppStmt, 8) != SQLITE_NULL)
	{
		const char *hashStr = (char *) sqlite3_column_text(query->ppStmt, 8);
		s->hash = (uint32_t) strtoul(hashStr, NULL, 16);
	}

	/* r.nspname — quoted schema name, DML rows only */
	if (sqlite3_column_type(query->ppStmt, 9) != SQLITE_NULL)
	{
		strlcpy(s->nspname,
				(char *) sqlite3_column_text(query->ppStmt, 9),
				sizeof(s->nspname));
	}

	/* r.relname — quoted relation name, DML rows only */
	if (sqlite3_column_type(query->ppStmt, 10) != SQLITE_NULL)
	{
		strlcpy(s->relname,
				(char *) sqlite3_column_text(query->ppStmt, 10),
				sizeof(s->relname));
	}

	log_debug("ld_store_replay_fetch: %lld %c xid=%u lsn=%X/%X endlsn=%X/%X",
			  (long long) s->id, s->action, s->xid,
			  LSN_FORMAT_ARGS(s->lsn), LSN_FORMAT_ARGS(s->endlsn));

	return true;
}


/*
 * ld_store_iter_replay_init initialises an iterator over the replay table of
 * the given replayDB, starting after previousLSN.  The replay table is joined
 * with stmt so that every row already carries the SQL template.
 *
 * Rows are returned in insertion order (by replay.id) which is the same as
 * the original WAL ordering produced by the transform step.
 */
bool
ld_store_iter_replay_init(ReplayDBReplayIterator *iter)
{
	DatabaseCatalog *catalog = iter->catalog;
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_iter_replay_init: db is NULL");
		return false;
	}

	iter->current =
		(ReplayDBStmt *) calloc(1, sizeof(ReplayDBStmt));

	if (iter->current == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	/*
	 * Select all replay rows that still need to be applied, joined with the
	 * stmt table to include the SQL template.
	 *
	 * We use a LEFT JOIN so that internal rows without a stmt (KEEPALIVE,
	 * SWITCH, ENDPOS) still appear.
	 *
	 * Use >= so that rows whose lsn equals previousLSN are included.
	 * This handles the case where the first DML transaction's BEGIN/INSERT
	 * share an LSN with the replication origin's starting position
	 * (e.g. when the slot was created immediately before the DML).
	 *
	 * Idempotency is preserved: when a COMMIT appears at lsn == previousLSN
	 * on a restart, stream_apply_sql skips it via the reachedStartPos check
	 * (which requires previousLSN < metadata->txnCommitLSN, so equal lsn
	 * means skip).
	 */
	char *sql =
		"   select r.id, r.action, r.xid, r.lsn, r.endlsn, r.timestamp, "
		"          s.sql, r.stmt_args, r.stmt_hash, r.nspname, r.relname "
		"     from replay r "
		"left join stmt s on r.stmt_hash = s.hash "
		"    where r.lsn >= $1 or r.lsn is null "
		" order by r.id";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->current;
	query->fetchFunction = &ld_store_replay_fetch;

	if (!catalog_sql_prepare(db, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "previousLSN", iter->previousLSN, NULL }
	};

	if (!catalog_sql_bind(query, params, 1))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("ld_store_iter_replay_init: starting after LSN %X/%X",
			  LSN_FORMAT_ARGS(iter->previousLSN));

	return true;
}


/*
 * ld_store_iter_replay_next fetches the next row from the replay iterator.
 * Sets iter->current to NULL when there are no more rows.
 */
bool
ld_store_iter_replay_next(ReplayDBReplayIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->current = NULL;
		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through replay iterator: %s", query->sql);

		int errcode = sqlite3_extended_errcode(query->db);

		log_error("[SQLite] %s: %s",
				  sqlite3_errmsg(query->db),
				  sqlite3_errstr(errcode));

		return false;
	}

	return ld_store_replay_fetch(query);
}


/*
 * ld_store_iter_replay_finish cleans up the iterator's prepared statement.
 */
bool
ld_store_iter_replay_finish(ReplayDBReplayIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	iter->current = NULL;

	return true;
}


/*
 * ld_store_replay_event_fetch populates a ReplayDBStmt from the first 6
 * columns of the outer-event query (id, action, xid, lsn, endlsn, timestamp).
 * No stmt.sql / stmt_args / stmt_hash columns are present.
 */
bool
ld_store_replay_event_fetch(SQLiteQuery *query)
{
	ReplayDBStmt *s = (ReplayDBStmt *) query->context;

	bzero(s, sizeof(ReplayDBStmt));

	s->id = sqlite3_column_int64(query->ppStmt, 0);

	if (sqlite3_column_type(query->ppStmt, 1) != SQLITE_NULL)
	{
		char *action = (char *) sqlite3_column_text(query->ppStmt, 1);
		s->action = action[0];
	}

	if (sqlite3_column_type(query->ppStmt, 2) != SQLITE_NULL)
	{
		s->xid = sqlite3_column_int64(query->ppStmt, 2);
	}

	s->lsn = InvalidXLogRecPtr;
	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
	{
		s->lsn = sqlite3_column_int64(query->ppStmt, 3);
	}

	s->endlsn = InvalidXLogRecPtr;
	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
	{
		s->endlsn = sqlite3_column_int64(query->ppStmt, 4);
	}

	if (sqlite3_column_type(query->ppStmt, 5) != SQLITE_NULL)
	{
		strlcpy(s->timestamp,
				(char *) sqlite3_column_text(query->ppStmt, 5),
				sizeof(s->timestamp));
	}

	log_debug("ld_store_replay_event_fetch: %lld %c xid=%u lsn=%X/%X endlsn=%X/%X",
			  (long long) s->id, s->action, s->xid,
			  LSN_FORMAT_ARGS(s->lsn), LSN_FORMAT_ARGS(s->endlsn));

	return true;
}


/*
 * ld_store_lookup_replay_one runs a query that returns at most one row of the
 * replay table, with the columns of ld_store_replay_event_fetch, and fetches
 * it into s: left zeroed (its action is STREAM_ACTION_UNKNOWN) when there is
 * none.
 */
static bool
ld_store_lookup_replay_one(DatabaseCatalog *catalog,
						   const char *sql,
						   BindParam *params,
						   ReplayDBStmt *s)
{
	bzero(s, sizeof(ReplayDBStmt));

	SQLiteQuery query = {
		.errorOnZeroRows = false,
		.context = s,
		.fetchFunction = &ld_store_replay_event_fetch
	};

	if (!catalog_sql_prepare_cached(catalog, sql, &query) ||
		!catalog_sql_bind(&query, params, 1) ||
		!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * ld_store_replay_next_event returns the next event to apply after
 * previousLSN into s.  s->action is STREAM_ACTION_UNKNOWN when no rows.
 *
 * For transactions: returns the BEGIN row only when the matching COMMIT
 * already exists in the replay table (endlsn > previousLSN ensures this).
 *
 * For non-transactional events (KEEPALIVE only — ENDPOS rows are no longer
 * written to the replay table): returns the first row at lsn > keepaliveLSN.
 *
 * Apply exits when no more rows AND pipeline_state['transform'].run_end_lsn
 * has been reached (set at stream_apply_catchup startup).
 *
 * The UNION ALL orders by lsn ASC, id ASC so that a non-txn event whose LSN
 * is earlier than any pending BEGIN is returned first.
 */
bool
ld_store_replay_next_event(DatabaseCatalog *catalog,
						   uint64_t previousLSN,
						   uint64_t keepaliveLSN,
						   ReplayDBStmt *s)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_replay_next_event: db is NULL");
		return false;
	}

	/*
	 * Return the next event that is ready to apply.
	 *
	 * For transactions (action='B'): only return the BEGIN row when the
	 * matching COMMIT or ROLLBACK row already exists in the replay table.
	 * The transform writes BEGIN first (with endlsn populated), then DML
	 * rows, then COMMIT, and its pass commits them together; the EXISTS
	 * check keeps a BEGIN from coming back before its COMMIT is there.
	 *
	 * For non-transactional events (KEEPALIVE only — ENDPOS rows are no
	 * longer written to the replay table): return the first KEEPALIVE row
	 * at lsn STRICTLY greater than keepaliveLSN.
	 *
	 * Keepalives get their own cursor because previousLSN also selects the
	 * transactions still to apply.  A keepalive LSN sits above transactions
	 * that commit below it and have not been applied yet, so filtering them
	 * with it would drop them.  The cursor is still strict (>), because with
	 * >= the row apply just consumed comes back and apply spins.
	 *
	 * Transactions come back in commit order, which is the order of their
	 * endlsn: the apply process moves previousLSN to the commit it just ran,
	 * and what commits below it is then skipped.  The BEGIN LSN, taken at the
	 * first change, does not follow commit order once transactions overlap.
	 * A KEEPALIVE sorts at its own lsn, and the first of the two goes first.
	 *
	 * Two single-row lookups rather than a UNION: see
	 * ld_store_lookup_output_after_lsn.  Each names its index because the
	 * planner's pick is wrong here: left alone, it walks the replay table in
	 * lsn order, which means reading past every transaction already applied,
	 * on every call.  With the index, ORDER BY and LIMIT make each a seek.
	 */
	static const char *txnSQL =
		"select id, action, xid, lsn, endlsn, timestamp "
		"  from replay indexed by r_a_endlsn "
		" where action = 'B' and endlsn > $1 "
		"   and exists (select 1 from replay c "
		"                where c.xid = replay.xid and c.action in ('C', 'R')) "
		"order by endlsn "
		"limit 1";

	static const char *keepaliveSQL =
		"select id, action, xid, lsn, endlsn, timestamp "
		"  from replay indexed by r_a_lsn "
		" where action = 'K' "          /* KEEPALIVE only — no ENDPOS rows */
		"   and lsn > $1 "              /* strict: avoids re-delivering same row */
		"order by lsn, id "
		"limit 1";

	ReplayDBStmt txn = { 0 };
	ReplayDBStmt keepalive = { 0 };

	BindParam txnParams[] = {
		{ BIND_PARAMETER_TYPE_INT64, "previousLSN", previousLSN, NULL }
	};

	BindParam keepaliveParams[] = {
		{ BIND_PARAMETER_TYPE_INT64, "keepaliveLSN", keepaliveLSN, NULL }
	};

	if (!ld_store_lookup_replay_one(catalog, txnSQL, txnParams, &txn) ||
		!ld_store_lookup_replay_one(catalog, keepaliveSQL, keepaliveParams,
									&keepalive))
	{
		/* errors have already been logged */
		return false;
	}

	/* the next event is the one with the lower position, then the lower id */
	bool haveTxn = txn.action != STREAM_ACTION_UNKNOWN;
	bool haveKeepalive = keepalive.action != STREAM_ACTION_UNKNOWN;

	bool keepaliveFirst =
		haveKeepalive &&
		(!haveTxn ||
		 keepalive.lsn < txn.endlsn ||
		 (keepalive.lsn == txn.endlsn && keepalive.id < txn.id));

	*s = keepaliveFirst ? keepalive : txn;

	return true;
}


/*
 * ld_store_iter_replay_txn_init prepares the inner-transaction iterator.
 * It fetches all rows for the given xid with id >= begin_id, in id order.
 * This covers: the BEGIN row itself, all DML rows, and the COMMIT/ROLLBACK row.
 */
bool
ld_store_iter_replay_txn_init(ReplayDBReplayTxnIterator *iter)
{
	DatabaseCatalog *catalog = iter->catalog;
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_iter_replay_txn_init: db is NULL");
		return false;
	}

	iter->current =
		(ReplayDBStmt *) calloc(1, sizeof(ReplayDBStmt));

	if (iter->current == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	char *sql =
		"   select r.id, r.action, r.xid, r.lsn, r.endlsn, r.timestamp, "
		"          s.sql, r.stmt_args, r.stmt_hash, r.nspname, r.relname "
		"     from replay r "
		"left join stmt s on r.stmt_hash = s.hash "
		"    where r.xid = $1 and r.id >= $2 "
		" order by r.id";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->current;
	query->fetchFunction = &ld_store_replay_fetch;

	if (!catalog_sql_prepare_cached(iter->catalog, sql, query))
	{
		/* errors have already been logged */
		return false;
	}

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid", iter->xid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "begin_id", iter->begin_id, NULL }
	};

	if (!catalog_sql_bind(query, params, 2))
	{
		/* errors have already been logged */
		return false;
	}

	log_debug("ld_store_iter_replay_txn_init: xid %u begin_id %lld",
			  iter->xid, (long long) iter->begin_id);

	return true;
}


/*
 * ld_store_iter_replay_txn_next fetches the next row from the txn iterator.
 * Sets iter->current to NULL when there are no more rows.
 */
bool
ld_store_iter_replay_txn_next(ReplayDBReplayTxnIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	int rc = catalog_sql_step(query);

	if (rc == SQLITE_DONE)
	{
		iter->current = NULL;
		return true;
	}

	if (rc != SQLITE_ROW)
	{
		log_error("Failed to step through txn replay iterator: %s", query->sql);

		int errcode = sqlite3_extended_errcode(query->db);

		log_error("[SQLite] %s: %s",
				  sqlite3_errmsg(query->db),
				  sqlite3_errstr(errcode));

		return false;
	}

	return ld_store_replay_fetch(query);
}


/*
 * ld_store_iter_replay_txn_finish cleans up the txn iterator's statement.
 */
bool
ld_store_iter_replay_txn_finish(ReplayDBReplayTxnIterator *iter)
{
	SQLiteQuery *query = &(iter->query);

	if (!catalog_sql_finalize(query))
	{
		/* errors have already been logged */
		return false;
	}

	iter->current = NULL;

	return true;
}
