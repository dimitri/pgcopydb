/*
 * src/bin/pgcopydb/ld_store.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "parson.h"

#include "copydb.h"
#include "ld_store.h"
#include "ld_stream.h"
#include "lock_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pg_utils.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"


/*
 * ld_store_open_replaydb opens the current replaydb file if it already exists,
 * or create a new replaydb SQLite file for processing the streaming data.
 */
bool
ld_store_open_replaydb(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);
	DatabaseCatalog *replayDB = specs->replayDB;

	if (!ld_store_set_current_cdc_filename(specs))
	{
		/* errors have already been logged */
		return false;
	}

	bool createReplayDB = IS_EMPTY_STRING_BUFFER(replayDB->dbfile);

	/* if we don't have a replayDB filename yet, it's time to create it */
	if (IS_EMPTY_STRING_BUFFER(replayDB->dbfile))
	{
		if (privateContext->timeline == 0)
		{
			log_error("BUG: ld_store_open_replaydb: timeline is zero");
			return false;
		}

		sformat(replayDB->dbfile, MAXPGPATH, "%s/%08d-%08X-%08X.db",
				specs->paths.dir,
				privateContext->timeline,
				LSN_FORMAT_ARGS(privateContext->startpos));
	}

	log_info("%s CDC file \"%s\"",
			 createReplayDB ? "Creating" : "Opening",
			 replayDB->dbfile);

	/* now open the replaydb */
	if (!catalog_init(replayDB))
	{
		log_error("Failed to open the current replay database \"%s\", "
				  "see above for details",
				  replayDB->dbfile);
		return false;
	}

	if (createReplayDB)
	{
		if (!ld_store_insert_cdc_filename(specs))
		{
			log_error("Failed to register the current replay database \"%s\", "
					  "see above for details",
					  replayDB->dbfile);
			return false;
		}
	}

	return true;
}


/*
 * ld_store_set_current_filename queries the sourceDB SQLite catalog for an
 * open file for the current timeline.
 */
bool
ld_store_set_current_cdc_filename(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

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
		"     and startpos <= $1 "
		"     and (endpos is null or $2 <= endpos) "
		"     and case when $3 > 0 then timeline = $4 end "
		"order by timeline desc "
		"   limit 1";

	SQLiteQuery query = {
		.context = specs->replayDB->dbfile,
		.fetchFunction = &ld_store_cdc_filename_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	uint64_t startpos = privateContext->startpos;
	uint64_t endpos = privateContext->endpos;

	char slsn[PG_LSN_MAXLENGTH] = { 0 };
	char elsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(slsn, sizeof(slsn), "%08X/%08X", LSN_FORMAT_ARGS(startpos));
	sformat(elsn, sizeof(elsn), "%08X/%08X", LSN_FORMAT_ARGS(endpos));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "startpos", 0, slsn },
		{ BIND_PARAMETER_TYPE_TEXT, "endpos", 0, elsn },
		{ BIND_PARAMETER_TYPE_INT, "timeline", privateContext->timeline, 0 },
		{ BIND_PARAMETER_TYPE_INT, "timeline", privateContext->timeline, 0 },
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

		DatabaseCatalog *candidateDB =
			(DatabaseCatalog *) calloc(1, sizeof(DatabaseCatalog));

		if (candidateDB == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(candidateDB->dbfile, candidate, sizeof(candidateDB->dbfile));

		if (!catalog_open(candidateDB))
		{
			/* errors have already been logged */
			free(candidateDB);
			return false;
		}

		/*
		 * Verify the file contains at least one output row at or after the
		 * given LSN.  Use errorOnZeroRows = false: the transform_lsn might
		 * sit between two message LSNs (e.g. when it equals the stream
		 * startpos which precedes the first written message).
		 */
		ReplayDBOutputMessage output = { 0 };

		if (!ld_store_lookup_output_after_lsn(candidateDB, lsn, &output))
		{
			/* errors have already been logged */
			(void) catalog_close(candidateDB);
			free(candidateDB);
			return false;
		}

		if (output.lsn != InvalidXLogRecPtr)
		{
			/* this file contains messages at or after the given LSN */
			specs->replayDB = candidateDB;
			break;
		}

		(void) catalog_close(candidateDB);
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
		"  select id, action, xid, lsn, timestamp, message "
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
								 ReplayDBOutputMessage *output)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_lookup_output_after_lsn: db is NULL");
		return false;
	}

	char *sql =
		"  select id, action, xid, lsn, timestamp, message "
		"    from output "

		/* only a BEGIN action is expected to have the same LSN again */
		"   where lsn >= $1 and action = 'B' "
		" union all "
		"  select id, action, xid, lsn, timestamp, message "
		"    from output "
		"   where lsn > $2 "
		"order by id "
		"   limit 1";

	log_debug("ld_store_lookup_output_after_lsn: %X/%X", LSN_FORMAT_ARGS(lsn));

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "lsn", lsn },
		{ BIND_PARAMETER_TYPE_INT64, "lsn", lsn }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
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

	char *sql =
		"  select id, action, xid, lsn, timestamp, message "
		"    from output "
		"   where xid = $1 and (action = 'C' or action = 'R') "
		"order by id "
		"   limit 1";

	log_debug("ld_store_lookup_output_xid_end: %u", xid);

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
		{ BIND_PARAMETER_TYPE_INT64, "xid", xid },
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
		{ BIND_PARAMETER_TYPE_TEXT, "filename", 0, specs->replayDB->dbfile },
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
		"insert into output(action, xid, lsn, timestamp, message)"
		"values($1, $2, $3, $4, $5) ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		return false;
	}

	/* only BEGIN/COMMIT messages have an xid */
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
		"insert or replace into output(action, lsn, timestamp)"
		"values($1, $2, $3)";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
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
			return false;
		}
	}

	char action[2] = { message->action, '\0' };

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
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

		if (!catalog_sql_prepare(db, sql, &query))
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
		"(action, xid, lsn, endlsn, timestamp, stmt_hash, stmt_args)"
		"values($1, $2, $3, $4, $5, $6, $7)";

	SQLiteQuery query = { 0 };

	if (!catalog_sql_prepare(db, sql, &query))
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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ xidParamType, "xid", replayStmt->xid, NULL },
		{ lsnParamType, "lsn", replayStmt->lsn, NULL },
		{ endlsnParamType, "endlsn", replayStmt->endlsn, NULL },
		{ timeParamType, "timestamp", 0, timestr },
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
 */
bool
ld_store_iter_output(StreamSpecs *specs, ReplayDBOutputIterFun *callback)
{
	ReplayDBOutputIterator *iter =
		(ReplayDBOutputIterator *) calloc(1, sizeof(ReplayDBOutputIterator));

	iter->catalog = specs->replayDB;
	iter->transform_lsn = specs->sentinel.transform_lsn;
	iter->endpos = specs->endpos;

	DatabaseCatalog *catalog = iter->catalog;

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!ld_store_iter_output_init(iter))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	if (iter->output == NULL ||
		iter->output->action == STREAM_ACTION_UNKNOWN)
	{
		/* no rows returned from the init */
		log_debug("ld_store_iter_output: no rows yet at LSN %X/%X",
				  LSN_FORMAT_ARGS(iter->transform_lsn));
		(void) semaphore_unlock(&(catalog->sema));
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
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}

		if (!ld_store_iter_output_finish(iter))
		{
			/* errors have already been logged */
			(void) semaphore_unlock(&(catalog->sema));
			return false;
		}

		(void) semaphore_unlock(&(catalog->sema));

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
				(void) semaphore_unlock(&(catalog->sema));
				return false;
			}

			ReplayDBOutputMessage *output = iter->output;

			if (output == NULL)
			{
				if (!ld_store_iter_output_finish(iter))
				{
					/* errors have already been logged */
					(void) semaphore_unlock(&(catalog->sema));
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
				(void) semaphore_unlock(&(catalog->sema));
				return false;
			}

			if (stop)
			{
				if (!ld_store_iter_output_finish(iter))
				{
					/* errors have already been logged */
					(void) semaphore_unlock(&(catalog->sema));
					return false;
				}
				break;
			}
		}
	}

	(void) semaphore_unlock(&(catalog->sema));

	return true;
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
	 * (action is SWITCH, ENDPOS, or KEEPALIVE) return it. If the message is a
	 * BEGIN message, lookup the associated COMMIT message's lsn (same xid) and
	 * then grab all the messages from that transaction.
	 */
	if (!ld_store_lookup_output_after_lsn(catalog, iter->transform_lsn, &first))
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
		case STREAM_ACTION_SWITCH:
		case STREAM_ACTION_KEEPALIVE:
		case STREAM_ACTION_ENDPOS:
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
				iter->output = NULL;
				return true;
			}

			/* return the first message we iterate over */
			*(iter->output) = first;

			break;
		}

		default:
		{
			log_error("Failed to start iterating over output at LSN %X/%X "
					  "with unexpected action %s",
					  LSN_FORMAT_ARGS(iter->transform_lsn),
					  StreamActionToString(first.action));
			iter->output = NULL;
			return false;
		}
	}

	char *sql =
		"   select id, action, xid, lsn, timestamp, message "
		"     from output "
		"    where xid = $1 "
		" order by id";

	SQLiteQuery *query = &(iter->query);

	query->context = iter->output;
	query->fetchFunction = &ld_store_output_fetch;

	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "xid", first.xid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_prepare(db, sql, query))
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

	log_debug("ld_store_iter_output_init: iterating xid = %u", first.xid);

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
		s->xid = sqlite3_column_int64(query->ppStmt, 2);

	s->lsn = InvalidXLogRecPtr;
	if (sqlite3_column_type(query->ppStmt, 3) != SQLITE_NULL)
		s->lsn = sqlite3_column_int64(query->ppStmt, 3);

	s->endlsn = InvalidXLogRecPtr;
	if (sqlite3_column_type(query->ppStmt, 4) != SQLITE_NULL)
		s->endlsn = sqlite3_column_int64(query->ppStmt, 4);

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
	 * Select all replay rows after previousLSN, joined with the stmt table
	 * to include the SQL template.  Rows with lsn IS NULL (e.g. some DML
	 * rows where lsn tracks the BEGIN) are also included as long as their
	 * id is greater than the last applied row's id.
	 *
	 * We use a LEFT JOIN so that internal rows without a stmt (KEEPALIVE,
	 * SWITCH, ENDPOS) still appear.
	 */
	char *sql =
		"   select r.id, r.action, r.xid, r.lsn, r.endlsn, r.timestamp, "
		"          s.sql, r.stmt_args "
		"     from replay r "
		"left join stmt s on r.stmt_hash = s.hash "
		"    where r.lsn > $1 or r.lsn is null "
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
