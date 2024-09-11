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

	if (!ld_store_current_filename(specs))
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
 * ld_store_current_filename queries the sourceDB SQLite catalog for an open
 * file for the current timeline.
 */
bool
ld_store_current_filename(StreamSpecs *specs)
{
	StreamContext *privateContext = &(specs->private);

	sqlite3 *db = specs->sourceDB->db;

	if (db == NULL)
	{
		log_error("BUG: ld_store_current_filename: db is NULL");
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
		.fetchFunction = &ld_store_current_filename_fetch
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

	sformat(slsn, sizeof(slsn), "%X/%X", LSN_FORMAT_ARGS(startpos));
	sformat(elsn, sizeof(elsn), "%X/%X", LSN_FORMAT_ARGS(endpos));

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
 * ld_store_current_filename_fetch is a SQLiteQuery callback.
 */
bool
ld_store_current_filename_fetch(SQLiteQuery *query)
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

		log_info("ld_store_current_filename_fetch: \"%s\"", filename);
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
	sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(privateContext->startpos));

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

	sformat(slsn, sizeof(slsn), "%X/%X", LSN_FORMAT_ARGS(startpos));
	sformat(elsn, sizeof(elsn), "%X/%X", LSN_FORMAT_ARGS(endpos));

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
		"values($1, $2, $3, $4, $5)";

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
	char lsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(metadata->lsn));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ xidParamType, "xid", metadata->xid, NULL },
		{ BIND_PARAMETER_TYPE_TEXT, "lsn", 0, lsn },
		{ BIND_PARAMETER_TYPE_TEXT, "timestamp", 0, metadata->timestamp },
		{ BIND_PARAMETER_TYPE_TEXT, "message", 0, metadata->jsonBuffer }
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
	char lsn[PG_LSN_MAXLENGTH] = { 0 };

	sformat(lsn, sizeof(lsn), "%X/%X", LSN_FORMAT_ARGS(message->lsn));

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_TEXT, "action", 0, action },
		{ BIND_PARAMETER_TYPE_TEXT, "lsn", 0, lsn },
		{ timeParamType, "timestamp", 0, message->timeStr }
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
