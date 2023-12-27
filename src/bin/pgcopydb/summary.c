/*
 * src/bin/pgcopydb/summary.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <unistd.h>

#include "parson.h"

#include "catalog.h"
#include "copydb.h"
#include "env_utils.h"
#include "log.h"
#include "pidfile.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"


static void prepareLineSeparator(char dashes[], int size);

static bool prepare_summary_table_hook(void *context, SourceTable *table);
static bool prepare_summary_table_index_hook(void *ctx, SourceIndex *index);


/*
 * summary_lookup_oid looks-up for a table summary in our catalogs.
 *
 * This is used in the context of pg_dump/pg_restore filtering, which concerns
 * index and constraint oids. See copydb_objectid_has_been_processed_already.
 */
bool
summary_lookup_oid(DatabaseCatalog *catalog, uint32_t oid, bool *done)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_lookup_oid: db is NULL");
		return false;
	}

	char *sql =
		"  select pid, start_time_epoch, done_time_epoch, duration "
		"    from summary "
		"   where indexoid = $1 or conoid = $2 ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	CopyOidSummary s = { 0 };

	SQLiteQuery query = {
		.context = &s,
		.fetchFunction = &summary_oid_done_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "oid", oid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
	if (!catalog_sql_execute_once(&query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	*done = s.pid > 0 && s.doneTime > 0;

	(void) semaphore_unlock(&(catalog->sema));

	return true;
}


/*
 * summary_pid_done_fetch fetches a generic CopyOidSummary from a SQLiteQuery
 * result.
 */
bool
summary_oid_done_fetch(SQLiteQuery *query)
{
	CopyOidSummary *s = (CopyOidSummary *) query->context;

	s->pid = sqlite3_column_int64(query->ppStmt, 0);
	s->startTime = sqlite3_column_int64(query->ppStmt, 1);
	s->doneTime = sqlite3_column_int64(query->ppStmt, 2);
	s->durationMs = sqlite3_column_int64(query->ppStmt, 3);

	return true;
}


/*
 * summary_lookup_table looks-up for a table summary in our catalogs, in case
 * the given table (partition) has already been done in a previous run.
 */
bool
summary_lookup_table(DatabaseCatalog *catalog, CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_lookup_table: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;
	CopyTableSummary *tableSummary = &(tableSpecs->summary);

	tableSummary->table = tableSpecs->sourceTable;

	char *sql =
		"  select pid, start_time_epoch, done_time_epoch, duration, "
		"         bytes, command "
		"    from summary "
		"   where tableoid = $1 and partnum = $2";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = tableSummary,
		.fetchFunction = &summary_table_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "partnum",
		  table->partition.partNumber, NULL },
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
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
 * table_summary_fetch fetches a CopyTableSummary entry from a SQLite ppStmt
 * result set.
 */
bool
summary_table_fetch(SQLiteQuery *query)
{
	CopyTableSummary *tableSummary = (CopyTableSummary *) query->context;

	tableSummary->pid = sqlite3_column_int64(query->ppStmt, 0);
	tableSummary->startTime = sqlite3_column_int64(query->ppStmt, 1);
	tableSummary->doneTime = sqlite3_column_int64(query->ppStmt, 2);
	tableSummary->durationMs = sqlite3_column_int64(query->ppStmt, 3);
	tableSummary->bytesTransmitted = sqlite3_column_int64(query->ppStmt, 4);

	if (sqlite3_column_type(query->ppStmt, 5) == SQLITE_NULL)
	{
		tableSummary->command = NULL;
	}
	else
	{
		int len = sqlite3_column_bytes(query->ppStmt, 5);
		int bytes = len + 1;

		tableSummary->command = (char *) calloc(bytes, sizeof(char));

		if (tableSummary->command == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(tableSummary->command,
				(char *) sqlite3_column_text(query->ppStmt, 5),
				bytes);
	}

	/* no serialization for that internal in-memory only data */
	tableSummary->startTimeInstr = (instr_time) {
		0
	};
	tableSummary->durationInstr = (instr_time) {
		0
	};

	INSTR_TIME_SET_CURRENT(tableSummary->startTimeInstr);

	return true;
}


/*
 * summary_delete_table DELETEs the summary entry for the given table.
 */
bool
summary_delete_table(DatabaseCatalog *catalog, CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_delete_table: db is NULL");
		return false;
	}

	char *sql = "delete from summary where tableoid = $1 and partnumber = $2";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid",
		  tableSpecs->sourceTable->oid, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "partnum",
		  tableSpecs->sourceTable->partition.partNumber, NULL }
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
 * summary_add_table INSERTs a SourceTable summary entry to our internal
 * catalogs database.
 */
bool
summary_add_table(DatabaseCatalog *catalog, CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;
	CopyTableSummary *tableSummary = &(tableSpecs->summary);

	tableSummary->pid = getpid();
	tableSummary->table = tableSpecs->sourceTable;

	if (!table_summary_init(tableSummary))
	{
		log_error("Failed to initialize table summary for pid %d and "
				  "table %s",
				  getpid(),
				  table->qname);
		return false;
	}

	char *sql =
		"insert into summary(pid, tableoid, partnum, start_time_epoch, command)"
		"values($1, $2, $3, $4, $5)";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "pid", tableSummary->pid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "partnum",
		  table->partition.partNumber, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "start_time_epoch",
		  tableSummary->startTime, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "command",
		  0, (char *) tableSummary->command }
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
 * summary_finish_table UPDATEs a SourceTable summary entry to our internal
 * catalogs database.
 */
bool
summary_finish_table(DatabaseCatalog *catalog, CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;
	CopyTableSummary *tableSummary = &(tableSpecs->summary);

	if (!table_summary_finish(tableSummary))
	{
		log_error("Failed to finish summary for table %s", table->qname);
		return false;
	}

	char *sql =
		"update summary set done_time_epoch = $1, duration = $2, bytes = $3 "
		"where pid = $4 and tableoid = $5 and partnum = $6";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "done_time_epoch",
		  tableSummary->doneTime, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "duration",
		  tableSummary->durationMs, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "bytes",
		  tableSummary->bytesTransmitted, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "pid", getpid(), NULL },
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "partnum",
		  table->partition.partNumber, NULL }
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
 * summary_table_all_parts_done sets tableSpecs->allPartsAreDone to true when
 * all the parts have already been done in the summary table of our internal
 * catalogs.
 */
bool
summary_table_count_parts_done(DatabaseCatalog *catalog,
							   CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;

	char *sql =
		"select count(s.oid) "
		" from s_table t "
		"      join s_table_part p on t.oid = p.oid "
		"      left join summary s "
		"             on s.tableoid = p.oid "
		"            and s.partnum = p.partnum "
		"where tableoid = $1 "
		"  and s.pid > 0 and s.done_time_epoch > 0";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = tableSpecs,
		.fetchFunction = &summary_table_fetch_count_parts_done
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL }
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
 * summary_table_fetch_count_parts_done fetches the count of parts already done
 * in our summary from a SQLiteQuere ppStmt result.
 */
bool
summary_table_fetch_count_parts_done(SQLiteQuery *query)
{
	CopyTableDataSpec *tableSpecs = (CopyTableDataSpec *) query->context;

	tableSpecs->countPartsDone = sqlite3_column_int(query->ppStmt, 0);

	return true;
}


/*
 * summary_add_table_parts_done registers the first pid that sees all tables
 * parts aredone, using SQLite insert-or-ignore returning facility to ensure
 * concurrency control.
 */
bool
summary_add_table_parts_done(DatabaseCatalog *catalog,
							 CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table_parts_done: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;

	char *sql =
		"insert or ignore into s_table_parts_done(tableoid, pid) "
		"values($1, $2)";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "pid", getpid(), NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
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
 * summary_lookup_table_parts_done selects the PID that went there first.
 *
 * We could use insert or ignore ... returning ... but that's supported by
 * SQLite since version 3.35.0 (2021-03-12) and debian oldstable (bullseye) is
 * still around with Package: libsqlite3-0 (3.34.1-3).
 */
bool
summary_lookup_table_parts_done(DatabaseCatalog *catalog,
								CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;

	char *sql = "select pid from s_table_parts_done where tableoid = $1 ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = tableSpecs,
		.fetchFunction = &summary_table_parts_done_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL }
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
 * summary_table_parts_done_fetch fetches a row from s_table_parts_done.
 */
bool
summary_table_parts_done_fetch(SQLiteQuery *query)
{
	CopyTableDataSpec *tableSpecs = (CopyTableDataSpec *) query->context;

	tableSpecs->partsDonePid = sqlite3_column_int(query->ppStmt, 0);

	return true;
}


/*
 * summary_lookup_index looks-up for an index summary in our catalogs, in case
 * the given index has already been done in a previous run.
 */
bool
summary_lookup_index(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_lookup_index: db is NULL");
		return false;
	}

	SourceIndex *index = indexSpecs->sourceIndex;
	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	indexSummary->index = indexSpecs->sourceIndex;

	char *sql =
		"  select pid, start_time_epoch, done_time_epoch, duration, command "
		"    from summary "
		"   where indexoid = $1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = indexSummary,
		.fetchFunction = &summary_index_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "indexoid", index->indexOid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
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
 * summary_lookup_constraint looks-up for an constraint summary in our
 * catalogs.
 */
bool
summary_lookup_constraint(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_lookup_constraint: db is NULL");
		return false;
	}

	SourceIndex *index = indexSpecs->sourceIndex;
	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	indexSummary->index = indexSpecs->sourceIndex;

	char *sql =
		"  select pid, start_time_epoch, done_time_epoch, duration, command "
		"    from summary "
		"   where conoid = $1";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = indexSummary,
		.fetchFunction = &summary_index_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "conoid", index->constraintOid, NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
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
 * index_summary_fetch fetches a CopyIndexSummary entry from a SQLite ppStmt
 * result set.
 */
bool
summary_index_fetch(SQLiteQuery *query)
{
	CopyIndexSummary *indexSummary = (CopyIndexSummary *) query->context;

	indexSummary->pid = sqlite3_column_int64(query->ppStmt, 0);
	indexSummary->startTime = sqlite3_column_int64(query->ppStmt, 1);
	indexSummary->doneTime = sqlite3_column_int64(query->ppStmt, 2);
	indexSummary->durationMs = sqlite3_column_int64(query->ppStmt, 3);

	if (sqlite3_column_type(query->ppStmt, 4) == SQLITE_NULL)
	{
		indexSummary->command = NULL;
	}
	else
	{
		int len = sqlite3_column_bytes(query->ppStmt, 4);
		int bytes = len + 1;

		indexSummary->command = (char *) calloc(bytes, sizeof(char));

		if (indexSummary->command == NULL)
		{
			log_fatal(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(indexSummary->command,
				(char *) sqlite3_column_text(query->ppStmt, 4),
				bytes);
	}

	/* no serialization for that internal in-memory only data */
	indexSummary->startTimeInstr = (instr_time) {
		0
	};
	indexSummary->durationInstr = (instr_time) {
		0
	};

	INSTR_TIME_SET_CURRENT(indexSummary->startTimeInstr);

	return true;
}


/*
 * summary_delete_index DELETEs the summary entry for the given index.
 */
bool
summary_delete_index(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_delete_index: db is NULL");
		return false;
	}

	char *sql = "delete from summary where indexoid = $1";

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

	SourceIndex *index = indexSpecs->sourceIndex;

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "indexoid", index->indexOid, NULL }
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
 * summary_add_index INSERTs a SourceIndex summary entry to our internal
 * catalogs database.
 */
bool
summary_add_index(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_index: db is NULL");
		return false;
	}

	SourceIndex *index = indexSpecs->sourceIndex;
	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	indexSummary->pid = getpid();
	indexSummary->index = indexSpecs->sourceIndex;

	if (!index_summary_init(indexSummary))
	{
		log_error("Failed to initialize index summary for pid %d and "
				  "index %s",
				  getpid(),
				  index->indexQname);
		return false;
	}

	char *sql =
		"insert into summary(pid, indexoid, start_time_epoch, command)"
		"values($1, $2, $3, $4)";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "pid", indexSummary->pid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "indexoid", index->indexOid, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "start_time_epoch",
		  indexSummary->startTime, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "command",
		  0, (char *) indexSummary->command }
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
 * summary_finish_index UPDATEs a SourceIndex summary entry to our internal
 * catalogs database.
 */
bool
summary_finish_index(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_index: db is NULL");
		return false;
	}

	SourceIndex *index = indexSpecs->sourceIndex;
	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	if (!index_summary_finish(indexSummary))
	{
		log_error("Failed to finish summary for index %s", index->indexQname);
		return false;
	}

	char *sql =
		"update summary set done_time_epoch = $1, duration = $2 "
		"where pid = $3 and indexoid = $4";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "done_time_epoch",
		  indexSummary->doneTime, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "duration",
		  indexSummary->durationMs, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "pid", getpid(), NULL },
		{ BIND_PARAMETER_TYPE_INT64, "indexoid", index->indexOid, NULL }
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
 * summary_add_constraint INSERTs a SourceIndex summary entry to our internal
 * catalogs database.
 */
bool
summary_add_constraint(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_constraint: db is NULL");
		return false;
	}

	SourceIndex *index = indexSpecs->sourceIndex;
	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	indexSummary->pid = getpid();
	indexSummary->index = indexSpecs->sourceIndex;

	if (!index_summary_init(indexSummary))
	{
		log_error("Failed to initialize constraint summary for pid %d and "
				  "constraint %s",
				  getpid(),
				  index->constraintName);
		return false;
	}

	char *sql =
		"insert or replace into summary(pid, conoid, start_time_epoch, command)"
		"values($1, $2, $3, $4)";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "pid", indexSummary->pid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "conoid", index->constraintOid, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "start_time_epoch",
		  indexSummary->startTime, NULL },

		{ BIND_PARAMETER_TYPE_TEXT, "command",
		  0, (char *) indexSummary->command }
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
 * summary_finish_constraint UPDATEs a SourceIndex summary entry to our internal
 * catalogs database.
 */
bool
summary_finish_constraint(DatabaseCatalog *catalog, CopyIndexSpec *indexSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_finish_constraint: db is NULL");
		return false;
	}

	SourceIndex *index = indexSpecs->sourceIndex;
	CopyIndexSummary *indexSummary = &(indexSpecs->summary);

	if (!index_summary_finish(indexSummary))
	{
		log_error("Failed to finish summary for constraint %s",
				  index->constraintName);
		return false;
	}

	char *sql =
		"update summary set done_time_epoch = $1, duration = $2 "
		"where pid = $3 and conoid = $4";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "done_time_epoch",
		  indexSummary->doneTime, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "duration",
		  indexSummary->durationMs, NULL },

		{ BIND_PARAMETER_TYPE_INT64, "pid", getpid(), NULL },
		{ BIND_PARAMETER_TYPE_INT64, "conoid", index->constraintOid, NULL }
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
 * summary_table_all_indexes_done sets tableSpecs->allIndexesAreDone to true
 * when all the indexes have already been done in the summary table of our
 * internal catalogs.
 */
bool
summary_table_count_indexes_left(DatabaseCatalog *catalog,
								 CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_table_count_indexes_left: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;

	/*
	 * When asked to create an index for a constraint and the index is neither
	 * a UNIQUE nor a PRIMARY KEY index, then we can't use the ALTER TABLE ...
	 * ADD CONSTRAINT ... USING INDEX ... command, because this only works with
	 * UNIQUE and PRIMARY KEY indexes.
	 *
	 * This means that we have to skip creating the index first, and will only
	 * then create it during the constraint phase, as part of the "plain" ALTER
	 * TABLE ... ADD CONSTRAINT ... command.
	 *
	 * So when counting the indexes that are left to be created before we can
	 * install the constraints, we should also skip counting these.
	 */
	char *sql =
		"with idx(indexoid) as"
		" ("
		"  select i.oid as indexoid "
		"    from s_table t join s_index i on i.tableoid = t.oid"
		"   where tableoid = $1 "
		" ), "
		" skipidx(indexoid) as "
		" ("
		"  select i.oid as indexoid "
		"    from s_table t "
		"         join s_index i on i.tableoid = t.oid "
		"         join s_constraint c on c.indexoid = i.oid "
		"   where not i.isprimary and not i.isunique"
		"     and tableoid = $2 "
		" ),"
		" indexlist(indexoid) as"
		" ( "
		"  select indexoid from idx "
		"  except "
		"  select indexoid from skipidx "
		" ) "
		" select count(l.indexoid) "
		"   from indexlist l "
		"  where not exists "
		"        ( "
		"          select 1 "
		"            from summary s "
		"           where s.indexoid = l.indexoid "
		"             and s.pid > 0 and s.done_time_epoch > 0"
		"        ) ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = tableSpecs,
		.fetchFunction = &summary_table_fetch_count_indexes_left
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL }
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
 * summary_table_fetch_count_indexes_left fetches the count of indexes already
 * done in our summary from a SQLiteQuere ppStmt result.
 */
bool
summary_table_fetch_count_indexes_left(SQLiteQuery *query)
{
	CopyTableDataSpec *tableSpecs = (CopyTableDataSpec *) query->context;

	tableSpecs->countIndexesLeft = sqlite3_column_int(query->ppStmt, 0);

	return true;
}


/*
 * summary_add_table_indexes_done registers the first pid that sees all tables
 * indexes are done, using SQLite insert-or-ignore returning facility to ensure
 * concurrency control.
 */
bool
summary_add_table_indexes_done(DatabaseCatalog *catalog,
							   CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table_indexes_done: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;

	char *sql =
		"insert or ignore into s_table_indexes_done(tableoid, pid) "
		"values($1, $2)";

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

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL },
		{ BIND_PARAMETER_TYPE_INT64, "pid", getpid(), NULL }
	};

	int count = sizeof(params) / sizeof(params[0]);

	if (!catalog_sql_bind(&query, params, count))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* now execute the query, which return exactly one row */
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
 * summary_lookup_table_indexes_done selects the PID that went there first.
 *
 * We could use insert or ignore ... returning ... but that's supported by
 * SQLite since version 3.35.0 (2021-03-12) and debian oldstable (bullseye) is
 * still around with Package: libsqlite3-0 (3.34.1-3).
 */
bool
summary_lookup_table_indexes_done(DatabaseCatalog *catalog,
								  CopyTableDataSpec *tableSpecs)
{
	sqlite3 *db = catalog->db;

	if (db == NULL)
	{
		log_error("BUG: summary_add_table: db is NULL");
		return false;
	}

	SourceTable *table = tableSpecs->sourceTable;

	char *sql = "select pid from s_table_indexes_done where tableoid = $1 ";

	if (!semaphore_lock(&(catalog->sema)))
	{
		/* errors have already been logged */
		return false;
	}

	SQLiteQuery query = {
		.context = tableSpecs,
		.fetchFunction = &summary_table_indexes_done_fetch
	};

	if (!catalog_sql_prepare(db, sql, &query))
	{
		/* errors have already been logged */
		(void) semaphore_unlock(&(catalog->sema));
		return false;
	}

	/* bind our parameters now */
	BindParam params[] = {
		{ BIND_PARAMETER_TYPE_INT64, "tableoid", table->oid, NULL }
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
 * summary_table_indexes_done_fetch fetches a row from s_table_indexes_done.
 */
bool
summary_table_indexes_done_fetch(SQLiteQuery *query)
{
	CopyTableDataSpec *tableSpecs = (CopyTableDataSpec *) query->context;

	tableSpecs->indexesDonePid = sqlite3_column_int(query->ppStmt, 0);

	return true;
}


/*
 * prepare_table_summary_as_json prepares the summary information as a JSON
 * object within the given JSON_Object under the given key.
 */
bool
prepare_table_summary_as_json(CopyTableSummary *summary,
							  JSON_Object *jsobj,
							  const char *key)
{
	JSON_Value *jsSummary = json_value_init_object();
	JSON_Object *jsSummaryObj = json_value_get_object(jsSummary);

	json_object_set_number(jsSummaryObj, "pid", (double) summary->pid);

	json_object_set_number(jsSummaryObj,
						   "start-time-epoch",
						   (double) summary->startTime);

	/* pretty print start time */
	time_t secs = summary->startTime;
	struct tm ts = { 0 };
	char startTimeStr[BUFSIZE] = { 0 };

	if (localtime_r(&secs, &ts) == NULL)
	{
		log_error("Failed to convert seconds %lld to local time: %m",
				  (long long) secs);
		return false;
	}

	strftime(startTimeStr, sizeof(startTimeStr), "%Y-%m-%d %H:%M:%S %Z", &ts);

	json_object_set_string(jsSummaryObj,
						   "start-time-string",
						   startTimeStr);

	/* pretty print transmitted bytes */
	char bytesPretty[BUFSIZE] = { 0 };
	pretty_print_bytes(bytesPretty, BUFSIZE, summary->bytesTransmitted);

	/*
	 * XXX We should also include the transmit rate here, but that would require
	 * having the durationMs information in the summary, which we don't have yet.
	 */
	json_object_dotset_number(jsSummaryObj, "network.bytes", summary->bytesTransmitted);
	json_object_dotset_string(jsSummaryObj, "network.bytes-pretty", bytesPretty);

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsSummary);

	return true;
}


/*
 * table_summary_init initializes the time elements of a table summary.
 */
bool
table_summary_init(CopyTableSummary *summary)
{
	summary->startTime = time(NULL);
	summary->doneTime = 0;
	summary->durationMs = 0;
	summary->startTimeInstr = (instr_time) {
		0
	};
	summary->durationInstr = (instr_time) {
		0
	};

	INSTR_TIME_SET_CURRENT(summary->startTimeInstr);

	return true;
}


/*
 * table_summary_finish sets the duration of the summary fields.
 */
bool
table_summary_finish(CopyTableSummary *summary)
{
	summary->doneTime = time(NULL);

	INSTR_TIME_SET_CURRENT(summary->durationInstr);
	INSTR_TIME_SUBTRACT(summary->durationInstr, summary->startTimeInstr);

	summary->durationMs = INSTR_TIME_GET_MILLISEC(summary->durationInstr);

	return true;
}


/*
 * prepare_index_summary_as_json prepares the summary information as a JSON
 * object within the given JSON_Value.
 */
bool
prepare_index_summary_as_json(CopyIndexSummary *summary,
							  JSON_Object *jsobj,
							  const char *key)
{
	JSON_Value *jsSummary = json_value_init_object();
	JSON_Object *jsSummaryObj = json_value_get_object(jsSummary);

	json_object_set_number(jsSummaryObj, "pid", (double) summary->pid);

	json_object_set_number(jsSummaryObj,
						   "start-time-epoch",
						   (double) summary->startTime);

	/* pretty print start time */
	time_t secs = summary->startTime;
	struct tm ts = { 0 };
	char startTimeStr[BUFSIZE] = { 0 };

	if (localtime_r(&secs, &ts) == NULL)
	{
		log_error("Failed to convert seconds %lld to local time: %m",
				  (long long) secs);
		return false;
	}

	strftime(startTimeStr, sizeof(startTimeStr), "%Y-%m-%d %H:%M:%S %Z", &ts);

	json_object_set_string(jsSummaryObj,
						   "start-time-string",
						   startTimeStr);

	/* attach the JSON array to the main JSON object under the provided key */
	json_object_set_value(jsobj, key, jsSummary);

	return true;
}


/*
 * index_summary_init initializes the time elements of an index summary.
 */
bool
index_summary_init(CopyIndexSummary *summary)
{
	summary->startTime = time(NULL);
	summary->doneTime = 0;
	summary->durationMs = 0;
	summary->startTimeInstr = (instr_time) {
		0
	};
	summary->durationInstr = (instr_time) {
		0
	};

	INSTR_TIME_SET_CURRENT(summary->startTimeInstr);

	return true;
}


/*
 * index_summary_finish updates the duration of the summary fields.
 */
bool
index_summary_finish(CopyIndexSummary *summary)
{
	summary->doneTime = time(NULL);

	INSTR_TIME_SET_CURRENT(summary->durationInstr);
	INSTR_TIME_SUBTRACT(summary->durationInstr, summary->startTimeInstr);

	summary->durationMs = INSTR_TIME_GET_MILLISEC(summary->durationInstr);

	return true;
}


/*
 * write_blobs_summary writes the given pre-filled summary to disk.
 */
bool
write_blobs_summary(CopyBlobsSummary *summary, char *filename)
{
	JSON_Value *js = json_value_init_object();
	JSON_Object *jsObj = json_value_get_object(js);

	json_object_set_number(jsObj, "pid", (double) summary->pid);
	json_object_set_number(jsObj, "count", (double) summary->count);
	json_object_set_number(jsObj, "duration", summary->durationMs);
	json_object_set_number(jsObj, "start-time-epoch", summary->startTime);
	json_object_set_number(jsObj, "done-time-epoch", summary->doneTime);

	/* pretty print start time */
	time_t secs = summary->startTime;
	struct tm ts = { 0 };
	char startTimeStr[BUFSIZE] = { 0 };

	if (localtime_r(&secs, &ts) == NULL)
	{
		log_error("Failed to convert seconds %lld to local time: %m",
				  (long long) secs);
		return false;
	}

	strftime(startTimeStr, sizeof(startTimeStr), "%Y-%m-%d %H:%M:%S %Z", &ts);

	json_object_set_string(jsObj, "start-time-string", startTimeStr);

	char *serialized_string = json_serialize_to_string_pretty(js);
	size_t len = strlen(serialized_string);

	/* write the summary to the doneFile */
	bool success = write_file(serialized_string, len, filename);

	json_free_serialized_string(serialized_string);
	json_value_free(js);

	if (!success)
	{
		log_error("Failed to write table summary file \"%s\"", filename);
		return false;
	}

	return true;
}


/*
 * read_blobs_summary reads a blobs process summary file from disk.
 */
bool
read_blobs_summary(CopyBlobsSummary *summary, char *filename)
{
	JSON_Value *json = json_parse_file(filename);

	if (json == NULL)
	{
		log_error("Failed to parse summary file \"%s\"", filename);
		return false;
	}

	JSON_Object *jsObj = json_value_get_object(json);

	summary->pid = json_object_get_number(jsObj, "pid");
	summary->count = json_object_get_number(jsObj, "count");
	summary->durationMs = json_object_get_number(jsObj, "duration");
	summary->startTime = json_object_get_number(jsObj, "start-time-epoch");
	summary->doneTime = json_object_get_number(jsObj, "done-time-epoch");

	json_value_free(json);
	return true;
}


/*
 * summary_set_current_time sets the current timing to the appropriate
 * TopLevelTimings entry given the step we're at.
 */
void
summary_set_current_time(TopLevelTimings *timings, TimingStep step)
{
	switch (step)
	{
		case TIMING_STEP_START:
		{
			INSTR_TIME_SET_CURRENT(timings->startTime);
			break;
		}

		case TIMING_STEP_BEFORE_SCHEMA_FETCH:
		{
			INSTR_TIME_SET_CURRENT(timings->beforeSchemaFetch);
			break;
		}

		case TIMING_STEP_BEFORE_SCHEMA_DUMP:
		{
			INSTR_TIME_SET_CURRENT(timings->beforeSchemaDump);
			break;
		}

		case TIMING_STEP_BEFORE_PREPARE_SCHEMA:
		{
			INSTR_TIME_SET_CURRENT(timings->beforePrepareSchema);
			break;
		}

		case TIMING_STEP_AFTER_PREPARE_SCHEMA:
		{
			INSTR_TIME_SET_CURRENT(timings->afterPrepareSchema);
			break;
		}

		case TIMING_STEP_BEFORE_FINALIZE_SCHEMA:
		{
			INSTR_TIME_SET_CURRENT(timings->beforeFinalizeSchema);
			break;
		}

		case TIMING_STEP_AFTER_FINALIZE_SCHEMA:
		{
			INSTR_TIME_SET_CURRENT(timings->afterFinalizeSchema);
			break;
		}

		case TIMING_STEP_END:
		{
			INSTR_TIME_SET_CURRENT(timings->endTime);
			break;
		}
	}
}


/* avoid non-initialized durations or clock oddities */
#define INSTR_TIME_MS(x) \
	(INSTR_TIME_GET_MILLISEC(x) > 0 ? INSTR_TIME_GET_MILLISEC(x) : 0)

/*
 * summary_prepare_toplevel_durations prepares the top-level durations in a
 * form that's suitable for printing on-screen.
 */
void
summary_prepare_toplevel_durations(Summary *summary)
{
	TopLevelTimings *timings = &(summary->timings);

	instr_time duration;
	uint64_t durationMs;

	/* compute schema dump duration, part of schemaDurationMs */
	duration = timings->beforeSchemaDump;
	INSTR_TIME_SUBTRACT(duration, timings->beforeSchemaFetch);
	durationMs = INSTR_TIME_MS(duration);

	IntervalToString(durationMs, timings->dumpSchemaMs, INTSTRING_MAX_DIGITS);

	timings->schemaDurationMs = durationMs;
	timings->dumpSchemaDurationMs = durationMs;

	/* compute schema fetch duration, part of schemaDurationMs */
	duration = timings->beforePrepareSchema;
	INSTR_TIME_SUBTRACT(duration, timings->beforeSchemaDump);
	durationMs = INSTR_TIME_MS(duration);

	IntervalToString(durationMs, timings->fetchSchemaMs, INTSTRING_MAX_DIGITS);

	timings->schemaDurationMs += durationMs;
	timings->fetchSchemaDurationMs = durationMs;

	/* compute prepare schema duration, part of schemaDurationMs */
	duration = timings->afterPrepareSchema;
	INSTR_TIME_SUBTRACT(duration, timings->beforePrepareSchema);
	durationMs = INSTR_TIME_MS(duration);

	IntervalToString(durationMs, timings->prepareSchemaMs, INTSTRING_MAX_DIGITS);

	timings->schemaDurationMs += durationMs;
	timings->prepareSchemaDurationMs = durationMs;

	/* compute data + index duration, between prepare schema and finalize */
	duration = timings->beforeFinalizeSchema;
	INSTR_TIME_SUBTRACT(duration, timings->afterPrepareSchema);
	durationMs = INSTR_TIME_MS(duration);

	IntervalToString(durationMs, timings->dataAndIndexMs, INTSTRING_MAX_DIGITS);

	timings->dataAndIndexesDurationMs = durationMs;

	/* compute finalize schema duration, part of schemaDurationMs */
	duration = timings->afterFinalizeSchema;
	INSTR_TIME_SUBTRACT(duration, timings->beforeFinalizeSchema);
	durationMs = INSTR_TIME_MS(duration);

	IntervalToString(durationMs, timings->finalizeSchemaMs, INTSTRING_MAX_DIGITS);

	timings->schemaDurationMs += durationMs;
	timings->finalizeSchemaDurationMs = durationMs;

	/* compute total duration, wall clock elapsed time */
	duration = timings->endTime;
	INSTR_TIME_SUBTRACT(duration, timings->startTime);
	durationMs = INSTR_TIME_MS(duration);

	IntervalToString(durationMs, timings->totalMs, INTSTRING_MAX_DIGITS);

	timings->totalDurationMs = durationMs;

	/* prepare the pretty printed string for the cumulative parallel part */
	IntervalToString(timings->tableDurationMs,
					 timings->totalTableMs,
					 INTSTRING_MAX_DIGITS);

	IntervalToString(timings->indexDurationMs,
					 timings->totalIndexMs,
					 INTSTRING_MAX_DIGITS);
}


/*
 * print_toplevel_summary prints a summary of the top-level timings.
 */
void
print_toplevel_summary(Summary *summary)
{
	char *d10s = "----------";
	char *s10s = "          ";
	char *d12s = "------------";
	char *d50s = "--------------------------------------------------";

	fformat(stdout, "\n");

	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n",
			"Step", "Connection", "Duration", "Transfer", "Concurrency");

	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n", d50s, d10s, d10s, d10s, d12s);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n", "Dump Schema", "source",
			summary->timings.dumpSchemaMs, s10s, 1);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n",
			"Catalog Queries (table ordering, filtering, etc)",
			"source",
			summary->timings.fetchSchemaMs,
			s10s,
			1);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n", "Prepare Schema", "target",
			summary->timings.prepareSchemaMs, s10s, 1);

	char concurrency[BUFSIZE] = { 0 };
	sformat(concurrency, sizeof(concurrency), "%d + %d",
			summary->tableJobs,
			summary->tableJobs + summary->indexJobs);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n",
			"COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)", "both",
			summary->timings.dataAndIndexMs,
			s10s,
			concurrency);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n",
			"COPY (cumulative)", "both",
			summary->timings.totalTableMs,
			summary->table.totalBytesStr,
			summary->tableJobs);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n",
			"Large Objects (cumulative)", "both",
			summary->timings.blobsMs,
			s10s,
			summary->lObjectJobs);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n",
			"CREATE INDEX, CONSTRAINTS (cumulative)", "target",
			summary->timings.totalIndexMs,
			s10s,
			summary->indexJobs);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n", "Finalize Schema", "target",
			summary->timings.finalizeSchemaMs, s10s, 1);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n", d50s, d10s, d10s, d10s, d12s);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n",
			"Total Wall Clock Duration", "both",
			summary->timings.totalMs,
			s10s,
			concurrency);

	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n", d50s, d10s, d10s, d10s, d12s);

	fformat(stdout, "\n");
}


/*
 * print_summary_table loops over a fully prepared summary table and prints
 * each element. It also prints the headers.
 */
void
print_summary_table(SummaryTable *summary)
{
	SummaryTableHeaders *headers = &(summary->headers);

	if (summary->count == 0)
	{
		return;
	}

	fformat(stdout, "\n");

	fformat(stdout, "%*s | %*s | %*s | %*s | %*s | %*s | %*s | %*s \n",
			headers->maxOidSize, "OID",
			headers->maxNspnameSize, "Schema",
			headers->maxRelnameSize, "Name",
			headers->maxPartCountSize, "Parts",
			headers->maxTableMsSize, "copy duration",
			headers->maxBytesSize, "transmitted bytes",
			headers->maxIndexCountSize, "indexes",
			headers->maxIndexMsSize, "create index duration");

	fformat(stdout, "%s-+-%s-+-%s-+-%s-+-%s-+-%s-+-%s-+-%s\n",
			headers->oidSeparator,
			headers->nspnameSeparator,
			headers->relnameSeparator,
			headers->partCountSeparator,
			headers->tableMsSeparator,
			headers->bytesSeparator,
			headers->indexCountSeparator,
			headers->indexMsSeparator);

	for (int i = 0; i < summary->count; i++)
	{
		SummaryTableEntry *entry = &(summary->array[i]);

		fformat(stdout, "%*s | %*s | %*s | %*s | %*s | %*s | %*s | %*s\n",
				headers->maxOidSize, entry->oidStr,
				headers->maxNspnameSize, entry->nspname,
				headers->maxRelnameSize, entry->relname,
				headers->maxPartCountSize, entry->partCount,
				headers->maxTableMsSize, entry->tableMs,
				headers->maxBytesSize, entry->bytesStr,
				headers->maxIndexCountSize, entry->indexCount,
				headers->maxIndexMsSize, entry->indexMs);
	}

	fformat(stdout, "\n");
}


/*
 * print_summary_as_json writes the current summary of operations (with
 * timings) to given filename, as a structured JSON document.
 */
void
print_summary_as_json(Summary *summary, const char *filename)
{
	log_notice("Storing migration summary in JSON file \"%s\"", filename);

	JSON_Value *js = json_value_init_object();
	JSON_Object *jsobj = json_value_get_object(js);

	json_object_dotset_number(jsobj, "setup.table-jobs", summary->tableJobs);
	json_object_dotset_number(jsobj, "setup.index-jobs", summary->indexJobs);

	TopLevelTimings *timings = &(summary->timings);

	JSON_Value *jsSteps = json_value_init_array();
	JSON_Array *jsStepArray = json_value_get_array(jsSteps);

	JSON_Value *jsDumpSchema = json_value_init_object();
	JSON_Object *jsDSObj = json_value_get_object(jsDumpSchema);

	json_object_set_string(jsDSObj, "label", "dump schema");
	json_object_set_string(jsDSObj, "conn", "source");
	json_object_set_number(jsDSObj, "duration",
						   timings->dumpSchemaDurationMs);
	json_object_set_number(jsDSObj, "concurrency", 1);

	json_array_append_value(jsStepArray, jsDumpSchema);

	JSON_Value *jsCatalog = json_value_init_object();
	JSON_Object *jsCatObj = json_value_get_object(jsCatalog);

	json_object_set_string(jsCatObj, "label", "Catalog Queries");
	json_object_set_string(jsCatObj, "conn", "source");
	json_object_set_number(jsCatObj, "duration",
						   timings->fetchSchemaDurationMs);
	json_object_set_number(jsCatObj, "concurrency", 1);

	json_array_append_value(jsStepArray, jsCatalog);

	JSON_Value *jsPrep = json_value_init_object();
	JSON_Object *jsPrepObj = json_value_get_object(jsPrep);

	json_object_set_string(jsPrepObj, "label", "Prepare Schema");
	json_object_set_string(jsPrepObj, "conn", "target");
	json_object_set_number(jsPrepObj, "duration",
						   timings->prepareSchemaDurationMs);
	json_object_set_number(jsPrepObj, "concurrency", 1);

	json_array_append_value(jsStepArray, jsPrep);

	JSON_Value *jsDB = json_value_init_object();
	JSON_Object *jsDBObj = json_value_get_object(jsDB);

	json_object_set_string(jsDBObj, "label",
						   "COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)");
	json_object_set_string(jsDBObj, "conn", "both");
	json_object_set_number(jsDBObj, "duration",
						   timings->dataAndIndexesDurationMs);
	json_object_set_number(jsDBObj, "concurrency",
						   summary->tableJobs + summary->indexJobs);

	json_array_append_value(jsStepArray, jsDB);

	JSON_Value *jsCopy = json_value_init_object();
	JSON_Object *jsCopyObj = json_value_get_object(jsCopy);

	json_object_set_string(jsCopyObj, "label", "COPY (cumulative)");
	json_object_set_string(jsCopyObj, "conn", "both");
	json_object_set_number(jsCopyObj, "duration", timings->tableDurationMs);
	json_object_set_number(jsCopyObj, "concurrency", summary->tableJobs);

	json_object_dotset_number(jsCopyObj, "network.bytes",
							  summary->table.totalBytes);
	json_object_dotset_string(jsCopyObj, "network.bytes-pretty",
							  summary->table.totalBytesStr);

	json_array_append_value(jsStepArray, jsCopy);

	JSON_Value *jsBlob = json_value_init_object();
	JSON_Object *jsBlobObj = json_value_get_object(jsBlob);

	json_object_set_string(jsBlobObj, "label", "Large Objects (cumulative)");
	json_object_set_string(jsBlobObj, "conn", "both");
	json_object_set_number(jsBlobObj, "duration", timings->blobDurationMs);
	json_object_set_number(jsBlobObj, "concurrency", summary->lObjectJobs);

	json_array_append_value(jsStepArray, jsBlob);

	JSON_Value *jsIndex = json_value_init_object();
	JSON_Object *jsIndexObj = json_value_get_object(jsIndex);

	json_object_set_string(jsIndexObj, "label",
						   "CREATE INDEX, CONSTRAINTS (cumulative)");
	json_object_set_string(jsIndexObj, "conn", "target");
	json_object_set_number(jsIndexObj, "duration", timings->indexDurationMs);
	json_object_set_number(jsIndexObj, "concurrency", summary->indexJobs);

	json_array_append_value(jsStepArray, jsIndex);

	JSON_Value *jsFin = json_value_init_object();
	JSON_Object *jsFinObj = json_value_get_object(jsFin);

	json_object_set_string(jsFinObj, "label", "Finalize Schema");
	json_object_set_string(jsFinObj, "conn", "target");
	json_object_set_number(jsFinObj, "duration", timings->finalizeSchemaDurationMs);
	json_object_set_number(jsFinObj, "concurrency", 1);

	json_array_append_value(jsStepArray, jsFin);

	JSON_Value *jsTotal = json_value_init_object();
	JSON_Object *jsTotalObj = json_value_get_object(jsTotal);

	json_object_set_string(jsTotalObj, "label", "Total Wall Clock Duration");
	json_object_set_string(jsTotalObj, "conn", "both");
	json_object_set_number(jsTotalObj, "duration", timings->totalDurationMs);

	json_object_set_number(jsTotalObj,
						   "concurrency",
						   summary->tableJobs + summary->indexJobs);

	json_array_append_value(jsStepArray, jsTotal);

	json_object_set_value(jsobj, "steps", jsSteps);

	SummaryTable *summaryTable = &(summary->table);

	JSON_Value *jsTables = json_value_init_array();
	JSON_Array *jsTableArray = json_value_get_array(jsTables);

	for (int i = 0; i < summaryTable->count; i++)
	{
		SummaryTableEntry *entry = &(summaryTable->array[i]);

		JSON_Value *jsTable = json_value_init_object();
		JSON_Object *jsTableObj = json_value_get_object(jsTable);

		json_object_set_number(jsTableObj, "oid", entry->oid);
		json_object_set_string(jsTableObj, "schema", entry->nspname);
		json_object_set_string(jsTableObj, "name", entry->relname);

		json_object_dotset_number(jsTableObj,
								  "duration", entry->durationTableMs);

		json_object_dotset_number(jsTableObj,
								  "network.bytes", entry->bytes);
		json_object_dotset_string(jsTableObj,
								  "network.bytes-pretty", entry->bytesStr);
		json_object_dotset_string(jsTableObj,
								  "network.transmit-rate", entry->transmitRate);

		json_object_dotset_number(jsTableObj,
								  "index.count", entry->indexArray.count);
		json_object_dotset_number(jsTableObj,
								  "index.duration", entry->durationIndexMs);

		JSON_Value *jsIndexes = json_value_init_array();
		JSON_Array *jsIndexArray = json_value_get_array(jsIndexes);

		for (int j = 0; j < entry->indexArray.count; j++)
		{
			SummaryIndexEntry *indexEntry = &(entry->indexArray.array[j]);

			JSON_Value *jsIndex = json_value_init_object();
			JSON_Object *jsIndexObj = json_value_get_object(jsIndex);

			json_object_set_number(jsIndexObj, "oid", indexEntry->oid);
			json_object_set_string(jsIndexObj, "schema", indexEntry->nspname);
			json_object_set_string(jsIndexObj, "name", indexEntry->relname);
			json_object_set_string(jsIndexObj, "sql", indexEntry->sql);
			json_object_dotset_number(jsIndexObj, "ms", indexEntry->durationMs);

			json_array_append_value(jsIndexArray, jsIndex);
		}

		/* add the index array to the current table */
		json_object_set_value(jsTableObj, "indexes", jsIndexes);

		JSON_Value *jsConstraints = json_value_init_array();
		JSON_Array *jsConstraintArray = json_value_get_array(jsConstraints);

		for (int j = 0; j < entry->constraintArray.count; j++)
		{
			SummaryIndexEntry *cEntry = &(entry->constraintArray.array[j]);

			JSON_Value *jsConstraint = json_value_init_object();
			JSON_Object *jsConstraintObj = json_value_get_object(jsConstraint);

			json_object_set_number(jsConstraintObj, "oid", cEntry->oid);
			json_object_set_string(jsConstraintObj, "schema", cEntry->nspname);
			json_object_set_string(jsConstraintObj, "name", cEntry->relname);
			json_object_set_string(jsConstraintObj, "sql", cEntry->sql);
			json_object_dotset_number(jsConstraintObj, "ms", cEntry->durationMs);

			json_array_append_value(jsConstraintArray, jsConstraint);
		}

		/* add the constraint array to the current table */
		json_object_set_value(jsTableObj, "constraints", jsConstraints);

		/* append the current table to the table array */
		json_array_append_value(jsTableArray, jsTable);
	}

	/* add the table array to the main JSON top-level dict */
	json_object_set_value(jsobj, "tables", jsTables);

	char *serialized_string = json_serialize_to_string_pretty(js);
	size_t len = strlen(serialized_string);

	if (!write_file(serialized_string, len, filename))
	{
		log_error("Failed to write summary JSON file, see above for details");
	}

	json_free_serialized_string(serialized_string);
	json_value_free(js);
}


/*
 * prepare_summary_table_headers computes the actual max length of all the
 * columns that we are going to display, and fills in the dashed separators
 * too.
 */
void
prepare_summary_table_headers(SummaryTable *summary)
{
	SummaryTableHeaders *headers = &(summary->headers);

	/* assign static maximums from the lenghts of the column headers */
	headers->maxOidSize = 3;        /* "oid" */
	headers->maxNspnameSize = 6;    /* "schema" */
	headers->maxRelnameSize = 4;    /* "name" */
	headers->maxPartCountSize = 5;    /* "parts" */
	headers->maxTableMsSize = 13;   /* "copy duration" */
	headers->maxBytesSize = 17;     /* "transmitted bytes" */
	headers->maxIndexCountSize = 7; /* "indexes" */
	headers->maxIndexMsSize = 21;   /* "create index duration" */

	/* now adjust to the actual table's content */
	for (int i = 0; i < summary->count; i++)
	{
		int len = 0;
		SummaryTableEntry *entry = &(summary->array[i]);

		len = strlen(entry->oidStr);

		if (headers->maxOidSize < len)
		{
			headers->maxOidSize = len;
		}

		len = strlen(entry->nspname);

		if (headers->maxNspnameSize < len)
		{
			headers->maxNspnameSize = len;
		}

		len = strlen(entry->relname);

		if (headers->maxRelnameSize < len)
		{
			headers->maxRelnameSize = len;
		}

		len = strlen(entry->partCount);

		if (headers->maxPartCountSize < len)
		{
			headers->maxPartCountSize = len;
		}

		len = strlen(entry->tableMs);

		if (headers->maxTableMsSize < len)
		{
			headers->maxTableMsSize = len;
		}

		len = strlen(entry->bytesStr);

		if (headers->maxBytesSize < len)
		{
			headers->maxBytesSize = len;
		}

		len = strlen(entry->indexCount);

		if (headers->maxIndexCountSize < len)
		{
			headers->maxIndexCountSize = len;
		}

		len = strlen(entry->indexMs);

		if (headers->maxIndexMsSize < len)
		{
			headers->maxIndexMsSize = len;
		}
	}

	/* now prepare the header line with dashes */
	prepareLineSeparator(headers->oidSeparator, headers->maxOidSize);
	prepareLineSeparator(headers->nspnameSeparator, headers->maxNspnameSize);
	prepareLineSeparator(headers->relnameSeparator, headers->maxRelnameSize);
	prepareLineSeparator(headers->partCountSeparator, headers->maxPartCountSize);
	prepareLineSeparator(headers->tableMsSeparator, headers->maxTableMsSize);
	prepareLineSeparator(headers->bytesSeparator, headers->maxBytesSize);
	prepareLineSeparator(headers->indexCountSeparator, headers->maxIndexCountSize);
	prepareLineSeparator(headers->indexMsSeparator, headers->maxIndexMsSize);
}


/*
 * prepareLineSeparator fills in the pre-allocated given string with the
 * expected amount of dashes to use as a separator line in our tabular output.
 */
static void
prepareLineSeparator(char dashes[], int size)
{
	for (int i = 0; i <= size; i++)
	{
		if (i < size)
		{
			dashes[i] = '-';
		}
		else
		{
			dashes[i] = '\0';
			break;
		}
	}
}


/*
 * print_summary prints a summary of the pgcopydb operations on stdout.
 *
 * The summary contains a line per table that has been copied and then the
 * count of indexes created for each table, and then the sum of the timing of
 * creating those indexes.
 */
bool
print_summary(Summary *summary, CopyDataSpec *specs)
{
	SummaryTable *summaryTable = &(summary->table);

	summary->tableJobs = specs->tableJobs;
	summary->indexJobs = specs->indexJobs;
	summary->lObjectJobs = specs->lObjectJobs;

	/* first, we have to scan the available data from memory and files */
	if (!prepare_summary_table(summary, specs))
	{
		log_error("Failed to prepare the summary table");
		return false;
	}

	/* print the summary.json file */
	(void) print_summary_as_json(summary, specs->cfPaths.summaryfile);

	/* then we can prepare the headers and print the table */
	if (specs->section == DATA_SECTION_TABLE_DATA ||
		specs->section == DATA_SECTION_ALL)
	{
		(void) prepare_summary_table_headers(summaryTable);
		(void) print_summary_table(summaryTable);
	}

	/* and then finally prepare the top-level counters and print them */
	(void) summary_prepare_toplevel_durations(summary);
	(void) print_toplevel_summary(summary);

	return true;
}


typedef struct SummaryTableContext
{
	Summary *summary;
	CopyDataSpec *specs;
	uint32_t tableIndex;
	uint64_t indexingDurationMs;
} SummaryTableContext;


/*
 * prepare_summary_table prepares the summary table array with the durations
 * read from disk in the doneFile for each oid that has been processed.
 */
bool
prepare_summary_table(Summary *summary, CopyDataSpec *specs)
{
	TopLevelTimings *timings = &(summary->timings);
	SummaryTable *summaryTable = &(summary->table);

	summaryTable->totalBytes = 0;

	DatabaseCatalog *sourceDB = &(specs->catalogs.source);
	CatalogCounts count = { 0 };

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		return false;
	}

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count indexes and constraints in our catalogs");
		return false;
	}

	log_info("Printing summary for %lld tables and %lld indexes",
			 (long long) count.tables,
			 (long long) count.indexes);

	summaryTable->count = count.tables;
	summaryTable->array =
		(SummaryTableEntry *) calloc(count.tables, sizeof(SummaryTableEntry));

	if (summaryTable->array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	SummaryTableContext context = {
		.specs = specs,
		.summary = summary,
		.tableIndex = 0
	};

	if (!catalog_iter_s_table(sourceDB,
							  &context,
							  &prepare_summary_table_hook))
	{
		log_error("Failed to prepare the table summary");
		return false;
	}

	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	/* write pretty printed total bytes value */
	(void) pretty_print_bytes(summary->table.totalBytesStr,
							  BUFSIZE,
							  summary->table.totalBytes);

	/*
	 * Also read the blobs summary file.
	 */
	if (file_exists(specs->cfPaths.done.blobs))
	{
		CopyBlobsSummary blobsSummary = { 0 };

		if (!read_blobs_summary(&blobsSummary, specs->cfPaths.done.blobs))
		{
			log_error("Failed to read blog summary file \"%s\"",
					  specs->cfPaths.done.blobs);
			return false;
		}

		timings->blobDurationMs = blobsSummary.durationMs;

		(void) IntervalToString(blobsSummary.durationMs,
								timings->blobsMs,
								sizeof(timings->blobsMs));
	}

	return true;
}


/*
 * prepare_summary_table_hook is an iterator callback function.
 */
static bool
prepare_summary_table_hook(void *ctx, SourceTable *table)
{
	SummaryTableContext *context = (SummaryTableContext *) ctx;

	CopyDataSpec *specs = (CopyDataSpec *) context->specs;
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	TopLevelTimings *timings = &(context->summary->timings);
	SummaryTable *summaryTable = &(context->summary->table);

	SummaryTableEntry *entry = &(summaryTable->array[context->tableIndex]);

	int partCount =
		table->partition.partCount == 0 ? 1 : table->partition.partCount;

	/* prepare some of the information we already have */
	IntString oidString = intToString(table->oid);
	IntString pcStr = intToString(partCount);

	entry->oid = table->oid;
	strlcpy(entry->partCount, pcStr.strValue, sizeof(entry->partCount));
	strlcpy(entry->oidStr, oidString.strValue, sizeof(entry->oidStr));
	strlcpy(entry->nspname, table->nspname, sizeof(entry->nspname));
	strlcpy(entry->relname, table->relname, sizeof(entry->relname));

	entry->durationTableMs = table->durationMs;
	timings->tableDurationMs += table->durationMs;

	(void) IntervalToString(table->durationMs,
							entry->tableMs,
							sizeof(entry->tableMs));

	entry->bytes = table->bytesTransmitted;
	summaryTable->totalBytes += table->bytesTransmitted;

	pretty_print_bytes(entry->bytesStr,
					   sizeof(entry->bytesStr),
					   entry->bytes);

	pretty_print_bytes_per_second(entry->transmitRate,
								  sizeof(entry->transmitRate),
								  entry->bytes,
								  entry->durationTableMs);

	/* read the index oid list from the table oid */
	context->indexingDurationMs = 0;

	if (!catalog_s_table_count_indexes(sourceDB, table))
	{
		/* errors have already been logged */
		return false;
	}

	/* make sure to always initialize this memory area */
	entry->indexArray.count = 0;
	entry->indexArray.array = NULL;

	entry->constraintArray.count = 0;
	entry->constraintArray.array = NULL;

	if (table->indexCount > 0)
	{
		/* prepare for as many constraints as indexes */
		entry->indexArray.array =
			(SummaryIndexEntry *) calloc(table->indexCount,
										 sizeof(SummaryIndexEntry));

		entry->constraintArray.array =
			(SummaryIndexEntry *) calloc(table->constraintCount,
										 sizeof(SummaryIndexEntry));

		if (entry->indexArray.array == NULL ||
			entry->constraintArray.array == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		if (!catalog_iter_s_index_table(sourceDB,
										table->nspname,
										table->relname,
										ctx,
										prepare_summary_table_index_hook))
		{
			/* errors have already been logged */
			return false;
		}
	}

	IntString indexCountString = intToString(table->indexCount);

	strlcpy(entry->indexCount,
			indexCountString.strValue,
			sizeof(entry->indexCount));

	(void) IntervalToString(context->indexingDurationMs,
							entry->indexMs,
							sizeof(entry->indexMs));

	entry->durationIndexMs = context->indexingDurationMs;

	/* prepare context for next iteration */
	++context->tableIndex;

	return true;
}


/*
 * prepare_summary_table_hook is an iterator callback function.
 */
static bool
prepare_summary_table_index_hook(void *ctx, SourceIndex *index)
{
	SummaryTableContext *context = (SummaryTableContext *) ctx;

	CopyDataSpec *specs = (CopyDataSpec *) context->specs;
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	TopLevelTimings *timings = &(context->summary->timings);
	SummaryTable *summaryTable = &(context->summary->table);

	SummaryTableEntry *entry = &(summaryTable->array[context->tableIndex]);

	SummaryIndexEntry *indexEntry =
		&(entry->indexArray.array[(entry->indexArray.count)++]);

	if (!summary_prepare_index_entry(sourceDB,
									 index,
									 false, /* constraint */
									 indexEntry))
	{
		log_error("Failed to read index summary");
		return false;
	}

	/* accumulate total duration of creating all the indexes */
	timings->indexDurationMs += indexEntry->durationMs;
	context->indexingDurationMs += indexEntry->durationMs;

	if (index->constraintOid > 0)
	{
		SummaryIndexArray *constraintArray =
			&(entry->constraintArray);

		SummaryIndexEntry *constraintEntry =
			&(constraintArray->array[(constraintArray->count)++]);

		if (!summary_prepare_index_entry(sourceDB,
										 index,
										 true, /* constraint */
										 constraintEntry))
		{
			log_error("Failed to read constraint summary");
			return false;
		}

		/* accumulate total duration of creating all the indexes */
		timings->indexDurationMs += constraintEntry->durationMs;
		context->indexingDurationMs += constraintEntry->durationMs;
	}

	return true;
}


/*
 * summary_read_index_donefile reads a donefile for an index and populates the
 * information found in the SummaryIndexEntry structure.
 */
bool
summary_prepare_index_entry(DatabaseCatalog *catalog,
							SourceIndex *index,
							bool constraint,
							SummaryIndexEntry *indexEntry)
{
	CopyIndexSpec indexSpecs = { .sourceIndex = index };
	CopyIndexSummary *indexSummary = &(indexSpecs.summary);

	if (constraint)
	{
		if (!summary_lookup_constraint(catalog, &indexSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		indexEntry->oid = index->constraintOid;

		IntString oidString = intToString(index->constraintOid);
		strlcpy(indexEntry->oidStr,
				oidString.strValue,
				sizeof(indexEntry->oidStr));
	}
	else
	{
		if (!summary_lookup_index(catalog, &indexSpecs))
		{
			/* errors have already been logged */
			return false;
		}

		indexEntry->oid = index->indexOid;

		IntString oidString = intToString(index->indexOid);
		strlcpy(indexEntry->oidStr,
				oidString.strValue,
				sizeof(indexEntry->oidStr));
	}

	strlcpy(indexEntry->nspname,
			indexSummary->index->indexNamespace,
			sizeof(indexEntry->nspname));

	strlcpy(indexEntry->relname,
			index->indexRelname,
			sizeof(indexEntry->relname));

	if (indexSummary->command != NULL)
	{
		indexEntry->sql = strdup(indexSummary->command);
	}

	indexEntry->durationMs = indexSummary->durationMs;

	(void) IntervalToString(indexSummary->durationMs,
							indexEntry->indexMs,
							sizeof(indexEntry->indexMs));

	return true;
}
