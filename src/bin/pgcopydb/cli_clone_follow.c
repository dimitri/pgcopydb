/*
 * src/bin/pgcopydb/cli_clone_follow.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "catalog.h"
#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "file_utils.h"
#include "ld_stream.h"
#include "log.h"
#include "multi_db.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "progress.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

#define PGCOPYDB_CLONE_GETOPTS_HELP \
	"  --source                      Postgres URI to the source database\n" \
	"  --target                      Postgres URI to the target database\n" \
	"  --dir                         Work directory to use\n" \
	"  --table-jobs                  Number of concurrent COPY jobs to run\n" \
	"  --index-jobs                  Number of concurrent CREATE INDEX jobs to run\n" \
	"  --restore-jobs                Number of concurrent jobs for pg_restore\n" \
	"  --large-objects-jobs          Number of concurrent Large Objects jobs to run\n" \
	"  --split-tables-larger-than    Same-table concurrency size threshold\n" \
	"  --split-max-parts             Maximum number of jobs for Same-table concurrency \n" \
	"  --estimate-table-sizes        Allow using estimates for relation sizes\n" \
	"  --drop-if-exists              On the target database, clean-up from a previous run first\n" \
	"  --roles                       Also copy roles found on source to target\n" \
	"  --no-role-passwords           Do not dump passwords for roles\n" \
	"  --no-owner                    Do not set ownership of objects to match the original database\n" \
	"  --no-acl                      Prevent restoration of access privileges (grant/revoke commands).\n" \
	"  --no-comments                 Do not output commands to restore comments\n" \
	"  --no-tablespaces              Do not output commands to select tablespaces\n" \
	"  --skip-large-objects          Skip copying large objects (blobs)\n" \
	"  --skip-extensions             Skip restoring extensions\n" \
	"  --skip-ext-comments           Skip restoring COMMENT ON EXTENSION\n" \
	"  --skip-collations             Skip restoring collations\n" \
	"  --skip-vacuum                 Skip running VACUUM ANALYZE\n" \
	"  --skip-analyze                Skip running vacuumdb --analyze-only\n" \
	"  --skip-db-properties          Skip copying ALTER DATABASE SET properties\n" \
	"  --skip-split-by-ctid          Skip spliting tables by ctid\n" \
	"  --requirements <filename>     List extensions requirements\n" \
	"  --filters <filename>          Use the filters defined in <filename>\n" \
	"  --fail-fast                   Abort early in case of error\n" \
	"  --restart                     Allow restarting when temp files exist already\n" \
	"  --resume                      Allow resuming operations after a failure\n" \
	"  --not-consistent              Allow taking a new snapshot on the source database\n" \
	"  --snapshot                    Use snapshot obtained with pg_export_snapshot\n" \
	"  --follow                      Implement logical decoding to replay changes\n" \
	"  --plugin                      Output plugin to use (pgoutput, test_decoding, wal2json)\n" \
	"  --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin\n" \
	"  --slot-name                   Use this Postgres replication slot name\n" \
	"  --create-slot                 Create the replication slot\n" \
	"  --origin                      Use this Postgres replication origin node name\n" \
	"  --endpos                      Stop replaying changes when reaching this LSN\n" \
	"  --use-copy-binary             Use the COPY BINARY format for COPY operations\n" \
	"  --all-databases               Clone all databases found on the source instance\n" \

CommandLine clone_command =
	make_command(
		"clone",
		"Clone an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		PGCOPYDB_CLONE_GETOPTS_HELP,
		cli_copy_db_getopts,
		cli_clone);

CommandLine fork_command =
	make_command(
		"fork",
		"Clone an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		PGCOPYDB_CLONE_GETOPTS_HELP,
		cli_copy_db_getopts,
		cli_clone);


CommandLine follow_command =
	make_command(
		"follow",
		"Replay changes from the source database to the target database",
		" --source ... --target ...  ",
		"  --source                      Postgres URI to the source database\n"
		"  --target                      Postgres URI to the target database\n"
		"  --dir                         Work directory to use\n"
		"  --filters <filename>          Use the filters defined in <filename>\n"
		"  --restart                     Allow restarting when temp files exist already\n"
		"  --resume                      Allow resuming operations after a failure\n"
		"  --not-consistent              Allow taking a new snapshot on the source database\n"
		"  --snapshot                    Use snapshot obtained with pg_export_snapshot\n"
		"  --plugin                      Output plugin to use (pgoutput, test_decoding, wal2json)\n"
		"  --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin\n"
		"  --slot-name                   Use this Postgres replication slot name\n"
		"  --create-slot                 Create the replication slot\n"
		"  --origin                      Use this Postgres replication origin node name\n"
		"  --endpos                      Stop replaying changes when reaching this LSN\n",
		cli_copy_db_getopts,
		cli_follow);


static void clone_and_follow(CopyDataSpec *copySpecs);

static bool start_clone_process(CopyDataSpec *copySpecs, pid_t *pid);

static bool start_follow_process(CopyDataSpec *copySpecs,
								 StreamSpecs *streamSpecs,
								 pid_t *pid);

static bool cli_clone_follow_wait_subprocess(const char *name, pid_t pid);


/*
 * alldb_timing_agg_hook aggregates per-database timing rows into the global
 * topLevelTimingArray.  For phases that run in parallel (Phase I + TOTAL_DATA)
 * we take the MAX of durations; for everything else we SUM.  Counts and bytes
 * are always summed.  Pass a pointer to a bool set to true for the first
 * database; it is set to false after the first call.
 */
static bool
alldb_timing_agg_hook(void *ctx, TopLevelTiming *timing)
{
	bool *first = (bool *) ctx;

	if (timing->section == TIMING_SECTION_UNKNOWN ||
		timing->section >= topLevelTimingArrayCount)
	{
		return true;
	}

	TopLevelTiming *entry = &(topLevelTimingArray[timing->section]);

	/* parallel phases: MAX duration; sequential/cumulative: SUM */
	bool useMax = (timing->section == TIMING_SECTION_CATALOG_QUERIES ||
				   timing->section == TIMING_SECTION_DUMP_SCHEMA ||
				   timing->section == TIMING_SECTION_PREPARE_SCHEMA ||
				   timing->section == TIMING_SECTION_TOTAL_DATA ||
				   timing->section == TIMING_SECTION_TOTAL);

	if (*first)
	{
		entry->section = timing->section;
		entry->durationMs = timing->durationMs;
		entry->count = timing->count;
		entry->bytes = timing->bytes;
		entry->startTime = timing->startTime;
		entry->doneTime = timing->doneTime;
	}
	else
	{
		entry->count += timing->count;
		entry->bytes += timing->bytes;

		if (useMax)
		{
			if (timing->durationMs > entry->durationMs)
			{
				entry->durationMs = timing->durationMs;
			}
		}
		else
		{
			entry->durationMs += timing->durationMs;
		}
	}

	return true;
}


/*
 * cli_clone_all_databases_consolidated_summary prints a single unified summary
 * for all databases after a successful --all-databases clone.
 *
 * Per-table section: one combined table with a leading "Database" column.
 * Steps section: timings aggregated across all databases (MAX for parallel
 * phases, SUM for sequential/cumulative ones).
 */
static void
cli_clone_all_databases_consolidated_summary(CopyDataSpec *copySpecs)
{
	DatabaseCatalog *instanceCatalog = &copySpecs->catalogs.source;

	if (!catalog_init(instanceCatalog))
	{
		log_warn("Could not open instance catalog for summary");
		return;
	}

	/* First pass: count total tables and number of databases */
	int totalTables = 0;
	int dbCount = 0;

	SourceDatabaseIterator iter1 = { .catalog = instanceCatalog };

	if (!catalog_iter_s_database_init(&iter1))
	{
		(void) catalog_close(instanceCatalog);
		return;
	}

	for (;;)
	{
		if (!catalog_iter_s_database_next(&iter1))
		{
			break;
		}

		SourceDatabase *db = iter1.dat;

		if (db == NULL)
		{
			break;
		}

		dbCount++;

		DatabaseCatalog dbCat = { .type = DATABASE_CATALOG_TYPE_SOURCE };

		sformat(dbCat.dbfile, sizeof(dbCat.dbfile),
				"%s/db/%s/schema/source.db",
				copySpecs->cfPaths.topdir, db->datname);

		if (!catalog_init(&dbCat))
		{
			continue;
		}

		CatalogCounts count = { 0 };

		if (catalog_count_objects(&dbCat, &count))
		{
			totalTables += (int) count.tables;
		}

		(void) catalog_close(&dbCat);
	}

	(void) catalog_iter_s_database_finish(&iter1);

	if (totalTables == 0 || dbCount == 0)
	{
		(void) catalog_close(instanceCatalog);
		return;
	}

	/* Allocate the combined summary table */
	SummaryTable combinedTable = { .count = totalTables };

	combinedTable.array =
		(SummaryTableEntry *) calloc(totalTables, sizeof(SummaryTableEntry));

	if (combinedTable.array == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		(void) catalog_close(instanceCatalog);
		return;
	}

	/* Reset timing globals so we aggregate from scratch */
	summary_reset_toplevel_timings();

	bool firstDb = true;
	int tableOffset = 0;

	SourceDatabaseIterator iter2 = { .catalog = instanceCatalog };

	if (!catalog_iter_s_database_init(&iter2))
	{
		free(combinedTable.array);
		(void) catalog_close(instanceCatalog);
		return;
	}

	for (;;)
	{
		if (!catalog_iter_s_database_next(&iter2))
		{
			break;
		}

		SourceDatabase *db = iter2.dat;

		if (db == NULL)
		{
			break;
		}

		CopyDataSpec dbSpecs = *copySpecs;

		dbSpecs.catalogs.source.db = NULL;

		sformat(dbSpecs.catalogs.source.dbfile,
				sizeof(dbSpecs.catalogs.source.dbfile),
				"%s/db/%s/schema/source.db",
				copySpecs->cfPaths.topdir, db->datname);

		sformat(dbSpecs.cfPaths.summaryfile,
				sizeof(dbSpecs.cfPaths.summaryfile),
				"%s/db/%s/summary.json",
				copySpecs->cfPaths.topdir, db->datname);

		if (!catalog_init(&dbSpecs.catalogs.source))
		{
			log_warn("Could not open catalog for database \"%s\"", db->datname);
			continue;
		}

		Summary dbSummary = {
			.tableJobs = copySpecs->tableJobs,
			.indexJobs = copySpecs->indexJobs,
			.vacuumJobs = copySpecs->vacuumJobs,
			.lObjectJobs = copySpecs->lObjectJobs,
			.restoreJobs = copySpecs->restoreOptions.jobs
		};

		if (prepare_summary_table(&dbSummary, &dbSpecs))
		{
			for (int i = 0;
				 i < dbSummary.table.count && tableOffset + i < totalTables;
				 i++)
			{
				SummaryTableEntry *dst =
					&(combinedTable.array[tableOffset + i]);

				*dst = dbSummary.table.array[i];
				strlcpy(dst->datname, db->datname, sizeof(dst->datname));
			}

			tableOffset += dbSummary.table.count;
			combinedTable.totalBytes += dbSummary.table.totalBytes;
		}

		/* Aggregate timing data into topLevelTimingArray */
		(void) summary_iter_timing(&dbSpecs.catalogs.source,
								   &firstDb,
								   &alldb_timing_agg_hook);
		firstDb = false;

		(void) catalog_close(&dbSpecs.catalogs.source);
	}

	(void) catalog_iter_s_database_finish(&iter2);

	/*
	 * Overlay Phase II (TOTAL_DATA) wall-clock from the instance catalog.
	 * This timing is written by clone_all_databases to the instance catalog
	 * and is absent from per-db catalogs, so alldb_timing_agg_hook leaves it
	 * at zero.
	 */
	{
		TopLevelTiming t = { .section = TIMING_SECTION_TOTAL_DATA };

		if (summary_lookup_timing(instanceCatalog, &t, TIMING_SECTION_TOTAL_DATA))
		{
			TopLevelTiming *entry = &(topLevelTimingArray[TIMING_SECTION_TOTAL_DATA]);

			entry->durationMs = t.durationMs;
			entry->count = t.count;
			entry->bytes = t.bytes;
		}
	}

	/*
	 * Overlay total wall-clock from the instance catalog (written by
	 * cli_clone around clone_all_databases).
	 */
	{
		TopLevelTiming t = { .section = TIMING_SECTION_TOTAL };

		if (summary_lookup_timing(instanceCatalog, &t, TIMING_SECTION_TOTAL))
		{
			TopLevelTiming *entry = &(topLevelTimingArray[TIMING_SECTION_TOTAL]);

			entry->durationMs = t.durationMs;
		}
	}

	(void) catalog_close(instanceCatalog);

	/* Re-pretty-print aggregated timing strings */
	for (int i = 0; i < topLevelTimingArrayCount; i++)
	{
		TopLevelTiming *entry = &(topLevelTimingArray[i]);

		if (entry->section == TIMING_SECTION_UNKNOWN)
		{
			continue;
		}

		(void) IntervalToString(entry->durationMs,
								entry->ppDuration,
								sizeof(entry->ppDuration));

		(void) pretty_print_bytes(entry->ppBytes,
								  sizeof(entry->ppBytes),
								  entry->bytes);
	}

	/* Print the combined per-table section */
	(void) pretty_print_bytes(combinedTable.totalBytesStr,
							  sizeof(combinedTable.totalBytesStr),
							  combinedTable.totalBytes);
	(void) prepare_summary_table_headers(&combinedTable);
	(void) print_summary_table(&combinedTable);

	/* Print aggregated step durations with correct Phase I concurrency */
	int phaseOneConcurrency =
		(dbCount < copySpecs->tableJobs) ? dbCount : copySpecs->tableJobs;

	char *d10s = "----------";
	char *d12s = "------------";
	char *d50s = "--------------------------------------------------";

	Summary summary = {
		.tableJobs = copySpecs->tableJobs,
		.indexJobs = copySpecs->indexJobs,
		.vacuumJobs = copySpecs->vacuumJobs,
		.lObjectJobs = copySpecs->lObjectJobs,
		.restoreJobs = copySpecs->restoreOptions.jobs
	};

	fformat(stdout, "\n");
	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n",
			"Step", "Connection", "Duration", "Transfer", "Concurrency");
	fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n",
			d50s, d10s, d10s, d10s, d12s);

	for (int i = 0; i < topLevelTimingArrayCount; i++)
	{
		TopLevelTiming *timing = &(topLevelTimingArray[i]);

		if (timing->section == TIMING_SECTION_UNKNOWN)
		{
			continue;
		}

		if (i + 1 == topLevelTimingArrayCount)
		{
			fformat(stdout, " %50s   %10s  %10s  %10s  %12s\n",
					d50s, d10s, d10s, d10s, d12s);
		}

		/* Phase I runs min(tableJobs, dbCount) parallel DB workers */
		int concurrency;

		if (timing->section == TIMING_SECTION_CATALOG_QUERIES ||
			timing->section == TIMING_SECTION_DUMP_SCHEMA ||
			timing->section == TIMING_SECTION_PREPARE_SCHEMA)
		{
			concurrency = phaseOneConcurrency;
		}
		else
		{
			concurrency = TopLevelTimingConcurrency(&summary, timing);
		}

		fformat(stdout, " %50s   %10s  %10s  %10s  %12d\n",
				timing->label,
				timing->conn,
				timing->ppDuration,
				timing->bytes > 0 ? timing->ppBytes : "",
				concurrency);
	}

	fformat(stdout, "\n");

	free(combinedTable.array);
}


/*
 * cli_clone implements the command: pgcopydb clone
 */
void
cli_clone(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	/* at the moment these are not covered by cli_copy_prepare_specs() */
	copySpecs.follow = copyDBoptions.follow;
	copySpecs.allDatabases = copyDBoptions.allDatabases;

	/*
	 * When --all-databases is used, delegate to the multi-database orchestrator.
	 */
	if (copySpecs.allDatabases)
	{
		/*
		 * Start the total wall-clock timer before Phase I begins.  We open
		 * the instance catalog briefly to persist the start timestamp, then
		 * close it — clone_all_databases will reopen it as needed.
		 */
		if (!catalog_init_from_specs(&copySpecs))
		{
			log_error("Failed to open instance catalog for TOTAL timing");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
		(void) summary_start_timing(&copySpecs.catalogs.source,
									TIMING_SECTION_TOTAL);
		(void) catalog_close_from_specs(&copySpecs);

		if (!clone_all_databases(&copySpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		/* Record total wall-clock stop time in the instance catalog. */
		if (catalog_init_from_specs(&copySpecs))
		{
			(void) summary_stop_timing(&copySpecs.catalogs.source,
									   TIMING_SECTION_TOTAL);
			(void) catalog_close_from_specs(&copySpecs);
		}

		cli_clone_all_databases_consolidated_summary(&copySpecs);
		exit(EXIT_CODE_QUIT);
	}

	/*
	 * When pgcopydb clone --follow is used, we call the clone_and_follow()
	 * function which does it all, and just quit.
	 */
	if (copySpecs.follow)
	{
		(void) clone_and_follow(&copySpecs);
		exit(EXIT_CODE_QUIT);
	}

	/*
	 * From now on, we know the --follow option has not been used, it's all
	 * about doing a bare clone operation.
	 *
	 * First, make sure to export a snapshot.
	 */
	bool exportSnapshot = copydb_should_export_snapshot(&copySpecs);

	if (exportSnapshot && !copydb_prepare_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pid_t clonePID = -1;

	if (!start_clone_process(&copySpecs, &clonePID))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* wait until the clone process is finished */
	bool success = cli_clone_follow_wait_subprocess("clone", clonePID);

	/* close our top-level copy db connection and snapshot */
	if (exportSnapshot && !copydb_close_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/* make sure all sub-processes are now finished */
	bool allExitsAreZero = copydb_wait_for_subprocesses(copySpecs.failFast);

	if (!success || !allExitsAreZero)
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * clone_and_follow implements the command: pgcopydb clone --follow
 */
static void
clone_and_follow(CopyDataSpec *copySpecs)
{
	StreamSpecs streamSpecs = { 0 };

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	if (!stream_init_specs(&streamSpecs,
						   &(copySpecs->cfPaths.cdc),
						   &(copySpecs->connStrings),
						   &(copyDBoptions.slot),
						   copyDBoptions.origin,
						   copyDBoptions.endpos,
						   STREAM_MODE_CATCHUP,
						   &(copySpecs->catalogs.source),
						   &(copySpecs->catalogs.output),
						   &(copySpecs->catalogs.replay),
						   copyDBoptions.stdIn,
						   copyDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	streamSpecs.maxReplayDBSize = copyDBoptions.maxReplayDBSize > 0
								  ? copyDBoptions.maxReplayDBSize
								  : (1ULL << 30);

	/* optional follow coordinator TCP endpoint (--host/--port) */
	strlcpy(streamSpecs.coordHost, copyDBoptions.host,
			sizeof(streamSpecs.coordHost));
	streamSpecs.coordPort = copyDBoptions.port;

	/*
	 * When using pgcopydb clone --follow --restart we first cleanup the
	 * previous setup, and that includes dropping the replication slot.
	 */
	if (copySpecs->restart)
	{
		log_info("Clean-up replication setup, per --restart");

		if (!stream_cleanup_databases(copySpecs,
									  copyDBoptions.slot.slotName,
									  copyDBoptions.origin))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	/*
	 * First create/export a snapshot for the whole clone --follow operations.
	 */
	if (!follow_export_snapshot(copySpecs, &streamSpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/*
	 * When --follow has been used, we start two subprocess (clone, follow).
	 * Before doing that though, we want to make sure it was possible to setup
	 * the source and target database for Change Data Capture.
	 */
	if (!follow_setup_databases(copySpecs, &streamSpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * We fetch the schema here, rather than later in the clone subprocess,
	 * which simply reuses this cached data. This is done to avoid lock
	 * contention between the clone and follow subprocesses, as they both try to
	 * write concurrently to the source.db SQLite database, leading one to
	 * failure. This is also necessary for plugins like test_decoding, which
	 * require information such as primary keys.
	 *
	 * In the future, if the follow subprocess doesn't need a catalog (e.g. if
	 * we remove test_decoding), we should separate out tables for the follow
	 * subprocess into their own database.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Preparation and snapshot are now done, time to fork our two main worker
	 * processes.
	 *
	 * SQLite database connections are not safe to use across fork(). Close
	 * them here in the parent before forking so that each child process opens
	 * its own fresh connection. The IPC semaphores (semId) are inherited by
	 * the children and remain valid — only the sqlite3 handles are recycled.
	 * The catalog data written above persists on disk; children will reopen
	 * and reuse it without refetching.
	 */
	if (!catalog_close_from_specs(copySpecs))
	{
		log_error("Failed to close catalog connections before forking");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pid_t clonePID = -1;
	pid_t followPID = -1;

	if (!start_clone_process(copySpecs, &clonePID))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!start_follow_process(copySpecs, &streamSpecs, &followPID))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* wait until the clone process is finished */
	bool success =
		cli_clone_follow_wait_subprocess("clone", clonePID);

	/* close our top-level copy db connection and snapshot */
	if (!copydb_close_snapshot(copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/*
	 * If we failed to do the clone parts (midway through, or entirely maybe),
	 * we need to make it so that the follow sub-process isn't going to wait
	 * forever to reach the apply mode and then the endpos. That will never
	 * happen.
	 */
	if (!success)
	{
		log_warn("Failed to clone the source database, see above for details");

		if (!copydb_fatal_exit())
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* now wait until the follow process is finished, if it's been started */
	if (followPID != -1)
	{
		success = success &&
				  cli_clone_follow_wait_subprocess("follow", followPID);
	}

	/*
	 * Now is a good time to reset the sequences on the target database to
	 * match the state they are in at the moment on the source database.
	 * Postgres logical decoding lacks support for syncing sequences.
	 *
	 * This step is implement as if running the following command:
	 *
	 *   $ pgcopydb copy sequences --resume --not-consistent
	 *
	 * The whole idea is to fetch the "new" current values of the
	 * sequences, not the ones that were current when the main snapshot was
	 * exported.
	 */
	if (success)
	{
		if (!follow_reset_sequences(copySpecs, &streamSpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_TARGET);
		}
	}

	/* make sure all sub-processes are now finished */
	success = success && copydb_wait_for_subprocesses(copySpecs->failFast);

	if (!success)
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_follow implements the command: pgcopydb follow
 */
void
cli_follow(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(copyDBoptions.slot),
						   copyDBoptions.origin,
						   copyDBoptions.endpos,
						   STREAM_MODE_CATCHUP,
						   &(copySpecs.catalogs.source),
						   &(copySpecs.catalogs.output),
						   &(copySpecs.catalogs.replay),
						   copyDBoptions.stdIn,
						   copyDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	specs.maxReplayDBSize = copyDBoptions.maxReplayDBSize > 0
							? copyDBoptions.maxReplayDBSize
							: (1ULL << 30);

	/* optional follow coordinator TCP endpoint (--host/--port or env) */
	strlcpy(specs.coordHost, copyDBoptions.host, sizeof(specs.coordHost));
	specs.coordPort = copyDBoptions.port;

	/*
	 * First create/export a snapshot for the whole clone --follow operations.
	 */
	if (!follow_export_snapshot(&copySpecs, &specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/*
	 * First create the replication slot on the source database, and the origin
	 * (replication progress tracking) on the target database.
	 */
	if (!follow_setup_databases(&copySpecs, &specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Before starting the receive, transform, and apply sub-processes, we need
	 * to set the sentinel endpos to the command line --endpos option, when
	 * given.
	 *
	 * Also fetch the current values from the pgcopydb.sentinel. It might have
	 * been updated from a previous run of the command, and we might have
	 * nothing to catch-up to when e.g. the endpos was reached already.
	 */
	CopyDBSentinel sentinel = { 0 };

	if (!follow_init_sentinel(&specs, &sentinel))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (sentinel.endpos != InvalidXLogRecPtr &&
		sentinel.endpos <= sentinel.replay_lsn)
	{
		log_info("Current endpos %X/%X was previously reached at %X/%X",
				 LSN_FORMAT_ARGS(sentinel.endpos),
				 LSN_FORMAT_ARGS(sentinel.replay_lsn));

		exit(EXIT_CODE_QUIT);
	}

	/* make sure that we have our own process local connection */
	TransactionSnapshot snapshot = { 0 };

	if (!copydb_copy_snapshot(&copySpecs, &snapshot))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/* swap the new instance in place of the previous one */
	copySpecs.sourceSnapshot = snapshot;

	if (!copydb_set_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!follow_main_loop(&copySpecs, &specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * start_clone_process starts a sub-process that clones the source database
 * into the target database.
 */
static bool
start_clone_process(CopyDataSpec *copySpecs, pid_t *pid)
{
	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to prefetch changes: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: clone");

			log_notice("Starting the clone sub-process");

			if (!copydb_clone_database(copySpecs))
			{
				log_error("Failed to clone source database, "
						  "see above for details");
				exit(EXIT_CODE_SOURCE);
			}

			/* and we're done */
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			*pid = fpid;
			return true;
		}
	}

	return true;
}


/*
 * copydb_clone_database clones a source database into a target database.
 */
bool
copydb_clone_database(CopyDataSpec *copySpecs)
{
	/*
	 * The top-level process implements the preparation steps and exports a
	 * snapshot, unless the --snapshot option has been used. Then the rest of
	 * the work is split into a clone sub-process and a follow sub-process that
	 * work concurrently.
	 */
	DatabaseCatalog *sourceDB = &(copySpecs->catalogs.source);

	/* grab startTime before opening the catalogs */
	TopLevelTiming *timing = &(topLevelTimingArray[TIMING_SECTION_TOTAL]);
	(void) catalog_start_timing(timing);

	/* fetch schema information from source catalogs, including filtering */
	log_info("STEP 1: fetch source database tables, indexes, and sequences");

	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* now register in the catalogs the already known startTime */
	if (!summary_start_timing(sourceDB, TIMING_SECTION_TOTAL))
	{
		/* errors have already been logged */
		return false;
	}

	if (copySpecs->roles)
	{
		log_info("Copy the source database roles, per --roles");

		if (!pg_copy_roles(&(copySpecs->pgPaths),
						   &(copySpecs->connStrings),
						   copySpecs->dumpPaths.rolesFilename,
						   copySpecs->noRolesPasswords))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* make sure that we have our own process local connection */
	TransactionSnapshot snapshot = { 0 };

	if (!copydb_copy_snapshot(copySpecs, &snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	/* swap the new instance in place of the previous one */
	copySpecs->sourceSnapshot = snapshot;

	log_info("STEP 2: dump the source database schema (pre/post data)");

	if (!copydb_dump_source_schema(copySpecs, copySpecs->sourceSnapshot.snapshot))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("STEP 3: restore the pre-data section to the target database");

	if (!copydb_target_prepare_schema(copySpecs))
	{
		log_error("Failed to prepare schema on the target database, "
				  "see above for details");
		return false;
	}

	/* STEPs 4, 5, 6, 7, 8, and 9 are printed when starting the sub-processes */
	if (!copydb_copy_all_table_data(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("STEP 10: restore the post-data section to the target database");

	if (!copydb_target_finalize_schema(copySpecs))
	{
		log_error("Failed to finalize schema on the target database, "
				  "see above for details");
		return false;
	}

	/*
	 * When --follow has been used, now is the time to allow for the catchup
	 * process to start applying the prefetched changes.
	 */
	if (copySpecs->follow)
	{
		log_info("Updating the pgcopydb.sentinel to enable applying changes");

		if (!sentinel_update_apply(sourceDB, true))
		{
			/* errors have already been logged */
			return false;
		}
	}

	/* stop the timing wall-clock, and print the top-level summary */
	if (!summary_stop_timing(sourceDB, TIMING_SECTION_TOTAL))
	{
		/* errors have already been logged */
		return false;
	}

	log_info("All step are now done, %s elapsed", timing->ppDuration);

	(void) print_summary(copySpecs);

	/* time to close the catalogs now */
	if (!catalog_close_from_specs(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * start_follow_process starts a sub-process that clones the source database
 * into the target database.
 */
static bool
start_follow_process(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs,
					 pid_t *pid)
{
	/*
	 * Before starting the receive, transform, and apply sub-processes, we need
	 * to set the sentinel endpos to the command line --endpos option, when
	 * given.
	 *
	 * Also fetch the current values from the pgcopydb.sentinel. It might have
	 * been updated from a previous run of the command, and we might have
	 * nothing to catch-up to when e.g. the endpos was reached already.
	 */
	CopyDBSentinel *sentinel = &(streamSpecs->sentinel);

	if (!follow_init_sentinel(streamSpecs, sentinel))
	{
		log_error("Failed to initialise sentinel, see above for details");
		return false;
	}

	if (sentinel->endpos != InvalidXLogRecPtr &&
		sentinel->endpos <= sentinel->replay_lsn)
	{
		log_info("Current endpos %X/%X was previously reached at %X/%X",
				 LSN_FORMAT_ARGS(sentinel->endpos),
				 LSN_FORMAT_ARGS(sentinel->replay_lsn));

		return true;
	}

	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to prefetch changes: %m");
			return false;
		}

		case 0:
		{
			/* child process runs the command */
			(void) set_ps_title("pgcopydb: follow");
			log_notice("Starting the follow sub-process");

			if (!follow_main_loop(copySpecs, streamSpecs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			/* and we're done */
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			*pid = fpid;
			return true;
		}
	}

	return true;
}


/*
 * cli_clone_follow_wait_subprocesses waits until both sub-processes are
 * finished.
 */
static bool
cli_clone_follow_wait_subprocess(const char *name, pid_t pid)
{
	bool exited = false;
	int returnCode = -1;
	int sig = 0;

	if (pid < 0)
	{
		log_error("BUG: cli_clone_follow_wait_subprocess(%s, %d)", name, pid);
		return false;
	}

	while (!exited)
	{
		if (!follow_wait_pid(pid, &exited, &returnCode, &sig))
		{
			/* errors have already been logged */
			return false;
		}

		if (exited)
		{
			char details[BUFSIZE] = { 0 };
			bool exitedSuccessfully = returnCode == 0 && signal_is_handled(sig);

			if (sig != 0)
			{
				sformat(details, sizeof(details), " (%s [%d])",
						signal_to_string(sig),
						sig);
			}

			log_level(exitedSuccessfully ? LOG_DEBUG : LOG_ERROR,
					  "%s process %d has terminated [%d]%s",
					  name,
					  pid,
					  returnCode,
					  details);
		}

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
	}

	return returnCode == 0 && signal_is_handled(sig);
}
