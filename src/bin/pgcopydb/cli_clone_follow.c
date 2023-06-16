/*
 * src/bin/pgcopydb/cli_copy.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "ld_stream.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "progress.h"
#include "string_utils.h"
#include "summary.h"

#define PGCOPYDB_CLONE_GETOPTS_HELP \
	"  --source                   Postgres URI to the source database\n" \
	"  --target                   Postgres URI to the target database\n" \
	"  --dir                      Work directory to use\n" \
	"  --table-jobs               Number of concurrent COPY jobs to run\n" \
	"  --index-jobs               Number of concurrent CREATE INDEX jobs to run\n" \
	"  --split-tables-larger-than Same-table concurrency size threshold\n" \
	"  --drop-if-exists           On the target database, clean-up from a previous run first\n" \
	"  --roles                    Also copy roles found on source to target\n" \
	"  --no-role-passwords        Do not dump passwords for roles\n" \
	"  --no-owner                 Do not set ownership of objects to match the original database\n" \
	"  --no-acl                   Prevent restoration of access privileges (grant/revoke commands).\n" \
	"  --no-comments              Do not output commands to restore comments\n" \
	"  --skip-large-objects       Skip copying large objects (blobs)\n" \
	"  --skip-extensions          Skip restoring extensions\n" \
	"  --skip-collations          Skip restoring collations\n" \
	"  --skip-vacuum              Skip running VACUUM ANALYZE\n" \
	"  --filters <filename>       Use the filters defined in <filename>\n" \
	"  --fail-fast                Abort early in case of error\n" \
	"  --restart                  Allow restarting when temp files exist already\n" \
	"  --resume                   Allow resuming operations after a failure\n" \
	"  --not-consistent           Allow taking a new snapshot on the source database\n" \
	"  --snapshot                 Use snapshot obtained with pg_export_snapshot\n" \
	"  --follow                   Implement logical decoding to replay changes\n" \
	"  --plugin                   Output plugin to use (test_decoding, wal2json)\n" \
	"  --slot-name                Use this Postgres replication slot name\n" \
	"  --create-slot              Create the replication slot\n" \
	"  --origin                   Use this Postgres replication origin node name\n" \
	"  --endpos                   Stop replaying changes when reaching this LSN\n" \

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

/* pgcopydb copy db is an alias for pgcopydb clone */
CommandLine copy__db_command =
	make_command(
		"copy-db",
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
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n"
		"  --filters <filename>  Use the filters defined in <filename>\n"
		"  --restart             Allow restarting when temp files exist already\n"
		"  --resume              Allow resuming operations after a failure\n"
		"  --not-consistent      Allow taking a new snapshot on the source database\n"
		"  --snapshot            Use snapshot obtained with pg_export_snapshot\n"
		"  --plugin              Output plugin to use (test_decoding, wal2json)\n" \
		"  --slot-name           Use this Postgres replication slot name\n"
		"  --create-slot         Create the replication slot\n"
		"  --origin              Use this Postgres replication origin node name\n"
		"  --endpos              Stop replaying changes when reaching this LSN\n",
		cli_copy_db_getopts,
		cli_follow);


static void clone_and_follow(CopyDataSpec *copySpecs);

static bool start_clone_process(CopyDataSpec *copySpecs, pid_t *pid);

static bool start_follow_process(CopyDataSpec *copySpecs,
								 StreamSpecs *streamSpecs,
								 pid_t *pid);

static bool cli_clone_follow_wait_subprocess(const char *name, pid_t pid);

static bool cloneDB(CopyDataSpec *copySpecs);


/*
 * cli_clone implements the command: pgcopydb clone
 */
void
cli_clone(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	/* at the moment this is not covered by cli_copy_prepare_specs() */
	copySpecs.follow = copyDBoptions.follow;

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
	if (!copydb_prepare_snapshot(&copySpecs))
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
	if (!copydb_close_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/* make sure all sub-processes are now finished */
	success = success && copydb_wait_for_subprocesses(copySpecs.failFast);

	if (!success)
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
						   &(copySpecs->catalog),
						   copyDBoptions.stdIn,
						   copyDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
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
	 * Preparation and snapshot are now done, time to fork our two main worker
	 * processes.
	 */
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
	if (!follow_reset_sequences(copySpecs, &streamSpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
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
						   &(copySpecs.catalog),
						   copyDBoptions.stdIn,
						   copyDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

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
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			log_notice("Starting the clone sub-process");

			if (!cloneDB(copySpecs))
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
 * cloneDB clones a source database into a target database.
 */
static bool
cloneDB(CopyDataSpec *copySpecs)
{
	/*
	 * The top-level process implements the preparation steps and exports a
	 * snapshot, unless the --snapshot option has been used. Then the rest of
	 * the work is split into a clone sub-process and a follow sub-process that
	 * work concurrently.
	 */
	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

	if (copySpecs->roles)
	{
		log_info("STEP 0: copy the source database roles");

		if (!pg_copy_roles(&(copySpecs->pgPaths),
						   &(copySpecs->connStrings),
						   copySpecs->dumpPaths.rolesFilename,
						   copySpecs->noRolesPasswords))
		{
			/* errors have already been logged */
			return false;
		}
	}

	log_info("STEP 1: dump the source database schema (pre/post data)");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_SCHEMA_DUMP);

	if (!copydb_dump_source_schema(copySpecs,
								   copySpecs->sourceSnapshot.snapshot,
								   PG_DUMP_SECTION_SCHEMA))
	{
		/* errors have already been logged */
		return false;
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

	if (!copydb_set_snapshot(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* fetch schema information from source catalogs, including filtering */

	log_info("STEP 2: fetch source database tables, indexes, and sequences");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_SCHEMA_FETCH);

	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* prepare the schema JSON file with all the migration details */
	if (!copydb_prepare_schema_json_file(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_PREPARE_SCHEMA);

	log_info("STEP 3: restore the pre-data section to the target database");

	if (!copydb_target_prepare_schema(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_PREPARE_SCHEMA);

	/* STEPs 4, 5, 6, 7, 8, and 9 are printed when starting the sub-processes */
	if (!copydb_copy_all_table_data(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* close our snapshot: commit transaction and finish connection */
	(void) copydb_close_snapshot(copySpecs);

	log_info("STEP 10: restore the post-data section to the target database");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_FINALIZE_SCHEMA);

	if (!copydb_target_finalize_schema(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * When --follow has been used, now is the time to allow for the catchup
	 * process to start applying the prefetched changes.
	 */
	if (copySpecs->follow)
	{
		PGSQL pgsql = { 0 };

		log_info("Updating the pgcopydb.sentinel to enable applying changes");

		if (!pgsql_init(&pgsql,
						copySpecs->connStrings.source_pguri,
						PGSQL_CONN_SOURCE))
		{
			/* errors have already been logged */
			return false;
		}

		if (!pgsql_update_sentinel_apply(&pgsql, true))
		{
			/* errors have already been logged */
			return false;
		}
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_FINALIZE_SCHEMA);
	(void) summary_set_current_time(timings, TIMING_STEP_END);

	(void) print_summary(&summary, copySpecs);

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
	CopyDBSentinel sentinel = { 0 };

	if (!follow_init_sentinel(streamSpecs, &sentinel))
	{
		/* errors have already been logged */
		return false;
	}

	if (sentinel.endpos != InvalidXLogRecPtr &&
		sentinel.endpos <= sentinel.replay_lsn)
	{
		log_info("Current endpos %X/%X was previously reached at %X/%X",
				 LSN_FORMAT_ARGS(sentinel.endpos),
				 LSN_FORMAT_ARGS(sentinel.replay_lsn));

		return true;
	}

	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to prefetch changes: %m");
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
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

	if (pid < 0)
	{
		log_error("BUG: cli_clone_follow_wait_subprocess(%s, %d)", name, pid);
		return false;
	}

	while (!exited)
	{
		if (!follow_wait_pid(pid, &exited, &returnCode))
		{
			/* errors have already been logged */
			return false;
		}

		if (exited)
		{
			log_level(returnCode == 0 ? LOG_DEBUG : LOG_ERROR,
					  "%s process %d has terminated [%d]",
					  name,
					  pid,
					  returnCode);
		}

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
	}

	return returnCode == 0;
}
