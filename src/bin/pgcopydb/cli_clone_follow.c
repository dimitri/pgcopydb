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
#include "log.h"
#include "parsing.h"
#include "pgsql.h"
#include "stream.h"
#include "string_utils.h"
#include "summary.h"

CommandLine clone_command =
	make_command(
		"clone",
		"Clone an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n"
		"  --table-jobs          Number of concurrent COPY jobs to run\n"
		"  --index-jobs          Number of concurrent CREATE INDEX jobs to run\n"
		"  --drop-if-exists      On the target database, clean-up from a previous run first\n"
		"  --roles               Also copy roles found on source to target\n"
		"  --no-owner            Do not set ownership of objects to match the original database\n"
		"  --no-acl              Prevent restoration of access privileges (grant/revoke commands).\n"
		"  --no-comments         Do not output commands to restore comments\n"
		"  --skip-large-objects  Skip copying large objects (blobs)\n"
		"  --filters <filename>  Use the filters defined in <filename>\n"
		"  --restart             Allow restarting when temp files exist already\n"
		"  --resume              Allow resuming operations after a failure\n"
		"  --not-consistent      Allow taking a new snapshot on the source database\n"
		"  --snapshot            Use snapshot obtained with pg_export_snapshot\n"
		"  --follow              Implement logical decoding to replay changes\n"
		"  --slot-name           Use this Postgres replication slot name\n"
		"  --create-slot         Create the replication slot\n"
		"  --origin              Use this Postgres replication origin node name\n"
		"  --endpos              Stop replaying changes when reaching this LSN\n",
		cli_copy_db_getopts,
		cli_clone);

CommandLine fork_command =
	make_command(
		"fork",
		"Clone an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n"
		"  --table-jobs          Number of concurrent COPY jobs to run\n"
		"  --index-jobs          Number of concurrent CREATE INDEX jobs to run\n"
		"  --drop-if-exists      On the target database, clean-up from a previous run first\n"
		"  --roles               Also copy roles found on source to target\n"
		"  --no-owner            Do not set ownership of objects to match the original database\n"
		"  --no-acl              Prevent restoration of access privileges (grant/revoke commands).\n"
		"  --no-comments         Do not output commands to restore comments\n"
		"  --skip-large-objects  Skip copying large objects (blobs)\n"
		"  --filters <filename>  Use the filters defined in <filename>\n"
		"  --restart             Allow restarting when temp files exist already\n"
		"  --resume              Allow resuming operations after a failure\n"
		"  --not-consistent      Allow taking a new snapshot on the source database\n"
		"  --snapshot            Use snapshot obtained with pg_export_snapshot\n"
		"  --follow              Implement logical decoding to replay changes\n"
		"  --slot-name           Use this Postgres replication slot name\n"
		"  --create-slot         Create the replication slot\n"
		"  --origin              Use this Postgres replication origin node name\n"
		"  --endpos              Stop replaying changes when reaching this LSN\n",
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
		"  --slot-name           Use this Postgres replication slot name\n"
		"  --create-slot         Create the replication slot\n"
		"  --origin              Use this Postgres replication origin node name\n"
		"  --endpos              Stop replaying changes when reaching this LSN\n",
		cli_copy_db_getopts,
		cli_follow);


static bool start_clone_process(CopyDataSpec *copySpecs, pid_t *pid);

static bool start_follow_process(CopyDataSpec *copySpecs,
								 StreamSpecs *streamSpecs,
								 pid_t *pid);

static bool cli_clone_follow_wait_subprocess(const char *name, pid_t pid);

static bool cloneDB(CopyDataSpec *copySpecs);
static bool followDB(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs);


/*
 * cli_clone implements the command: pgcopydb clone
 */
void
cli_clone(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };
	StreamSpecs streamSpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	/* at the moment this is not covered by cli_copy_prepare_specs() */
	copySpecs.follow = copyDBoptions.follow;

	if (copySpecs.follow)
	{
		if (!stream_init_specs(&streamSpecs,
							   &(copySpecs.cfPaths.cdc),
							   copySpecs.source_pguri,
							   copySpecs.target_pguri,
							   copyDBoptions.slotName,
							   copyDBoptions.origin,
							   copyDBoptions.endpos,
							   STREAM_MODE_PREFETCH))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	/*
	 * First, we need to open a snapshot that we're going to re-use in all our
	 * connections to the source database. When the --snapshot option has been
	 * used, instead of exporting a new snapshot, we can just re-use it.
	 */
	if (!copydb_prepare_snapshot(&copySpecs))
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

	if (!start_clone_process(&copySpecs, &clonePID))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (copySpecs.follow)
	{
		if (!start_follow_process(&copySpecs, &streamSpecs, &followPID))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	/* wait until the clone process is finished */
	bool success =
		cli_clone_follow_wait_subprocess("clone", clonePID);

	/* close our top-level copy db connection and snapshot */
	if (!copydb_close_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/* now wait until the follow process is finished */
	if (copySpecs.follow)
	{
		success = success &&
				  cli_clone_follow_wait_subprocess("follow", followPID);

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
		CopyDataSpec seqSpecs = { 0 };

		/* copy our structure wholesale */
		seqSpecs = copySpecs;

		/* then force some options such as --resume --not-consistent */
		seqSpecs.restart = false;
		seqSpecs.resume = true;
		seqSpecs.consistent = false;
		seqSpecs.section = DATA_SECTION_SET_SEQUENCES;

		/* we don't want to re-use any snapshot */
		TransactionSnapshot snapshot = { 0 };

		seqSpecs.sourceSnapshot = snapshot;

		/* fetch schema information from source catalogs, including filtering */
		if (!copydb_fetch_schema_and_prepare_specs(&seqSpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_TARGET);
		}

		if (!copydb_copy_all_sequences(&seqSpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

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

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   copySpecs.source_pguri,
						   copySpecs.target_pguri,
						   copyDBoptions.slotName,
						   copyDBoptions.origin,
						   copyDBoptions.endpos,
						   STREAM_MODE_PREFETCH))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * First create the replication slot on the source database, and the origin
	 * (replication progress tracking) on the target database.
	 */
	if (!stream_setup_databases(&copySpecs, specs.slotName, specs.origin))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Now remove the possibly still existing stream context files from
	 * previous round of operations (--resume, etc). We want to make sure that
	 * the catchup process reads the files created on this connection.
	 */
	if (!stream_cleanup_context(&specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pid_t prefetch = -1;
	pid_t catchup = -1;

	if (!follow_start_prefetch(&specs, &prefetch))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!follow_start_catchup(&specs, &catchup))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!follow_wait_subprocesses(&specs, prefetch, catchup))
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
			log_error("Failed to fork a subprocess to prefetch changes");
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			log_debug("Starting the clone sub-process");

			if (!cloneDB(copySpecs))
			{
				/* errors have already been logged */
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

		if (!copydb_copy_roles(copySpecs))
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

	log_info("STEP 2: restore the pre-data section to the target database");

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
	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_PREPARE_SCHEMA);

	if (!copydb_target_prepare_schema(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_PREPARE_SCHEMA);

	log_info("STEP 3: copy data from source to target in sub-processes");
	log_info("STEP 4: create indexes and constraints in parallel");
	log_info("STEP 5: vacuum analyze each table");

	if (!copydb_copy_all_table_data(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/* close our snapshot: commit transaction and finish connection */
	(void) copydb_close_snapshot(copySpecs);

	log_info("STEP 7: restore the post-data section to the target database");

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

		if (!pgsql_init(&pgsql, copySpecs->source_pguri, PGSQL_CONN_SOURCE))
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
	/* now we can fork a sub-process to transform the current file */
	pid_t fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to prefetch changes");
			return -1;
		}

		case 0:
		{
			/* child process runs the command */
			log_debug("Starting the follow sub-process");

			if (!followDB(copySpecs, streamSpecs))
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
 * followDB implements a logical decoding client for streaming changes from the
 * source database into the target database.
 */
static bool
followDB(CopyDataSpec *copySpecs, StreamSpecs *streamSpecs)
{
	/*
	 * Create the replication slot on the source database, and the origin
	 * (replication progress tracking) on the target database.
	 */
	if (!stream_setup_databases(copySpecs,
								streamSpecs->slotName,
								streamSpecs->origin))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Remove the possibly still existing stream context files from
	 * previous round of operations (--resume, etc). We want to make sure
	 * that the catchup process reads the files created on this connection.
	 */
	if (!stream_cleanup_context(streamSpecs))
	{
		/* errors have already been logged */
		return false;
	}

	pid_t prefetch = -1;
	pid_t catchup = -1;

	if (!follow_start_prefetch(streamSpecs, &prefetch))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Second, start the catchup process.
	 */
	if (!follow_start_catchup(streamSpecs, &catchup))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Finally wait until the process are finished.
	 *
	 * This happens when the sentinel endpos is set, typically using the
	 * command: pgcopydb stream sentinel set endpos --current.
	 */
	if (!follow_wait_subprocesses(streamSpecs, prefetch, catchup))
	{
		/* errors have already been logged */
		return false;
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
	bool success = true;

	while (!exited)
	{
		int returnCode = -1;

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

		success = success || returnCode == 0;

		/* avoid busy looping, wait for 150ms before checking again */
		pg_usleep(150 * 1000);
	}

	return true;
}
