/*
 * src/bin/pgcopydb/cli_copy.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "log.h"
#include "parsing.h"
#include "pgsql.h"
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


/*
 * cli_clone implements the command: pgcopydb clone
 */
void
cli_clone(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

	if (copySpecs.roles)
	{
		log_info("STEP 0: copy the source database roles");

		if (!copydb_copy_roles(&copySpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	log_info("STEP 1: dump the source database schema (pre/post data)");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_SCHEMA_DUMP);

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

	if (!copydb_dump_source_schema(&copySpecs,
								   copySpecs.sourceSnapshot.snapshot,
								   PG_DUMP_SECTION_SCHEMA))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("STEP 2: restore the pre-data section to the target database");

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_PREPARE_SCHEMA);

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_PREPARE_SCHEMA);

	log_info("STEP 3: copy data from source to target in sub-processes");
	log_info("STEP 4: create indexes and constraints in parallel");
	log_info("STEP 5: vacuum analyze each table");

	if (!copydb_copy_all_table_data(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* now close the snapshot we kept for the whole operation */
	(void) copydb_close_snapshot(&copySpecs);

	log_info("STEP 7: restore the post-data section to the target database");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_FINALIZE_SCHEMA);

	if (!copydb_target_finalize_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_FINALIZE_SCHEMA);
	(void) summary_set_current_time(timings, TIMING_STEP_END);

	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_follow implements the command: pgcopydb follow
 */
void
cli_follow(int argc, char **argv)
{
	log_fatal("pgcopydb follow is not implemented yet");
	exit(EXIT_CODE_INTERNAL_ERROR);
}
