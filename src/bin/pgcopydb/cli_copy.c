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
#include "parsing_utils.h"
#include "pgsql.h"
#include "string_utils.h"
#include "summary.h"

static void cli_copy_roles(int argc, char **argv);
static void cli_copy_extensions(int argc, char **argv);
static void cli_copy_schema(int argc, char **argv);
static void cli_copy_data(int argc, char **argv);
static void cli_copy_table_data(int argc, char **argv);
static void cli_copy_sequences(int argc, char **argv);
static void cli_copy_indexes(int argc, char **argv);
static void cli_copy_constraints(int argc, char **argv);
static void cli_copy_blobs(int argc, char **argv);

static CommandLine copy_db_command =
	make_command(
		"db",
		"Copy an entire database from source to target",
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
		"  --snapshot            Use snapshot obtained with pg_export_snapshot\n",
		cli_copy_db_getopts,
		cli_clone);

static CommandLine copy_schema_command =
	make_command(
		"schema",
		"Copy the database schema from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n"
		"  --filters <filename>  Use the filters defined in <filename>\n"
		"  --restart             Allow restarting when temp files exist already\n"
		"  --resume              Allow resuming operations after a failure\n"
		"  --not-consistent      Allow taking a new snapshot on the source database\n"
		"  --snapshot            Use snapshot obtained with pg_export_snapshot\n",
		cli_copy_db_getopts,
		cli_copy_schema);

static CommandLine copy_roles_command =
	make_command(
		"roles",
		"Copy the roles from the source instance to the target instance",
		" --source ... --target ... ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n",
		cli_copy_db_getopts,
		cli_copy_roles);

static CommandLine copy_extensions_command =
	make_command(
		"extensions",
		"Copy the extensions from the source instance to the target instance",
		" --source ... --target ... ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n",
		cli_copy_db_getopts,
		cli_copy_extensions);

/*
 * pgcopydb copy data does the data section only, skips pre-data and post-data
 * both.
 */
static CommandLine copy_data_command =
	make_command(
		"data",
		"Copy the data section from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n"
		"  --table-jobs          Number of concurrent COPY jobs to run\n"
		"  --index-jobs          Number of concurrent CREATE INDEX jobs to run\n"
		"  --skip-large-objects  Skip copying large objects (blobs)\n"
		"  --filters <filename>  Use the filters defined in <filename>\n"
		"  --restart             Allow restarting when temp files exist already\n"
		"  --resume              Allow resuming operations after a failure\n"
		"  --not-consistent      Allow taking a new snapshot on the source database\n"
		"  --snapshot            Use snapshot obtained with pg_export_snapshot\n",
		cli_copy_db_getopts,
		cli_copy_data);

static CommandLine copy_table_data_command =
	make_command(
		"table-data",
		"Copy the data from all tables in database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --table-jobs         Number of concurrent COPY jobs to run\n"
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n"
		"  --snapshot           Use snapshot obtained with pg_export_snapshot\n",
		cli_copy_db_getopts,
		cli_copy_table_data);

static CommandLine copy_blobs_command =
	make_command(
		"blobs",
		"Copy the blob data from ther source database to the target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Postgres URI to the target database\n"
		"  --dir             Work directory to use\n"
		"  --drop-if-exists  On the target database, drop and create large objects\n"
		"  --restart         Allow restarting when temp files exist already\n"
		"  --resume          Allow resuming operations after a failure\n"
		"  --not-consistent  Allow taking a new snapshot on the source database\n"
		"  --snapshot        Use snapshot obtained with pg_export_snapshot\n",
		cli_copy_db_getopts,
		cli_copy_blobs);

static CommandLine copy_sequence_command =
	make_command(
		"sequences",
		"Copy the current value from all sequences in database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n"
		"  --snapshot           Use snapshot obtained with pg_export_snapshot\n",
		cli_copy_db_getopts,
		cli_copy_sequences);

static CommandLine copy_indexes_command =
	make_command(
		"indexes",
		"Create all the indexes found in the source database in the target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --index-jobs         Number of concurrent CREATE INDEX jobs to run\n"
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n",
		cli_copy_db_getopts,
		cli_copy_indexes);

static CommandLine copy_constraints_command =
	make_command(
		"constraints",
		"Create all the constraints found in the source database in the target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n",
		cli_copy_db_getopts,
		cli_copy_constraints);

static CommandLine *copy_subcommands[] = {
	&copy_db_command,
	&copy_roles_command,
	&copy_extensions_command,
	&copy_schema_command,
	&copy_data_command,
	&copy_table_data_command,
	&copy_blobs_command,
	&copy_sequence_command,
	&copy_indexes_command,
	&copy_constraints_command,
	NULL
};

CommandLine copy_commands =
	make_command_set("copy",
					 "Implement the data section of the database copy",
					 NULL, NULL, NULL, copy_subcommands);


/*
 * cli_copy_schema implements the command: pgcopydb copy schema
 */
static void
cli_copy_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_SCHEMA);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

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

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	/* now close the snapshot we kept for the whole operation */
	(void) copydb_close_snapshot(&copySpecs);

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_PREPARE_SCHEMA);

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_PREPARE_SCHEMA);

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
 * cli_copy_data implements the data section of the pgcopydb program, skipping
 * the pre-data and post-data operations on the schema. It expects the tables
 * to have already been created (empty) on the target database.
 *
 * It could creatively be used to federate/merge data from different sources
 * all into the same single target instance, too.
 */
static void
cli_copy_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

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

	log_info("Copy data from source to target in sub-processes");
	log_info("Create indexes and constraints in parallel");
	log_info("Vacuum analyze each table");

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_copy_all_table_data(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_END);
	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_copy_table_data implements only the TABLE DATA parts of the pg_dump |
 * pg_restore job, using our own internal COPY based implementation to avoid
 * the need to spill to disk.
 */
static void
cli_copy_table_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_TABLE_DATA);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

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

	log_info("Copy data from source to target in sub-processes");

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_copy_all_table_data(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_END);
	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_copy_sequences implements the SEQUENCE SET parts of the pg_dump |
 * pg_restore job, using our own internal implementation for it, as pg_dump
 * considers SEQUENCE SET operations parts of the data section, and thus it's
 * not possible to set sequences without also dumping the whole content of the
 * source database.
 */
static void
cli_copy_sequences(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_SET_SEQUENCES);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

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

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_copy_all_sequences(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_END);
	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_copy_indexes implements only the CREATE INDEX parts of the whole copy
 * operations.
 */
static void
cli_copy_indexes(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_INDEXES);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

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

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_copy_all_indexes(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_END);
	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_copy_constraints implements only the ALTER TABLE ... ADD CONSTRAINT
 * parts of the whole copy operations. The tables and indexes should have
 * already been created before hand.
 */
static void
cli_copy_constraints(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_CONSTRAINTS);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

	log_info("Create constraints");

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

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_copy_all_indexes(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_END);
	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_copy_blobs copies the large object data from the source to the target
 * database instanceds, preserving the OIDs.
 */
static void
cli_copy_blobs(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_BLOBS);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

	log_info("Copy large objects");

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

	if (!copydb_copy_blobs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_END);
	(void) print_summary(&summary, &copySpecs);
}


/*
 * cli_copy_roles copies the roles found on the source instance to the target
 * instance, skipping those that already exist on the target instance.
 */
static void
cli_copy_roles(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_SCHEMA);

	if (!pg_copy_roles(&(copySpecs.pgPaths),
					   copySpecs.source_pguri,
					   copySpecs.target_pguri,
					   copySpecs.dumpPaths.rolesFilename))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_copy_extensions copies the extensions found on the source instance to
 * the target instance, skipping those that already exist on the target
 * instance.
 *
 * The command also copies the schemas that the extensions depend on, the
 * extnamespace column in the pg_extension catalog, using pg_dump and
 * pg_restore for them.
 *
 * In most cases, CREATE EXTENSION requires superuser. It might be best to then
 * implement:
 *
 *  1. pgcopydb snapshot &
 *  2. pgcopydb copy extensions --target <superuser connection>
 *  3. pgcopydb clone
 *
 */
static void
cli_copy_extensions(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_EXTENSION);

	if (!copydb_prepare_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* fetch schema information from source catalogs, including filtering */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		(void) copydb_close_snapshot(&copySpecs);
		exit(EXIT_CODE_TARGET);
	}

	bool createExtensions = true;

	if (!copydb_copy_extensions(&copySpecs, createExtensions))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	/* now close the snapshot we kept for the whole operation */
	(void) copydb_close_snapshot(&copySpecs);
}
