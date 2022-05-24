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

CopyDBOptions copyDBoptions = { 0 };

static int cli_copy_db_getopts(int argc, char **argv);

static void cli_copy_db(int argc, char **argv);
static void cli_copy_schema(int argc, char **argv);
static void cli_copy_data(int argc, char **argv);
static void cli_copy_table_data(int argc, char **argv);
static void cli_copy_sequences(int argc, char **argv);
static void cli_copy_indexes(int argc, char **argv);
static void cli_copy_constraints(int argc, char **argv);
static void cli_copy_blobs(int argc, char **argv);

static void cli_copy_prepare_specs(CopyDataSpec *copySpecs,
								   CopyDataSection section);


CommandLine copy__db_command =
	make_command(
		"copy-db",
		"Copy an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --dir                 Work directory to use\n"
		"  --table-jobs          Number of concurrent COPY jobs to run\n"
		"  --index-jobs          Number of concurrent CREATE INDEX jobs to run\n"
		"  --drop-if-exists      On the target database, clean-up from a previous run first\n"
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
		cli_copy_db);

/* have pgcopydb copy-db and pgcopydb copy db aliases to each-other */
static CommandLine copy_db_command =
	make_command(
		"db",
		"Copy an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n"
		"  --table-jobs          Number of concurrent COPY jobs to run\n"
		"  --index-jobs          Number of concurrent CREATE INDEX jobs to run\n"
		"  --drop-if-exists      On the target database, clean-up from a previous run first\n"
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
		cli_copy_db);

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
 * cli_copy_db_getopts parses the CLI options for the `copy db` command.
 */
static int
cli_copy_db_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0, errors = 0;
	int verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "jobs", required_argument, NULL, 'J' },
		{ "table-jobs", required_argument, NULL, 'J' },
		{ "index-jobs", required_argument, NULL, 'I' },
		{ "drop-if-exists", no_argument, NULL, 'c' }, /* pg_restore -c */
		{ "no-owner", no_argument, NULL, 'O' },       /* pg_restore -O */
		{ "no-comments", no_argument, NULL, 'X' },
		{ "no-acl", no_argument, NULL, 'x' }, /* pg_restore -x */
		{ "skip-blobs", no_argument, NULL, 'B' },
		{ "skip-large-objects", no_argument, NULL, 'B' },
		{ "filter", required_argument, NULL, 'F' },
		{ "filters", required_argument, NULL, 'F' },
		{ "restart", no_argument, NULL, 'r' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'C' },
		{ "snapshot", required_argument, NULL, 'N' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	/* install default values */
	options.tableJobs = 4;
	options.indexJobs = 4;

	/* read values from the environment */
	if (!cli_copydb_getenv(&options))
	{
		log_fatal("Failed to read default values from the environment");
		exit(EXIT_CODE_BAD_ARGS);
	}

	while ((c = getopt_long(argc, argv, "S:T:D:J:I:cOBrRCN:xXVvqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'S':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --source connection string, "
							  "see above for details.");
					++errors;
				}
				strlcpy(options.source_pguri, optarg, MAXCONNINFO);
				log_trace("--source %s", options.source_pguri);
				break;
			}

			case 'T':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --target connection string, "
							  "see above for details.");
					++errors;
				}
				strlcpy(options.target_pguri, optarg, MAXCONNINFO);
				log_trace("--target %s", options.target_pguri);
				break;
			}

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 'J':
			{
				if (!stringToInt(optarg, &options.tableJobs) ||
					options.tableJobs < 1 ||
					options.tableJobs > 128)
				{
					log_fatal("Failed to parse --jobs count: \"%s\"", optarg);
					++errors;
				}
				log_trace("--table-jobs %d", options.tableJobs);
				break;
			}

			case 'I':
			{
				if (!stringToInt(optarg, &options.indexJobs) ||
					options.indexJobs < 1 ||
					options.indexJobs > 128)
				{
					log_fatal("Failed to parse --index-jobs count: \"%s\"", optarg);
					++errors;
				}
				log_trace("--jobs %d", options.indexJobs);
				break;
			}

			case 'c':
			{
				options.restoreOptions.dropIfExists = true;
				log_trace("--drop-if-exists");
				break;
			}

			case 'O':
			{
				options.restoreOptions.noOwner = true;
				log_trace("--no-owner");
				break;
			}

			case 'x':
			{
				options.restoreOptions.noACL = true;
				log_trace("--no-ack");
				break;
			}

			case 'X':
			{
				options.restoreOptions.noComments = true;
				log_trace("--no-comments");
				break;
			}

			case 'B':
			{
				options.skipLargeObjects = true;
				log_trace("--skip-large-objects");
				break;
			}

			case 'r':
			{
				options.restart = true;
				log_trace("--restart");

				if (options.resume)
				{
					log_fatal("Options --resume and --restart are not compatible");
				}
				break;
			}

			case 'R':
			{
				options.resume = true;
				log_trace("--resume");

				if (options.restart)
				{
					log_fatal("Options --resume and --restart are not compatible");
				}
				break;
			}

			case 'C':
			{
				options.notConsistent = true;
				log_trace("--not-consistent");
				break;
			}

			case 'N':
			{
				strlcpy(options.snapshot, optarg, sizeof(options.snapshot));
				log_trace("--snapshot %s", options.snapshot);
				break;
			}

			case 'F':
			{
				strlcpy(options.filterFileName, optarg, MAXPGPATH);
				log_trace("--filters \"%s\"", options.filterFileName);

				if (!file_exists(options.filterFileName))
				{
					log_error("Filters file \"%s\" does not exists",
							  options.filterFileName);
					++errors;
				}
				break;
			}

			case 'V':
			{
				/* keeper_cli_print_version prints version and exits. */
				cli_print_version(argc, argv);
				break;
			}

			case 'v':
			{
				++verboseCount;
				switch (verboseCount)
				{
					case 1:
					{
						log_set_level(LOG_INFO);
						break;
					}

					case 2:
					{
						log_set_level(LOG_DEBUG);
						break;
					}

					default:
					{
						log_set_level(LOG_TRACE);
						break;
					}
				}
				break;
			}

			case 'q':
			{
				log_set_level(LOG_ERROR);
				break;
			}

			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}
		}
	}

	if (IS_EMPTY_STRING_BUFFER(options.source_pguri) ||
		IS_EMPTY_STRING_BUFFER(options.target_pguri))
	{
		log_fatal("Options --source and --target are mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!cli_copydb_is_consistent(&options))
	{
		log_fatal("Option --resume requires option --not-consistent");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	copyDBoptions = options;

	return optind;
}


/*
 * cli_copy_db implements the command: pgcopydb copy db
 */
static void
cli_copy_db(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_copy_prepare_specs(&copySpecs, DATA_SECTION_ALL);

	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	(void) summary_set_current_time(timings, TIMING_STEP_START);

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
 * cli_copy_prepare_specs initializes our internal data structure that are used
 * to drive the operations.
 */
static void
cli_copy_prepare_specs(CopyDataSpec *copySpecs, CopyDataSection section)
{
	PostgresPaths *pgPaths = &(copySpecs->pgPaths);

	char scrubbedSourceURI[MAXCONNINFO] = { 0 };
	char scrubbedTargetURI[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(copyDBoptions.source_pguri,
											 scrubbedSourceURI);

	(void) parse_and_scrub_connection_string(copyDBoptions.target_pguri,
											 scrubbedTargetURI);

	log_info("[SOURCE] Copying database from \"%s\"", scrubbedSourceURI);
	log_info("[TARGET] Copying database into \"%s\"", scrubbedTargetURI);

	(void) find_pg_commands(pgPaths);

	log_debug("Using pg_dump for Postgres \"%s\" at \"%s\"",
			  copySpecs->pgPaths.pg_version,
			  copySpecs->pgPaths.pg_dump);

	log_debug("Using pg_restore for Postgres \"%s\" at \"%s\"",
			  copySpecs->pgPaths.pg_version,
			  copySpecs->pgPaths.pg_restore);

	char *dir =
		IS_EMPTY_STRING_BUFFER(copyDBoptions.dir)
		? NULL
		: copyDBoptions.dir;

	if (!copydb_init_workdir(copySpecs,
							 dir,
							 copyDBoptions.restart,
							 copyDBoptions.resume))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(copySpecs,
						   copyDBoptions.source_pguri,
						   copyDBoptions.target_pguri,
						   copyDBoptions.tableJobs,
						   copyDBoptions.indexJobs,
						   section,
						   copyDBoptions.snapshot,
						   copyDBoptions.restoreOptions,
						   copyDBoptions.skipLargeObjects,
						   copyDBoptions.restart,
						   copyDBoptions.resume,
						   !copyDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!IS_EMPTY_STRING_BUFFER(copyDBoptions.filterFileName))
	{
		SourceFilters *filters = &(copySpecs->filters);

		if (!parse_filters(copyDBoptions.filterFileName, filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  copyDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}
}
