/*
 * src/bin/pgcopydb/cli_copy.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_copy.h"
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

static bool cli_copydb_getenv(CopyDBOptions *options);
static int cli_copy_db_getopts(int argc, char **argv);
static void cli_copy_db(int argc, char **argv);


CommandLine copy_db_command =
	make_command(
		"copy-db",
		"Copy an entire database from source to target",
		" --source ... --target ... [ --table-jobs ... --index-jobs ... ] ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Postgres URI to the target database\n"
		"  --table-jobs      Number of concurrent COPY jobs to run\n"
		"  --index-jobs      Number of concurrent CREATE INDEX jobs to run\n"
		"  --drop-if-exists  On the target database, clean-up from a previous run first\n"
		"  --no-owner        Do not set ownership of objects to match the original database\n",
		cli_copy_db_getopts,
		cli_copy_db);


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
		{ "jobs", required_argument, NULL, 'J' },
		{ "table-jobs", required_argument, NULL, 'J' },
		{ "index-jobs", required_argument, NULL, 'I' },
		{ "drop-if-exists", no_argument, NULL, 'c' }, /* pg_restore -c */
		{ "no-owner", no_argument, NULL, 'O' },       /* pg_restore -O */
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

	while ((c = getopt_long(argc, argv, "S:T:J:I:cOVvqh",
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
				options.dropIfExists = true;
				log_trace("--drop-if-exists");
				break;
			}

			case 'O':
			{
				options.noOwner = true;
				log_trace("--no-owner");
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
 * cli_copydb_getenv reads from the environment variables and fills-in the
 * command line options.
 */
static bool
cli_copydb_getenv(CopyDBOptions *options)
{
	int errors = 0;

	/* now some of the options can be also set from the environment */
	if (env_exists(PGCOPYDB_SOURCE_PGURI))
	{
		if (!get_env_copy(PGCOPYDB_SOURCE_PGURI,
						  options->source_pguri,
						  sizeof(options->source_pguri)))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (env_exists(PGCOPYDB_TARGET_PGURI))
	{
		if (!get_env_copy(PGCOPYDB_TARGET_PGURI,
						  options->target_pguri,
						  sizeof(options->target_pguri)))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (env_exists(PGCOPYDB_TARGET_TABLE_JOBS))
	{
		char jobs[BUFSIZE] = { 0 };

		if (get_env_copy(PGCOPYDB_TARGET_TABLE_JOBS, jobs, sizeof(jobs)))
		{
			if (!stringToInt(jobs, &options->tableJobs) ||
				options->tableJobs < 1 ||
				options->tableJobs > 128)
			{
				log_fatal("Failed to parse PGCOPYDB_TARGET_TABLE_JOBS: \"%s\"",
						  jobs);
				++errors;
			}
		}
		else
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (env_exists(PGCOPYDB_TARGET_INDEX_JOBS))
	{
		char jobs[BUFSIZE] = { 0 };

		if (get_env_copy(PGCOPYDB_TARGET_INDEX_JOBS, jobs, sizeof(jobs)))
		{
			if (!stringToInt(jobs, &options->indexJobs) ||
				options->indexJobs < 1 ||
				options->indexJobs > 128)
			{
				log_fatal("Failed to parse PGCOPYDB_TARGET_INDEX_JOBS: \"%s\"",
						  jobs);
				++errors;
			}
		}
		else
		{
			/* errors have already been logged */
			++errors;
		}
	}

	/* when --drop-if-exists has not been used, check PGCOPYDB_DROP_IF_EXISTS */
	if (!options->dropIfExists)
	{
		if (env_exists(PGCOPYDB_DROP_IF_EXISTS))
		{
			char DROP_IF_EXISTS[BUFSIZE] = { 0 };

			if (!get_env_copy(PGCOPYDB_DROP_IF_EXISTS,
							  DROP_IF_EXISTS,
							  sizeof(DROP_IF_EXISTS)))
			{
				/* errors have already been logged */
				++errors;
			}
			else if (!parse_bool(DROP_IF_EXISTS, &(options->dropIfExists)))
			{
				log_error("Failed to parse environment variable \"%s\" "
						  "value \"%s\", expected a boolean (on/off)",
						  PGCOPYDB_DROP_IF_EXISTS,
						  DROP_IF_EXISTS);
				++errors;
			}
		}
	}

	return errors == 0;
}


/*
 * cli_copy_db implements the command: pgcopydb copy db
 */
static void
cli_copy_db(int argc, char **argv)
{
	Summary summary = { 0 };
	TopLevelTimings *timings = &(summary.timings);

	CopyFilePaths cfPaths = { 0 };
	PostgresPaths pgPaths = { 0 };

	(void) summary_set_current_time(timings, TIMING_STEP_START);

	log_info("[SOURCE] Copying database from \"%s\"", copyDBoptions.source_pguri);
	log_info("[TARGET] Copying database into \"%s\"", copyDBoptions.target_pguri);

	(void) find_pg_commands(&pgPaths);

	if (!copydb_init_workdir(&cfPaths, NULL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	CopyDataSpec copySpecs = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   &cfPaths,
						   &pgPaths,
						   copyDBoptions.source_pguri,
						   copyDBoptions.target_pguri,
						   copyDBoptions.tableJobs,
						   copyDBoptions.indexJobs,
						   copyDBoptions.dropIfExists,
						   copyDBoptions.noOwner))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("STEP 1: dump the source database schema (pre/post data)");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_SCHEMA_DUMP);

	if (!copydb_dump_source_schema(&copySpecs, PG_DUMP_SECTION_SCHEMA))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("STEP 2: restore the pre-data section to the target database");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_PREPARE_SCHEMA);

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_PREPARE_SCHEMA);

	log_info("STEP 3: copy data from source to target in sub-processes");
	log_info("STEP 4: create indexes and constraints in parallel");
	log_info("STEP 5: vacuum analyze each table");

	if (!copydb_copy_all_table_data(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("STEP 6: restore the post-data section to the target database");

	(void) summary_set_current_time(timings, TIMING_STEP_BEFORE_FINALIZE_SCHEMA);

	if (!copydb_target_finalize_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	(void) summary_set_current_time(timings, TIMING_STEP_AFTER_FINALIZE_SCHEMA);

	(void) print_summary(&summary, &copySpecs);
}
