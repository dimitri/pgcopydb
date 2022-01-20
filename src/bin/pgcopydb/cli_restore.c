/*
 * src/bin/pgcopydb/cli_restore.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_restore.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "log.h"
#include "parsing.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "string_utils.h"

RestoreDBOptions restoreDBoptions = { 0 };

static int cli_restore_schema_getopts(int argc, char **argv);
static void cli_restore_schema(int argc, char **argv);
static void cli_restore_schema_pre_data(int argc, char **argv);
static void cli_restore_schema_post_data(int argc, char **argv);

static void cli_restore_prepare_specs(CopyDataSpec *copySpecs);

static CommandLine restore_schema_command =
	make_command(
		"schema",
		"Restore a database schema from custom files to target database",
		" --source <dir> --target <URI> ",
		"  --source          Directory where to find the schema custom files\n"
		"  --target          Postgres URI to the source database\n"
		"  --drop-if-exists  On the target database, clean-up from a previous run first\n"
		"  --no-owner        Do not set ownership of objects to match the original database\n",
		cli_restore_schema_getopts,
		cli_restore_schema);

static CommandLine restore_schema_pre_data_command =
	make_command(
		"pre-data",
		"Restore a database pre-data schema from custom file to target database",
		" --source <dir> --target <URI> ",
		"  --source          Directory where to find the schema custom files\n"
		"  --target          Postgres URI to the source database\n"
		"  --drop-if-exists  On the target database, clean-up from a previous run first\n"
		"  --no-owner        Do not set ownership of objects to match the original database\n",
		cli_restore_schema_getopts,
		cli_restore_schema_pre_data);

static CommandLine restore_schema_post_data_command =
	make_command(
		"post-data",
		"Restore a database post-data schema from custom file to target database",
		" --source <dir> --target <URI> ",
		"  --source          Directory where to find the schema custom files\n"
		"  --target          Postgres URI to the source database\n"
		"  --no-owner        Do not set ownership of objects to match the original database\n",
		cli_restore_schema_getopts,
		cli_restore_schema_post_data);

static CommandLine *restore_subcommands[] = {
	&restore_schema_command,
	&restore_schema_pre_data_command,
	&restore_schema_post_data_command,
	NULL
};

CommandLine restore_commands =
	make_command_set("restore",
					 "Restore database objects into a Postgres instance",
					 NULL, NULL, NULL, restore_subcommands);


/*
 * cli_restore_schema_getopts parses the CLI options for the `restore db` command.
 */
static int
cli_restore_schema_getopts(int argc, char **argv)
{
	RestoreDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "schema", required_argument, NULL, 's' },
		{ "drop-if-exists", no_argument, NULL, 'c' }, /* pg_restore -c */
		{ "no-owner", no_argument, NULL, 'O' },       /* pg_restore -O */
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "S:T:cOVvqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'S':
			{
				strlcpy(options.source_dir, optarg, MAXPGPATH);
				log_trace("--source %s", options.source_dir);
				break;
			}

			case 'T':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --target connection string, "
							  "see above for details.");
					exit(EXIT_CODE_BAD_ARGS);
				}
				strlcpy(options.target_pguri, optarg, MAXCONNINFO);
				log_trace("--target %s", options.target_pguri);
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

	/* restore commands support the target URI environment variable */
	if (IS_EMPTY_STRING_BUFFER(options.target_pguri))
	{
		if (env_exists(PGCOPYDB_TARGET_PGURI))
		{
			if (!get_env_copy(PGCOPYDB_TARGET_PGURI,
							  options.target_pguri,
							  sizeof(options.target_pguri)))
			{
				/* errors have already been logged */
				++errors;
			}
		}
	}

	if (IS_EMPTY_STRING_BUFFER(options.target_pguri))
	{
		log_fatal("Option --target is mandatory");
		++errors;
	}

	/* when --drop-if-exists has not been used, check PGCOPYDB_DROP_IF_EXISTS */
	if (!options.dropIfExists)
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
			else if (!parse_bool(DROP_IF_EXISTS, &(options.dropIfExists)))
			{
				log_error("Failed to parse environment variable \"%s\" "
						  "value \"%s\", expected a boolean (on/off)",
						  PGCOPYDB_DROP_IF_EXISTS,
						  DROP_IF_EXISTS);
				++errors;
			}
		}
	}

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	restoreDBoptions = options;

	return optind;
}


/*
 * cli_restore_schema implements the command: pgrestoredb restore schema
 */
static void
cli_restore_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_target_finalize_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_restore_schema implements the command: pgrestoredb restore pre-data
 */
static void
cli_restore_schema_pre_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_restore_schema implements the command: pgrestoredb restore post-data
 */
static void
cli_restore_schema_post_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	if (!copydb_target_finalize_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_restore_prepare_specs prepares the CopyDataSpecs needed to drive the
 * restore commands.
 */
static void
cli_restore_prepare_specs(CopyDataSpec *copySpecs)
{
	CopyFilePaths *cfPaths = &(copySpecs->cfPaths);
	PostgresPaths *pgPaths = &(copySpecs->pgPaths);

	(void) find_pg_commands(pgPaths);

	char *dir =
		IS_EMPTY_STRING_BUFFER(restoreDBoptions.source_dir)
		? NULL
		: restoreDBoptions.source_dir;

	bool removeDir = false;

	if (!copydb_init_workdir(cfPaths, dir, removeDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(copySpecs,
						   NULL, /* source_pguri */
						   restoreDBoptions.target_pguri,
						   1,    /* table jobs */
						   1,    /* index jobs */
						   DATA_SECTION_NONE,
						   restoreDBoptions.dropIfExists,
						   restoreDBoptions.noOwner,
						   false)) /* skipLargeObjects */
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Restoring database from \"%s\"", cfPaths->topdir);
	log_info("Restoring database into \"%s\"", copySpecs->target_pguri);

	log_info("Using pg_restore for Postgres \"%s\" at \"%s\"",
			 pgPaths->pg_version,
			 pgPaths->pg_restore);
}
