/*
 * src/bin/pgcopydb/cli_dump.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_dump.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "string_utils.h"

DumpDBOptions dumpDBoptions = { 0 };

static int cli_dump_schema_getopts(int argc, char **argv);
static void cli_dump_schema(int argc, char **argv);
static void cli_dump_schema_pre_data(int argc, char **argv);
static void cli_dump_schema_post_data(int argc, char **argv);

static void cli_dump_schema_section(DumpDBOptions *dumpDBoptions,
									PostgresDumpSection section);

static CommandLine dump_schema_command =
	make_command(
		"schema",
		"Dump source database schema as custom files in target directory",
		" --source <URI> --target <dir> ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n",
		cli_dump_schema_getopts,
		cli_dump_schema);

static CommandLine dump_schema_pre_data_command =
	make_command(
		"pre-data",
		"Dump source database pre-data schema as custom files in target directory",
		" --source <URI> --target <dir> ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n",
		cli_dump_schema_getopts,
		cli_dump_schema_pre_data);

static CommandLine dump_schema_post_data_command =
	make_command(
		"post-data",
		"Dump source database post-data schema as custom files in target directory",
		" --source <URI> --target <dir>",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n",
		cli_dump_schema_getopts,
		cli_dump_schema_post_data);

static CommandLine *dump_subcommands[] = {
	&dump_schema_command,
	&dump_schema_pre_data_command,
	&dump_schema_post_data_command,
	NULL
};

CommandLine dump_commands =
	make_command_set("dump",
					 "Dump database objects from a Postgres instance",
					 NULL, NULL, NULL, dump_subcommands);


/*
 * cli_dump_schema_getopts parses the CLI options for the `dump db` command.
 */
static int
cli_dump_schema_getopts(int argc, char **argv)
{
	DumpDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "S:T:Vvqh",
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
					exit(EXIT_CODE_BAD_ARGS);
				}
				strlcpy(options.source_pguri, optarg, MAXCONNINFO);
				log_trace("--source %s", options.source_pguri);
				break;
			}

			case 'T':
			{
				strlcpy(options.target_dir, optarg, MAXPGPATH);
				log_trace("--target %s", options.target_dir);
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

	/* dump commands support the source URI environment variable */
	if (IS_EMPTY_STRING_BUFFER(options.source_pguri))
	{
		if (env_exists(PGCOPYDB_SOURCE_PGURI))
		{
			if (!get_env_copy(PGCOPYDB_SOURCE_PGURI,
							  options.source_pguri,
							  sizeof(options.source_pguri)))
			{
				/* errors have already been logged */
				++errors;
			}
		}
	}

	if (IS_EMPTY_STRING_BUFFER(options.source_pguri))
	{
		log_fatal("Option --source is mandatory");
		++errors;
	}

	if (IS_EMPTY_STRING_BUFFER(options.target_dir))
	{
		log_fatal("Option --target is mandatory");
		++errors;
	}

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	dumpDBoptions = options;

	return optind;
}


/*
 * cli_dump_schema implements the command: pgcopydb dump schema
 */
static void
cli_dump_schema(int argc, char **argv)
{
	(void) cli_dump_schema_section(&dumpDBoptions, PG_DUMP_SECTION_SCHEMA);
}


/*
 * cli_dump_schema implements the command: pgcopydb dump pre-data
 */
static void
cli_dump_schema_pre_data(int argc, char **argv)
{
	(void) cli_dump_schema_section(&dumpDBoptions, PG_DUMP_SECTION_PRE_DATA);
}


/*
 * cli_dump_schema implements the command: pgcopydb dump post-data
 */
static void
cli_dump_schema_post_data(int argc, char **argv)
{
	(void) cli_dump_schema_section(&dumpDBoptions, PG_DUMP_SECTION_POST_DATA);
}


/*
 * cli_dump_schema_section implements the actual work for the commands in this
 * file.
 */
static void
cli_dump_schema_section(DumpDBOptions *dumpDBoptions,
						PostgresDumpSection section)
{
	CopyFilePaths cfPaths = { 0 };
	PostgresPaths pgPaths = { 0 };

	log_info("Dumping database from \"%s\"", dumpDBoptions->source_pguri);
	log_info("Dumping database into directory \"%s\"", dumpDBoptions->target_dir);

	(void) find_pg_commands(&pgPaths);

	if (!copydb_init_workdir(&cfPaths, dumpDBoptions->target_dir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Using pg_dump for Postgres \"%s\" at \"%s\"",
			 pgPaths.pg_version,
			 pgPaths.pg_dump);

	CopyDataSpec copySpecs = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   &cfPaths,
						   &pgPaths,
						   dumpDBoptions->source_pguri,
						   NULL, /* target_pguri */
						   1,    /* table jobs */
						   1,    /* index jobs */
						   false, /* dropIfExists */
						   false)) /* noOwner */
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_dump_source_schema(&copySpecs, section))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}
