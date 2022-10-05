/*
 * src/bin/pgcopydb/cli_dump.c
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
#include "pgcmd.h"
#include "pgsql.h"
#include "string_utils.h"

CopyDBOptions dumpDBoptions = { 0 };

static int cli_dump_schema_getopts(int argc, char **argv);
static void cli_dump_schema(int argc, char **argv);
static void cli_dump_schema_pre_data(int argc, char **argv);
static void cli_dump_schema_post_data(int argc, char **argv);
static void cli_dump_roles(int argc, char **argv);

static void cli_dump_schema_section(CopyDBOptions *dumpDBoptions,
									PostgresDumpSection section);

static CommandLine dump_schema_command =
	make_command(
		"schema",
		"Dump source database schema as custom files in work directory",
		" --source <URI> ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n"
		"  --dir             Work directory to use\n"
		"  --snapshot        Use snapshot obtained with pg_export_snapshot\n",
		cli_dump_schema_getopts,
		cli_dump_schema);

static CommandLine dump_schema_pre_data_command =
	make_command(
		"pre-data",
		"Dump source database pre-data schema as custom files in work directory",
		" --source <URI> ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n"
		"  --dir             Work directory to use\n"
		"  --snapshot        Use snapshot obtained with pg_export_snapshot\n",
		cli_dump_schema_getopts,
		cli_dump_schema_pre_data);

static CommandLine dump_schema_post_data_command =
	make_command(
		"post-data",
		"Dump source database post-data schema as custom files in work directory",
		" --source <URI>",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n"
		"  --dir             Work directory to use\n"
		"  --snapshot        Use snapshot obtained with pg_export_snapshot\n",
		cli_dump_schema_getopts,
		cli_dump_schema_post_data);

static CommandLine dump_roles_command =
	make_command(
		"roles",
		"Dump source database roles as custome file in work directory",
		" --source <URI>",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n"
		"  --dir             Work directory to use\n",
		cli_dump_schema_getopts,
		cli_dump_roles);

static CommandLine *dump_subcommands[] = {
	&dump_schema_command,
	&dump_schema_pre_data_command,
	&dump_schema_post_data_command,
	&dump_roles_command,
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
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "restart", no_argument, NULL, 'r' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'C' },
		{ "snapshot", required_argument, NULL, 'N' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "debug", no_argument, NULL, 'd' },
		{ "trace", no_argument, NULL, 'z' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	/* read values from the environment */
	if (!cli_copydb_getenv(&options))
	{
		log_fatal("Failed to read default values from the environment");
		exit(EXIT_CODE_BAD_ARGS);
	}

	while ((c = getopt_long(argc, argv, "S:T:Vvdzqh",
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

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 'r':
			{
				options.restart = true;
				log_trace("--restart");
				break;
			}

			case 'R':
			{
				options.resume = true;
				log_trace("--resume");
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
						log_set_level(LOG_NOTICE);
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

			case 'd':
			{
				verboseCount = 2;
				log_set_level(LOG_DEBUG);
				break;
			}

			case 'z':
			{
				verboseCount = 3;
				log_set_level(LOG_TRACE);
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

	if (IS_EMPTY_STRING_BUFFER(options.source_pguri))
	{
		log_fatal("Option --source is mandatory");
		++errors;
	}

	if (!cli_copydb_is_consistent(&options))
	{
		log_fatal("Option --resume requires option --not-consistent");
		exit(EXIT_CODE_BAD_ARGS);
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
 * cli_dump_roles implements the command: pgcopydb dump roles
 */
static void
cli_dump_roles(int argc, char **argv)
{
	(void) cli_dump_schema_section(&dumpDBoptions, PG_DUMP_SECTION_ROLES);
}


/*
 * cli_dump_schema_section implements the actual work for the commands in this
 * file.
 */
static void
cli_dump_schema_section(CopyDBOptions *dumpDBoptions,
						PostgresDumpSection section)
{
	CopyDataSpec copySpecs = { 0 };

	CopyFilePaths *cfPaths = &(copySpecs.cfPaths);
	PostgresPaths *pgPaths = &(copySpecs.pgPaths);

	(void) find_pg_commands(pgPaths);

	char *dir =
		IS_EMPTY_STRING_BUFFER(dumpDBoptions->dir)
		? NULL
		: dumpDBoptions->dir;

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 dumpDBoptions->restart,
							 dumpDBoptions->resume,
							 false))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   dumpDBoptions->source_pguri,
						   NULL, /* target_pguri */
						   1,    /* table jobs */
						   1,    /* index jobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_NONE,
						   dumpDBoptions->snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   dumpDBoptions->restart,
						   dumpDBoptions->resume,
						   !dumpDBoptions->notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	char scrubbedSourceURI[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(copySpecs.source_pguri,
											 scrubbedSourceURI);

	log_info("Dumping database from \"%s\"", scrubbedSourceURI);
	log_info("Dumping database into directory \"%s\"", cfPaths->topdir);

	if (section == PG_DUMP_SECTION_ROLES)
	{
		log_info("Using pg_dumpall for Postgres \"%s\" at \"%s\"",
				 pgPaths->pg_version,
				 pgPaths->pg_dump);
	}
	else
	{
		log_info("Using pg_dump for Postgres \"%s\" at \"%s\"",
				 pgPaths->pg_version,
				 pgPaths->pg_dumpall);
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

	if (section == PG_DUMP_SECTION_ROLES)
	{
		if (!pg_dumpall_roles(&(copySpecs.pgPaths),
							  copySpecs.source_pguri,
							  copySpecs.dumpPaths.rolesFilename))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		if (!copydb_dump_source_schema(&copySpecs,
									   copySpecs.sourceSnapshot.snapshot,
									   section))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	if (!copydb_close_snapshot(&copySpecs))
	{
		log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
				  copySpecs.sourceSnapshot.snapshot,
				  copySpecs.sourceSnapshot.pguri);
		exit(EXIT_CODE_SOURCE);
	}
}
