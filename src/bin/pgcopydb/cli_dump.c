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
#include "commandline.h"
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "string_utils.h"

DumpDBOptions dumpDBoptions = { 0 };

static int cli_dump_db_getopts(int argc, char **argv);
static void cli_dump_db(int argc, char **argv);

static CommandLine dump_db_command =
	make_command(
		"db",
		"Dump an entire database from source to target (schema only)",
		" --source ... --target ... ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n",
		cli_dump_db_getopts,
		cli_dump_db);

static CommandLine dump_table_command =
	make_command(
		"table",
		"Dump a given table from source to target (schema only)",
		" --source ... --target ... ",
		"  --source          Postgres URI to the source database\n"
		"  --target          Directory where to save the dump files\n"
		"  --schema-name     Name of the schema where to find the table\n"
		"  --table-name      Name of the target table\n",
		cli_dump_db_getopts,
		cli_dump_db);


static CommandLine *dump_subcommands[] = {
	&dump_db_command,
	&dump_table_command,
	NULL
};

CommandLine dump_commands =
	make_command_set("dump",
					 "Dump database objects from a Postgres instance",
					 NULL, NULL, NULL, dump_subcommands);


/*
 * cli_dump_db_getopts parses the CLI options for the `dump db` command.
 */
static int
cli_dump_db_getopts(int argc, char **argv)
{
	DumpDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "schema", required_argument, NULL, 's' },
		{ "table", required_argument, NULL, 't' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	/* install default values */
	strlcpy(options.schema_name, "public", NAMEDATALEN);

	while ((c = getopt_long(argc, argv, "S:T:j:s:t:Vvqh",
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

			case 's':
			{
				strlcpy(options.schema_name, optarg, NAMEDATALEN);
				log_trace("--schema %s", options.schema_name);
				break;
			}

			case 't':
			{
				strlcpy(options.table_name, optarg, NAMEDATALEN);
				log_trace("--table %s", options.table_name);
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
 * cli_dump_db implements the command: pgdumpdb dump db
 */
static void
cli_dump_db(int argc, char **argv)
{
	PostgresPaths pgPaths = { 0 };

	log_info("Dumping database from \"%s\"", dumpDBoptions.source_pguri);
	log_info("Dumping database into directory \"%s\"", dumpDBoptions.target_dir);

	(void) find_pg_commands(&pgPaths);

	log_info("Using pg_dump for Postgres \"%s\" at \"%s\"",
			 pgPaths.pg_version,
			 pgPaths.pg_dump);

	char *dir = dumpDBoptions.target_dir;
	char preFilename[MAXPGPATH] = { 0 };
	char postFilename[MAXPGPATH] = { 0 };

	if (!directory_exists(dir))
	{
		log_debug("mkdir -p \"%s\"", dir);
		if (!ensure_empty_dir(dir, 0700))
		{
			/* errors have already been logged. */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	sformat(preFilename, MAXPGPATH, "%s/%s", dir, "pre.dump");
	sformat(postFilename, MAXPGPATH, "%s/%s", dir, "post.dump");

	if (!pg_dump_db(&pgPaths,
					dumpDBoptions.source_pguri,
					"pre-data",
					preFilename))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pg_dump_db(&pgPaths,
					dumpDBoptions.source_pguri,
					"post-data",
					postFilename))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}
