/*
 * src/bin/pgcopydb/cli_list.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_list.h"
#include "cli_root.h"
#include "commandline.h"
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "string_utils.h"

ListDBOptions listDBoptions = { 0 };

static int cli_list_db_getopts(int argc, char **argv);
static void cli_list_tables(int argc, char **argv);
static void cli_list_indexes(int argc, char **argv);

static CommandLine list_tables_command =
	make_command(
		"tables",
		"List all the source tables to copy data from",
		" --source ... ",
		"  --source          Postgres URI to the source database\n",
		cli_list_db_getopts,
		cli_list_tables);

static CommandLine list_indexes_command =
	make_command(
		"indexes",
		"List all the indexes to create again after copying the data",
		" --source ... [ --schema-name [ --table-name ] ]",
		"  --source          Postgres URI to the source database\n"
		"  --schema-name     Name of the schema where to find the table\n"
		"  --table-name      Name of the target table\n",
		cli_list_db_getopts,
		cli_list_indexes);


static CommandLine *list_subcommands[] = {
	&list_tables_command,
	&list_indexes_command,
	NULL
};

CommandLine list_commands =
	make_command_set("list",
					 "List database objects from a Postgres instance",
					 NULL, NULL, NULL, list_subcommands);


/*
 * cli_list_db_getopts parses the CLI options for the `list db` command.
 */
static int
cli_list_db_getopts(int argc, char **argv)
{
	ListDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "schema", required_argument, NULL, 's' },
		{ "table", required_argument, NULL, 't' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

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

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	listDBoptions = options;

	return optind;
}


/*
 * cli_list_tables implements the command: pglistdb list tables
 */
static void
cli_list_tables(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceTableArray tableArray = { 0, NULL };

	log_info("Listing ordinary tables in \"%s\"",
			 listDBoptions.source_pguri);

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_ordinary_tables(&pgsql, &tableArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d tables", tableArray.count);

	fformat(stdout, "%8s | %20s | %20s | %15s\n",
			"OID", "Schema Name", "Table Name", "Est. Row Count");

	fformat(stdout, "%8s-+-%20s-+-%20s-+-%15s\n",
			"--------",
			"--------------------",
			"--------------------",
			"---------------");

	for (int i = 0; i < tableArray.count; i++)
	{
		fformat(stdout, "%8d | %20s | %20s | %15lld\n",
				tableArray.array[i].oid,
				tableArray.array[i].nspname,
				tableArray.array[i].relname,
				(long long) tableArray.array[i].reltuples);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_indexes implements the command: pglistdb list indexes
 */
static void
cli_list_indexes(int argc, char **argv)
{
	log_fatal("Not Implemented Yet");
	exit(EXIT_CODE_INTERNAL_ERROR);
}
