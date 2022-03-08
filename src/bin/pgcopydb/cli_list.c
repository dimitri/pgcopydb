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
#include "env_utils.h"
#include "log.h"
#include "parsing.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "string_utils.h"

ListDBOptions listDBoptions = { 0 };

static int cli_list_db_getopts(int argc, char **argv);
static void cli_list_tables(int argc, char **argv);
static void cli_list_sequences(int argc, char **argv);
static void cli_list_indexes(int argc, char **argv);

static CommandLine list_tables_command =
	make_command(
		"tables",
		"List all the source tables to copy data from",
		" --source ... ",
		"  --source          Postgres URI to the source database\n"
		"  --without-pkey    List only tables that have no primary key\n",
		cli_list_db_getopts,
		cli_list_tables);

static CommandLine list_sequences_command =
	make_command(
		"sequences",
		"List all the source sequences to copy data from",
		" --source ... ",
		"  --source          Postgres URI to the source database\n",
		cli_list_db_getopts,
		cli_list_sequences);

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
	&list_sequences_command,
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
		{ "schema-name", required_argument, NULL, 's' },
		{ "table-name", required_argument, NULL, 't' },
		{ "without-pkey", no_argument, NULL, 'P' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "S:T:j:s:t:PVvqh",
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

			case 'P':
			{
				options.noPKey = true;
				log_trace("--without-pkey");
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

	/* list commands support the source URI environment variable */
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

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (listDBoptions.noPKey)
	{
		log_info("Listing tables without primary key in \"%s\"",
				 listDBoptions.source_pguri);

		if (!schema_list_ordinary_tables_without_pk(&pgsql, &tableArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		log_info("Listing ordinary tables in \"%s\"",
				 listDBoptions.source_pguri);

		if (!schema_list_ordinary_tables(&pgsql, &tableArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	log_info("Fetched information for %d tables", tableArray.count);

	fformat(stdout, "%8s | %20s | %20s | %15s | %15s\n",
			"OID", "Schema Name", "Table Name",
			"Est. Row Count", "On-disk size");

	fformat(stdout, "%8s-+-%20s-+-%20s-+-%15s-+-%15s\n",
			"--------",
			"--------------------",
			"--------------------",
			"---------------",
			"---------------");

	for (int i = 0; i < tableArray.count; i++)
	{
		fformat(stdout, "%8d | %20s | %20s | %15lld | %15s\n",
				tableArray.array[i].oid,
				tableArray.array[i].nspname,
				tableArray.array[i].relname,
				(long long) tableArray.array[i].reltuples,
				tableArray.array[i].bytesPretty);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_tables implements the command: pglistdb list tables
 */
static void
cli_list_sequences(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceSequenceArray sequenceArray = { 0, NULL };

	log_info("Listing ordinary sequences in \"%s\"",
			 listDBoptions.source_pguri);

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_sequences(&pgsql, &sequenceArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d sequences", sequenceArray.count);

	fformat(stdout, "%8s | %20s | %30s\n",
			"OID", "Schema Name", "Sequence Name");

	fformat(stdout, "%8s-+-%20s-+-%30s\n",
			"--------",
			"--------------------",
			"------------------------------");

	for (int i = 0; i < sequenceArray.count; i++)
	{
		fformat(stdout, "%8d | %20s | %30s\n",
				sequenceArray.array[i].oid,
				sequenceArray.array[i].nspname,
				sequenceArray.array[i].relname);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_indexes implements the command: pglistdb list indexes
 */
static void
cli_list_indexes(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceIndexArray indexArray = { 0, NULL };

	char scrubbedSourceURI[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(listDBoptions.source_pguri,
											 scrubbedSourceURI);

	log_info("Listing indexes in \"%s\"", scrubbedSourceURI);

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.table_name) &&
		IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name))
	{
		log_info("Fetching all indexes in source database");

		if (!schema_list_all_indexes(&pgsql, &indexArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else if (IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name) &&
			 !IS_EMPTY_STRING_BUFFER(listDBoptions.table_name))
	{
		log_info("Fetching all indexes for table \"public\".\"%s\"",
				 listDBoptions.table_name);

		if (!schema_list_table_indexes(&pgsql,
									   "public",
									   listDBoptions.table_name,
									   &indexArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else if (!IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name) &&
			 !IS_EMPTY_STRING_BUFFER(listDBoptions.table_name))
	{
		log_info("Fetching all indexes for table \"%s\".\"%s\"",
				 listDBoptions.schema_name,
				 listDBoptions.table_name);

		if (!schema_list_table_indexes(&pgsql,
									   listDBoptions.schema_name,
									   listDBoptions.table_name,
									   &indexArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		log_error("Option --schema-name can't be used without --table-name");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d indexes", indexArray.count);

	fformat(stdout, "%8s | %10s | %20s | %20s | %25s | %s\n",
			"OID", "Schema",
			"Index Name", "Constraint Name",
			"Constraint", "DDL");

	fformat(stdout, "%8s-+-%10s-+-%20s-+-%20s-+-%25s-+-%s\n",
			"--------",
			"----------",
			"--------------------",
			"--------------------",
			"-------------------------",
			"--------------------");

	for (int i = 0; i < indexArray.count; i++)
	{
		SourceIndex *index = &(indexArray.array[i]);

		if (!IS_EMPTY_STRING_BUFFER(index->constraintName))
		{
			if (index->isPrimary || index->isUnique)
			{
				fformat(stdout, "%8d | %10s | %20s | %20s | %25s | %s\n",
						index->indexOid,
						index->indexNamespace,
						index->indexRelname,
						index->constraintName,
						index->constraintDef,
						index->indexDef);
			}
			else
			{
				/*
				 * We can't create the index separately when it's not a UNIQUE
				 * or PRIMARY KEY index. EXCLUDE USING constraints are done
				 * with indexes that don't implement the constraint themselves.
				 */
				fformat(stdout, "%8d | %10s | %20s | %20s | %25s | %s\n",
						index->indexOid,
						index->indexNamespace,
						"",
						index->constraintName,
						index->constraintDef,
						"");
			}
		}
		else
		{
			/* when the constraint name is empty, the default display is ok */
			fformat(stdout, "%8d | %10s | %20s | %20s | %25s | %s\n",
					index->indexOid,
					index->indexNamespace,
					index->indexRelname,
					index->constraintName,
					index->constraintDef,
					index->indexDef);
		}
	}

	fformat(stdout, "\n");
}
