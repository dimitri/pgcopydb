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
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "progress.h"
#include "schema.h"
#include "string_utils.h"

ListDBOptions listDBoptions = { 0 };

static int cli_list_db_getopts(int argc, char **argv);
static void cli_list_extensions(int argc, char **argv);
static void cli_list_collations(int argc, char **argv);
static void cli_list_tables(int argc, char **argv);
static void cli_list_table_parts(int argc, char **argv);
static void cli_list_sequences(int argc, char **argv);
static void cli_list_indexes(int argc, char **argv);
static void cli_list_depends(int argc, char **argv);
static void cli_list_schema(int argc, char **argv);
static void cli_list_progress(int argc, char **argv);

static CommandLine list_extensions_command =
	make_command(
		"extensions",
		"List all the source extensions to copy",
		" --source ... ",
		"  --source            Postgres URI to the source database\n",
		cli_list_db_getopts,
		cli_list_extensions);

static CommandLine list_collations_command =
	make_command(
		"collations",
		"List all the source collations to copy",
		" --source ... ",
		"  --source            Postgres URI to the source database\n",
		cli_list_db_getopts,
		cli_list_collations);

static CommandLine list_tables_command =
	make_command(
		"tables",
		"List all the source tables to copy data from",
		" --source ... ",
		"  --source            Postgres URI to the source database\n"
		"  --filter <filename> Use the filters defined in <filename>\n"
		"  --cache             Cache table size in relation pgcopydb.table_size\n"
		"  --drop-cache        Drop relation pgcopydb.table_size\n"
		"  --list-skipped      List only tables that are setup to be skipped\n"
		"  --without-pkey      List only tables that have no primary key\n",
		cli_list_db_getopts,
		cli_list_tables);

static CommandLine list_table_parts_command =
	make_command(
		"table-parts",
		"List a source table copy partitions",
		" --source ... ",
		"  --source                    Postgres URI to the source database\n"
		"  --schema-name               Name of the schema where to find the table\n"
		"  --table-name                Name of the target table\n"
		"  --split-tables-larger-than  Size threshold to consider partitioning\n",
		cli_list_db_getopts,
		cli_list_table_parts);

static CommandLine list_sequences_command =
	make_command(
		"sequences",
		"List all the source sequences to copy data from",
		" --source ... ",
		"  --source            Postgres URI to the source database\n"
		"  --filter <filename> Use the filters defined in <filename>\n"
		"  --list-skipped      List only tables that are setup to be skipped\n",
		cli_list_db_getopts,
		cli_list_sequences);

static CommandLine list_indexes_command =
	make_command(
		"indexes",
		"List all the indexes to create again after copying the data",
		" --source ... [ --schema-name [ --table-name ] ]",
		"  --source            Postgres URI to the source database\n"
		"  --schema-name       Name of the schema where to find the table\n"
		"  --table-name        Name of the target table\n"
		"  --filter <filename> Use the filters defined in <filename>\n"
		"  --list-skipped      List only tables that are setup to be skipped\n",
		cli_list_db_getopts,
		cli_list_indexes);

static CommandLine list_depends_command =
	make_command(
		"depends",
		"List all the dependencies to filter-out",
		" --source ... [ --schema-name [ --table-name ] ]",
		"  --source            Postgres URI to the source database\n"
		"  --schema-name       Name of the schema where to find the table\n"
		"  --table-name        Name of the target table\n"
		"  --filter <filename> Use the filters defined in <filename>\n"
		"  --list-skipped      List only tables that are setup to be skipped\n",
		cli_list_db_getopts,
		cli_list_depends);

static CommandLine list_schema_command =
	make_command(
		"schema",
		"List the schema to migrate, formatted in JSON",
		" --source ... ",
		"  --source            Postgres URI to the source database\n"
		"  --filter <filename> Use the filters defined in <filename>\n",
		cli_list_db_getopts,
		cli_list_schema);

static CommandLine list_progress_command =
	make_command(
		"progress",
		"List the progress",
		" --source ... ",
		"  --source  Postgres URI to the source database\n"
		"  --json    Format the output using JSON\n"
		"  --dir     Work directory to use\n",
		cli_list_db_getopts,
		cli_list_progress);


static CommandLine *list_subcommands[] = {
	&list_extensions_command,
	&list_collations_command,
	&list_tables_command,
	&list_table_parts_command,
	&list_sequences_command,
	&list_indexes_command,
	&list_depends_command,
	&list_schema_command,
	&list_progress_command,
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
		{ "dir", required_argument, NULL, 'D' },
		{ "schema-name", required_argument, NULL, 's' },
		{ "table-name", required_argument, NULL, 't' },
		{ "filter", required_argument, NULL, 'F' },
		{ "filters", required_argument, NULL, 'F' },
		{ "list-skipped", no_argument, NULL, 'x' },
		{ "without-pkey", no_argument, NULL, 'P' },
		{ "split-tables-larger-than", required_argument, NULL, 'L' },
		{ "split-at", required_argument, NULL, 'L' },
		{ "cache", no_argument, NULL, 'c' },
		{ "drop-cache", no_argument, NULL, 'C' },
		{ "json", no_argument, NULL, 'J' },
		{ "version", no_argument, NULL, 'V' },
		{ "debug", no_argument, NULL, 'd' },
		{ "trace", no_argument, NULL, 'z' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "S:T:D:j:s:t:PL:cCJVvdzqh",
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

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 't':
			{
				strlcpy(options.table_name, optarg, NAMEDATALEN);
				log_trace("--table %s", options.table_name);
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

			case 'x':
			{
				options.listSkipped = true;
				log_trace("--list-skipped");
				break;
			}

			case 'P':
			{
				options.noPKey = true;
				log_trace("--without-pkey");
				break;
			}

			case 'L':
			{
				if (!cli_parse_bytes_pretty(
						optarg,
						&options.splitTablesLargerThan,
						(char *) &options.splitTablesLargerThanPretty,
						sizeof(options.splitTablesLargerThanPretty)))
				{
					log_fatal("Failed to parse --split-tables-larger-than: \"%s\"",
							  optarg);
					++errors;
				}

				log_trace("--split-tables-larger-than %s (%lld)",
						  options.splitTablesLargerThanPretty,
						  (long long) options.splitTablesLargerThan);
				break;
			}

			case 'c':
			{
				if (options.dropCache)
				{
					log_fatal("Please choose either --cache or --drop-cache");
					++errors;
				}

				options.cache = true;
				log_trace("--cache");
				break;
			}

			case 'C':
			{
				if (options.cache)
				{
					log_fatal("Please choose either --cache or --drop-cache");
					++errors;
				}

				options.dropCache = true;
				log_trace("--drop-cache");
				break;
			}

			case 'J':
			{
				outputJSON = true;
				log_trace("--json");
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

			default:
			{
				++errors;
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

	if (options.listSkipped && IS_EMPTY_STRING_BUFFER(options.filterFileName))
	{
		log_fatal("Option --list-skipped requires using option --filters");
		++errors;
	}

	if (env_exists(PGCOPYDB_SPLIT_TABLES_LARGER_THAN))
	{
		char bytes[BUFSIZE] = { 0 };

		if (get_env_copy(PGCOPYDB_SPLIT_TABLES_LARGER_THAN, bytes, sizeof(bytes)))
		{
			if (!cli_parse_bytes_pretty(
					bytes,
					&options.splitTablesLargerThan,
					(char *) &options.splitTablesLargerThanPretty,
					sizeof(options.splitTablesLargerThanPretty)))
			{
				log_fatal("Failed to parse PGCOPYDB_SPLIT_TABLES_LARGER_THAN: "
						  " \"%s\"",
						  bytes);
				++errors;
			}
		}
		else
		{
			/* errors have already been logged */
			++errors;
		}
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
 * cli_list_extensions implements the command: pgcopydb list extensions
 */
static void
cli_list_extensions(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceExtensionArray extensionArray = { 0, NULL };

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_extensions(&pgsql, &extensionArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d extensions", extensionArray.count);

	fformat(stdout, "%10s | %20s | %20s | %10s | %s\n",
			"OID",
			"Name",
			"Schema",
			"Count",
			"Config");

	fformat(stdout, "%10s-+-%20s-+-%20s-+-%10s-+-%10s\n",
			"----------",
			"--------------------",
			"--------------------",
			"----------",
			"----------");

	for (int i = 0; i < extensionArray.count; i++)
	{
		SourceExtension *ext = &(extensionArray.array[i]);

		char config[BUFSIZE] = { 0 };

		for (int c = 0; c < ext->config.count; c++)
		{
			sformat(config, sizeof(config), "%s%s\"%s\".\"%s\"",
					config,
					c == 0 ? "" : ",",
					ext->config.array[c].nspname,
					ext->config.array[c].relname);
		}

		fformat(stdout, "%10u | %20s | %20s | %10d | %s\n",
				ext->oid,
				ext->extname,
				ext->extnamespace,
				ext->config.count,
				config);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_collations implements the command: pgcopydb list collations
 */
static void
cli_list_collations(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceCollationArray collationArray = { 0, NULL };

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_collations(&pgsql, &collationArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d collations", collationArray.count);

	fformat(stdout, "%10s | %20s | %20s \n",
			"OID",
			"Name",
			"Object name");

	fformat(stdout, "%10s-+-%20s-+-%20s\n",
			"----------",
			"--------------------",
			"--------------------");

	for (int i = 0; i < collationArray.count; i++)
	{
		SourceCollation *coll = &(collationArray.array[i]);

		fformat(stdout, "%10u | %20s | %20s \n",
				coll->oid,
				coll->collname,
				coll->desc);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_tables implements the command: pgcopydb list tables
 */
static void
cli_list_tables(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceTableArray tableArray = { 0, NULL };
	SourceFilters filters = { 0 };

	if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		if (!parse_filters(listDBoptions.filterFileName, &filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  listDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}

		if (listDBoptions.listSkipped)
		{
			if (filters.type != SOURCE_FILTER_TYPE_NONE)
			{
				filters.type = filterTypeComplement(filters.type);

				if (filters.type == SOURCE_FILTER_TYPE_NONE)
				{
					log_error("BUG: can't list skipped tables from filtering "
							  "type %d",
							  filters.type);
					exit(EXIT_CODE_INTERNAL_ERROR);
				}
			}
		}
	}

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (listDBoptions.dropCache)
	{
		log_info("Dropping cache table pgcopydb.table_size");
		if (!schema_drop_pgcopydb_table_size(&pgsql))
		{
			exit(EXIT_CODE_SOURCE);
		}

		exit(EXIT_CODE_QUIT);
	}

	if (!pgsql_begin(&pgsql))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	bool createdTableSizeTable = false;

	if (!schema_prepare_pgcopydb_table_size(&pgsql,
											&filters,
											listDBoptions.cache, /* force */
											&createdTableSizeTable))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (listDBoptions.noPKey)
	{
		log_info("Listing tables without primary key in source database");

		if (!schema_list_ordinary_tables_without_pk(&pgsql,
													&filters,
													&tableArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		log_info("Listing ordinary tables in source database");

		if (!schema_list_ordinary_tables(&pgsql, &filters, &tableArray))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	/* compute total bytes and total reltuples, pretty print them */
	uint64_t totalBytes = 0;
	uint64_t totalTuples = 0;

	for (int i = 0; i < tableArray.count; i++)
	{
		totalBytes += tableArray.array[i].bytes;
		totalTuples += tableArray.array[i].reltuples;
	}

	char bytesPretty[BUFSIZE] = { 0 };
	char relTuplesPretty[BUFSIZE] = { 0 };

	(void) pretty_print_bytes(bytesPretty, BUFSIZE, totalBytes);
	(void) pretty_print_count(relTuplesPretty, BUFSIZE, totalTuples);

	log_info("Fetched information for %d tables, "
			 "with an estimated total of %s tuples and %s",
			 tableArray.count,
			 relTuplesPretty,
			 bytesPretty);

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

	if (createdTableSizeTable && !listDBoptions.cache)
	{
		if (!schema_drop_pgcopydb_table_size(&pgsql))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	if (!pgsql_commit(&pgsql))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_list_tables implements the command: pgcopydb list table-parts
 */
static void
cli_list_table_parts(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	char scrubbedSourceURI[MAXCONNINFO] = { 0 };

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.table_name))
	{
		log_fatal("Option --table-name is mandatory");
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name))
	{
		strlcpy(listDBoptions.schema_name, "public", NAMEDATALEN);
	}

	(void) parse_and_scrub_connection_string(listDBoptions.source_pguri,
											 scrubbedSourceURI);

	log_info("Listing COPY partitions for table \"%s\".\"%s\" in \"%s\"",
			 listDBoptions.schema_name,
			 listDBoptions.table_name,
			 scrubbedSourceURI);

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	/*
	 * Build a filter that includes only the given target table, our command
	 * line is built to work on a single table at a time (--schema-name default
	 * to "public" and --table-name is mandatory).
	 */
	SourceFilterTable *tableFilter =
		(SourceFilterTable *) malloc(1 * sizeof(SourceFilterTable));

	strlcpy(tableFilter[0].nspname, listDBoptions.schema_name, NAMEDATALEN);
	strlcpy(tableFilter[0].relname, listDBoptions.table_name, NAMEDATALEN);

	SourceFilters filter =
	{
		.type = SOURCE_FILTER_TYPE_INCL,
		.includeOnlyTableList =
		{
			.count = 1,
			.array = tableFilter
		}
	};

	SourceTableArray tableArray = { 0 };

	if (!schema_list_ordinary_tables(&pgsql, &filter, &tableArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (tableArray.count != 1)
	{
		log_error("Expected to fetch a single table with schema name \"%s\" "
				  "and table name \"%s\", fetched %d instead",
				  listDBoptions.schema_name,
				  listDBoptions.table_name,
				  tableArray.count);

		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	SourceTable *table = &(tableArray.array[0]);

	if (IS_EMPTY_STRING_BUFFER(table->partKey))
	{
		log_info("Table \"%s\".\"%s\" (%s) will not be split: "
				 "table lacks a unique integer column (int2/int4/int8).",
				 table->nspname,
				 table->relname,
				 table->bytesPretty);
		exit(EXIT_CODE_QUIT);
	}

	if (!schema_list_partitions(&pgsql, table,
								listDBoptions.splitTablesLargerThan))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (table->partsArray.count <= 1)
	{
		log_info("Table \"%s\".\"%s\" (%s) will not be split",
				 table->nspname,
				 table->relname,
				 table->bytesPretty);
		exit(EXIT_CODE_QUIT);
	}

	log_info("Table \"%s\".\"%s\" COPY will be split %d-ways",
			 table->nspname,
			 table->relname,
			 table->partsArray.count);

	fformat(stdout, "%10s | %10s | %10s | %10s\n",
			"Part", "Min", "Max", "Count");

	fformat(stdout, "%10s-+-%10s-+-%10s-+-%10s\n",
			"----------",
			"----------",
			"----------",
			"----------");

	for (int i = 0; i < table->partsArray.count; i++)
	{
		SourceTableParts *part = &(table->partsArray.array[i]);

		char partNC[BUFSIZE] = { 0 };

		sformat(partNC, sizeof(partNC), "%d/%d",
				part->partNumber,
				part->partCount);

		fformat(stdout, "%10s | %10lld | %10lld | %10lld\n",
				partNC,
				(long long) part->min,
				(long long) part->max,
				(long long) part->count);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_tables implements the command: pgcopydb list tables
 */
static void
cli_list_sequences(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceFilters filters = { 0 };
	SourceSequenceArray sequenceArray = { 0, NULL };

	log_info("Listing ordinary sequences in source database");

	if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		if (!parse_filters(listDBoptions.filterFileName, &filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  listDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}

		if (listDBoptions.listSkipped)
		{
			if (filters.type != SOURCE_FILTER_TYPE_NONE)
			{
				filters.type = filterTypeComplement(filters.type);

				if (filters.type == SOURCE_FILTER_TYPE_NONE)
				{
					log_error("BUG: can't list skipped sequences "
							  " from filtering type %d",
							  filters.type);
					exit(EXIT_CODE_INTERNAL_ERROR);
				}
			}
		}
	}

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_sequences(&pgsql, &filters, &sequenceArray))
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
 * cli_list_indexes implements the command: pgcopydb list indexes
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

		SourceFilters filters = { 0 };

		if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
		{
			if (!parse_filters(listDBoptions.filterFileName, &filters))
			{
				log_error("Failed to parse filters in file \"%s\"",
						  listDBoptions.filterFileName);
				exit(EXIT_CODE_BAD_ARGS);
			}

			if (listDBoptions.listSkipped)
			{
				if (filters.type != SOURCE_FILTER_TYPE_NONE)
				{
					filters.type = filterTypeComplement(filters.type);

					if (filters.type == SOURCE_FILTER_TYPE_NONE)
					{
						log_error("BUG: can't list skipped indexes "
								  " from filtering type %d",
								  filters.type);
						exit(EXIT_CODE_INTERNAL_ERROR);
					}
				}
			}
		}

		if (!schema_list_all_indexes(&pgsql, &filters, &indexArray))
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


/*
 * cli_list_indexes implements the command: pgcopydb list depends
 */
static void
cli_list_depends(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	SourceFilters filters = { 0 };
	SourceDependArray dependArray = { 0, NULL };

	log_info("Listing dependencies in source database");

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		log_fatal("Option --filter is mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!parse_filters(listDBoptions.filterFileName, &filters))
	{
		log_error("Failed to parse filters in file \"%s\"",
				  listDBoptions.filterFileName);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (listDBoptions.listSkipped)
	{
		if (filters.type != SOURCE_FILTER_TYPE_NONE)
		{
			filters.type = filterTypeComplement(filters.type);

			if (filters.type == SOURCE_FILTER_TYPE_NONE)
			{
				log_error("BUG: can't list skipped sequences "
						  " from filtering type %d",
						  filters.type);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}
		}
	}

	if (!pgsql_init(&pgsql, listDBoptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_pg_depend(&pgsql, &filters, &dependArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d dependencies", dependArray.count);

	fformat(stdout, "%20s | %30s | %8s | %8s | %20s | %s\n",
			"Schema Name", "Table Name", "Catalog", "OID", "Type", "Identity");

	fformat(stdout, "%20s-+-%30s-+-%8s-+-%8s-+-%20s-+-%30s\n",
			"--------------------",
			"------------------------------",
			"--------",
			"--------",
			"--------------------",
			"------------------------------");

	for (int i = 0; i < dependArray.count; i++)
	{
		fformat(stdout, "%20s | %30s | %8u | %8u | %20s | %s\n",
				dependArray.array[i].nspname,
				dependArray.array[i].relname,
				dependArray.array[i].classid,
				dependArray.array[i].objid,
				dependArray.array[i].type,
				dependArray.array[i].identity);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_schema implements the command: pgcopydb list schema
 */
static void
cli_list_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	/*
	 * Assume --resume so that we can run the command alongside the main
	 * process being active.
	 */
	bool createWorkDir = true;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 false, /* service */
							 NULL,  /* serviceName */
							 false, /* restart */
							 true, /* resume */
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   listDBoptions.source_pguri,
						   "",  /* target_pguri */
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   listDBoptions.splitTablesLargerThan,
						   listDBoptions.splitTablesLargerThanPretty,
						   DATA_SECTION_ALL,
						   "",  /* snapshot */
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   false, /* skipCollations */
						   false, /* noRolesPasswords */
						   false, /* restart */
						   true,  /* resume */
						   false)) /* consistent */
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* parse filters if provided */
	if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		if (!parse_filters(listDBoptions.filterFileName, &(copySpecs.filters)))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  listDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}

	char scrubbedSourceURI[MAXCONNINFO] = { 0 };

	(void) parse_and_scrub_connection_string(copySpecs.source_pguri,
											 scrubbedSourceURI);

	log_info("Fetching schema from \"%s\"", scrubbedSourceURI);
	log_info("Dumping schema into JSON file \"%s\"",
			 copySpecs.cfPaths.schemafile);

	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!copydb_prepare_schema_json_file(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Wrote \"%s\"", copySpecs.cfPaths.schemafile);

	/* output the JSON contents from the json schema file */
	char *json = NULL;
	long size = 0L;

	if (!read_file(copySpecs.cfPaths.schemafile, &json, &size))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "%s\n", json);
	free(json);
}


/*
 * cli_list_progress implements the command: pgcopydb list progress
 */
static void
cli_list_progress(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	char *dir =
		IS_EMPTY_STRING_BUFFER(listDBoptions.dir)
		? NULL
		: listDBoptions.dir;

	/*
	 * Assume --resume so that we can run the command alongside the main
	 * process being active.
	 */
	bool createWorkDir = false;

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 false, /* service */
							 NULL,  /* serviceName */
							 false, /* restart */
							 true, /* resume */
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   listDBoptions.source_pguri,
						   "",  /* target_pguri */
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   listDBoptions.splitTablesLargerThan,
						   listDBoptions.splitTablesLargerThanPretty,
						   DATA_SECTION_ALL,
						   "",  /* snapshot */
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   false, /* skipCollations */
						   false, /* noRolesPasswords */
						   false, /* restart */
						   true,  /* resume */
						   false)) /* consistent */
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_parse_schema_json_file(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	CopyProgress progress = { 0 };

	if (!copydb_update_progress(&copySpecs, &progress))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_object();

		if (!copydb_progress_as_json(&copySpecs, &progress, js))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		char *serialized_string = json_serialize_to_string_pretty(js);

		fformat(stdout, "%s\n", serialized_string);

		json_free_serialized_string(serialized_string);
		json_value_free(js);
	}
	else
	{
		fformat(stdout, "%12s | %12s | %12s | %12s\n",
				"",
				"Total Count",
				"In Progress",
				"Done");

		fformat(stdout, "%12s-+-%12s-+-%12s-+-%12s\n",
				"------------",
				"------------",
				"------------",
				"------------");

		fformat(stdout, "%12s | %12d | %12d | %12d\n",
				"Tables",
				progress.tableCount,
				progress.tableInProgress.count,
				progress.tableDoneCount);

		fformat(stdout, "%12s | %12d | %12d | %12d\n",
				"Indexes",
				progress.indexCount,
				progress.indexInProgress.count,
				progress.indexDoneCount);
	}
}
