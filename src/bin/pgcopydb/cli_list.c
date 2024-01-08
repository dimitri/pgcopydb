/*
 * src/bin/pgcopydb/cli_list.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "catalog.h"
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
static void cli_list_databases(int argc, char **argv);
static void cli_list_extensions(int argc, char **argv);
static void cli_list_extension_versions(int argc, char **argv);
static void cli_list_extension_requirements(int argc, char **argv);
static void cli_list_collations(int argc, char **argv);
static void cli_list_tables(int argc, char **argv);
static void cli_list_table_parts(int argc, char **argv);
static void cli_list_sequences(int argc, char **argv);
static void cli_list_indexes(int argc, char **argv);
static void cli_list_depends(int argc, char **argv);
static void cli_list_schema(int argc, char **argv);
static void cli_list_progress(int argc, char **argv);

static bool copydb_init_specs_from_listdboptions(CopyDataSpec *copySpecs,
												 ListDBOptions *listDBoptions,
												 CopyDataSection section,
												 bool createWorkDir);


/* Iterator Hooks */
static bool cli_list_databases_hook(void *context, SourceDatabase *dat);
static bool cli_list_extension_json_hook(void *ctx, SourceExtension *ext);
static bool cli_list_extension_print_hook(void *ctx, SourceExtension *ext);
static bool cli_list_colls_hook(void *context, SourceCollation *coll);
static bool cli_list_table_print_hook(void *context, SourceTable *table);
static bool cli_list_table_part_print_hook(void *ctx, SourceTableParts *part);
static bool cli_list_seq_print_hook(void *context, SourceSequence *seq);
static bool cli_list_index_print_hook(void *context, SourceIndex *index);
static bool cli_list_depends_hook(void *ctx, SourceDepend *dep);


static CommandLine list_catalogs_command =
	make_command(
		"databases",
		"List databases",
		" --source ... ",
		"  --source            Postgres URI to the source database\n",
		cli_list_db_getopts,
		cli_list_databases);

static CommandLine list_extensions_command =
	make_command(
		"extensions",
		"List all the source extensions to copy",
		" --source ... ",
		"  --source              Postgres URI to the source database\n"
		"  --json                Format the output using JSON\n"
		"  --available-versions  List available extension versions\n"
		"  --requirements        List extensions requirements\n",
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
		"  --force             Force fetching catalogs again\n"
		"  --cache             Cache table size in relation pgcopydb.pgcopydb_table_size\n"
		"  --drop-cache        Drop relation pgcopydb.pgcopydb_table_size\n"
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
		"  --force                     Force fetching catalogs again\n"
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
		"  --force             Force fetching catalogs again\n"
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
		"  --force             Force fetching catalogs again\n"
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
		"  --force             Force fetching catalogs again\n"
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
		"  --force             Force fetching catalogs again\n"
		"  --filter <filename> Use the filters defined in <filename>\n",
		cli_list_db_getopts,
		cli_list_schema);

static CommandLine list_progress_command =
	make_command(
		"progress",
		"List the progress",
		" --source ... ",
		"  --source  Postgres URI to the source database\n"
		"  --summary List the summary, requires --json\n"
		"  --json    Format the output using JSON\n"
		"  --dir     Work directory to use\n",
		cli_list_db_getopts,
		cli_list_progress);


static CommandLine *list_subcommands[] = {
	&list_catalogs_command,
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
 * cli_list_getenv reads from the environment variables and fills-in the
 * command line options.
 */
static int
cli_list_getenv(ListDBOptions *options)
{
	int errors = 0;

	if (!cli_copydb_getenv_source_pguri(&(options->connStrings.source_pguri)))
	{
		/* errors have already been logged */
		++errors;
	}

	if (!cli_copydb_getenv_split(&(options->splitTablesLargerThan)))
	{
		/* errors have already been logged */
		++errors;
	}

	return errors == 0;
}


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
		{ "force", no_argument, NULL, 'f' },
		{ "cache", no_argument, NULL, 'c' },
		{ "drop-cache", no_argument, NULL, 'C' },
		{ "summary", no_argument, NULL, 'y' },
		{ "available-versions", no_argument, NULL, 'a' },
		{ "requirements", no_argument, NULL, 'r' },
		{ "json", no_argument, NULL, 'J' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'I' },
		{ "snapshot", required_argument, NULL, 'N' },
		{ "version", no_argument, NULL, 'V' },
		{ "debug", no_argument, NULL, 'd' },
		{ "trace", no_argument, NULL, 'z' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "notice", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	/* read values from the environment */
	if (!cli_list_getenv(&options))
	{
		log_fatal("Failed to read default values from the environment");
		exit(EXIT_CODE_BAD_ARGS);
	}

	while ((c = getopt_long(argc, argv, "S:D:s:t:F:xPL:fcCyarJRIN:Vdzvqh",
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
				options.connStrings.source_pguri = pg_strdup(optarg);
				log_trace("--source %s", options.connStrings.source_pguri);
				break;
			}

			case 's':
			{
				strlcpy(options.schema_name, optarg, PG_NAMEDATALEN);
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
				strlcpy(options.table_name, optarg, PG_NAMEDATALEN);
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
						&(options.splitTablesLargerThan.bytes),
						(char *) &(options.splitTablesLargerThan.bytesPretty),
						sizeof(options.splitTablesLargerThan.bytesPretty)))
				{
					log_fatal("Failed to parse --split-tables-larger-than: \"%s\"",
							  optarg);
					++errors;
				}

				log_trace("--split-tables-larger-than %s (%lld)",
						  options.splitTablesLargerThan.bytesPretty,
						  (long long) options.splitTablesLargerThan.bytes);
				break;
			}

			case 'f':
			{
				options.force = true;
				log_trace("--force");
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

			case 'y':
			{
				options.summary = true;
				log_trace("--summary");
				break;
			}

			case 'a':
			{
				options.availableVersions = true;
				log_trace("--available-versions");
				break;
			}

			case 'r':
			{
				options.requirements = true;
				log_trace("--requirements");
				break;
			}

			case 'J':
			{
				outputJSON = true;
				log_trace("--json");
				break;
			}

			case 'R':
			{
				options.resume = true;
				log_trace("--resume");
				break;
			}

			case 'I':
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
						log_set_level(LOG_SQL);
						break;
					}

					case 3:
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
				verboseCount = 3;
				log_set_level(LOG_DEBUG);
				break;
			}

			case 'z':
			{
				verboseCount = 4;
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

			case '?':
			default:
			{
				++errors;
			}
		}
	}

	if (options.connStrings.source_pguri == NULL)
	{
		log_fatal("Option --source is mandatory");
		++errors;
	}

	/* prepare safe versions of the connection strings (without password) */
	if (!cli_prepare_pguris(&(options.connStrings)))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (options.listSkipped && IS_EMPTY_STRING_BUFFER(options.filterFileName))
	{
		log_fatal("Option --list-skipped requires using option --filters");
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
 * cli_list_databases implements the command: pgcopydb list databases
 */
static void
cli_list_databases(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_ALL,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* compute total bytes and total reltuples, pretty print them */
	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	PGSQL pgsql = { 0 };
	ConnStrings *dsn = &(listDBoptions.connStrings);

	if (!pgsql_init(&pgsql, dsn->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_databases(&pgsql, sourceDB))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count local catalogs objects");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %lld databases",
			 (long long) count.databases);

	fformat(stdout, "%10s | %20s | %20s\n",
			"OID",
			"Database Name",
			"On-disk size");

	fformat(stdout, "%10s-+-%20s-+-%20s\n",
			"----------",
			"--------------------",
			"--------------------");

	if (!catalog_iter_s_database(sourceDB, NULL, &cli_list_databases_hook))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_databases_print_hook is an iterator callback function.
 */
static bool
cli_list_databases_hook(void *context, SourceDatabase *dat)
{
	fformat(stdout, "%10u | %20s | %20s\n",
			dat->oid,
			dat->datname,
			dat->bytesPretty);

	return true;
}


typedef struct ListExtensionContext
{
	DatabaseCatalog *filtersDB;
	JSON_Array *jsArray;
} ListExtensionContext;


/*
 * cli_list_extensions implements the command: pgcopydb list extensions
 */
static void
cli_list_extensions(int argc, char **argv)
{
	/* --available-versions is implemented as its own command */
	if (listDBoptions.availableVersions)
	{
		(void) cli_list_extension_versions(argc, argv);
		exit(EXIT_CODE_QUIT);
	}

	/* --requirements is implemented as its own command */
	if (listDBoptions.requirements)
	{
		(void) cli_list_extension_requirements(argc, argv);
		exit(EXIT_CODE_QUIT);
	}

	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_EXTENSIONS,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	Catalogs *catalogs = &(copySpecs.catalogs);
	DatabaseCatalog *filtersDB = &(catalogs->filter);

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		ListExtensionContext context = {
			.filtersDB = filtersDB,
			.jsArray = jsArray
		};

		if (!catalog_iter_s_extension(filtersDB,
									  &context,
									  &cli_list_extension_json_hook))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		char *serialized_string = json_serialize_to_string_pretty(js);

		fformat(stdout, "%s\n", serialized_string);

		json_free_serialized_string(serialized_string);
		json_value_free(js);
	}
	else
	{
		fformat(stdout, "%10s | %25s | %20s | %10s | %s\n",
				"OID",
				"Name",
				"Schema",
				"Count",
				"Config");

		fformat(stdout, "%10s-+-%25s-+-%20s-+-%10s-+-%10s\n",
				"----------",
				"-------------------------",
				"--------------------",
				"----------",
				"----------");

		ListExtensionContext context = { .filtersDB = filtersDB };

		if (!catalog_iter_s_extension(filtersDB,
									  &context,
									  &cli_list_extension_print_hook))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		fformat(stdout, "\n");
	}
}


/*
 * cli_list_extension_json_hook is an iterator callback function.
 */
static bool
cli_list_extension_json_hook(void *ctx, SourceExtension *ext)
{
	ListExtensionContext *context = (ListExtensionContext *) ctx;

	DatabaseCatalog *filtersDB = context->filtersDB;
	JSON_Array *jsArray = context->jsArray;

	JSON_Value *jsExt = json_value_init_object();
	JSON_Object *jsObj = json_value_get_object(jsExt);

	json_object_set_number(jsObj, "oid", ext->oid);
	json_object_set_string(jsObj, "name", ext->extname);
	json_object_set_string(jsObj, "schema", ext->extnamespace);

	JSON_Value *jsConfig = json_value_init_array();
	JSON_Array *jsConfigArray = json_value_get_array(jsConfig);

	if (!catalog_s_ext_fetch_extconfig(filtersDB, ext))
	{
		/* errors have already been logged */
		return false;
	}

	for (int c = 0; c < ext->config.count; c++)
	{
		JSON_Value *jsConf = json_value_init_object();
		JSON_Object *jsConfObj = json_value_get_object(jsConf);

		json_object_set_string(jsConfObj,
							   "schema", ext->config.array[c].nspname);

		json_object_set_string(jsConfObj,
							   "name", ext->config.array[c].relname);

		json_array_append_value(jsConfigArray, jsConf);
	}

	json_object_set_value(jsObj, "config", jsConfig);

	json_array_append_value(jsArray, jsExt);

	return true;
}


/*
 * cli_list_extension_json_hook is an iterator callback function.
 */
static bool
cli_list_extension_print_hook(void *ctx, SourceExtension *ext)
{
	ListExtensionContext *context = (ListExtensionContext *) ctx;
	DatabaseCatalog *filtersDB = context->filtersDB;

	if (!catalog_s_ext_fetch_extconfig(filtersDB, ext))
	{
		/* errors have already been logged */
		return false;
	}

	char config[BUFSIZE] = { 0 };

	for (int c = 0; c < ext->config.count; c++)
	{
		sformat(config, sizeof(config), "%s%s\"%s\".\"%s\"",
				config,
				c == 0 ? "" : ",",
				ext->config.array[c].nspname,
				ext->config.array[c].relname);
	}

	fformat(stdout, "%10u | %25s | %20s | %10d | %s\n",
			ext->oid,
			ext->extname,
			ext->extnamespace,
			ext->config.count,
			config);

	return true;
}


/*
 * cli_list_extension_versions implements the command:
 *
 *   pgcopydb list extensions --available-versions
 */
static void
cli_list_extension_versions(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	ExtensionsVersionsArray evArray = { 0, NULL };

	ConnStrings *dsn = &(listDBoptions.connStrings);

	if (!pgsql_init(&pgsql, dsn->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!schema_list_ext_versions(&pgsql, &evArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d extension versions", evArray.count);

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		for (int i = 0; i < evArray.count; i++)
		{
			ExtensionsVersions *ev = &(evArray.array[i]);

			JSON_Value *jsExtVersion = json_value_init_object();
			JSON_Object *jsEVObj = json_value_get_object(jsExtVersion);

			json_object_set_string(jsEVObj, "name", ev->name);
			json_object_set_value(jsEVObj, "versions", ev->json);

			/* add the JSON object to the array */
			json_array_append_value(jsArray, jsExtVersion);
		}

		char *serialized_string = json_serialize_to_string_pretty(js);

		fformat(stdout, "%s\n", serialized_string);

		json_free_serialized_string(serialized_string);
		json_value_free(js);
	}
	else
	{
		fformat(stdout, "%20s | %20s | %s\n",
				"Name",
				"Default",
				"Available");

		fformat(stdout, "%20s-+-%20s-+-%20s\n",
				"--------------------",
				"--------------------",
				"--------------------");

		for (int i = 0; i < evArray.count; i++)
		{
			ExtensionsVersions *ev = &(evArray.array[i]);

			char *strArray = json_serialize_to_string(ev->json);

			fformat(stdout, "%20s | %20s | %s\n",
					ev->name,
					ev->defaultVersion,
					strArray);

			json_free_serialized_string(strArray);
		}

		fformat(stdout, "\n");
	}
}


/*
 * cli_list_extension_requirements implements the command:
 *
 *   pgcopydb list extensions --requirements --json
 */
static void
cli_list_extension_requirements(int argc, char **argv)
{
	PGSQL pgsql = { 0 };
	ConnStrings *dsn = &(listDBoptions.connStrings);

	if (!pgsql_init(&pgsql, dsn->source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	ExtensionsVersionsArray evArray = { 0, NULL };

	if (!schema_list_ext_versions(&pgsql, &evArray))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %d extension versions", evArray.count);

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		for (int i = 0; i < evArray.count; i++)
		{
			ExtensionsVersions *ev = &(evArray.array[i]);

			JSON_Value *jsExtVersion = json_value_init_object();
			JSON_Object *jsEVObj = json_value_get_object(jsExtVersion);

			json_object_set_string(jsEVObj, "name", ev->name);
			json_object_set_string(jsEVObj, "version", ev->defaultVersion);

			/* add the JSON object to the array */
			json_array_append_value(jsArray, jsExtVersion);
		}

		char *serialized_string = json_serialize_to_string_pretty(js);

		fformat(stdout, "%s\n", serialized_string);

		json_free_serialized_string(serialized_string);
		json_value_free(js);
	}
	else
	{
		fformat(stdout, "%30s | %s\n", "Name", "Version");

		fformat(stdout, "%30s-+-%20s\n",
				"------------------------------",
				"--------------------");

		for (int i = 0; i < evArray.count; i++)
		{
			ExtensionsVersions *ev = &(evArray.array[i]);

			fformat(stdout, "%30s | %s\n",
					ev->name,
					ev->defaultVersion);
		}

		fformat(stdout, "\n");
	}
}


/*
 * cli_list_collations implements the command: pgcopydb list collations
 */
static void
cli_list_collations(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_ALL,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* pretend we're using --skip-collations so that we fetch the data */
	copySpecs.skipCollations = true;

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* collations are only used to filter pg_restore --list */
	DatabaseCatalog *filterDB = &(copySpecs.catalogs.filter);
	CatalogCounts count = { 0 };

	if (!catalog_count_objects(filterDB, &count))
	{
		log_error("Failed to count local catalogs objects");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %lld collations",
			 (long long) count.colls);

	fformat(stdout, "%10s | %20s | %-20s \n",
			"OID",
			"Name",
			"Object name");

	fformat(stdout, "%10s-+-%20s-+-%20s\n",
			"----------",
			"--------------------",
			"--------------------");

	if (!catalog_iter_s_coll(filterDB, NULL, &cli_list_colls_hook))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_colls_print_hook is an iterator callback function.
 */
static bool
cli_list_colls_hook(void *context, SourceCollation *coll)
{
	fformat(stdout, "%10u | %20s | %s \n",
			coll->oid,
			coll->collname,
			coll->desc);

	return true;
}


/*
 * cli_list_tables implements the command: pgcopydb list tables
 */
static void
cli_list_tables(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_TABLE_DATA,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	SourceFilters *filters = &(copySpecs.filters);

	if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		if (!parse_filters(listDBoptions.filterFileName, filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  listDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}

		if (listDBoptions.listSkipped)
		{
			copySpecs.fetchFilteredOids = true;
		}
	}

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* when --list-skipped then use the filters catalogs */
	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);
	DatabaseCatalog *filtersDB = &(copySpecs.catalogs.filter);

	DatabaseCatalog *catalog =
		listDBoptions.listSkipped ? filtersDB : sourceDB;

	/* compute total bytes and total reltuples, pretty print them */
	CatalogTableStats stats = { 0 };

	if (!catalog_s_table_stats(catalog, &stats))
	{
		log_error("Failed to compute source table statistics, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %lld tables, "
			 "with an estimated total of %s tuples and %s on-disk",
			 (long long) stats.count,
			 stats.relTuplesPretty,
			 stats.bytesPretty);

	fformat(stdout, "%8s | %20s | %20s | %15s | %15s\n",
			"OID", "Schema Name", "Table Name",
			"Est. Row Count", "On-disk size");

	fformat(stdout, "%8s-+-%20s-+-%20s-+-%15s-+-%15s\n",
			"--------",
			"--------------------",
			"--------------------",
			"---------------",
			"---------------");

	if (listDBoptions.noPKey)
	{
		if (!catalog_iter_s_table_nopk(catalog,
									   NULL,
									   &cli_list_table_print_hook))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		if (!catalog_iter_s_table(catalog, NULL, &cli_list_table_print_hook))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_table_print_hook is an iterator callback function.
 */
static bool
cli_list_table_print_hook(void *context, SourceTable *table)
{
	if (table == NULL)
	{
		log_error("BUG: cli_list_table_print_hook called with a NULL table");
		return false;
	}

	fformat(stdout, "%8d | %20s | %20s | %15lld | %15s\n",
			table->oid,
			table->nspname,
			table->relname,
			(long long) table->reltuples,
			table->bytesPretty);

	return true;
}


typedef struct ListTablePartContext
{
	SourceTable *table;
} ListTablePartContext;

/*
 * cli_list_table_parts implements the command: pgcopydb list table-parts
 */
static void
cli_list_table_parts(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (listDBoptions.splitTablesLargerThan.bytes == 0)
	{
		log_warn("Option --split-tables-larger-than is set to zero bytes, "
				 "skipping");
		exit(EXIT_CODE_QUIT);
	}

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_TABLE_DATA_PARTS,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.table_name))
	{
		log_fatal("Option --table-name is mandatory");
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name))
	{
		strlcpy(listDBoptions.schema_name, "public", PG_NAMEDATALEN);
	}

	if (!catalog_init_from_specs(&copySpecs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	Catalogs *catalogs = &(copySpecs.catalogs);
	DatabaseCatalog *sourceDB = &(catalogs->source);
	SourceTable *table = (SourceTable *) calloc(1, sizeof(SourceTable));

	if (table == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!catalog_lookup_s_table_by_name(sourceDB,
										listDBoptions.schema_name,
										listDBoptions.table_name,
										table))
	{
		log_error("Failed to lookup for table \"%s\".\"%s\" in our "
				  "internal catalogs",
				  listDBoptions.schema_name,
				  listDBoptions.table_name);

		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (table->bytes < listDBoptions.splitTablesLargerThan.bytes)
	{
		log_info("Table %s (%s) will not be split",
				 table->qname,
				 table->bytesPretty);
		exit(EXIT_CODE_QUIT);
	}

	if (IS_EMPTY_STRING_BUFFER(table->partKey))
	{
		log_info("Table %s is %s large "
				 "which is larger than --split-tables-larger-than %s, "
				 "and does not have a unique column of type integer: "
				 "splitting by CTID",
				 table->qname,
				 table->bytesPretty,
				 listDBoptions.splitTablesLargerThan.bytesPretty);

		strlcpy(table->partKey, "ctid", sizeof(table->partKey));
	}

	log_info("Table %s COPY will be split %d-ways",
			 table->qname,
			 table->partition.partCount);

	fformat(stdout, "%12s | %12s | %12s | %12s\n",
			"Part", "Min", "Max", "Count");

	fformat(stdout, "%12s-+-%12s-+-%12s-+-%12s\n",
			"------------",
			"------------",
			"------------",
			"------------");

	ListTablePartContext context = { .table = table };

	if (!catalog_iter_s_table_parts(sourceDB,
									table->oid,
									&context,
									&cli_list_table_part_print_hook))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_table_part_print_hook is an iterator callback function.
 */
static bool
cli_list_table_part_print_hook(void *ctx, SourceTableParts *part)
{
	if (part == NULL)
	{
		log_error("BUG: cli_list_table_print_hook called with a NULL part");
		return false;
	}

	ListTablePartContext *context = (ListTablePartContext *) ctx;
	SourceTable *table = context->table;

	if (streq(table->partKey, "ctid"))
	{
		char partNC[BUFSIZE] = { 0 };
		char partMin[BUFSIZE] = { 0 };
		char partMax[BUFSIZE] = { 0 };

		sformat(partNC, sizeof(partNC), "%d/%d",
				part->partNumber,
				part->partCount);

		sformat(partMin, BUFSIZE, "(%lld,0)", (long long) part->min);
		sformat(partMax, BUFSIZE, "(%lld,0)", (long long) part->max);

		fformat(stdout, "%12s | %12s | %12s | %12lld\n",
				partNC, partMin, partMax, (long long) part->count);
	}
	else
	{
		char partNC[BUFSIZE] = { 0 };

		sformat(partNC, sizeof(partNC), "%d/%d",
				part->partNumber,
				part->partCount);

		fformat(stdout, "%12s | %12lld | %12lld | %12lld\n",
				partNC,
				(long long) part->min,
				(long long) part->max,
				(long long) part->count);
	}

	return true;
}


/*
 * cli_list_sequences implements the command: pgcopydb list sequences
 */
static void
cli_list_sequences(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_SET_SEQUENCES,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	SourceFilters *filters = &(copySpecs.filters);

	if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		if (!parse_filters(listDBoptions.filterFileName, filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  listDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}

		if (listDBoptions.listSkipped)
		{
			copySpecs.fetchFilteredOids = true;
		}
	}

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "%8s | %20s | %30s | %10s | %10s | %10s \n",
			"OID", "Schema Name", "Sequence Name",
			"Owned By", "attrelid", "attroid");

	fformat(stdout, "%8s-+-%20s-+-%30s-+-%10s-+-%10s-+-%10s\n",
			"--------",
			"--------------------",
			"------------------------------",
			"----------",
			"----------",
			"----------");

	/* when --list-skipped then use the filters catalogs */
	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);
	DatabaseCatalog *filtersDB = &(copySpecs.catalogs.filter);

	DatabaseCatalog *catalog =
		listDBoptions.listSkipped ? filtersDB : sourceDB;

	if (!catalog_iter_s_seq(catalog, NULL, &cli_list_seq_print_hook))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_seq_print_hook is an iterator callback function.
 */
static bool
cli_list_seq_print_hook(void *context, SourceSequence *seq)
{
	if (seq == NULL)
	{
		log_error("BUG: cli_list_seq_print_hook called with a NULL seq");
		return false;
	}

	fformat(stdout, "%8d | %20s | %30s | %10d | %10d | %10d\n",
			seq->oid,
			seq->nspname,
			seq->relname,
			seq->ownedby,
			seq->attrelid,
			seq->attroid);

	return true;
}


/*
 * cli_list_indexes implements the command: pgcopydb list indexes
 */
static void
cli_list_indexes(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_INDEXES,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	SourceFilters *filters = &(copySpecs.filters);

	if (!IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		if (!parse_filters(listDBoptions.filterFileName, filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  listDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}

		if (listDBoptions.listSkipped)
		{
			copySpecs.fetchFilteredOids = true;
		}
	}

	bool listTableIndexList = false;

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.table_name) &&
		IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name))
	{
		/* list all indexes */
		listTableIndexList = false;
	}
	else if (IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name) &&
			 !IS_EMPTY_STRING_BUFFER(listDBoptions.table_name))
	{
		/* list indexes for just this one table, in schema "public" */
		strlcpy(listDBoptions.schema_name,
				"public",
				sizeof(listDBoptions.schema_name));

		listTableIndexList = true;
	}
	else if (!IS_EMPTY_STRING_BUFFER(listDBoptions.schema_name) &&
			 !IS_EMPTY_STRING_BUFFER(listDBoptions.table_name))
	{
		/* list indexes for just this one table */
		listTableIndexList = true;
	}
	else
	{
		log_error("Option --schema-name can't be used without --table-name");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

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

	/* when --list-skipped then use the filters catalogs */
	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);
	DatabaseCatalog *filtersDB = &(copySpecs.catalogs.filter);

	DatabaseCatalog *catalog =
		listDBoptions.listSkipped ? filtersDB : sourceDB;

	/*
	 * Initialize our Index Iterator, depending on if we're listing indexes for
	 * just a single table or all the indexes we have fetched in our local copy
	 * of the source database catalogs.
	 */
	if (listTableIndexList)
	{
		if (!catalog_iter_s_index_table(catalog,
										listDBoptions.schema_name,
										listDBoptions.table_name,
										NULL,
										&cli_list_index_print_hook))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		if (!catalog_iter_s_index(catalog,
								  NULL,
								  &cli_list_index_print_hook))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_index_print_hook is an iterator callback function.
 */
static bool
cli_list_index_print_hook(void *context, SourceIndex *index)
{
	if (index == NULL)
	{
		log_error("BUG: cli_list_index_print_hook called with a NULL index");
		return false;
	}

	if (!IS_EMPTY_STRING_BUFFER(index->constraintName))
	{
		if (index->isPrimary || index->isUnique)
		{
			fformat(stdout, "%8d | %10s | %20s | %20s | %25s | %s\n",
					index->indexOid,
					index->indexNamespace,
					index->indexRelname,
					index->constraintName,
					NULL_AS_EMPTY_STRING(index->constraintDef),
					NULL_AS_EMPTY_STRING(index->indexDef));
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
					NULL_AS_EMPTY_STRING(index->constraintDef),
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
				NULL_AS_EMPTY_STRING(index->constraintDef),
				NULL_AS_EMPTY_STRING(index->indexDef));
	}

	return true;
}


/*
 * cli_list_depends implements the command: pgcopydb list depends
 */
static void
cli_list_depends(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };
	SourceFilters *filters = &(copySpecs.filters);

	if (!listDBoptions.listSkipped)
	{
		log_error("pgcopydb list depends --list-skipped option is mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_DEPENDS,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	log_info("Listing dependencies in source database");

	if (IS_EMPTY_STRING_BUFFER(listDBoptions.filterFileName))
	{
		log_fatal("Option --filter is mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!parse_filters(listDBoptions.filterFileName, filters))
	{
		log_error("Failed to parse filters in file \"%s\"",
				  listDBoptions.filterFileName);
		exit(EXIT_CODE_BAD_ARGS);
	}

	copySpecs.fetchFilteredOids = true;

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
	if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
	{
		log_error("Failed to fetch a local copy of the catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *filtersDB = &(copySpecs.catalogs.filter);

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(filtersDB, &count))
	{
		log_error("Failed to count local catalogs objects");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Fetched information for %lld dependencies",
			 (long long) count.depends);

	fformat(stdout, "%20s | %30s | %8s | %8s | %20s | %s\n",
			"Schema Name", "Table Name", "Catalog", "OID", "Type", "Identity");

	fformat(stdout, "%20s-+-%30s-+-%8s-+-%8s-+-%20s-+-%30s\n",
			"--------------------",
			"------------------------------",
			"--------",
			"--------",
			"--------------------",
			"------------------------------");

	if (!catalog_iter_s_depend(filtersDB, NULL, &cli_list_depends_hook))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "\n");
}


/*
 * cli_list_depends_hook is an iterator callback function.
 */
static bool
cli_list_depends_hook(void *ctx, SourceDepend *dep)
{
	fformat(stdout, "%20s | %30s | %8u | %8u | %20s | %s\n",
			dep->nspname,
			dep->relname,
			dep->classid,
			dep->objid,
			dep->type,
			dep->identity);

	return true;
}


/*
 * cli_list_schema implements the command: pgcopydb list schema
 */
static void
cli_list_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	bool createWorkDir = true;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_ALL,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
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

	ConnStrings *dsn = &(listDBoptions.connStrings);

	log_info("Fetching schema from \"%s\"", dsn->safeSourcePGURI.pguri);
	log_info("Dumping schema into JSON file \"%s\"",
			 copySpecs.cfPaths.schemafile);

	copySpecs.fetchFilteredOids = true;

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results. When --force is used then we fetch the catalogs again.
	 */
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

	bool createWorkDir = false;

	if (!copydb_init_specs_from_listdboptions(&copySpecs,
											  &listDBoptions,
											  DATA_SECTION_NONE,
											  createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (listDBoptions.summary)
	{
		if (outputJSON)
		{
			const char *filename = copySpecs.cfPaths.summaryfile;

			if (!file_exists(filename))
			{
				log_fatal("Summary JSON file \"%s\" does not exists", filename);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			char *fileContents = NULL;
			long fileSize = 0L;

			if (!read_file(filename, &fileContents, &fileSize))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			fformat(stdout, "%s\n", fileContents);

			exit(EXIT_CODE_QUIT);
		}
		else
		{
			/*
			 * TODO: parse the JSON summary file, prepare our internal data
			 * structure with the information found, including pretty printed
			 * strings for durations etc, and then call print_summary().
			 */
			log_fatal("Failed to display summary, please use --json");
			exit(EXIT_CODE_BAD_ARGS);
		}
	}

	if (!catalog_init_from_specs(&copySpecs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
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


/*
 * copydb_init_specs_from_listdboptions initializes a CopyDBOptions structure
 * from a listDBoptions structure.
 */
static bool
copydb_init_specs_from_listdboptions(CopyDataSpec *copySpecs,
									 ListDBOptions *listDBoptions,
									 CopyDataSection section,
									 bool createWorkDir)
{
	(void) find_pg_commands(&(copySpecs->pgPaths));

	char *dir =
		IS_EMPTY_STRING_BUFFER(listDBoptions->dir)
		? NULL
		: listDBoptions->dir;

	bool service = false;
	char *serviceName = NULL;

	/* pretend --resume, allowing to work on an existing directory */
	bool restart = listDBoptions->force;
	bool resume = true;

	if (!copydb_init_workdir(copySpecs,
							 dir,
							 service,
							 serviceName,
							 restart,
							 resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	CopyDBOptions options = { 0 };

	strlcpy(options.dir, listDBoptions->dir, MAXPGPATH);
	options.connStrings = listDBoptions->connStrings;
	options.splitTablesLargerThan = listDBoptions->splitTablesLargerThan;

	/* process the --resume --not-consistent --snapshot options now */
	options.resume = listDBoptions->resume;
	options.notConsistent = listDBoptions->notConsistent;

	if (!cli_copydb_is_consistent(&options))
	{
		log_fatal("Option --resume requires option --not-consistent");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!copydb_init_specs(copySpecs, &options, section))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	return true;
}
