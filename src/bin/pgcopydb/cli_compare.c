/*
 * src/bin/pgcopydb/cli_compare.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "catalog.h"
#include "cli_common.h"
#include "cli_root.h"
#include "commandline.h"
#include "copydb.h"
#include "env_utils.h"
#include "ld_stream.h"
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "progress.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"

static int cli_compare_getopts(int argc, char **argv);
static void cli_compare_schema(int argc, char **argv);
static void cli_compare_data(int argc, char **argv);

static bool cli_compare_data_table_hook(void *ctx, SourceTable *table);

static CommandLine compare_schema_command =
	make_command(
		"schema",
		"Compare source and target schema",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n",
		cli_compare_getopts,
		cli_compare_schema);

static CommandLine compare_data_command =
	make_command(
		"data",
		"Compare source and target data",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --json           Format the output using JSON\n",
		cli_compare_getopts,
		cli_compare_data);

static CommandLine *compare_subcommands[] = {
	&compare_schema_command,
	&compare_data_command,
	NULL
};

CommandLine compare_commands =
	make_command_set("compare",
					 "Compare source and target databases",
					 NULL, NULL, NULL, compare_subcommands);

CopyDBOptions compareOptions = { 0 };


static int
cli_compare_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "jobs", required_argument, NULL, 'j' },
		{ "table-jobs", required_argument, NULL, 'j' },
		{ "json", no_argument, NULL, 'J' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "notice", no_argument, NULL, 'v' },
		{ "debug", no_argument, NULL, 'd' },
		{ "trace", no_argument, NULL, 'z' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	/* install default values */
	options.tableJobs = DEFAULT_TABLE_JOBS;
	options.indexJobs = DEFAULT_INDEX_JOBS;
	options.lObjectJobs = DEFAULT_LARGE_OBJECTS_JOBS;

	/* read values from the environment */
	if (!cli_copydb_getenv(&options))
	{
		log_fatal("Failed to read default values from the environment");
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* bypass computing partitionning specs */
	options.splitTablesLargerThan.bytes = 0;

	while ((c = getopt_long(argc, argv, "S:T:D:j:JVvdzqh",
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

			case 'T':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --target connection string, "
							  "see above for details.");
					++errors;
				}
				options.connStrings.target_pguri = pg_strdup(optarg);
				log_trace("--target %s", options.connStrings.target_pguri);
				break;
			}

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 'j':
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

	if (options.connStrings.source_pguri == NULL ||
		options.connStrings.target_pguri == NULL)
	{
		log_fatal("Option --source and --target are mandatory");
		++errors;
	}

	/* prepare safe versions of the connection strings (without password) */
	if (!cli_prepare_pguris(&(options.connStrings)))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	compareOptions = options;

	return optind;
}


/*
 * cli_compare_schema compares the schema on the source and target databases.
 */
static void
cli_compare_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	char *dir =
		IS_EMPTY_STRING_BUFFER(compareOptions.dir)
		? NULL
		: compareOptions.dir;

	bool createWorkDir = true;
	bool service = true;
	char *serviceName = "snapshot";

	/* pretend that --resume --not-consistent have been used */
	compareOptions.resume = true;
	compareOptions.notConsistent = true;

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 service,
							 serviceName,
							 compareOptions.restart,
							 compareOptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* bypass computing partitionning specs */
	SplitTableLargerThan empty = { 0 };
	compareOptions.splitTablesLargerThan = empty;

	if (!copydb_init_specs(&copySpecs, &compareOptions, DATA_SECTION_ALL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!compare_schemas(&copySpecs))
	{
		log_fatal("Comparing the schemas failed, see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_compare_data compares the data on the source and target databases.
 */
static void
cli_compare_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	char *dir =
		IS_EMPTY_STRING_BUFFER(compareOptions.dir)
		? NULL
		: compareOptions.dir;

	bool createWorkDir = true;
	bool service = true;
	char *serviceName = "snapshot";

	/* pretend that --resume --not-consistent have been used */
	compareOptions.resume = true;
	compareOptions.notConsistent = true;

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 service,
							 serviceName,
							 compareOptions.restart,
							 compareOptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &compareOptions, DATA_SECTION_TABLE_DATA))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!compare_data(&copySpecs))
	{
		log_fatal("Failed to compute checksums, see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	if (!catalog_init(sourceDB))
	{
		log_error("Failed to open internal catalogs in COPY worker process, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_array();
		JSON_Array *jsArray = json_value_get_array(js);

		if (!catalog_iter_s_table(sourceDB,
								  jsArray,
								  &cli_compare_data_table_hook))
		{
			log_error("Failed to compare tables, see above for details");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		char *serialized_string = json_serialize_to_string_pretty(js);

		fformat(stdout, "%s\n", serialized_string);

		json_free_serialized_string(serialized_string);
	}
	else
	{
		fformat(stdout, "%30s | %s | %36s | %36s \n",
				"Table Name", "!", "Source Checksum", "Target Checksum");

		fformat(stdout, "%30s-+-%s-+-%36s-+-%36s \n",
				"------------------------------",
				"-",
				"------------------------------------",
				"------------------------------------");

		if (!catalog_iter_s_table(sourceDB,
								  NULL,
								  &cli_compare_data_table_hook))
		{
			log_error("Failed to compare tables, see above for details");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		fformat(stdout, "\n");
	}

	if (!catalog_close(sourceDB))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * compare_queue_table_hook is an iterator callback function.
 */
static bool
cli_compare_data_table_hook(void *ctx, SourceTable *table)
{
	if (outputJSON)
	{
		JSON_Array *jsArray = (JSON_Array *) ctx;

		JSON_Value *jsComp = json_value_init_object();
		JSON_Object *jsObj = json_value_get_object(jsComp);

		json_object_dotset_string(jsObj, "schema", table->nspname);
		json_object_dotset_string(jsObj, "name", table->relname);

		json_object_dotset_number(jsObj,
								  "source.rowcount",
								  table->sourceChecksum.rowcount);

		json_object_dotset_string(jsObj,
								  "source.checksum",
								  table->sourceChecksum.checksum);

		json_object_dotset_number(jsObj,
								  "target.rowcount",
								  table->targetChecksum.rowcount);

		json_object_dotset_string(jsObj,
								  "target.checksum",
								  table->targetChecksum.checksum);

		json_array_append_value(jsArray, jsComp);
	}
	else
	{
		TableChecksum *srcChk = &(table->sourceChecksum);
		TableChecksum *dstChk = &(table->targetChecksum);

		fformat(stdout, "%30s | %s | %36s | %36s \n",
				table->qname,
				streq(srcChk->checksum, dstChk->checksum) ? " " : "!",
				srcChk->checksum,
				dstChk->checksum);
	}

	return true;
}
