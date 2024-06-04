/*
 * src/bin/pgcopydb/cli_sentinel.c
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
#include "ld_stream.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "string_utils.h"

CopyDBOptions sentinelDBoptions = { 0 };

static int cli_sentinel_getopts(int argc, char **argv);
static void cli_sentinel_setup(int argc, char **argv);
static void cli_sentinel_set_startpos(int argc, char **argv);
static void cli_sentinel_set_endpos(int argc, char **argv);
static void cli_sentinel_set_apply(int argc, char **argv);
static void cli_sentinel_set_prefetch(int argc, char **argv);
static void cli_sentinel_get(int argc, char **argv);

static bool cli_sentinel_init_specs(CopyDataSpec *copySpecs);

CommandLine sentinel_setup_command =
	make_command(
		"setup",
		"Setup the sentinel table",
		"",
		"  --startpos    Start replaying changes when reaching this LSN\n"
		"  --endpos      Stop replaying changes when reaching this LSN\n",
		cli_sentinel_getopts,
		cli_sentinel_setup);

CommandLine sentinel_get_command =
	make_command(
		"get",
		"Get the sentinel table values",
		" --source ... ",
		"  --source      Postgres URI to the source database\n"
		"  --json        Format the output using JSON\n",
		cli_sentinel_getopts,
		cli_sentinel_get);

CommandLine sentinel_set_startpos_command =
	make_command(
		"startpos",
		"Set the sentinel start position LSN",
		" --source ... <start LSN>",
		"  --source      Postgres URI to the source database\n",
		cli_sentinel_getopts,
		cli_sentinel_set_startpos);

CommandLine sentinel_set_endpos_command =
	make_command(
		"endpos",
		"Set the sentinel end position LSN",
		" --source ... <end LSN>",
		"  --source      Postgres URI to the source database\n"
		"  --current     Use pg_current_wal_flush_lsn() as the endpos\n",
		cli_sentinel_getopts,
		cli_sentinel_set_endpos);

CommandLine sentinel_set_apply_command =
	make_command(
		"apply",
		"Set the sentinel apply mode",
		"",
		"  --source      Postgres URI to the source database\n",
		cli_sentinel_getopts,
		cli_sentinel_set_apply);

CommandLine sentinel_set_prefetch_command =
	make_command(
		"prefetch",
		"Set the sentinel prefetch mode",
		"",
		"  --source      Postgres URI to the source database\n",
		cli_sentinel_getopts,
		cli_sentinel_set_prefetch);

static CommandLine *sentinel_set_subcommands[] = {
	&sentinel_set_startpos_command,
	&sentinel_set_endpos_command,
	&sentinel_set_apply_command,
	&sentinel_set_prefetch_command,
	NULL
};

static CommandLine sentinel_set_commands =
	make_command_set("set",
					 "Set the sentinel table values",
					 NULL, NULL, NULL, sentinel_set_subcommands);

static CommandLine *sentinel_subcommands[] = {
	&sentinel_setup_command,
	&sentinel_get_command,
	&sentinel_set_commands,
	NULL
};

CommandLine sentinel_commands =
	make_command_set("sentinel",
					 "Maintain a sentinel table",
					 NULL, NULL, NULL, sentinel_subcommands);


/*
 * cli_sentinel_getopts parses the CLI options for the sentinel commands.
 */
static int
cli_sentinel_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "dir", required_argument, NULL, 'D' },
		{ "endpos", required_argument, NULL, 'E' },
		{ "startpos", required_argument, NULL, 's' },
		{ "current", no_argument, NULL, 'C' },
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

	/* read values from the environment */
	if (!cli_copydb_getenv(&options))
	{
		log_fatal("Failed to read default values from the environment");
		exit(EXIT_CODE_BAD_ARGS);
	}

	while ((c = getopt_long(argc, argv, "S:D:E:s:CJVvdzqh",
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

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 'E':
			{
				if (!parseLSN(optarg, &(options.endpos)))
				{
					log_fatal("Failed to parse endpos LSN: \"%s\"", optarg);
					exit(EXIT_CODE_BAD_ARGS);
				}

				log_trace("--endpos %X/%X",
						  (uint32_t) (options.endpos >> 32),
						  (uint32_t) options.endpos);
				break;
			}

			case 's':
			{
				if (!parseLSN(optarg, &(options.startpos)))
				{
					log_fatal("Failed to parse endpos LSN: \"%s\"", optarg);
					exit(EXIT_CODE_BAD_ARGS);
				}

				log_trace("--startpos %X/%X",
						  LSN_FORMAT_ARGS(options.startpos));
				break;
			}

			case 'C':
			{
				options.currentpos = true;
				log_trace("--current");
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

	if (options.currentpos)
	{
		if (options.endpos != InvalidXLogRecPtr)
		{
			log_fatal("Please choose only one of --endpos and --current");
			++errors;
		}

		if (options.connStrings.source_pguri == NULL)
		{
			log_fatal("Options --source is mandatory when using --current");
			++errors;
		}

		/* prepare safe versions of the connection strings (without password) */
		if (!cli_prepare_pguris(&(options.connStrings)))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	sentinelDBoptions = options;

	return optind;
}


/*
 * cli_sentinel_setup sets-up the sentinel table in pgcopydb catalogs.
 */
static void
cli_sentinel_setup(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	if (!cli_sentinel_init_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	if (!sentinel_setup(sourceDB,
						sentinelDBoptions.startpos,
						sentinelDBoptions.endpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_sentinel_set_startpos updates the startpos registered on the pgcopydb
 * sentinel.
 */
static void
cli_sentinel_set_startpos(int argc, char **argv)
{
	uint64_t startpos = InvalidXLogRecPtr;

	switch (argc)
	{
		case 0:
		{
			startpos = sentinelDBoptions.startpos;

			if (sentinelDBoptions.startpos == InvalidXLogRecPtr)
			{
				log_fatal("Please provide <startpos> or --startpos option");
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
			}

			break;
		}

		case 1:
		{
			if (!parseLSN(argv[0], &startpos))
			{
				log_fatal("Failed to parse startpos LSN: \"%s\"", argv[0]);
				exit(EXIT_CODE_BAD_ARGS);
			}

			break;
		}

		default:
		{
			commandline_help(stderr);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}

	CopyDataSpec copySpecs = { 0 };

	if (!cli_sentinel_init_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	if (!sentinel_update_startpos(sourceDB, startpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_sentinel_set_endpos updates the endpos registered on the pgcopydb
 * sentinel.
 */
static void
cli_sentinel_set_endpos(int argc, char **argv)
{
	uint64_t endpos = InvalidXLogRecPtr;
	bool useCurrentLSN = sentinelDBoptions.currentpos;

	switch (argc)
	{
		case 0:
		{
			endpos = sentinelDBoptions.endpos;

			if (!useCurrentLSN &&
				sentinelDBoptions.endpos == InvalidXLogRecPtr)
			{
				log_fatal("Please provide <endpos> or --endpos option");
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
			}

			break;
		}

		case 1:
		{
			if (useCurrentLSN)
			{
				log_fatal("Please choose only one of <endpos> and --current");
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
			}

			if (!parseLSN(argv[0], &endpos))
			{
				log_fatal("Failed to parse endpos LSN: \"%s\"", argv[0]);
				exit(EXIT_CODE_BAD_ARGS);
			}

			break;
		}

		default:
		{
			commandline_help(stderr);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}

	CopyDataSpec copySpecs = { 0 };

	if (!cli_sentinel_init_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (useCurrentLSN)
	{
		char *pguri = (char *) sentinelDBoptions.connStrings.source_pguri;

		if (!stream_fetch_current_lsn(&endpos, pguri, PGSQL_CONN_SOURCE))
		{
			exit(EXIT_CODE_SOURCE);
		}

		log_info("Fetched endpos %X/%X from source database",
				 LSN_FORMAT_ARGS(endpos));
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	if (!sentinel_update_endpos(sourceDB, endpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_get(sourceDB, &sentinel))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	log_info("pgcopydb sentinel endpos has been set to %X/%X",
			 LSN_FORMAT_ARGS(sentinel.endpos));

	fformat(stdout, "%X/%X\n", LSN_FORMAT_ARGS(sentinel.endpos));
}


/*
 * cli_sentinel_set_apply updates the apply boolean registered on the pgcopydb
 * sentinel. When the apply boolean is true,
 * catching-up is allowed: it's not only prefetching anymore.
 */
static void
cli_sentinel_set_apply(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	if (!cli_sentinel_init_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	if (!sentinel_update_apply(sourceDB, true))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_sentinel_set_prefetch updates the apply boolean registered on the
 * pgcopydb sentinel. When the apply boolean is false,
 * catching-up is not allowed: it's prefetching only.
 */
static void
cli_sentinel_set_prefetch(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	if (!cli_sentinel_init_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	if (!sentinel_update_apply(sourceDB, false))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_sentinel_get fetches and prints the current pgcopydb sentinel values.
 */
static void
cli_sentinel_get(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	if (!cli_sentinel_init_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	DatabaseCatalog *sourceDB = &(copySpecs.catalogs.source);

	CopyDBSentinel sentinel = { 0 };

	if (!sentinel_get(sourceDB, &sentinel))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_object();
		JSON_Object *jsobj = json_value_get_object(js);

		char startpos[PG_LSN_MAXLENGTH] = { 0 };
		char endpos[PG_LSN_MAXLENGTH] = { 0 };
		char write_lsn[PG_LSN_MAXLENGTH] = { 0 };
		char flush_lsn[PG_LSN_MAXLENGTH] = { 0 };
		char replay_lsn[PG_LSN_MAXLENGTH] = { 0 };

		sformat(startpos, PG_LSN_MAXLENGTH, "%X/%X",
				LSN_FORMAT_ARGS(sentinel.startpos));
		sformat(endpos, PG_LSN_MAXLENGTH, "%X/%X",
				LSN_FORMAT_ARGS(sentinel.endpos));
		sformat(write_lsn, PG_LSN_MAXLENGTH, "%X/%X",
				LSN_FORMAT_ARGS(sentinel.write_lsn));
		sformat(flush_lsn, PG_LSN_MAXLENGTH, "%X/%X",
				LSN_FORMAT_ARGS(sentinel.flush_lsn));
		sformat(replay_lsn, PG_LSN_MAXLENGTH, "%X/%X",
				LSN_FORMAT_ARGS(sentinel.replay_lsn));

		json_object_set_string(jsobj, "startpos", startpos);
		json_object_set_string(jsobj, "endpos", startpos);
		json_object_set_boolean(jsobj, "apply", sentinel.apply);
		json_object_set_string(jsobj, "write_lsn", write_lsn);
		json_object_set_string(jsobj, "flush_lsn", flush_lsn);
		json_object_set_string(jsobj, "replay_lsn", replay_lsn);

		char *serialized_string = json_serialize_to_string_pretty(js);

		fformat(stdout, "%s\n", serialized_string);

		json_free_serialized_string(serialized_string);
	}
	else
	{
		fformat(stdout, "%-10s %X/%X\n", "startpos",
				LSN_FORMAT_ARGS(sentinel.startpos));
		fformat(stdout, "%-10s %X/%X\n", "endpos",
				LSN_FORMAT_ARGS(sentinel.endpos));
		fformat(stdout, "%-10s %s\n", "apply",
				sentinel.apply ? "enabled" : "disabled");
		fformat(stdout, "%-10s %X/%X\n", "write_lsn",
				LSN_FORMAT_ARGS(sentinel.write_lsn));
		fformat(stdout, "%-10s %X/%X\n", "flush_lsn",
				LSN_FORMAT_ARGS(sentinel.flush_lsn));
		fformat(stdout, "%-10s %X/%X\n", "replay_lsn",
				LSN_FORMAT_ARGS(sentinel.replay_lsn));
	}
}


/*
 * cli_sentinel_init_specs initializes our CopyDataSpec from cli options.
 */
static bool
cli_sentinel_init_specs(CopyDataSpec *copySpecs)
{
	char *dir =
		IS_EMPTY_STRING_BUFFER(sentinelDBoptions.dir)
		? NULL
		: sentinelDBoptions.dir;

	bool service = false;
	char *serviceName = NULL;
	bool createWorkDir = false;

	/* pretend --resume, allowing to work on an existing directory */
	bool restart = false;
	bool resume = true;

	sentinelDBoptions.notConsistent = true;

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

	if (!copydb_init_specs(copySpecs, &sentinelDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!catalog_init_from_specs(copySpecs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	return true;
}
