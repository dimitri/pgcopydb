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
static void cli_sentinel_create(int argc, char **argv);
static void cli_sentinel_drop(int argc, char **argv);
static void cli_sentinel_set_startpos(int argc, char **argv);
static void cli_sentinel_set_endpos(int argc, char **argv);
static void cli_sentinel_set_apply(int argc, char **argv);
static void cli_sentinel_set_prefetch(int argc, char **argv);
static void cli_sentinel_get(int argc, char **argv);

CommandLine sentinel_create_command =
	make_command(
		"create",
		"Create the sentinel table on the source database",
		" --source ... ",
		"  --source      Postgres URI to the source database\n"
		"  --startpos    Start replaying changes when reaching this LSN\n"
		"  --endpos      Stop replaying changes when reaching this LSN\n",
		cli_sentinel_getopts,
		cli_sentinel_create);

CommandLine sentinel_drop_command =
	make_command(
		"drop",
		"Drop the sentinel table on the source database",
		" --source ... ",
		"  --source      Postgres URI to the source database\n",
		cli_sentinel_getopts,
		cli_sentinel_drop);

CommandLine sentinel_get_command =
	make_command(
		"get",
		"Get the sentinel table values on the source database",
		" --source ... ",
		"  --source      Postgres URI to the source database\n"
		"  --json        Format the output using JSON\n",
		cli_sentinel_getopts,
		cli_sentinel_get);

CommandLine sentinel_set_startpos_command =
	make_command(
		"startpos",
		"Set the sentinel start position LSN on the source database",
		" --source ... <start LSN>",
		"  --source      Postgres URI to the source database\n",
		cli_sentinel_getopts,
		cli_sentinel_set_startpos);

CommandLine sentinel_set_endpos_command =
	make_command(
		"endpos",
		"Set the sentinel end position LSN on the source database",
		" --source ... <end LSN>",
		"  --source      Postgres URI to the source database\n"
		"  --current     Use pg_current_wal_flush_lsn() as the endpos\n",
		cli_sentinel_getopts,
		cli_sentinel_set_endpos);

CommandLine sentinel_set_apply_command =
	make_command(
		"apply",
		"Set the sentinel apply mode on the source database",
		"",
		"  --source      Postgres URI to the source database\n",
		cli_sentinel_getopts,
		cli_sentinel_set_apply);

CommandLine sentinel_set_prefetch_command =
	make_command(
		"prefetch",
		"Set the sentinel prefetch mode on the source database",
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
					 "Maintain a sentinel table on the source database",
					 NULL, NULL, NULL, sentinel_set_subcommands);

static CommandLine *sentinel_subcommands[] = {
	&sentinel_create_command,
	&sentinel_drop_command,
	&sentinel_get_command,
	&sentinel_set_commands,
	NULL
};

CommandLine sentinel_commands =
	make_command_set("sentinel",
					 "Maintain a sentinel table on the source database",
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
		{ "endpos", required_argument, NULL, 'E' },
		{ "startpos", required_argument, NULL, 's' },
		{ "current", no_argument, NULL, 'C' },
		{ "json", no_argument, NULL, 'J' },
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

	while ((c = getopt_long(argc, argv, "S:s:E:CVvdzqh",
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

	/* stream commands support the source URI environment variable */
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
		log_fatal("Options --source is mandatory");
		++errors;
	}

	if (options.currentpos && options.endpos != InvalidXLogRecPtr)
	{
		log_fatal("Please choose only one of --endpos and --current");
		++errors;
	}

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	sentinelDBoptions = options;

	return optind;
}


/*
 * cli_sentinel_create creates the pgcopydb.sentinel table on the source
 * database.
 */
static void
cli_sentinel_create(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) find_pg_commands(&(copySpecs.pgPaths));

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   sentinelDBoptions.source_pguri,
						   sentinelDBoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   sentinelDBoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   false, /* skipCollations */
						   false, /* noRolesPasswords */
						   sentinelDBoptions.restart,
						   sentinelDBoptions.resume,
						   !sentinelDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_create_sentinel(&copySpecs,
								sentinelDBoptions.startpos,
								sentinelDBoptions.endpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_sentinel_drop drops the pgcopydb.sentinel table on the source database.
 */
static void
cli_sentinel_drop(int argc, char **argv)
{
	char *pguri = (char *) sentinelDBoptions.source_pguri;
	PGSQL pgsql = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_execute(&pgsql, "drop schema pgcopydb cascade"))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_sentinel_set_startpos updates the startpos registered on the pgcopydb
 * sentinel on the source database.
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

	char *pguri = (char *) sentinelDBoptions.source_pguri;
	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_update_sentinel_startpos(&pgsql, startpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_sentinel_set_endpos updates the endpos registered on the pgcopydb
 * sentinel on the source database.
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

	char *pguri = (char *) sentinelDBoptions.source_pguri;
	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_begin(&pgsql))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_server_version(&pgsql))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_update_sentinel_endpos(&pgsql, useCurrentLSN, endpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_commit(&pgsql))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (useCurrentLSN)
	{
		CopyDBSentinel sentinel = { 0 };

		if (!pgsql_get_sentinel(&pgsql, &sentinel))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_SOURCE);
		}

		log_info("pgcopydb sentinel endpos has been set to %X/%X",
				 LSN_FORMAT_ARGS(sentinel.endpos));

		fformat(stdout, "%X/%X\n", LSN_FORMAT_ARGS(sentinel.endpos));
	}
}


/*
 * cli_sentinel_set_apply updates the apply boolean registered on the pgcopydb
 * sentinel on the source database. When the apply boolean is true,
 * catching-up is allowed: it's not only prefetching anymore.
 */
static void
cli_sentinel_set_apply(int argc, char **argv)
{
	char *pguri = (char *) sentinelDBoptions.source_pguri;
	PGSQL pgsql = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_update_sentinel_apply(&pgsql, true))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_sentinel_set_prefetch updates the apply boolean registered on the
 * pgcopydb sentinel on the source database. When the apply boolean is false,
 * catching-up is not allowed: it's prefetching only.
 */
static void
cli_sentinel_set_prefetch(int argc, char **argv)
{
	char *pguri = (char *) sentinelDBoptions.source_pguri;
	PGSQL pgsql = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_update_sentinel_apply(&pgsql, false))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_sentinel_get fetches and prints the current pgcopydb sentinel values.
 */
static void
cli_sentinel_get(int argc, char **argv)
{
	char *pguri = (char *) sentinelDBoptions.source_pguri;
	PGSQL pgsql = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	CopyDBSentinel sentinel = { 0 };

	if (!pgsql_get_sentinel(&pgsql, &sentinel))
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
		json_value_free(js);
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
