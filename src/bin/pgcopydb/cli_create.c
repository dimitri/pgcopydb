/*
 * src/bin/pgcopydb/cli_create.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "commandline.h"
#include "copydb.h"
#include "env_utils.h"
#include "ld_stream.h"
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"

static int cli_create_snapshot_getopts(int argc, char **argv);
static void cli_create_snapshot(int argc, char **argv);

static int cli_create_slot_getopts(int argc, char **argv);
static void cli_create_slot(int argc, char **argv);
static void cli_drop_slot(int argc, char **argv);

static int cli_create_origin_getopts(int argc, char **argv);
static void cli_create_origin(int argc, char **argv);
static void cli_drop_origin(int argc, char **argv);


CommandLine create_snapshot_command =
	make_command(
		"snapshot",
		"Create and exports a snapshot on the source database",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n",
		cli_create_snapshot_getopts,
		cli_create_snapshot);

static CommandLine create_repl_slot_command =
	make_command(
		"slot",
		"Create a replication slot in the source database",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --snapshot       Use snapshot obtained with pg_export_snapshot\n"
		"  --plugin         Output plugin to use (test_decoding, wal2json)\n" \
		"  --slot-name      Use this Postgres replication slot name\n",
		cli_create_slot_getopts,
		cli_create_slot);

static CommandLine create_origin_command =
	make_command(
		"origin",
		"Create a replication origin in the target database",
		" --target ... ",
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --origin         Use this Postgres origin name\n"
		"  --start-pos      LSN position from where to start applying changes\n",
		cli_create_origin_getopts,
		cli_create_origin);

static CommandLine *create_subcommands[] = {
	&create_repl_slot_command,
	&create_origin_command,
	NULL
};

CommandLine create_commands =
	make_command_set("create",
					 "Create resources needed for pgcopydb",
					 NULL, NULL, NULL, create_subcommands);


static CommandLine drop_repl_slot_command =
	make_command(
		"slot",
		"Drop a replication slot in the source database",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --slot-name      Use this Postgres replication slot name\n",
		cli_create_slot_getopts,
		cli_drop_slot);

static CommandLine drop_origin_command =
	make_command(
		"origin",
		"Drop a replication origin in the target database",
		" --target ... ",
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --origin         Use this Postgres origin name\n",
		cli_create_origin_getopts,
		cli_drop_origin);

static CommandLine *drop_subcommands[] = {
	&drop_repl_slot_command,
	&drop_origin_command,
	NULL
};

CommandLine drop_commands =
	make_command_set("drop",
					 "Drop resources needed for pgcopydb",
					 NULL, NULL, NULL, drop_subcommands);

CopyDBOptions createSNoptions = { 0 };
CopyDBOptions createSlotOptions = { 0 };
CopyDBOptions createOriginOptions = { 0 };

static int
cli_create_snapshot_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "dir", required_argument, NULL, 'D' },
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

	while ((c = getopt_long(argc, argv, "S:T:D:Vvdzqh",
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

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
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
		log_fatal("Option --source is mandatory");
		++errors;
	}

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	createSNoptions = options;

	return optind;
}


/*
 * cli_create_snapshot creates a snapshot on the source database, and stays
 * connected until it receives a signal to quit.
 */
static void
cli_create_snapshot(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = true;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 createSNoptions.restart,
							 createSNoptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   createSNoptions.source_pguri,
						   createSNoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   createSNoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   createSNoptions.restart,
						   createSNoptions.resume,
						   !createSNoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_prepare_snapshot(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	fformat(stdout, "%s\n", copySpecs.sourceSnapshot.snapshot);

	for (;;)
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			TransactionSnapshot *snapshot = &(copySpecs.sourceSnapshot);
			PGSQL *pgsql = &(snapshot->pgsql);

			(void) pgsql_finish(pgsql);

			log_info("Asked to terminate, aborting");

			break;
		}

		/* sleep for 100ms between checks for interrupts */
		pg_usleep(100 * 1000);
	}
}


/*
 * cli_create_slot_getopts parses the command line options of the command:
 * pgcopydb create slot
 */
static int
cli_create_slot_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "dir", required_argument, NULL, 'D' },
		{ "plugin", required_argument, NULL, 'p' },
		{ "slot-name", required_argument, NULL, 's' },
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

	/* pretend that --resume was used */
	options.resume = true;

	while ((c = getopt_long(argc, argv, "S:T:D:p:s:Vvdzqh",
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

			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 's':
			{
				strlcpy(options.slotName, optarg, NAMEDATALEN);
				log_trace("--slot-name %s", options.slotName);
				break;
			}

			case 'p':
			{
				strlcpy(options.plugin, optarg, NAMEDATALEN);
				log_trace("--plugin %s", options.plugin);
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
		log_fatal("Option --source is mandatory");
		++errors;
	}

	/* when --slot-name is not used, use the default slot name "pgcopydb" */
	if (IS_EMPTY_STRING_BUFFER(options.slotName))
	{
		strlcpy(options.slotName, REPLICATION_SLOT_NAME, sizeof(options.slotName));
		log_info("Using default slot name \"%s\"", options.slotName);
	}

	if (IS_EMPTY_STRING_BUFFER(options.plugin))
	{
		strlcpy(options.plugin, REPLICATION_PLUGIN, sizeof(options.plugin));
		log_info("Using default output plugin \"%s\"", options.plugin);
	}
	else
	{
		if (OutputPluginFromString(options.plugin) == STREAM_PLUGIN_UNKNOWN)
		{
			log_fatal("Unknown replication plugin \"%s\", please use either "
					  "test_decoding (the default) or wal2json",
					  options.plugin);
			++errors;
		}
	}

	if (!cli_copydb_is_consistent(&options))
	{
		log_fatal("Option --resume requires option --not-consistent");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	createSlotOptions = options;

	return optind;
}


/*
 * cli_create_slot implements the comand: pgcopydb create slot
 */
static void
cli_create_slot(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 createSlotOptions.restart,
							 createSlotOptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   createSlotOptions.source_pguri,
						   createSlotOptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   createSlotOptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   createSlotOptions.restart,
						   createSlotOptions.resume,
						   !createSlotOptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	uint64_t lsn = 0;

	if (!stream_create_repl_slot(&copySpecs,
								 OutputPluginFromString(createSlotOptions.plugin),
								 createSlotOptions.slotName,
								 &lsn))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_create_slot implements the comand: pgcopydb drop slot
 */
static void
cli_drop_slot(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 createSlotOptions.restart,
							 createSlotOptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   createSlotOptions.source_pguri,
						   createSlotOptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   createSlotOptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   createSlotOptions.restart,
						   createSlotOptions.resume,
						   !createSlotOptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, createSlotOptions.source_pguri, PGSQL_CONN_SOURCE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}

	if (!pgsql_drop_replication_slot(&pgsql, createSlotOptions.slotName))
	{
		log_error("Failed to drop replication slot \"%s\"",
				  createSlotOptions.slotName);
		exit(EXIT_CODE_SOURCE);
	}
}


/*
 * cli_create_origin_getopts parses the command line options of the command:
 * pgcopydb create slot
 */
static int
cli_create_origin_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "origin", required_argument, NULL, 'o' },
		{ "startpos", required_argument, NULL, 's' },
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

	/* pretend that --resume was used */
	options.resume = true;

	while ((c = getopt_long(argc, argv, "T:D:o:s:Vvdzqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'T':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --target connection string, "
							  "see above for details.");
					++errors;
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

			case 'o':
			{
				strlcpy(options.origin, optarg, NAMEDATALEN);
				log_trace("--origin %s", options.origin);
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
	if (IS_EMPTY_STRING_BUFFER(options.target_pguri))
	{
		if (env_exists(PGCOPYDB_TARGET_PGURI))
		{
			if (!get_env_copy(PGCOPYDB_TARGET_PGURI,
							  options.target_pguri,
							  sizeof(options.target_pguri)))
			{
				/* errors have already been logged */
				++errors;
			}
		}
	}

	if (IS_EMPTY_STRING_BUFFER(options.target_pguri))
	{
		log_fatal("Option --target is mandatory");
		++errors;
	}

	/* when --origin is not used, use the default slot name "pgcopydb" */
	if (IS_EMPTY_STRING_BUFFER(options.origin))
	{
		strlcpy(options.origin, REPLICATION_ORIGIN, sizeof(options.origin));
	}

	if (!cli_copydb_is_consistent(&options))
	{
		log_fatal("Option --resume requires option --not-consistent");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	createOriginOptions = options;

	return optind;
}


/*
 * cli_create_origin implements the comand: pgcopydb create origin
 */
static void
cli_create_origin(int argc, char **argv)
{
	if (createOriginOptions.startpos == 0)
	{
		log_fatal("Option --startpos is mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 createOriginOptions.restart,
							 createOriginOptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   createOriginOptions.source_pguri,
						   createOriginOptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   createOriginOptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   createOriginOptions.restart,
						   createOriginOptions.resume,
						   !createOriginOptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_create_origin(&copySpecs,
							  createOriginOptions.origin,
							  createOriginOptions.startpos))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_create_origin implements the comand: pgcopydb drop origin
 */
static void
cli_drop_origin(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 createOriginOptions.restart,
							 createOriginOptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   createOriginOptions.source_pguri,
						   createOriginOptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   createOriginOptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   createOriginOptions.restart,
						   createOriginOptions.resume,
						   !createOriginOptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	PGSQL dst = { 0 };
	char *nodeName = createOriginOptions.origin;

	if (!pgsql_init(&dst, copySpecs.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!pgsql_replication_origin_drop(&dst, nodeName))
	{
		log_error("Failed to drop replication origin \"%s\"", nodeName);
		exit(EXIT_CODE_TARGET);
	}
}
