/*
 * src/bin/pgcopydb/cli_stream.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "postgres.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

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

CopyDBOptions streamDBoptions = { 0 };

static int cli_stream_getopts(int argc, char **argv);
static void cli_stream_receive(int argc, char **argv);
static void cli_stream_transform(int argc, char **argv);
static void cli_stream_apply(int argc, char **argv);

static void cli_stream_setup(int argc, char **argv);
static void cli_stream_cleanup(int argc, char **argv);
static void cli_stream_prefetch(int argc, char **argv);
static void cli_stream_catchup(int argc, char **argv);

static void stream_start_in_mode(LogicalStreamMode mode);

static CommandLine stream_setup_command =
	make_command(
		"setup",
		"Setup source and target systems for logical decoding",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --snapshot       Use snapshot obtained with pg_export_snapshot\n"
		"  --plugin         Output plugin to use (test_decoding, wal2json)\n" \
		"  --slot-name      Stream changes recorded by this slot\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_setup);

static CommandLine stream_cleanup_command =
	make_command(
		"cleanup",
		"cleanup source and target systems for logical decoding",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --snapshot       Use snapshot obtained with pg_export_snapshot\n"
		"  --slot-name      Stream changes recorded by this slot\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_cleanup);

static CommandLine stream_prefetch_command =
	make_command(
		"prefetch",
		"Stream JSON changes from the source database and transform them to SQL",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --slot-name      Stream changes recorded by this slot\n"
		"  --endpos         LSN position where to stop receiving changes",
		cli_stream_getopts,
		cli_stream_prefetch);

static CommandLine stream_catchup_command =
	make_command(
		"catchup",
		"Apply prefetched changes from SQL files to the target database",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --slot-name      Stream changes recorded by this slot\n"
		"  --endpos         LSN position where to stop receiving changes"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_catchup);

static CommandLine stream_receive_command =
	make_command(
		"receive",
		"Stream changes from the source database",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --slot-name      Stream changes recorded by this slot\n"
		"  --endpos         LSN position where to stop receiving changes",
		cli_stream_getopts,
		cli_stream_receive);

static CommandLine stream_transform_command =
	make_command(
		"transform",
		"Transform changes from the source database into SQL commands",
		" <json filename> <sql filename> ",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n",
		cli_stream_getopts,
		cli_stream_transform);

static CommandLine stream_apply_command =
	make_command(
		"apply",
		"Apply changes from the source database into the target database",
		" <sql filename> ",
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_apply);


static CommandLine *stream_subcommands[] = {
	&stream_setup_command,
	&stream_cleanup_command,
	&stream_prefetch_command,
	&stream_catchup_command,
	&create_commands,
	&drop_commands,
	&sentinel_commands,
	&stream_receive_command,
	&stream_transform_command,
	&stream_apply_command,
	NULL
};

CommandLine stream_commands =
	make_command_set("stream",
					 "Stream changes from the source database",
					 NULL, NULL, NULL, stream_subcommands);


/*
 * cli_stream_db_getopts parses the CLI options for the `stream db` command.
 */
static int
cli_stream_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "plugin", required_argument, NULL, 'p' },
		{ "slot-name", required_argument, NULL, 's' },
		{ "snapshot", required_argument, NULL, 'N' },
		{ "origin", required_argument, NULL, 'o' },
		{ "endpos", required_argument, NULL, 'E' },
		{ "restart", no_argument, NULL, 'r' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'C' },
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

	while ((c = getopt_long(argc, argv, "S:T:j:p:s:o:t:PVvdzqh",
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

			case 'o':
			{
				strlcpy(options.origin, optarg, NAMEDATALEN);
				log_trace("--origin %s", options.origin);
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

			case 'r':
			{
				options.restart = true;
				log_trace("--restart");

				if (options.resume)
				{
					log_fatal("Options --resume and --restart are not compatible");
				}
				break;
			}

			case 'R':
			{
				options.resume = true;
				log_trace("--resume");

				if (options.restart)
				{
					log_fatal("Options --resume and --restart are not compatible");
				}
				break;
			}

			case 'C':
			{
				options.notConsistent = true;
				log_trace("--not-consistent");
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

	if (IS_EMPTY_STRING_BUFFER(options.source_pguri) ||
		IS_EMPTY_STRING_BUFFER(options.target_pguri))
	{
		log_fatal("Options --source and --target are mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* when --slot-name is not used, use the default slot name "pgcopydb" */
	if (IS_EMPTY_STRING_BUFFER(options.slotName))
	{
		strlcpy(options.slotName,
				REPLICATION_SLOT_NAME,
				sizeof(options.slotName));
	}

	if (IS_EMPTY_STRING_BUFFER(options.origin))
	{
		strlcpy(options.origin, REPLICATION_ORIGIN, sizeof(options.origin));
		log_info("Using default origin node name \"%s\"", options.origin);
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

	if (errors > 0)
	{
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* publish our option parsing in the global variable */
	streamDBoptions = options;

	return optind;
}


/*
 * cli_stream_receive connects to the source database with the replication
 * protocol and streams changes associated with the replication slot
 * --slot-name.
 *
 * The replication slot is expected to use the wal2json replication plugin and
 * we store the received changes in JSON files named the same way as WAL files,
 * though with the ".json" suffix.
 */
static void
cli_stream_receive(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) stream_start_in_mode(STREAM_MODE_RECEIVE);
}


/*
 * cli_stream_prefetch implements receiving the JSON content and also
 * transforming it to SQL on the fly, as soon as a JSON file is closed.
 */
static void
cli_stream_prefetch(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) stream_start_in_mode(STREAM_MODE_PREFETCH);
}


/*
 * cli_stream_setup setups logical decoding on both the source and the target
 * database systems.
 *
 * On the source, it creates a replication slot (--slot-name) with the logical
 * replication plugin wal2json, and on the target it creates a replication
 * origin to track replay progress.
 */
static void
cli_stream_setup(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   streamDBoptions.source_pguri,
						   streamDBoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   streamDBoptions.restart,
						   streamDBoptions.resume,
						   !streamDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_setup_databases(&copySpecs,
								OutputPluginFromString(streamDBoptions.plugin),
								streamDBoptions.slotName,
								streamDBoptions.origin))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_stream_cleanup cleans-up by dropping source sentinel table and
 * replication slot, and dropping target replication origin.
 */
static void
cli_stream_cleanup(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool resume = true;         /* pretend --resume has been used */
	bool restart = false;       /* pretend --restart has NOT been used */
	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 restart,
							 resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   streamDBoptions.source_pguri,
						   streamDBoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   restart,
						   resume,
						   !streamDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_cleanup_databases(&copySpecs,
								  streamDBoptions.slotName,
								  streamDBoptions.origin))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_stream_catchup replays the SQL files that already exist, keeping track
 * and updating the replication origin.
 */
static void
cli_stream_catchup(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool auxilliary = false;

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 auxilliary))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   streamDBoptions.source_pguri,
						   streamDBoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   streamDBoptions.restart,
						   streamDBoptions.resume,
						   !streamDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   copySpecs.source_pguri,
						   copySpecs.target_pguri,
						   streamDBoptions.plugin,
						   streamDBoptions.slotName,
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_APPLY))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * First, we need to know enough about the source database system to be
	 * able to generate WAL file names. That's means the current timeline and
	 * the wal_segment_size.
	 */
	if (!stream_apply_catchup(&specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_stream_transform takes a logical decoding JSON file as obtained by the
 * previous command `pgcopydb stream receive` and transforms it into an SQL
 * file.
 */
static void
cli_stream_transform(int argc, char **argv)
{
	if (argc != 2)
	{
		log_fatal("Please provide a filename argument");
		commandline_help(stderr);

		exit(EXIT_CODE_BAD_ARGS);
	}

	char *jsonfilename = argv[0];
	char *sqlfilename = argv[1];

	if (!stream_transform_file(jsonfilename, sqlfilename))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_stream_apply takes a SQL file as obtained by the previous command
 * `pgcopydb stream transform` and applies it to the target database.
 */
static void
cli_stream_apply(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (argc != 1)
	{
		log_fatal("Please provide a filename argument");
		commandline_help(stderr);

		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) find_pg_commands(&(copySpecs.pgPaths));

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 false))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   streamDBoptions.source_pguri,
						   streamDBoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   streamDBoptions.restart,
						   streamDBoptions.resume,
						   !streamDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* prepare the replication origin tracking */
	StreamApplyContext context = { 0 };

	if (!setupReplicationOrigin(&context,
								&(copySpecs.cfPaths.cdc),
								streamDBoptions.source_pguri,
								streamDBoptions.target_pguri,
								streamDBoptions.origin,
								streamDBoptions.endpos,
								true))
	{
		log_error("Failed to setup replication origin on the target database");
		exit(EXIT_CODE_TARGET);
	}

	/*
	 * Force the SQL filename to the given argument, bypassing filename
	 * computations based on origin tracking. Already known-applied
	 * transactions are still skipped.
	 */
	char *sqlfilename = argv[0];

	strlcpy(context.sqlFileName, sqlfilename, sizeof(context.sqlFileName));

	if (!stream_apply_file(&context))
	{
		/* errors have already been logged */
		pgsql_finish(&(context.pgsql));
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pgsql_finish(&(context.pgsql));
}


/*
 * stream_start_in_mode initialises streaming replication for the given mode,
 * and then starts the logical replication client.
 */
static void
stream_start_in_mode(LogicalStreamMode mode)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 false))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	RestoreOptions restoreOptions = { 0 };

	if (!copydb_init_specs(&copySpecs,
						   streamDBoptions.source_pguri,
						   streamDBoptions.target_pguri,
						   1,   /* tableJobs */
						   1,   /* indexJobs */
						   0,   /* skip threshold */
						   "",  /* skip threshold pretty printed */
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* roles */
						   false, /* skipLargeObjects */
						   false, /* skipExtensions */
						   streamDBoptions.restart,
						   streamDBoptions.resume,
						   !streamDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   copySpecs.source_pguri,
						   copySpecs.target_pguri,
						   streamDBoptions.plugin,
						   streamDBoptions.slotName,
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   mode))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!startLogicalStreaming(&specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_SOURCE);
	}
}
