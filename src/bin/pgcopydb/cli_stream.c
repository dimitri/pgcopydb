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

CopyDBOptions streamDBoptions = { 0 };

static int cli_stream_getopts(int argc, char **argv);

static void cli_stream_receive(int argc, char **argv);
static void cli_stream_transform(int argc, char **argv);
static void cli_stream_apply(int argc, char **argv);

static void cli_stream_setup(int argc, char **argv);
static void cli_stream_cleanup(int argc, char **argv);

static void cli_stream_prefetch(int argc, char **argv);
static void cli_stream_catchup(int argc, char **argv);
static void cli_stream_replay(int argc, char **argv);

static void stream_start_in_mode(LogicalStreamMode mode);

static CommandLine stream_setup_command =
	make_command(
		"setup",
		"Setup source and target systems for logical decoding",
		"",
		"  --source                      Postgres URI to the source database\n"
		"  --target                      Postgres URI to the target database\n"
		"  --dir                         Work directory to use\n"
		"  --restart                     Allow restarting when temp files exist already\n"
		"  --resume                      Allow resuming operations after a failure\n"
		"  --not-consistent              Allow taking a new snapshot on the source database\n"
		"  --snapshot                    Use snapshot obtained with pg_export_snapshot\n"
		"  --plugin                      Output plugin to use (test_decoding, wal2json)\n"
		"  --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin\n"
		"  --slot-name                   Stream changes recorded by this slot\n"
		"  --origin                      Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_setup);

static CommandLine stream_cleanup_command =
	make_command(
		"cleanup",
		"cleanup source and target systems for logical decoding",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
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
		"  --endpos         LSN position where to stop receiving changes\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_catchup);

static CommandLine stream_replay_command =
	make_command(
		"replay",
		"Replay changes from the source to the target database, live",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --slot-name      Stream changes recorded by this slot\n"
		"  --endpos         LSN position where to stop receiving changes\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_replay);

static CommandLine stream_receive_command =
	make_command(
		"receive",
		"Stream changes from the source database",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --to-stdout      Stream logical decoding messages to stdout\n"
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
		"  --target         Postgres URI to the target database\n"
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
	&stream_replay_command,
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
	int c, option_index = 0, errors = 0;
	int verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "plugin", required_argument, NULL, 'p' },
		{ "wal2json-numeric-as-string", no_argument, NULL, 'w' },
		{ "slot-name", required_argument, NULL, 's' },
		{ "snapshot", required_argument, NULL, 'N' },
		{ "origin", required_argument, NULL, 'o' },
		{ "endpos", required_argument, NULL, 'E' },
		{ "restart", no_argument, NULL, 'r' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'C' },
		{ "to-stdout", no_argument, NULL, 'O' },
		{ "from-stdin", no_argument, NULL, 'I' },
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
					++errors;
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

			case 's':
			{
				strlcpy(options.slot.slotName, optarg, NAMEDATALEN);
				log_trace("--slot-name %s", options.slot.slotName);
				break;
			}

			case 'p':
			{
				options.slot.plugin = OutputPluginFromString(optarg);
				log_trace("--plugin %s",
						  OutputPluginToString(options.slot.plugin));
				break;
			}

			case 'w':
			{
				options.slot.wal2jsonNumericAsString = true;
				log_trace("--wal2json-numeric-as-string");
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
					++errors;
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

			case 'O':
			{
				options.stdOut = true;
				log_trace("--to-stdout");
				break;
			}

			case 'I':
			{
				options.stdIn = true;
				log_trace("--from-stdin");
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
			{
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
				break;
			}
		}
	}

	if (options.connStrings.source_pguri == NULL ||
		options.connStrings.target_pguri == NULL)
	{
		log_fatal("Options --source and --target are mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (options.slot.wal2jsonNumericAsString &&
		options.slot.plugin != STREAM_PLUGIN_WAL2JSON)
	{
		log_fatal("Option --wal2json-numeric-as-string "
				  "requires option --plugin=wal2json");
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* prepare safe versions of the connection strings (without password) */
	if (!cli_prepare_pguris(&(options.connStrings)))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
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

	bool createWorkDir = true;

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 false, /* service */
							 NULL,  /* serviceName */
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_CATCHUP,
						   &(copySpecs.catalogs.source),
						   streamDBoptions.stdIn,
						   streamDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_setup_databases(&copySpecs, &specs))
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

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	bool resume = true;    /* pretend --resume has been used */
	bool restart = false;  /* pretend --restart has NOT been used */

	bool createWorkDir = false;
	bool service = false;

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 service,
							 NULL,
							 restart,
							 resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_cleanup_databases(&copySpecs,
								  streamDBoptions.slot.slotName,
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

	/*
	 * Both the catchup and the replay command starts the "apply" service, so
	 * that they conflict with each other.
	 */
	bool createWorkDir = false;
	bool service = true;
	char *serviceName = "apply";

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 service,
							 serviceName,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_CATCHUP,
						   &(copySpecs.catalogs.source),
						   streamDBoptions.stdIn,
						   streamDBoptions.stdOut,
						   logSQL))
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
 * cli_stream_replay streams the DML changes from logical decoding on the
 * source database, stores them in JSON files locally, transforms them in SQL
 * statements to disk, and replays the SQL statements on the target database,
 * keeping track and updating the replication origin.
 */
static void
cli_stream_replay(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	(void) find_pg_commands(&(copySpecs.pgPaths));

	/*
	 * Both the receive and the prefetch command starts the "receive" service,
	 * so that they conflict with each other.
	 */
	bool createWorkDir = false;
	bool service = true;
	char *serviceName = "receive";

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 service,
							 serviceName,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_REPLAY,
						   &(copySpecs.catalogs.source),
						   true,  /* stdin */
						   true, /* stdout */
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Remove the possibly still existing stream context files from
	 * previous round of operations (--resume, etc). We want to make sure
	 * that the catchup process reads the files created on this connection.
	 */
	if (!stream_cleanup_context(&specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Before starting the receive, transform, and apply sub-processes, we need
	 * to set the sentinel endpos to the command line --endpos option, when
	 * given.
	 *
	 * Also fetch the current values from the pgcopydb.sentinel. It might have
	 * been updated from a previous run of the command, and we might have
	 * nothing to catch-up to when e.g. the endpos was reached already.
	 */
	CopyDBSentinel sentinel = { 0 };

	if (!follow_init_sentinel(&specs, &sentinel))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (sentinel.endpos != InvalidXLogRecPtr &&
		sentinel.endpos <= sentinel.replay_lsn)
	{
		log_info("Current endpos %X/%X was previously reached at %X/%X",
				 LSN_FORMAT_ARGS(sentinel.endpos),
				 LSN_FORMAT_ARGS(sentinel.replay_lsn));

		exit(EXIT_CODE_QUIT);
	}

	if (!followDB(&copySpecs, &specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
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
	CopyDataSpec copySpecs = { 0 };

	if (argc != 2)
	{
		log_fatal("Please provide a filename argument");
		commandline_help(stderr);

		exit(EXIT_CODE_BAD_ARGS);
	}

	char *jsonfilename = argv[0];
	char *sqlfilename = argv[1];

	(void) find_pg_commands(&(copySpecs.pgPaths));

	/*
	 * The command `pgcopydb stream transform` can be used with filenames, in
	 * which case it is not a service, or with the JSON file connected to the
	 * stdin stream (using '-' as the jsonfilename), in which case the command
	 * is a service.
	 *
	 * Finally, always assume --resume has been used so that we can re-use an
	 * existing work directory when it exists.
	 */
	bool createWorkDir = false;
	bool service = streq(jsonfilename, "-");
	char *serviceName = "transform";

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 service,
							 serviceName,
							 streamDBoptions.restart,
							 true, /* streamDBoptions.resume */
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_CATCHUP,
						   &(copySpecs.catalogs.source),
						   streamDBoptions.stdIn,
						   streamDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_init_context(&specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Do we use the file API, or the stream API?
	 *
	 * The filename arguments can be set to - to mean stdin and stdout
	 * respectively, and in that case we use the streaming API so that we're
	 * compatible with Unix pipes.
	 *
	 * When the input is a stream, even if the output is a file, we still use
	 * the streaming API, we just open the output stream here before calling
	 * into the stream API.
	 */
	if (streq(jsonfilename, "-"))
	{
		specs.in = stdin;
		specs.out = stdout;

		if (!streq(sqlfilename, "-"))
		{
			log_fatal("JSON filename is - (stdin), "
					  "SQL filename should be - (stdout)");
			log_fatal("When streaming from stdin, out filename is computed "
					  "automatically from the current LSN.");
			exit(EXIT_CODE_BAD_ARGS);
		}

		if (!stream_transform_stream(&specs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (fclose(specs.out) != 0)
		{
			log_error("Failed to close file \"%s\": %m", sqlfilename);
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		if (!catalog_open(specs.sourceDB))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!stream_transform_context_init_pgsql(&specs))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		bool success = stream_transform_file(&specs, jsonfilename, sqlfilename);

		pgsql_finish(&(specs.transformPGSQL));

		if (!catalog_close(specs.sourceDB))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!success)
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
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

	char *sqlfilename = argv[0];

	/*
	 * The command `pgcopydb stream apply` can be used with a filename, in
	 * which case it is not a service, or with the SQL file connected to the
	 * stdin stream (using '-' as the filename), in which case the command is a
	 * service.
	 *
	 * Then, both the catchup and the replay command starts the "apply"
	 * service, so that they conflict with each other.
	 *
	 * Finally, always assume --resume has been used so that we can re-use an
	 * existing work directory when it exists.
	 */
	bool createWorkDir = false;
	bool service = streq(sqlfilename, "-");
	char *serviceName = "apply";

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 service,
							 serviceName,
							 streamDBoptions.restart,
							 true, /* streamDBoptions.resume */
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	/*
	 * Force the SQL filename to the given argument, bypassing filename
	 * computations based on origin tracking. Already known-applied
	 * transactions are still skipped.
	 *
	 * The filename arguments can be set to - to mean stdin, and in that case
	 * we use the streaming API so that we're compatible with Unix pipes.
	 */
	if (streq(sqlfilename, "-"))
	{
		StreamSpecs specs = { 0 };

		if (!stream_init_specs(&specs,
							   &(copySpecs.cfPaths.cdc),
							   &(copySpecs.connStrings),
							   &(streamDBoptions.slot),
							   streamDBoptions.origin,
							   streamDBoptions.endpos,
							   STREAM_MODE_CATCHUP,
							   &(copySpecs.catalogs.source),
							   true, /* streamDBoptions.stdIn */
							   false, /* streamDBoptions.stdOut */
							   logSQL))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		specs.in = stdin;

		if (!stream_apply_replay(&specs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		/* prepare the replication origin tracking */
		StreamApplyContext context = { 0 };

		if (!stream_apply_init_context(&context,
									   &(copySpecs.catalogs.source),
									   &(copySpecs.cfPaths.cdc),
									   &(streamDBoptions.connStrings),
									   streamDBoptions.origin,
									   streamDBoptions.endpos))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_TARGET);
		}

		context.apply = true;
		strlcpy(context.sqlFileName, sqlfilename, sizeof(context.sqlFileName));

		if (!setupReplicationOrigin(&context, logSQL))
		{
			log_error("Failed to setup replication origin on the target database");
			exit(EXIT_CODE_TARGET);
		}

		if (!stream_apply_file(&context))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
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

	/*
	 * Both the receive and the prefetch command starts the "receive" service,
	 * so that they conflict with each other.
	 */
	bool createWorkDir = false;
	bool service = true;
	char *serviceName = "receive";

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 service,
							 serviceName,
							 streamDBoptions.restart,
							 streamDBoptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Refrain from logging SQL statements in the apply module, because they
	 * contain user data. That said, when --trace has been used, bypass that
	 * privacy feature.
	 */
	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   mode,
						   &(copySpecs.catalogs.source),
						   streamDBoptions.stdIn,
						   streamDBoptions.stdOut,
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	switch (specs.mode)
	{
		case STREAM_MODE_RECEIVE:
		{
			specs.out = stdout;

			if (!startLogicalStreaming(&specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_SOURCE);
			}
			break;
		}

		case STREAM_MODE_PREFETCH:
		{
			/*
			 * Remove the possibly still existing stream context files from
			 * previous round of operations (--resume, etc). We want to make
			 * sure that the catchup process reads the files created on this
			 * connection.
			 */
			if (!stream_cleanup_context(&specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			if (!followDB(&copySpecs, &specs))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_INTERNAL_ERROR);
			}
			break;
		}

		default:
		{
			log_fatal("BUG: stream_start_in_mode called in mode %d", mode);
			exit(EXIT_CODE_INTERNAL_ERROR);
			break;
		}
	}
}
