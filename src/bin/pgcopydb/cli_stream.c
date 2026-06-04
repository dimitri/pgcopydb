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
#include "ld_store.h"
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

static void cli_stream_init(int argc, char **argv);
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
		"Cleanup source and target systems for logical decoding",
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

static CommandLine stream_init_command =
	make_command(
		"init",
		"Initialise the pgcopydb streaming work directory and SQLite catalogs",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --slot-name      Replication slot name\n"
		"  --plugin         Logical decoding output plugin (test_decoding or wal2json)\n"
		"  --endpos         LSN position where to stop receiving changes",
		cli_stream_getopts,
		cli_stream_init);

static CommandLine stream_receive_command =
	make_command(
		"receive",
		"Stream changes from the source database",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --to-stdout      Stream logical decoding messages to stdout\n"
		"  --from-file      Read CDC messages from a JSON-lines file instead of\n"
		"                   a live replication connection\n"
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
		"Transform CDC messages from the replayDB output table into SQL",
		"",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --endpos         LSN position where to stop transforming\n",
		cli_stream_getopts,
		cli_stream_transform);

static CommandLine stream_apply_command =
	make_command(
		"apply",
		"Apply changes from the replayDB to the target database, or stdout",
		"",
		"  --target         Postgres URI to the target database\n"
		"                   Use '-' to emit SQL to stdout without connecting\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_apply);


static CommandLine *stream_subcommands[] = {
	&stream_init_command,
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
		{ "from-file", required_argument, NULL, 'f' },
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

	while ((c = getopt_long(argc, argv, "S:T:D:p:ws:N:o:E:rRCOIf:Vvdzqh",
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
				/* "-" means emit SQL to stdout — not a real connection string */
				if (!streq(optarg, "-") && !validate_connection_string(optarg))
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

			case 'f':
			{
				strlcpy(options.fromFile, optarg, sizeof(options.fromFile));
				log_trace("--from-file %s", options.fromFile);
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
			default:
			{
				commandline_help(stderr);
				exit(EXIT_CODE_BAD_ARGS);
				break;
			}
		}
	}

	/*
	 * --target is optional for commands that do not connect to a target
	 * database: stream init, stream receive --from-file, stream transform
	 * --target -, and stream apply --target -.  Only enforce --source when a
	 * real target URI (not the stdout sentinel "-") was provided.
	 */
	bool hasRealTarget =
		options.connStrings.target_pguri != NULL &&
		!streq(options.connStrings.target_pguri, "-");

	if (options.connStrings.source_pguri == NULL && !hasRealTarget)
	{
		/* Both absent (or target is "-") — acceptable for fixture sub-commands. */
	}
	else if (options.connStrings.source_pguri == NULL)
	{
		log_fatal("Option --source is mandatory");
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
 * cli_stream_init initialises the pgcopydb streaming work directory and
 * SQLite catalogs.  It fetches just enough schema metadata from the source
 * Postgres (table list + column attributes) so that subsequent transform and
 * apply commands can operate without a live database connection.
 *
 * This is the streaming equivalent of "pgcopydb ping": a lightweight preflight
 * tool useful for debugging and for populating the infrastructure needed by
 * the lower-level unit-test commands (stream receive --from-file, stream
 * transform, stream apply).
 */
static void
cli_stream_init(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool createWorkDir = true;

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 false,  /* not a service */
							 NULL,
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
	 * Open (and create if necessary) the source SQLite catalog and the
	 * replayDB.  This gives other stream commands a valid on-disk schema to
	 * work against even before any live Postgres connection is made.
	 */
	if (!catalog_init_from_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Fetch table + column metadata from the source Postgres so that the
	 * transform step can look up attisprimary / attisreplident for UPDATE
	 * WHERE-clause construction.
	 *
	 * This is the same query that "pgcopydb clone" runs as its first step,
	 * but here we stop right after the schema fetch.
	 */
	if (copySpecs.connStrings.source_pguri != NULL)
	{
		if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		log_info("Source schema cached into \"%s\"",
				 copySpecs.catalogs.source.dbfile);
	}
	else
	{
		log_info("No --source given; work directory initialised without "
				 "schema metadata.  Use --source or load a fixture with "
				 "sqlite3 to populate the source catalog.");
	}
}


/*
 * cli_stream_receive connects to the source database with the replication
 * protocol and streams changes associated with the replication slot
 * --slot-name.
 *
 * When --from-file is given, no live Postgres connection is made; instead the
 * named JSON-lines file is replayed through the same streamWrite() path,
 * populating the replayDB output table.
 */
static void
cli_stream_receive(int argc, char **argv)
{
	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (!IS_EMPTY_STRING_BUFFER(streamDBoptions.fromFile))
	{
		/* file-based receive: no live PG connection needed */
		CopyDataSpec copySpecs = { 0 };

		(void) find_pg_commands(&(copySpecs.pgPaths));

		bool createWorkDir = false;

		if (!copydb_init_workdir(&copySpecs,
								 streamDBoptions.dir,
								 false, /* not a service */
								 NULL,
								 streamDBoptions.restart,
								 true,  /* resume */
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

		bool logSQL = log_get_level() <= LOG_TRACE;

		StreamSpecs specs = { 0 };

		if (!stream_init_specs(&specs,
							   &(copySpecs.cfPaths.cdc),
							   &(copySpecs.connStrings),
							   &(streamDBoptions.slot),
							   streamDBoptions.origin,
							   streamDBoptions.endpos,
							   STREAM_MODE_PREFETCH,
							   &(copySpecs.catalogs.source),
							   &(copySpecs.catalogs.replay),
							   false, /* stdIn */
							   false, /* stdOut */
							   logSQL))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		/*
		 * For --from-file the source catalog must be open (stream_init does
		 * that in a separate process).  Open it here read-write.
		 */
		if (!catalog_open(specs.sourceDB))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		/*
		 * Set default timeline (1) so ld_store_open_replaydb can compute the
		 * replayDB filename.  A live receive gets this from IDENTIFY_SYSTEM;
		 * for fixture injection we use the default timeline 1.
		 */
		specs.private.timeline = 1;

		if (!ld_store_open_replaydb(&specs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!stream_receive_from_file(&specs, streamDBoptions.fromFile))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		return;
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
						   &(copySpecs.catalogs.replay),
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
						   &(copySpecs.catalogs.replay),
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
						   &(copySpecs.catalogs.replay),
						   true,  /* stdin */
						   true, /* stdout */
						   logSQL))
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
 * cli_stream_transform reads the replayDB output table and transforms it into
 * the stmt + replay tables (SQLite → SQLite).  It is silent: no SQL is written
 * to stdout.
 *
 * The legacy two-argument form `stream transform - -` (stdin → stdout pipe)
 * is kept for backward compatibility with the `stream receive | transform |
 * apply` pipeline.
 */
static void
cli_stream_transform(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	/*
	 * Legacy pipe mode: `pgcopydb stream transform - -`
	 * Kept for backward compatibility; reads JSON from stdin, writes SQL to
	 * stdout.
	 */
	if (argc == 2 && streq(argv[0], "-") && streq(argv[1], "-"))
	{
		bool createWorkDir = false;

		if (!copydb_init_workdir(&copySpecs,
								 streamDBoptions.dir,
								 true,  /* service */
								 "transform",
								 streamDBoptions.restart,
								 true,  /* resume */
								 createWorkDir))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

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
							   &(copySpecs.catalogs.replay),
							   true,    /* stdIn */
							   true,    /* stdOut */
							   logSQL))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		specs.in = stdin;
		specs.out = stdout;

		if (!stream_init_context(&specs))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!stream_transform_stream(&specs))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		return;
	}

	if (argc != 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	/*
	 * SQLite path: read the replayDB output table, write stmt + replay tables.
	 * No SQL is written to stdout.  Requires the work directory and replayDB
	 * to already exist (run `pgcopydb stream init` and
	 * `pgcopydb stream receive --from-file` first).
	 */
	bool createWorkDir = false;

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 false, /* not a service */
							 NULL,
							 streamDBoptions.restart,
							 true,  /* resume */
							 createWorkDir))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	bool logSQL = log_get_level() <= LOG_TRACE;

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_PREFETCH,
						   &(copySpecs.catalogs.source),
						   &(copySpecs.catalogs.replay),
						   false, /* stdIn */
						   false, /* stdOut */
						   logSQL))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!catalog_open(specs.sourceDB))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * The replayDB filename is stored in the sourceDB cdc_files table.
	 * Look it up before trying to open the replayDB.
	 */
	if (!ld_store_set_current_cdc_filename(&specs))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!catalog_open(specs.replayDB))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_init_context(&specs))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * Ensure the sentinel row exists.  The live streaming path creates it via
	 * startLogicalStreaming; the standalone transform path must seed it here so
	 * sentinel_sync_transform can update it after each COMMIT.
	 */
	if (!sentinel_setup(specs.sourceDB, specs.startpos, specs.endpos))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_transform_context_init(&specs))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_transform_cdc_file(&specs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pgsql_finish(&(specs.transformPGSQL));

	(void) catalog_close(specs.sourceDB);
}


/*
 * cli_stream_apply reads the replayDB stmt + replay tables and either:
 *
 *   - applies the SQL to the target Postgres (when --target is given), or
 *   - writes the SQL to stdout without a database connection (when --target
 *     is absent or set to '-'), mimicking pg_restore --no-target behaviour.
 *
 * The legacy `apply -` form (read SQL from stdin, apply to Postgres) is kept
 * for backward compatibility with the receive | transform | apply pipeline.
 */
static void
cli_stream_apply(int argc, char **argv)
{
	/*
	 * Legacy pipe mode: `pgcopydb stream apply -`
	 *
	 * Reads SQL text from stdin (output of `pgcopydb stream transform - -`)
	 * and applies it to the target database line by line.  Kept for backward
	 * compatibility with the receive | transform | apply Unix-pipe pipeline.
	 */
	if (argc == 1 && streq(argv[0], "-"))
	{
		CopyDataSpec copySpecs = { 0 };

		(void) find_pg_commands(&(copySpecs.pgPaths));

		bool createWorkDir = false;

		if (!copydb_init_workdir(&copySpecs,
								 streamDBoptions.dir,
								 false,
								 NULL,
								 streamDBoptions.restart,
								 true, /* resume */
								 createWorkDir))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!copydb_init_specs(&copySpecs, &streamDBoptions, DATA_SECTION_NONE))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

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
							   &(copySpecs.catalogs.replay),
							   true,   /* stdIn */
							   false,  /* stdOut */
							   logSQL))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		specs.in = stdin;

		if (!stream_init_context(&specs))
		{
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		StreamApplyContext context = { 0 };

		if (!stream_apply_setup(&specs, &context))
		{
			log_error("Failed to setup apply context for stdin mode");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!stream_apply_stdin(&specs, &context))
		{
			/* errors have already been logged */
			(void) stream_apply_cleanup(&context);
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		(void) stream_apply_cleanup(&context);
		return;
	}

	if (argc > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	CopyDataSpec copySpecs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	bool createWorkDir = false;

	if (!copydb_init_workdir(&copySpecs,
							 streamDBoptions.dir,
							 false, /* not a service */
							 NULL,
							 streamDBoptions.restart,
							 true,  /* resume */
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

	bool logSQL = log_get_level() <= LOG_TRACE;

	/*
	 * Stdout mode: --target is absent or explicitly set to "-".
	 * Read stmt + replay tables from the replayDB and emit SQL to stdout.
	 * No Postgres connection is opened.  "--target -" is needed when
	 * PGCOPYDB_TARGET_PGURI is set in the environment but the caller
	 * still wants stdout output (e.g. unit tests).
	 */
	bool stdoutMode =
		copySpecs.connStrings.target_pguri == NULL ||
		streq(copySpecs.connStrings.target_pguri, "-");

	StreamSpecs specs = { 0 };

	if (!stream_init_specs(&specs,
						   &(copySpecs.cfPaths.cdc),
						   &(copySpecs.connStrings),
						   &(streamDBoptions.slot),
						   streamDBoptions.origin,
						   streamDBoptions.endpos,
						   STREAM_MODE_CATCHUP,
						   &(copySpecs.catalogs.source),
						   &(copySpecs.catalogs.replay),
						   false, /* stdIn */
						   stdoutMode, /* stdOut */
						   logSQL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!catalog_open(specs.sourceDB))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * The replayDB filename is stored in the sourceDB cdc_files table.
	 * Look it up before trying to open the replayDB.
	 */
	if (!ld_store_set_current_cdc_filename(&specs))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!catalog_open(specs.replayDB))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_init_context(&specs))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (stdoutMode)
	{
		if (!stream_apply_to_stdout(&specs, stdout))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		StreamApplyContext context = { 0 };

		if (!stream_apply_setup(&specs, &context))
		{
			log_error("Failed to setup apply context");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		bool success = stream_apply_replaydb(&specs, &context);

		(void) stream_apply_cleanup(&context);

		if (!success)
		{
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
						   &(copySpecs.catalogs.replay),
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
