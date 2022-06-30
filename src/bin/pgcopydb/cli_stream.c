/*
 * src/bin/pgcopydb/cli_stream.c
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
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "stream.h"
#include "string_utils.h"

CopyDBOptions streamDBoptions = { 0 };

static int cli_stream_getopts(int argc, char **argv);
static void cli_stream_receive(int argc, char **argv);
static void cli_stream_transform(int argc, char **argv);
static void cli_stream_apply(int argc, char **argv);

static CommandLine stream_receive_command =
	make_command(
		"receive",
		"Stream changes from the source database",
		" --source ... ",
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
		" [ --source ... ] <json filename> <sql filename> ",
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
		" --target ... <sql filename> ",
		"  --target         Postgres URI to the target database\n"
		"  --dir            Work directory to use\n"
		"  --restart        Allow restarting when temp files exist already\n"
		"  --resume         Allow resuming operations after a failure\n"
		"  --not-consistent Allow taking a new snapshot on the source database\n"
		"  --origin         Name of the Postgres replication origin\n",
		cli_stream_getopts,
		cli_stream_apply);


static CommandLine *stream_subcommands[] = {
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
		{ "slot-name", required_argument, NULL, 's' },
		{ "origin", required_argument, NULL, 'o' },
		{ "endpos", required_argument, NULL, 'E' },
		{ "restart", no_argument, NULL, 'r' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'C' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "S:T:j:s:o:t:PVvqh",
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

	/* restore commands support the target URI environment variable */
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

	if (IS_EMPTY_STRING_BUFFER(options.origin))
	{
		strlcpy(options.origin, REPLICATION_ORIGIN, sizeof(options.origin));
		log_info("Using default origin node name \"%s\"", options.origin);
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
	CopyDataSpec copySpecs = { 0 };
	StreamSpecs specs = { 0 };

	(void) find_pg_commands(&(copySpecs.pgPaths));

	if (!copydb_init_workdir(&copySpecs,
							 NULL,
							 streamDBoptions.restart,
							 streamDBoptions.resume))
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
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* skipLargeObjects */
						   streamDBoptions.restart,
						   streamDBoptions.resume,
						   !streamDBoptions.notConsistent))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!stream_init_specs(&specs,
						   copySpecs.cfPaths.cdcdir,
						   copySpecs.source_pguri,
						   copySpecs.target_pguri,
						   streamDBoptions.slotName,
						   streamDBoptions.endpos))
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
							 streamDBoptions.resume))
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
						   DATA_SECTION_ALL,
						   streamDBoptions.snapshot,
						   restoreOptions,
						   false, /* skipLargeObjects */
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
								streamDBoptions.target_pguri,
								streamDBoptions.origin))
	{
		log_error("Failed to setup replication origin on the target database");
		exit(EXIT_CODE_TARGET);
	}

	char *sqlfilename = argv[0];

	if (!stream_apply_file(&context, sqlfilename))
	{
		/* errors have already been logged */
		pgsql_finish(&(context.pgsql));
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pgsql_finish(&(context.pgsql));
}
