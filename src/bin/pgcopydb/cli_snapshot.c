/*
 * src/bin/pgcopydb/cli_snapshot.c
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

CommandLine create_snapshot_command =
	make_command(
		"snapshot",
		"Create and exports a snapshot on the source database",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n"
		"  --follow         Implement logical decoding to replay changes\n"
		"  --plugin         Output plugin to use (test_decoding, wal2json)\n"
		"  --slot-name      Use this Postgres replication slot name\n",
		cli_create_snapshot_getopts,
		cli_create_snapshot);

CopyDBOptions createSNoptions = { 0 };

static int
cli_create_snapshot_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "dir", required_argument, NULL, 'D' },
		{ "follow", no_argument, NULL, 'f' },
		{ "plugin", required_argument, NULL, 'p' },
		{ "slot-name", required_argument, NULL, 's' },
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

			case 'f':
			{
				options.follow = true;
				log_trace("--follow");
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

	if (!cli_copydb_is_consistent(&options))
	{
		log_fatal("Option --resume requires option --not-consistent");
		++errors;
	}

	/* make sure we have all we need, even after using default values */
	if (createSNoptions.follow)
	{
		if (options.slot.plugin == STREAM_PLUGIN_UNKNOWN ||
			IS_EMPTY_STRING_BUFFER(options.slot.slotName))
		{
			log_fatal("Option --follow requires options --plugin and --slot-name");
			++errors;
		}
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

	char *dir =
		IS_EMPTY_STRING_BUFFER(createSNoptions.dir)
		? NULL
		: createSNoptions.dir;

	bool createWorkDir = true;
	bool service = true;
	char *serviceName = "snapshot";

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 service,
							 serviceName,
							 createSNoptions.restart,
							 createSNoptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_init_specs(&copySpecs, &createSNoptions, DATA_SECTION_ALL))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/*
	 * We have two ways to create a snapshot:
	 *
	 * - pg_export_snapshot() is used for pgcopydb clone commands,
	 *
	 * - replication protocol command CREATE_REPLICATION_SLOT is used when
	 *   preparing for pgcopydb clone --follow.
	 *
	 *   CREATE_REPLICATION_SLOT slot_name
	 *                   LOGICAL plugin
	 *               RESERVE_WAL true
	 *                  SNAPSHOT 'export'
	 *
	 * Using a snapshot created with pg_export_snapshot() to later create the
	 * logical replication slot creates a situation where we miss data,
	 * probably because the Postgres sytem doesn't know how to reserve the WAL
	 * to decode properly then.
	 */
	if (createSNoptions.follow)
	{
		StreamSpecs streamSpecs = { 0 };

		bool logSQL = log_get_level() <= LOG_TRACE;

		if (!stream_init_specs(&streamSpecs,
							   &(copySpecs.cfPaths.cdc),
							   &(copySpecs.connStrings),
							   &(createSNoptions.slot),
							   createSNoptions.origin,
							   createSNoptions.endpos,
							   STREAM_MODE_CATCHUP,
							   &(copySpecs.catalog),
							   createSNoptions.stdIn,
							   createSNoptions.stdOut,
							   logSQL))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		char *logrep_pguri = streamSpecs.connStrings->logrep_pguri;

		if (!copydb_create_logical_replication_slot(&copySpecs,
													logrep_pguri,
													&(streamSpecs.slot)))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
	}
	else
	{
		if (!copydb_prepare_snapshot(&copySpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}
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
