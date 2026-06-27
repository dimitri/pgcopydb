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

CommandLine snapshot_command =
	make_command(
		"snapshot",
		"Create and export a snapshot on the source database",
		" --source ... ",
		"  --source                      Postgres URI to the source database\n"
		"  --dir                         Work directory to use\n"
		"  --follow                      Implement logical decoding to replay changes\n"
		"  --plugin                      Output plugin to use (pgoutput, test_decoding, wal2json)\n"
		"  --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin\n"
		"  --slot-name                   Use this Postgres replication slot name\n",
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
		{ "wal2json-numeric-as-string", no_argument, NULL, 'w' },
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

	while ((c = getopt_long(argc, argv, "S:D:fp:ws:Vvdzqh",
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

			case 'w':
			{
				options.slot.wal2jsonNumericAsString = true;
				log_trace("--wal2json-numeric-as-string");
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
				break;
			}
		}
	}

	if (options.connStrings.source_pguri == NULL)
	{
		log_fatal("Option --source is mandatory");
		++errors;
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
		commandline_help(stderr);
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
							   &(copySpecs.catalogs.source),
							   &(copySpecs.catalogs.output),
							   &(copySpecs.catalogs.replay),
							   createSNoptions.stdIn,
							   createSNoptions.stdOut,
							   logSQL,
							   createSNoptions.replayNoOpUpdates,
							   &(copySpecs.filters),
							   &(copySpecs.catalogs.target)))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		streamSpecs.maxReplayDBSize = createSNoptions.maxReplayDBSize > 0
									  ? createSNoptions.maxReplayDBSize
									  : (1ULL << 30);

		/*
		 * Make sure to register our setup here, as usually the command
		 * `pgcopydb snapshot` is used first.
		 *
		 * snapshot is filter-agnostic: it holds the snapshot connection open
		 * but never reads or writes filter-dependent catalog data.  Setting
		 * skipFilterCheck causes setup to be registered with filters = NULL
		 * (not SOURCE_FILTER_TYPE_NONE), which signals to subsequent
		 * filter-owner commands (e.g. clone --filters) that no prior owner
		 * has populated the source catalog, so no invalidation is needed.
		 */
		copySpecs.skipFilterCheck = true;

		if (!catalog_register_setup_from_specs(&copySpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!follow_export_snapshot(&copySpecs, &streamSpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		/*
		 * When the replication slot already existed, the fast path in
		 * copydb_create_logical_replication_slot returned without creating a
		 * fresh snapshot.  sourceSnapshot.snapshot still holds the identifier
		 * read from the on-disk snapshot file written by a previous run; that
		 * snapshot is no longer live.  Clear the field so the print below stays
		 * silent and callers do not receive a stale identifier.
		 */
		if (!copySpecs.sourceSnapshot.exportedCreateSlotSnapshot)
		{
			copySpecs.sourceSnapshot.snapshot[0] = '\0';
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

	/*
	 * Print the snapshot identifier to stdout so that callers (e.g. a
	 * shell script running pgcopydb clone --snapshot "$(pgcopydb snapshot)")
	 * can capture it.  When the slot already existed but its snapshot has
	 * been closed, sourceSnapshot.snapshot is empty and we print nothing —
	 * a stale identifier would be worse than silence here.
	 */
	if (!IS_EMPTY_STRING_BUFFER(copySpecs.sourceSnapshot.snapshot))
	{
		fformat(stdout, "%s\n", copySpecs.sourceSnapshot.snapshot);
		fflush(stdout);
	}

	for (;;)
	{
		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			/*
			 * Close the snapshot connection properly: for a SQL snapshot this
			 * commits the transaction (releasing the PG snapshot); for a
			 * logical replication slot snapshot this closes the replication
			 * connection.
			 */
			(void) copydb_close_snapshot(&copySpecs);
			(void) catalog_close_from_specs(&copySpecs);

			log_info("Asked to terminate, aborting");

			break;
		}

		/* sleep for 100ms between checks for interrupts */
		pg_usleep(100 * 1000);
	}
}
