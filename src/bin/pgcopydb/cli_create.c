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
#include "log.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "schema.h"
#include "signals.h"
#include "stream.h"
#include "string_utils.h"

static int cli_create_snapshot_getopts(int argc, char **argv);
static void cli_create_snapshot(int argc, char **argv);

static CommandLine create_snapshot_command =
	make_command(
		"snapshot",
		"Create and exports a snapshot on the source database",
		" --source ... ",
		"  --source         Postgres URI to the source database\n"
		"  --dir            Work directory to use\n",
		cli_create_snapshot_getopts,
		cli_create_snapshot);

static CommandLine *create_subcommands[] = {
	&create_snapshot_command,
	NULL
};

CommandLine create_commands =
	make_command_set("create",
					 "Create resources needed for pgcopydb",
					 NULL, NULL, NULL, create_subcommands);


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
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
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

	while ((c = getopt_long(argc, argv, "S:T:D:Vvqh",
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
						   DATA_SECTION_ALL,
						   createSNoptions.snapshot,
						   restoreOptions,
						   false, /* skipLargeObjects */
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
