/*
 * src/bin/pgcopydb/cli_copy.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "log.h"
#include "pgsql.h"

static void cli_ping(int argc, char **argv);
int cli_ping_getopts(int argc, char **argv);

CommandLine ping_command =
	make_command(
		"ping",
		"Attempt to connect to the source and target instances",
		" --source ... --target ... ",
		"  --source              Postgres URI to the source database\n"
		"  --target              Postgres URI to the target database\n",
		cli_ping_getopts,
		cli_ping);


/*
 * cli_ping_getopts parses the CLI options for the `pgcopydb ping` command.
 */
int
cli_ping_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0, errors = 0;
	int verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
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

	while ((c = getopt_long(argc, argv, "S:T:Vvdzqh",
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

	if (options.connStrings.source_pguri == NULL ||
		options.connStrings.target_pguri == NULL)
	{
		log_fatal("Options --source and --target are mandatory");
		exit(EXIT_CODE_BAD_ARGS);
	}

	/* prepare safe versions of the connection strings (without password) */
	if (!cli_prepare_pguris(&(options.connStrings)))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	/* publish our option parsing in the global variable */
	copyDBoptions = options;

	return optind;
}


/*
 * cli_ping implements the pgcopydb ping command line.
 */
static void
cli_ping(int argc, char **argv)
{
	int errors = 0;

	ConnStrings *dsn = &(copyDBoptions.connStrings);

	char *source = dsn->source_pguri;
	char *target = dsn->target_pguri;

	/* ping both source and target databases concurrently */
	pid_t sPid = fork();

	switch (sPid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to ping source db: %m");
			++errors;
			break;
		}

		case 0:
		{
			/* child process runs the command */
			PGSQL src = { 0 };

			if (!pgsql_init(&src, source, PGSQL_CONN_SOURCE))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_SOURCE);
			}

			/* also set our GUC values for the source connection */
			if (!pgsql_server_version(&src))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_SOURCE);
			}

			GUC *settings =
				src.pgversion_num < 90600 ? srcSettings95 : srcSettings;

			if (!pgsql_set_gucs(&src, settings))
			{
				log_fatal("Failed to set our GUC settings on the source connection, "
						  "see above for details");
				pgsql_finish(&src);
				exit(EXIT_CODE_TARGET);
			}

			log_info("Successfully could connect to source database Postgres %s "
					 "at \"%s\"",
					 src.pgversion,
					 dsn->safeSourcePGURI.pguri);

			pgsql_finish(&src);

			/* and we're done */
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* pass */
			break;
		}
	}

	pid_t tPid = fork();

	switch (tPid)
	{
		case -1:
		{
			log_error("Failed to fork a subprocess to ping target db: %m");
			++errors;
			break;
		}

		case 0:
		{
			/* child process runs the command */
			PGSQL dst = { 0 };

			if (!pgsql_init(&dst, target, PGSQL_CONN_TARGET))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_TARGET);
			}

			if (!pgsql_server_version(&dst))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_TARGET);
			}

			if (!pgsql_set_gucs(&dst, dstSettings))
			{
				log_fatal("Failed to set our GUC settings on the target connection, "
						  "see above for details");
				pgsql_finish(&dst);
				exit(EXIT_CODE_TARGET);
			}

			log_info("Successfully could connect to target database Postgres %s "
					 "at \"%s\"",
					 dst.pgversion,
					 dsn->safeTargetPGURI.pguri);

			pgsql_finish(&dst);

			/* and we're done */
			exit(EXIT_CODE_QUIT);
		}

		default:
		{
			/* pass */
			break;
		}
	}

	/*
	 * In case of error in one sub-process, we still want the other to fully
	 * try.
	 */
	bool failFast = false;

	if (!copydb_wait_for_subprocesses(failFast))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}
