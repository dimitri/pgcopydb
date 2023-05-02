/*
 * src/bin/pgcopydb/cli_config.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "cli_common.h"
#include "cli_root.h"
#include "config.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "string_utils.h"

static void cli_config_get(int argc, char **argv);
static void cli_config_set(int argc, char **argv);
static int cli_config_getopts(int argc, char **argv);

static CommandLine config_get_command =
	make_command(
		"get",
		"Get configuration option value",
		"[ option-name ]",
		"  --json    Format the output using JSON\n",
		cli_config_getopts,
		cli_config_get);

static CommandLine config_set_command =
	make_command(
		"set",
		"Set configuration option value",
		"option-name value",
		"",
		cli_config_getopts,
		cli_config_set);

static CommandLine *config_subcommands[] = {
	&config_get_command,
	&config_set_command,
	NULL
};

CommandLine config_commands =
	make_command_set("config",
					 "Get and Set configuration options for pgcopydb",
					 NULL, NULL, NULL, config_subcommands);

/*
 * cli_config_getopts parses the CLI options for the `config` command.
 */
static int
cli_config_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "dir", required_argument, NULL, 'D' },
		{ "jobs", required_argument, NULL, 'J' },
		{ "table-jobs", required_argument, NULL, 'J' },
		{ "index-jobs", required_argument, NULL, 'I' },
		{ "json", no_argument, NULL, 'j' },
		{ "version", no_argument, NULL, 'V' },
		{ "debug", no_argument, NULL, 'd' },
		{ "trace", no_argument, NULL, 'z' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "notice", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 0;

	while ((c = getopt_long(argc, argv, "D:J:I:jVvdzqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
			{
				strlcpy(options.dir, optarg, MAXPGPATH);
				log_trace("--dir %s", options.dir);
				break;
			}

			case 'J':
			{
				if (!stringToInt(optarg, &options.tableJobs) ||
					options.tableJobs < 1 ||
					options.tableJobs > 128)
				{
					log_fatal("Failed to parse --jobs count: \"%s\"", optarg);
					++errors;
				}
				log_trace("--table-jobs %d", options.tableJobs);
				break;
			}

			case 'I':
			{
				if (!stringToInt(optarg, &options.indexJobs) ||
					options.indexJobs < 1 ||
					options.indexJobs > 128)
				{
					log_fatal("Failed to parse --index-jobs count: \"%s\"", optarg);
					++errors;
				}
				log_trace("--jobs %d", options.indexJobs);
				break;
			}

			case 'j':
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

			default:
			{
				++errors;
			}
		}
	}

	/* publish our option parsing in the global variable */
	copyDBoptions = options;

	return optind;
}


/*
 * cli_config_get implements the command: pgcopydb config get
 */
static void
cli_config_get(int argc, char **argv)
{
	CopyDBOptions config = copyDBoptions;

	char *dir =
		IS_EMPTY_STRING_BUFFER(copyDBoptions.dir)
		? NULL
		: copyDBoptions.dir;

	bool createWorkDir = false;

	CopyDataSpec copySpecs = { 0 };

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 false, /* service */
							 NULL,  /* serviceName */
							 false, /* restart */
							 true, /* resume */
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	const char *cfname = copySpecs.cfPaths.conffile;

	if (!file_exists(cfname))
	{
		log_fatal("Configuration file \"%s\" does not exists", cfname);
		exit(EXIT_CODE_BAD_CONFIG);
	}

	switch (argc)
	{
		case 0:
		{
			/* no argument, write the config out */
			if (!config_read_file(&config, cfname))
			{
				/* errors have already been logged */
				exit(EXIT_CODE_BAD_CONFIG);
			}

			if (outputJSON)
			{
				JSON_Value *js = json_value_init_object();

				if (!config_to_json(&config, js))
				{
					log_fatal("Failed to serialize configuration to JSON");
					exit(EXIT_CODE_BAD_CONFIG);
				}

				(void) cli_pprint_json(js);
			}
			else
			{
				config_write(stdout, &config);
				fformat(stdout, "\n");
			}

			break;
		}

		case 1:
		{
			/* single argument, find the option and display its value */
			char *path = argv[0];
			char value[BUFSIZE] = { 0 };

			if (config_get_setting(&config, cfname, path, value, BUFSIZE))
			{
				fformat(stdout, "%s\n", value);
			}
			else
			{
				log_error("Failed to lookup option %s", path);
				exit(EXIT_CODE_BAD_ARGS);
			}

			break;
		}

		default:
		{
			/* we only support 0 or 1 argument */
			log_fatal("Failed to parse command line arguments");
			commandline_help(stderr);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}
}


/*
 * cli_config_set implements the command: pgcopydb config set
 */
static void
cli_config_set(int argc, char **argv)
{
	CopyDBOptions config = copyDBoptions;

	char *dir =
		IS_EMPTY_STRING_BUFFER(copyDBoptions.dir)
		? NULL
		: copyDBoptions.dir;

	bool createWorkDir = false;

	CopyDataSpec copySpecs = { 0 };

	if (!copydb_init_workdir(&copySpecs,
							 dir,
							 false, /* service */
							 NULL,  /* serviceName */
							 false, /* restart */
							 true, /* resume */
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	const char *cfname = copySpecs.cfPaths.conffile;

	if (!file_exists(cfname))
	{
		log_fatal("Configuration file \"%s\" does not exists", cfname);
		exit(EXIT_CODE_BAD_CONFIG);
	}

	if (argc != 2)
	{
		log_fatal("Failed to parse command line arguments: "
				  "2 arguments are expected, found %d", argc);
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}
	else
	{
		/* we print out the value that we parsed, as a double-check */
		char value[BUFSIZE] = { 0 };

		if (!config_set_setting(&config, cfname, argv[0], argv[1]))
		{
			/* we already logged about it */
			exit(EXIT_CODE_BAD_CONFIG);
		}

		/* first write the new configuration settings to file */
		if (!config_write_file(&config, cfname))
		{
			log_fatal("Failed to write pgcopydb configuration file \"%s\", "
					  "see above for details",
					  cfname);
			exit(EXIT_CODE_BAD_CONFIG);
		}

		/* now read the value from just written file */
		if (config_get_setting(&config, cfname, argv[0], value, BUFSIZE))
		{
			fformat(stdout, "%s\n", value);
		}
		else
		{
			log_error("Failed to lookup option %s", argv[0]);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}
}
