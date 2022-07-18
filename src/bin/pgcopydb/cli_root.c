/*
 * src/bin/pgcopydb/cli_root.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include "cli_common.h"
#include "cli_root.h"
#include "commandline.h"
#include "log.h"

/* local bindings for all the commands */
CommandLine help =
	make_command("help", "print help message", "", "", NULL, cli_help);

CommandLine version =
	make_command("version", "print pgcopydb version", "", "",
				 cli_print_version_getopts,
				 cli_print_version);

/*
 * Command line options when using PGCOPYDB_DEBUG=1, as sub-processes do.
 */
CommandLine *root_subcommands_with_debug[] = {
	&clone_command,
	&fork_command,
	&follow_command,
	&copy__db_command,          /* backward compat */
	&create_snapshot_command,
	&copy_commands,
	&dump_commands,
	&restore_commands,
	&list_commands,
	&stream_commands,
	&help,
	&version,
	NULL
};

CommandLine root_with_debug =
	make_command_set("pgcopydb",
					 "pgcopydb tool",
					 "[ --verbose --quiet ]", NULL,
					 root_options, root_subcommands);

/*
 * Command line options intended to normal users.
 */
CommandLine *root_subcommands[] = {
	&clone_command,
	&fork_command,
	&follow_command,
	&copy__db_command,          /* backward compat */
	&create_snapshot_command,
	&copy_commands,
	&dump_commands,
	&restore_commands,
	&list_commands,
	&stream_commands,
	&help,
	&version,
	NULL
};

CommandLine root =
	make_command_set("pgcopydb",
					 "pgcopydb tool",
					 "[ --verbose --quiet ]", NULL,
					 root_options, root_subcommands);


/*
 * root_options parses flags from the list of arguments that are common to all
 * commands.
 */
int
root_options(int argc, char **argv)
{
	int verboseCount = 0;
	bool printVersion = false;

	static struct option long_options[] = {
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "json", no_argument, NULL, 'J' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	int c, option_index, errors = 0;

	optind = 0;

	while ((c = getopt_long(argc, argv, "JVvqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'J':
			{
				outputJSON = true;
				log_trace("--json");
				break;
			}

			case 'V':
			{
				printVersion = true;
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

			default:
			{
				/* getopt_long already wrote an error message */
				errors++;
				break;
			}
		}
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	if (printVersion)
	{
		cli_print_version(argc, argv);
	}

	return optind;
}
