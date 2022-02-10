/*
 * src/bin/pgcopydb/cli_common.c
 *     Implementation of a CLI which lets you run individual keeper routines
 *     directly
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the PostgreSQL License.
 *
 */
#include <errno.h>
#include <inttypes.h>
#include <getopt.h>
#include <signal.h>

#include "commandline.h"

#include "cli_common.h"
#include "cli_root.h"
#include "commandline.h"
#include "copydb.h"
#include "env_utils.h"
#include "file_utils.h"
#include "log.h"
#include "parsing.h"
#include "string_utils.h"

/* handle command line options for our setup. */
bool outputJSON = false;


/*
 * Provide help.
 */
void
cli_help(int argc, char **argv)
{
	CommandLine command = root;

	if (env_exists(PGCOPYDB_DEBUG))
	{
		command = root_with_debug;
	}

	(void) commandline_print_command_tree(&command, stdout);
}


/*
 * cli_print_version_getopts parses the CLI options for the pgcopydb version
 * command, which are the usual suspects.
 */
int
cli_print_version_getopts(int argc, char **argv)
{
	int c, option_index = 0;

	static struct option long_options[] = {
		{ "json", no_argument, NULL, 'J' },
		{ "version", no_argument, NULL, 'V' },
		{ "verbose", no_argument, NULL, 'v' },
		{ "quiet", no_argument, NULL, 'q' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};
	optind = 0;

	/*
	 * The only command lines that are using keeper_cli_getopt_pgdata are
	 * terminal ones: they don't accept subcommands. In that case our option
	 * parsing can happen in any order and we don't need getopt_long to behave
	 * in a POSIXLY_CORRECT way.
	 *
	 * The unsetenv() call allows getopt_long() to reorder arguments for us.
	 */
	unsetenv("POSIXLY_CORRECT");

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

			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}

			default:
			{
				/*
				 * Ignore errors, ignore most of the things, just print the
				 * version and exit(0)
				 */
				break;
			}
		}
	}
	return optind;
}


/*
 * keeper_cli_print_version prints the pgcopydb version and exits with
 * successful exit code of zero.
 */
void
cli_print_version(int argc, char **argv)
{
	const char *version = PGCOPYDB_VERSION;

	if (outputJSON)
	{
		JSON_Value *js = json_value_init_object();
		JSON_Object *root = json_value_get_object(js);

		json_object_set_string(root, "pgcopydb", version);
		json_object_set_string(root, "pg_major", PG_MAJORVERSION);
		json_object_set_string(root, "pg_version", PG_VERSION);
		json_object_set_string(root, "pg_version_str", PG_VERSION_STR);
		json_object_set_number(root, "pg_version_num", (double) PG_VERSION_NUM);

		(void) cli_pprint_json(js);
	}
	else
	{
		fformat(stdout, "pgcopydb version %s\n", version);
		fformat(stdout, "compiled with %s\n", PG_VERSION_STR);
		fformat(stdout, "compatible with Postgres 10, 11, 12, 13, and 14\n");
	}

	exit(0);
}


/*
 * cli_pprint_json pretty prints the given JSON value to stdout and frees the
 * JSON related memory.
 */
void
cli_pprint_json(JSON_Value *js)
{
	/* output our nice JSON object, pretty printed please */
	char *serialized_string = json_serialize_to_string_pretty(js);
	fformat(stdout, "%s\n", serialized_string);

	/* free intermediate memory */
	json_free_serialized_string(serialized_string);
	json_value_free(js);
}


/*
 * logLevelToString returns the string to use to enable the same logLevel in a
 * sub-process.
 *
 * enum { LOG_TRACE, LOG_DEBUG, LOG_INFO, LOG_WARN, LOG_ERROR, LOG_FATAL };
 */
char *
logLevelToString(int logLevel)
{
	switch (logLevel)
	{
		case LOG_TRACE:
		{
			return "-vvv";
		}

		case LOG_DEBUG:
		{
			return "-vv";
		}

		case LOG_WARN:
		case LOG_INFO:
		{
			return "-v";
		}

		case LOG_ERROR:
		case LOG_FATAL:
		{
			return "-q";
		}
	}

	return "";
}


/*
 * cli_copydb_getenv reads from the environment variables and fills-in the
 * command line options.
 */
bool
cli_copydb_getenv(CopyDBOptions *options)
{
	int errors = 0;

	/* now some of the options can be also set from the environment */
	if (env_exists(PGCOPYDB_SOURCE_PGURI))
	{
		if (!get_env_copy(PGCOPYDB_SOURCE_PGURI,
						  options->source_pguri,
						  sizeof(options->source_pguri)))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (env_exists(PGCOPYDB_TARGET_PGURI))
	{
		if (!get_env_copy(PGCOPYDB_TARGET_PGURI,
						  options->target_pguri,
						  sizeof(options->target_pguri)))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (env_exists(PGCOPYDB_TARGET_TABLE_JOBS))
	{
		char jobs[BUFSIZE] = { 0 };

		if (get_env_copy(PGCOPYDB_TARGET_TABLE_JOBS, jobs, sizeof(jobs)))
		{
			if (!stringToInt(jobs, &options->tableJobs) ||
				options->tableJobs < 1 ||
				options->tableJobs > 128)
			{
				log_fatal("Failed to parse PGCOPYDB_TARGET_TABLE_JOBS: \"%s\"",
						  jobs);
				++errors;
			}
		}
		else
		{
			/* errors have already been logged */
			++errors;
		}
	}

	if (env_exists(PGCOPYDB_TARGET_INDEX_JOBS))
	{
		char jobs[BUFSIZE] = { 0 };

		if (get_env_copy(PGCOPYDB_TARGET_INDEX_JOBS, jobs, sizeof(jobs)))
		{
			if (!stringToInt(jobs, &options->indexJobs) ||
				options->indexJobs < 1 ||
				options->indexJobs > 128)
			{
				log_fatal("Failed to parse PGCOPYDB_TARGET_INDEX_JOBS: \"%s\"",
						  jobs);
				++errors;
			}
		}
		else
		{
			/* errors have already been logged */
			++errors;
		}
	}

	/* when --snapshot has not been used, check PGCOPYDB_SNAPSHOT */
	if (env_exists(PGCOPYDB_SNAPSHOT))
	{
		if (!get_env_copy(PGCOPYDB_SNAPSHOT,
						  options->snapshot,
						  sizeof(options->snapshot)))
		{
			/* errors have already been logged */
			++errors;
		}
	}

	/* when --drop-if-exists has not been used, check PGCOPYDB_DROP_IF_EXISTS */
	if (!options->restoreOptions.dropIfExists)
	{
		if (env_exists(PGCOPYDB_DROP_IF_EXISTS))
		{
			char DROP_IF_EXISTS[BUFSIZE] = { 0 };

			if (!get_env_copy(PGCOPYDB_DROP_IF_EXISTS,
							  DROP_IF_EXISTS,
							  sizeof(DROP_IF_EXISTS)))
			{
				/* errors have already been logged */
				++errors;
			}
			else if (!parse_bool(DROP_IF_EXISTS,
								 &(options->restoreOptions.dropIfExists)))
			{
				log_error("Failed to parse environment variable \"%s\" "
						  "value \"%s\", expected a boolean (on/off)",
						  PGCOPYDB_DROP_IF_EXISTS,
						  DROP_IF_EXISTS);
				++errors;
			}
		}
	}

	return errors == 0;
}


/*
 * cli_copydb_is_consistent returns false when the option --not-consistent
 * should be used.
 */
bool
cli_copydb_is_consistent(CopyDBOptions *options)
{
	/* when --resume is not used, we're good */
	if (!options->resume)
	{
		return true;
	}

	/* when --resume and --not-consistent are used, we're good */
	if (options->resume && options->notConsistent)
	{
		return true;
	}

	/* okay --resume is being used, do we have a snapshot? */
	if (IS_EMPTY_STRING_BUFFER(options->snapshot))
	{
		/* --resume without --snapshot requires --not-consistent */
		return false;
	}

	/* okay, a --snapshot is required, is it the same as the previous run? */
	CopyFilePaths cfPaths = { 0 };

	char *dir =
		IS_EMPTY_STRING_BUFFER(options->dir) ? NULL : options->dir;

	if (!copydb_prepare_filepaths(&cfPaths, dir))
	{
		return false;
	}

	/*
	 * If the snapshot file does not exists, then it might be that a snapshot
	 * has been created by another script/tool, and pgcopydb is now asked to
	 * re-use that external snapshot. Just get along with it, and let Postgres
	 * check for the snapshot at SET TRANSACTION SNAPSHOT time.
	 */
	if (!file_exists(cfPaths.snfile))
	{
		return true;
	}

	char *previous_snapshot = NULL;
	long size = 0L;

	if (!read_file(cfPaths.snfile, &previous_snapshot, &size))
	{
		/* errors have already been logged */
		return false;
	}

	/* make sure to use only the first line of the file, without \n */
	char *snLines[BUFSIZE] = { 0 };
	int lineCount = splitLines(previous_snapshot, snLines, BUFSIZE);

	if (lineCount != 1 || strcmp(snLines[0], options->snapshot) != 0)
	{
		log_error("Failed to ensure a consistent snapshot to resume operations");
		log_error("Previous run was done with snapshot \"%s\" and current run "
				  "is using --resume --snapshot \"%s\"",
				  snLines[0],
				  options->snapshot);

		free(previous_snapshot);
		return false;
	}

	free(previous_snapshot);
	return true;
}
