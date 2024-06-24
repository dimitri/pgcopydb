/*
 * src/bin/pgcopydb/cli_restore.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "catalog.h"
#include "cli_common.h"
#include "cli_root.h"
#include "copydb.h"
#include "commandline.h"
#include "env_utils.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgcmd.h"
#include "pgsql.h"
#include "string_utils.h"

CopyDBOptions restoreDBoptions = { 0 };

static int cli_restore_schema_getopts(int argc, char **argv);
static void cli_restore_schema(int argc, char **argv);
static void cli_restore_schema_pre_data(int argc, char **argv);
static void cli_restore_schema_post_data(int argc, char **argv);
static void cli_restore_schema_parse_list(int argc, char **argv);
static void cli_restore_roles(int argc, char **argv);

static void cli_restore_prepare_specs(CopyDataSpec *copySpecs);

static CommandLine restore_schema_command =
	make_command(
		"schema",
		"Restore a database schema from custom files to target database",
		" --dir <dir> [ --source <URI> ] --target <URI> ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --restore-jobs       Number of concurrent jobs for pg_restore\n"
		"  --drop-if-exists     On the target database, clean-up from a previous run first\n"
		"  --no-owner           Do not set ownership of objects to match the original database\n"
		"  --no-acl             Prevent restoration of access privileges (grant/revoke commands).\n"
		"  --no-comments        Do not output commands to restore comments\n"
		"  --no-tablespaces     Do not output commands to select tablespaces\n"
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n",
		cli_restore_schema_getopts,
		cli_restore_schema);

static CommandLine restore_schema_pre_data_command =
	make_command(
		"pre-data",
		"Restore a database pre-data schema from custom file to target database",
		" --dir <dir> [ --source <URI> ] --target <URI> ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --restore-jobs       Number of concurrent jobs for pg_restore\n"
		"  --drop-if-exists     On the target database, clean-up from a previous run first\n"
		"  --no-owner           Do not set ownership of objects to match the original database\n"
		"  --no-acl             Prevent restoration of access privileges (grant/revoke commands).\n"
		"  --no-comments        Do not output commands to restore comments\n"
		"  --no-tablespaces     Do not output commands to select tablespaces\n"
		"  --skip-extensions    Skip restoring extensions\n" \
		"  --skip-ext-comments  Skip restoring COMMENT ON EXTENSION\n" \
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n",
		cli_restore_schema_getopts,
		cli_restore_schema_pre_data);

static CommandLine restore_schema_post_data_command =
	make_command(
		"post-data",
		"Restore a database post-data schema from custom file to target database",
		" --dir <dir> [ --source <URI> ] --target <URI> ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --restore-jobs       Number of concurrent jobs for pg_restore\n"
		"  --no-owner           Do not set ownership of objects to match the original database\n"
		"  --no-acl             Prevent restoration of access privileges (grant/revoke commands).\n"
		"  --no-comments        Do not output commands to restore comments\n"
		"  --no-tablespaces     Do not output commands to select tablespaces\n"
		"  --skip-extensions    Skip restoring extensions\n" \
		"  --skip-ext-comments  Skip restoring COMMENT ON EXTENSION\n" \
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n",
		cli_restore_schema_getopts,
		cli_restore_schema_post_data);

static CommandLine restore_roles_command =
	make_command(
		"roles",
		"Restore database roles from SQL file to target database",
		" --dir <dir> [ --source <URI> ] --target <URI> ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --restore-jobs       Number of concurrent jobs for pg_restore\n",
		cli_restore_schema_getopts,
		cli_restore_roles);

static CommandLine restore_schema_parse_list_command =
	make_command(
		"parse-list",
		"Parse pg_restore --list output from custom file",
		" [ <pre.list> ] ",
		"  --source             Postgres URI to the source database\n"
		"  --target             Postgres URI to the target database\n"
		"  --dir                Work directory to use\n"
		"  --filters <filename> Use the filters defined in <filename>\n"
		"  --skip-extensions    Skip restoring extensions\n"
		"  --skip-ext-comments  Skip restoring COMMENT ON EXTENSION\n"
		"  --restart            Allow restarting when temp files exist already\n"
		"  --resume             Allow resuming operations after a failure\n"
		"  --not-consistent     Allow taking a new snapshot on the source database\n",
		cli_restore_schema_getopts,
		cli_restore_schema_parse_list);


static CommandLine *restore_subcommands[] = {
	&restore_schema_command,
	&restore_schema_pre_data_command,
	&restore_schema_post_data_command,
	&restore_roles_command,
	&restore_schema_parse_list_command,
	NULL
};

CommandLine restore_commands =
	make_command_set("restore",
					 "Restore database objects into a Postgres instance",
					 NULL, NULL, NULL, restore_subcommands);


/*
 * cli_restore_schema_getopts parses the CLI options for the `restore db` command.
 */
static int
cli_restore_schema_getopts(int argc, char **argv)
{
	CopyDBOptions options = { 0 };
	int c, option_index = 0;
	int errors = 0, verboseCount = 0;

	static struct option long_options[] = {
		{ "source", required_argument, NULL, 'S' },
		{ "target", required_argument, NULL, 'T' },
		{ "dir", required_argument, NULL, 'D' },
		{ "drop-if-exists", no_argument, NULL, 'c' }, /* pg_restore -c */
		{ "no-owner", no_argument, NULL, 'O' },       /* pg_restore -O */
		{ "no-comments", no_argument, NULL, 'X' },
		{ "restore-jobs", required_argument, NULL, 'j' },      /* pg_restore --jobs */
		{ "no-acl", no_argument, NULL, 'x' }, /* pg_restore -x */
		{ "filter", required_argument, NULL, 'F' },
		{ "filters", required_argument, NULL, 'F' },
		{ "skip-extensions", no_argument, NULL, 'e' },
		{ "skip-ext-comment", no_argument, NULL, 'E' },
		{ "skip-ext-comments", no_argument, NULL, 'E' },
		{ "restart", no_argument, NULL, 'r' },
		{ "resume", no_argument, NULL, 'R' },
		{ "not-consistent", no_argument, NULL, 'C' },
		{ "snapshot", required_argument, NULL, 'N' },
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

	while ((c = getopt_long(argc, argv, "S:T:D:cOXj:xF:eErRCN:Vvdzqh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'S':
			{
				if (!validate_connection_string(optarg))
				{
					log_fatal("Failed to parse --target connection string, "
							  "see above for details.");
					exit(EXIT_CODE_BAD_ARGS);
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
					exit(EXIT_CODE_BAD_ARGS);
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

			case 'c':
			{
				options.restoreOptions.dropIfExists = true;
				log_trace("--drop-if-exists");
				break;
			}

			case 'O':
			{
				options.restoreOptions.noOwner = true;
				log_trace("--no-owner");
				break;
			}

			case 'j':
			{
				if (!stringToInt(optarg, &options.restoreOptions.jobs) ||
					options.restoreOptions.jobs < 1 ||
					options.restoreOptions.jobs > 128)
				{
					log_fatal("Failed to parse --restore-jobs count: \"%s\"", optarg);
					++errors;
				}
				log_trace("--restore-jobs %d", options.restoreOptions.jobs);
				break;
			}

			case 'x':
			{
				options.restoreOptions.noACL = true;
				log_trace("--no-ack");
				break;
			}

			case 'X':
			{
				options.restoreOptions.noComments = true;
				log_trace("--no-comments");
				break;
			}

			case 'e':
			{
				options.skipExtensions = true;
				log_trace("--skip-extensions");
				break;
			}

			case 'E':
			{
				options.skipCommentOnExtension = true;
				log_trace("--skip-extensions");
				break;
			}

			case 'r':
			{
				options.restart = true;
				log_trace("--restart");
				break;
			}

			case 'R':
			{
				options.resume = true;
				log_trace("--resume");
				break;
			}

			case 'C':
			{
				options.notConsistent = true;
				log_trace("--not-consistent");
				break;
			}

			case 'N':
			{
				strlcpy(options.snapshot, optarg, sizeof(options.snapshot));
				log_trace("--snapshot %s", options.snapshot);
				break;
			}

			case 'F':
			{
				strlcpy(options.filterFileName, optarg, MAXPGPATH);
				log_trace("--filters \"%s\"", options.filterFileName);

				if (!file_exists(options.filterFileName))
				{
					log_error("Filters file \"%s\" does not exists",
							  options.filterFileName);
					++errors;
				}
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
			}
		}
	}

	if (options.connStrings.target_pguri == NULL)
	{
		log_fatal("Option --target is mandatory");
		++errors;
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

	/* if we haven't set restore-jobs, set it to index-jobs */
	if (options.restoreOptions.jobs == DEFAULT_RESTORE_JOBS)
	{
		options.restoreOptions.jobs = options.indexJobs;
		log_trace("--restore-jobs %d", options.indexJobs);
	}

	/* publish our option parsing in the global variable */
	restoreDBoptions = options;

	return optind;
}


/*
 * cli_restore_schema implements the command: pgcopydb restore schema
 */
static void
cli_restore_schema(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	/* we need access to the catalogs to filter the pg_restore --list */
	if (!catalog_init_from_specs(&copySpecs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!copydb_target_finalize_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!catalog_close_from_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_restore_schema implements the command: pgcopydb restore pre-data
 */
static void
cli_restore_schema_pre_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	/* we need access to the catalogs to filter the pg_restore --list */
	if (!catalog_init_from_specs(&copySpecs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_target_prepare_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!catalog_close_from_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_restore_schema implements the command: pgcopydb restore post-data
 */
static void
cli_restore_schema_post_data(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	/* we need access to the catalogs to filter the pg_restore --list */
	if (!catalog_init_from_specs(&copySpecs))
	{
		log_error("Failed to initialize pgcopydb internal catalogs");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!copydb_target_finalize_schema(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	if (!catalog_close_from_specs(&copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_restore_roles implements the command: pgcopydb restore roles
 */
static void
cli_restore_roles(int argc, char **argv)
{
	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	if (!pg_restore_roles(&(copySpecs.pgPaths),
						  copySpecs.connStrings.target_pguri,
						  copySpecs.dumpPaths.rolesFilename))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}
}


/*
 * cli_restore_schema implements the command: pgcopydb restore parse-list
 */
static void
cli_restore_schema_parse_list(int argc, char **argv)
{
	if (argc == 1)
	{
		char *filename = argv[0];

		log_info("Parsing Archive Content pre.list file: \"%s\"", filename);

		ArchiveContentArray contents = { 0 };

		if (!parse_archive_list(filename, &contents))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		log_notice("Read %d archive items in \"%s\"", contents.count, filename);

		for (int i = 0; i < contents.count; i++)
		{
			ArchiveContentItem *item = &(contents.array[i]);

			fformat(stdout,
					"%d; %u %u %s %s\n",
					item->dumpId,
					item->catalogOid,
					item->objectOid,
					item->description ? item->description : "",
					item->restoreListName ? item->restoreListName : "");
		}

		exit(EXIT_CODE_QUIT);
	}

	CopyDataSpec copySpecs = { 0 };

	(void) cli_restore_prepare_specs(&copySpecs);

	SourceFilters *filters = &(copySpecs.filters);

	if (filters->type != SOURCE_FILTER_TYPE_NONE)
	{
		if (!copydb_prepare_snapshot(&copySpecs))
		{
			/* errors have already been logged */
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		/* fetch schema information from source catalogs, including filtering */
		if (!copydb_fetch_schema_and_prepare_specs(&copySpecs))
		{
			/* errors have already been logged */
			(void) copydb_close_snapshot(&copySpecs);
			exit(EXIT_CODE_TARGET);
		}

		(void) copydb_close_snapshot(&copySpecs);
	}

	log_info("Preparing the pg_restore --use-list for the pre-data "
			 "archive file \"%s\" at: \"%s\"",
			 copySpecs.dumpPaths.dumpFilename,
			 copySpecs.dumpPaths.preListFilename);

	if (!copydb_write_restore_list(&copySpecs, PG_DUMP_SECTION_PRE_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Preparing the pg_restore --use-list for the post-data "
			 "archive file \"%s\" at: \"%s\"",
			 copySpecs.dumpPaths.dumpFilename,
			 copySpecs.dumpPaths.postListFilename);

	if (!copydb_write_restore_list(&copySpecs, PG_DUMP_SECTION_POST_DATA))
	{
		log_error("Failed to prepare the pg_restore --use-list catalogs, "
				  "see above for details");
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}


/*
 * cli_restore_prepare_specs prepares the CopyDataSpecs needed to drive the
 * restore commands.
 */
static void
cli_restore_prepare_specs(CopyDataSpec *copySpecs)
{
	CopyFilePaths *cfPaths = &(copySpecs->cfPaths);
	PostgresPaths *pgPaths = &(copySpecs->pgPaths);

	(void) find_pg_commands(pgPaths);

	char *dir =
		IS_EMPTY_STRING_BUFFER(restoreDBoptions.dir)
		? NULL
		: restoreDBoptions.dir;

	bool createWorkDir = true;

	if (!copydb_init_workdir(copySpecs,
							 dir,
							 false, /* service */
							 NULL,  /* serviceName */
							 restoreDBoptions.restart,
							 restoreDBoptions.resume,
							 createWorkDir))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	log_info("Restoring database from existing files at \"%s\"", cfPaths->topdir);

	if (!copydb_init_specs(copySpecs, &restoreDBoptions, DATA_SECTION_NONE))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	if (!IS_EMPTY_STRING_BUFFER(restoreDBoptions.filterFileName))
	{
		SourceFilters *filters = &(copySpecs->filters);

		if (!parse_filters(restoreDBoptions.filterFileName, filters))
		{
			log_error("Failed to parse filters in file \"%s\"",
					  restoreDBoptions.filterFileName);
			exit(EXIT_CODE_BAD_ARGS);
		}
	}

	/*
	 * Prepare our internal catalogs for storing the source database catalog
	 * query results.
	 */
	copySpecs->section = DATA_SECTION_ALL;

	if (!copydb_fetch_schema_and_prepare_specs(copySpecs))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_TARGET);
	}

	copySpecs->section = DATA_SECTION_NONE;

	log_info("Using pg_restore for Postgres \"%s\" at \"%s\"",
			 pgPaths->pg_version,
			 pgPaths->pg_restore);

	ConnStrings *dsn = &(copySpecs->connStrings);

	if (!cli_prepare_pguris(dsn))
	{
		/* errors have already been logged */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	char *target = dsn->safeTargetPGURI.pguri;
	log_info("[TARGET] Restoring database into \"%s\"", target);
}
