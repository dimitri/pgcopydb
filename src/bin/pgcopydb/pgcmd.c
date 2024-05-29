/*
 * src/bin/pgcopydb/pgcmd.c
 *   API for running PostgreSQL commands such as pg_dump and pg_restore.
 */

#include <ctype.h>
#include <dirent.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"

#include "catalog.h"
#include "cli_root.h"
#include "defaults.h"
#include "env_utils.h"
#include "file_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgcmd.h"
#include "schema.h"
#include "signals.h"
#include "string_utils.h"

/*
 * Because we did include defaults.h previously in the same compilation unit (a
 * .c file), then the runprogram.h code is linked to the Garbage-Collector:
 * calls to malloc() in there are replaced by calls to GC_malloc() as per
 * defaults.h #define instructions.
 *
 * As a result we never call free_program().
 */
#define RUN_PROGRAM_IMPLEMENTATION
#include "runprogram.h"


/*
 * Get psql --version output in pgPaths->pg_version.
 */
bool
psql_version(PostgresPaths *pgPaths)
{
	Program *prog = run_program(pgPaths->psql, "--version", NULL);
	char pg_version_string[PG_VERSION_STRING_MAX] = { 0 };
	int pg_version = 0;

	if (prog == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (prog->returnCode != 0)
	{
		errno = prog->error;
		log_error("Failed to run \"psql --version\" using program \"%s\": %m",
				  pgPaths->psql);
		return false;
	}

	if (!parse_version_number(prog->stdOut,
							  pg_version_string,
							  PG_VERSION_STRING_MAX,
							  &pg_version))
	{
		/* errors have already been logged */
		return false;
	}

	strlcpy(pgPaths->pg_version, pg_version_string, PG_VERSION_STRING_MAX);

	return true;
}


/*
 * find_pg_commands finds the Postgres commands to use given either PG_CONFIG
 * in the environment, or finding the first psql entry in the PATH and taking
 * it from there.
 */
void
find_pg_commands(PostgresPaths *pgPaths)
{
	/* first, use PG_CONFIG when it exists in the environment */
	if (set_psql_from_PG_CONFIG(pgPaths))
	{
		(void) set_postgres_commands(pgPaths);
		return;
	}

	/* then, use PATH and fetch the first entry there for the monitor */
	if (search_path_first("psql", pgPaths->psql, LOG_WARN))
	{
		if (!psql_version(pgPaths))
		{
			/* errors have been logged in psql_version */
			exit(EXIT_CODE_PGCTL);
		}

		(void) set_postgres_commands(pgPaths);
		return;
	}

	/* then, use PATH and fetch pg_config --bindir from there */
	if (set_psql_from_pg_config(pgPaths))
	{
		(void) set_postgres_commands(pgPaths);
		return;
	}

	/* at this point we don't have any other ways to find a psql */
	exit(EXIT_CODE_PGCTL);
}


/*
 * set_postgres_commands sets the rest of the Postgres commands that pgcopydb
 * needs from knowing the pgPaths->psql absolute location already.
 */
void
set_postgres_commands(PostgresPaths *pgPaths)
{
	path_in_same_directory(pgPaths->psql, "pg_dump", pgPaths->pg_dump);
	path_in_same_directory(pgPaths->psql, "pg_dumpall", pgPaths->pg_dumpall);
	path_in_same_directory(pgPaths->psql, "pg_restore", pgPaths->pg_restore);
	path_in_same_directory(pgPaths->psql, "vacuumdb", pgPaths->vacuumdb);
}


/*
 * set_psql_from_PG_CONFIG sets the path to psql following the exported
 * environment variable PG_CONFIG, when it is found in the environment.
 *
 * Postgres developer environments often define PG_CONFIG in the environment to
 * build extensions for a specific version of Postgres. Let's use the hint here
 * too.
 */
bool
set_psql_from_PG_CONFIG(PostgresPaths *pgPaths)
{
	char PG_CONFIG[MAXPGPATH] = { 0 };

	if (!env_exists("PG_CONFIG"))
	{
		/* then we don't use PG_CONFIG to find psql */
		return false;
	}

	if (!get_env_copy("PG_CONFIG", PG_CONFIG, sizeof(PG_CONFIG)))
	{
		/* errors have already been logged */
		return false;
	}

	if (!file_exists(PG_CONFIG))
	{
		log_error("Failed to find a file for PG_CONFIG environment value \"%s\"",
				  PG_CONFIG);
		return false;
	}

	if (!set_psql_from_config_bindir(pgPaths, PG_CONFIG))
	{
		/* errors have already been logged */
		return false;
	}

	if (!psql_version(pgPaths))
	{
		log_fatal("Failed to get version info from %s --version",
				  pgPaths->psql);
		return false;
	}

	log_debug("Found psql for PostgreSQL %s at %s following PG_CONFIG",
			  pgPaths->pg_version, pgPaths->psql);

	return true;
}


/*
 * set_psql_from_PG_CONFIG sets given pgPaths->psql to the psql binary
 * installed in the bindir of the target Postgres installation:
 *
 *  $(${PG_CONFIG} --bindir)/psql
 */
bool
set_psql_from_config_bindir(PostgresPaths *pgPaths, const char *pg_config)
{
	char psql[MAXPGPATH] = { 0 };

	if (!file_exists(pg_config))
	{
		log_debug("set_psql_from_config_bindir: file not found: \"%s\"",
				  pg_config);
		return false;
	}

	Program *prog = run_program(pg_config, "--bindir", NULL);

	if (prog == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (prog->returnCode != 0)
	{
		errno = prog->error;
		log_error("Failed to run \"pg_config --bindir\" using program \"%s\": %m",
				  pg_config);
		return false;
	}

	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, prog->stdOut) || lbuf.count != 1)
	{
		log_error("Unable to parse output from pg_config --bindir");
		return false;
	}

	char *bindir = lbuf.lines[0];
	join_path_components(psql, bindir, "psql");

	if (!file_exists(psql))
	{
		log_error("Failed to find psql at \"%s\" from PG_CONFIG at \"%s\"",
				  pgPaths->psql,
				  pg_config);
		return false;
	}

	strlcpy(pgPaths->psql, psql, sizeof(pgPaths->psql));

	return true;
}


/*
 * set_psql_from_pg_config sets the path to psql by using pg_config
 * --bindir when there is a single pg_config found in the PATH.
 *
 * When using debian/ubuntu packaging then pg_config is installed as part as
 * the postgresql-common package in /usr/bin, whereas psql is installed in a
 * major version dependent location such as /usr/lib/postgresql/12/bin, and
 * those locations are not included in the PATH.
 *
 * So when we can't find psql anywhere in the PATH, we look for pg_config
 * instead, and then use pg_config --bindir to discover the psql we can use.
 */
bool
set_psql_from_pg_config(PostgresPaths *pgPaths)
{
	SearchPath all_pg_configs = { 0 };
	SearchPath pg_configs = { 0 };

	if (!search_path("pg_config", &all_pg_configs))
	{
		return false;
	}

	if (!search_path_deduplicate_symlinks(&all_pg_configs, &pg_configs))
	{
		log_error("Failed to resolve symlinks found in PATH entries, "
				  "see above for details");
		return false;
	}

	switch (pg_configs.found)
	{
		case 0:
		{
			log_warn("Failed to find either psql or pg_config in PATH");
			return false;
		}

		case 1:
		{
			if (!set_psql_from_config_bindir(pgPaths, pg_configs.matches[0]))
			{
				/* errors have already been logged */
				return false;
			}

			if (!psql_version(pgPaths))
			{
				log_fatal("Failed to get version info from %s --version",
						  pgPaths->psql);
				return false;
			}

			log_debug("Found psql for PostgreSQL %s at %s from pg_config "
					  "found in PATH at \"%s\"",
					  pgPaths->pg_version,
					  pgPaths->psql,
					  pg_configs.matches[0]);

			return true;
		}

		default:
		{
			log_info("Found more than one pg_config entry in current PATH:");

			for (int i = 0; i < pg_configs.found; i++)
			{
				PostgresPaths currentPgPaths = { 0 };

				strlcpy(currentPgPaths.psql,
						pg_configs.matches[i],
						sizeof(currentPgPaths.psql));

				if (!psql_version(&currentPgPaths))
				{
					/*
					 * Because of this it's possible that there's now only a
					 * single working version of psql found in PATH. If
					 * that's the case we will still not use that by default,
					 * since the users intention is unclear. They might have
					 * wanted to use the version of psql that we could not
					 * parse the version string for. So we warn and continue,
					 * the user should make their intention clear by using the
					 * --psql option (or changing PATH).
					 */
					log_warn("Failed to get version info from %s --version",
							 currentPgPaths.psql);
					continue;
				}

				log_info("Found \"%s\" for pg version %s",
						 currentPgPaths.psql,
						 currentPgPaths.pg_version);
			}

			log_info("HINT: export PG_CONFIG to a specific pg_config entry");

			return false;
		}
	}

	return false;
}


typedef struct DumpExtensionNamespaceContext
{
	char **extNamespaces;
	int *extNamespaceCount;
} DumpExtensionNamespaceContext;


/*
 * Call pg_dump and get the given section of the dump into the target file.
 */
bool
pg_dump_db(PostgresPaths *pgPaths,
		   ConnStrings *connStrings,
		   const char *snapshot,
		   const char *section,
		   SourceFilters *filters,
		   DatabaseCatalog *filtersDB,
		   const char *filename)
{
	char *args[PG_CMD_MAX_ARG];
	int argsIndex = 0;

	char command[BUFSIZE] = { 0 };

	char *PGPASSWORD = NULL;
	bool pgpassword_found_in_env = env_exists("PGPASSWORD");

	if (!env_exists("PGCONNECT_TIMEOUT"))
	{
		setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);
	}

	/* override PGPASSWORD environment variable if the pguri contains one */
	if (connStrings->safeSourcePGURI.password != NULL)
	{
		if (pgpassword_found_in_env && !get_env_dup("PGPASSWORD", &PGPASSWORD))
		{
			/* errors have already been logged */
			return false;
		}
		setenv("PGPASSWORD", connStrings->safeSourcePGURI.password, 1);
	}

	args[argsIndex++] = (char *) pgPaths->pg_dump;
	args[argsIndex++] = "-Fc";

	if (!IS_EMPTY_STRING_BUFFER(snapshot))
	{
		args[argsIndex++] = "--snapshot";
		args[argsIndex++] = (char *) snapshot;
	}

	args[argsIndex++] = "--section";
	args[argsIndex++] = (char *) section;

	/* apply [include-only-schema] filtering */
	for (int i = 0; i < filters->includeOnlySchemaList.count; i++)
	{
		char *nspname = filters->includeOnlySchemaList.array[i].nspname;

		/* check that we still have room for --includeOnly-schema args */
		if (PG_CMD_MAX_ARG < (argsIndex + 2))
		{
			log_error("Failed to call pg_dump, too many include-only-schema entries: "
					  "argsIndex %d > %d",
					  argsIndex + 2, PG_CMD_MAX_ARG);
			return false;
		}

		args[argsIndex++] = "--schema";
		args[argsIndex++] = nspname;
	}

	/* apply [exclude-schema] filtering */
	for (int i = 0; i < filters->excludeSchemaList.count; i++)
	{
		char *nspname = filters->excludeSchemaList.array[i].nspname;

		/* check that we still have room for --exclude-schema args */
		if (PG_CMD_MAX_ARG < (argsIndex + 2))
		{
			log_error("Failed to call pg_dump, too many exclude-schema entries: "
					  "argsIndex %d > %d",
					  argsIndex + 2,
					  PG_CMD_MAX_ARG);
			return false;
		}

		args[argsIndex++] = "--exclude-schema";
		args[argsIndex++] = nspname;
	}

	args[argsIndex++] = "--file";
	args[argsIndex++] = (char *) filename;
	args[argsIndex++] = (char *) connStrings->safeSourcePGURI.pguri;

	args[argsIndex] = NULL;

	/*
	 * We do not want to call setsid() when running pg_dump.
	 */
	Program program = { 0 };

	(void) initialize_program(&program, args, false);
	program.processBuffer = &processBufferCallback;

	/* log the exact command line we're using */
	int commandSize = snprintf_program_command_line(&program, command, BUFSIZE);

	if (commandSize >= BUFSIZE)
	{
		/* we only display the first BUFSIZE bytes of the real command */
		log_info("%s...", command);
	}
	else
	{
		log_info("%s", command);
	}

	(void) execute_subprogram(&program);

	/* make sure to reset the environment PGPASSWORD if we edited it */
	if (pgpassword_found_in_env &&
		connStrings->safeSourcePGURI.password != NULL)
	{
		setenv("PGPASSWORD", PGPASSWORD, 1);
	}

	if (program.returnCode != 0)
	{
		log_error("Failed to run pg_dump: exit code %d", program.returnCode);

		return false;
	}

	return true;
}


/*
 * Call `vacuumdb --analyze-only --jobs ${table-jobs}`
 */
bool
pg_vacuumdb_analyze_only(PostgresPaths *pgPaths, ConnStrings *connStrings, int jobs)
{
	char *args[16];
	int argsIndex = 0;

	char command[BUFSIZE] = { 0 };

	char *PGPASSWORD = NULL;
	bool pgpassword_found_in_env = env_exists("PGPASSWORD");

	if (!env_exists("PGCONNECT_TIMEOUT"))
	{
		setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);
	}

	/* override PGPASSWORD environment variable if the pguri contains one */
	if (connStrings->safeSourcePGURI.password != NULL)
	{
		if (pgpassword_found_in_env && !get_env_dup("PGPASSWORD", &PGPASSWORD))
		{
			/* errors have already been logged */
			return false;
		}
		setenv("PGPASSWORD", connStrings->safeSourcePGURI.password, 1);
	}

	args[argsIndex++] = (char *) pgPaths->vacuumdb;
	args[argsIndex++] = "--analyze-only";
	args[argsIndex++] = "--jobs";
	args[argsIndex++] = intToString(jobs).strValue;
	args[argsIndex++] = "--dbname";
	args[argsIndex++] = (char *) connStrings->safeSourcePGURI.pguri;

	args[argsIndex] = NULL;

	/*
	 * We do not want to call setsid() when running vacuumdb.
	 */
	Program program = { 0 };

	(void) initialize_program(&program, args, false);
	program.processBuffer = &processBufferCallback;

	/* log the exact command line we're using */
	int commandSize = snprintf_program_command_line(&program, command, BUFSIZE);

	if (commandSize >= BUFSIZE)
	{
		/* we only display the first BUFSIZE bytes of the real command */
		log_info("%s...", command);
	}
	else
	{
		log_info("%s", command);
	}

	(void) execute_subprogram(&program);

	/* make sure to reset the environment PGPASSWORD if we edited it */
	if (pgpassword_found_in_env &&
		connStrings->safeSourcePGURI.password != NULL)
	{
		setenv("PGPASSWORD", PGPASSWORD, 1);
	}

	if (program.returnCode != 0)
	{
		log_error("Failed to run vacuumdb: exit code %d", program.returnCode);

		return false;
	}

	return true;
}


/*
 * Call pg_dump and get the given section of the dump into the target file.
 */
bool
pg_dumpall_roles(PostgresPaths *pgPaths,
				 ConnStrings *connStrings,
				 const char *filename,
				 bool noRolesPasswords)
{
	char *args[16];
	int argsIndex = 0;

	char command[BUFSIZE] = { 0 };

	char *PGPASSWORD = NULL;
	bool pgpassword_found_in_env = env_exists("PGPASSWORD");

	if (!env_exists("PGCONNECT_TIMEOUT"))
	{
		setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);
	}

	/* override PGPASSWORD environment variable if the pguri contains one */
	if (connStrings->safeSourcePGURI.password != NULL)
	{
		if (pgpassword_found_in_env && !get_env_dup("PGPASSWORD", &PGPASSWORD))
		{
			/* errors have already been logged */
			return false;
		}
		setenv("PGPASSWORD", connStrings->safeSourcePGURI.password, 1);
	}

	args[argsIndex++] = (char *) pgPaths->pg_dumpall;
	args[argsIndex++] = "--roles-only";

	args[argsIndex++] = "--file";
	args[argsIndex++] = (char *) filename;

	args[argsIndex++] = "--dbname";
	args[argsIndex++] = (char *) connStrings->safeSourcePGURI.pguri;

	if (noRolesPasswords)
	{
		args[argsIndex++] = "--no-role-passwords";
	}

	args[argsIndex] = NULL;

	/*
	 * We do not want to call setsid() when running pg_dump.
	 */
	Program program = { 0 };

	(void) initialize_program(&program, args, false);
	program.processBuffer = &processBufferCallback;

	/* log the exact command line we're using */
	int commandSize = snprintf_program_command_line(&program, command, BUFSIZE);

	if (commandSize >= BUFSIZE)
	{
		/* we only display the first BUFSIZE bytes of the real command */
		log_info("%s...", command);
	}
	else
	{
		log_info("%s", command);
	}

	(void) execute_subprogram(&program);

	/* make sure to reset the environment PGPASSWORD if we edited it */
	if (pgpassword_found_in_env &&
		connStrings->safeSourcePGURI.password != NULL)
	{
		setenv("PGPASSWORD", PGPASSWORD, 1);
	}

	if (program.returnCode != 0)
	{
		log_error("Failed to run pg_dump: exit code %d", program.returnCode);

		return false;
	}

	return true;
}


/*
 * pg_restore_roles calls psql on the roles SQL file obtained with pg_dumpall
 * or the function pg_dumpall_roles.
 */
bool
pg_restore_roles(PostgresPaths *pgPaths,
				 const char *pguri,
				 const char *filename)
{
	char *content = NULL;
	long size = 0L;

	if (!env_exists("PGCONNECT_TIMEOUT"))
	{
		setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);
	}

	/*
	 * Rather than using psql --single-transaction --file filename, we read the
	 * given filename in memory and loop over the lines.
	 *
	 * We know that pg_dumpall --roles-only outputs a single SQL command per
	 * line, so that we don't actually have to be smart about parse the content.
	 *
	 * Then again there is no CREATE ROLE IF NOT EXISTS in Postgres, that's why
	 * we are reading the file and then sending the commands ourselves. When
	 * the script contains a line such as
	 *
	 *  CREATE ROLE dim;
	 *
	 * instead of applying it as-is, we parse the usename and check if it
	 * already exists on the target. We only send the SQL command when we fail
	 * to find the username.
	 */
	if (!read_file(filename, &content, &size))
	{
		/* errors have already been logged */
		return false;
	}

	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, content))
	{
		/* errors have already been logged */
		return false;
	}

	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, (char *) pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_begin(&pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * pg_dumpall always outputs first a line with the CREATE ROLE command and
	 * immediately after that a line with an ALTER ROLE command that sets the
	 * role options.
	 *
	 * When we skip a role, we also skip the next line, which is the ALTER ROLE
	 * command for the same role.
	 */
	bool skipNextLine = false;

	for (uint64_t l = 0; l < lbuf.count; l++)
	{
		char *currentLine = lbuf.lines[l];

		if (skipNextLine)
		{
			/* toggle the switch again, it's valid only once */
			skipNextLine = false;
			log_debug("Skipping line: %s", currentLine);
			continue;
		}

		if (strcmp(currentLine, "") == 0)
		{
			/* skip empty lines */
			continue;
		}

		char *createRole = "CREATE ROLE ";
		int createRoleLen = strlen(createRole);

		/* skip comments */
		if (strncmp(currentLine, "--", 2) == 0)
		{
			continue;
		}

		/* implement CREATE ROLE our own way (check if exists first) */
		else if (strncmp(currentLine, createRole, createRoleLen) == 0)
		{
			/* we have a create role command */
			int lineLen = strlen(currentLine);
			char lastChar = currentLine[lineLen - 1];

			if (lastChar != ';')
			{
				log_error("Failed to parse create role statement \"%s\"",
						  currentLine);
				return false;
			}

			/* chomp the last ';' character from the role name */
			currentLine[lineLen - 1] = '\0';

			char *roleNamePtr = currentLine + createRoleLen;
			char roleName[NAMEDATALEN] = { 0 };

			strlcpy(roleName, roleNamePtr, sizeof(roleName));

			bool exists = false;

			if (!pgsql_role_exists(&pgsql, roleName, &exists))
			{
				/* errors have already been logged */
				return false;
			}

			if (exists)
			{
				skipNextLine = true;
				log_info("Skipping CREATE ROLE %s, which already exists",
						 roleName);
				continue;
			}

			char createRole[BUFSIZE] = { 0 };

			sformat(createRole, sizeof(createRole), "CREATE ROLE %s", roleName);

			log_info("%s", createRole);

			if (!pgsql_execute(&pgsql, createRole))
			{
				/* errors have already been logged */
				return false;
			}
		}
		else
		{
			log_info("%s", currentLine);

			if (!pgsql_execute(&pgsql, currentLine))
			{
				/* errors have already been logged */
				return false;
			}
		}
	}

	if (!pgsql_commit(&pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * pg_copy_roles copies roles from the source instance into the target
 * instance, using pg_dumpall --roles-only and our own SQL client that reads
 * the file and applies SQL command on the target system.
 */
bool
pg_copy_roles(PostgresPaths *pgPaths,
			  ConnStrings *connStrings,
			  const char *filename,
			  bool noRolesPasswords)
{
	if (!pg_dumpall_roles(pgPaths, connStrings, filename, noRolesPasswords))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pg_restore_roles(pgPaths, connStrings->target_pguri, filename))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * Call pg_restore from the given filename and restores it to the target
 * database connection.
 */
bool
pg_restore_db(PostgresPaths *pgPaths,
			  ConnStrings *connStrings,
			  SourceFilters *filters,
			  const char *dumpFilename,
			  const char *listFilename,
			  RestoreOptions options)
{
	char *args[PG_CMD_MAX_ARG];
	int argsIndex = 0;

	char command[BUFSIZE] = { 0 };

	char *PGPASSWORD = NULL;
	bool pgpassword_found_in_env = env_exists("PGPASSWORD");

	if (!env_exists("PGCONNECT_TIMEOUT"))
	{
		setenv("PGCONNECT_TIMEOUT", POSTGRES_CONNECT_TIMEOUT, 1);
	}

	/* override PGPASSWORD environment variable if the pguri contains one */
	if (connStrings->safeTargetPGURI.password != NULL)
	{
		if (pgpassword_found_in_env && !get_env_dup("PGPASSWORD", &PGPASSWORD))
		{
			/* errors have already been logged */
			return false;
		}
		setenv("PGPASSWORD", connStrings->safeTargetPGURI.password, 1);
	}

	args[argsIndex++] = (char *) pgPaths->pg_restore;
	args[argsIndex++] = "--dbname";
	args[argsIndex++] = (char *) connStrings->safeTargetPGURI.pguri;

	if (options.jobs == 1)
	{
		args[argsIndex++] = "--single-transaction";
	}
	else
	{
		args[argsIndex++] = "--jobs";
		args[argsIndex++] = intToString(options.jobs).strValue;
	}

	if (options.dropIfExists)
	{
		args[argsIndex++] = "--clean";
		args[argsIndex++] = "--if-exists";
	}

	if (options.noOwner)
	{
		args[argsIndex++] = "--no-owner";
	}

	if (options.noComments)
	{
		args[argsIndex++] = "--no-comments";
	}

	if (options.noACL)
	{
		args[argsIndex++] = "--no-acl";
	}

	if (options.noTableSpaces)
	{
		args[argsIndex++] = "--no-tablespaces";
	}

	/*
	 * Do not apply [include-only-schema] filtering.
	 *
	 * When using pg_restore --schema foo then pg_restore only restores objects
	 * that are in the named schema, which does not include the schema itself.
	 * We want to include the CREATE SCHEMA statement in the pg_restore
	 * activity here, which means we can't use pg_restore --schema.
	 */

	/* apply [exclude-schema] filtering */
	for (int i = 0; i < filters->excludeSchemaList.count; i++)
	{
		char *nspname = filters->excludeSchemaList.array[i].nspname;

		/* check that we still have room for --exclude-schema args */
		if (PG_CMD_MAX_ARG < (argsIndex + 2))
		{
			log_error("Failed to call pg_restore, too many exclude-schema "
					  "entries: argsIndex %d > %d",
					  argsIndex + 2,
					  PG_CMD_MAX_ARG);
			return false;
		}

		args[argsIndex++] = "--exclude-schema";
		args[argsIndex++] = nspname;
	}

	if (listFilename != NULL)
	{
		args[argsIndex++] = "--use-list";
		args[argsIndex++] = (char *) listFilename;
	}

	args[argsIndex++] = (char *) dumpFilename;

	args[argsIndex] = NULL;

	/*
	 * We do not want to call setsid() when running pg_dump.
	 */
	Program program = { 0 };

	(void) initialize_program(&program, args, false);
	program.processBuffer = &processBufferCallback;

	/* log the exact command line we're using */
	int commandSize = snprintf_program_command_line(&program, command, BUFSIZE);

	if (commandSize >= BUFSIZE)
	{
		/* we only display the first BUFSIZE bytes of the real command */
		log_info("%s...", command);
	}
	else
	{
		log_info("%s", command);
	}

	(void) execute_subprogram(&program);

	/* make sure to reset the environment PGPASSWORD if we edited it */
	if (pgpassword_found_in_env &&
		connStrings->safeTargetPGURI.password != NULL)
	{
		setenv("PGPASSWORD", PGPASSWORD, 1);
	}

	if (program.returnCode != 0)
	{
		log_error("Failed to run pg_restore: exit code %d", program.returnCode);

		return false;
	}

	return true;
}


/*
 * pg_restore_list runs the command pg_restore -f- -l on the given custom
 * format dump file and returns an array of pg_dump archive objects.
 */
bool
pg_restore_list(PostgresPaths *pgPaths,
				const char *restoreFilename,
				const char *listFilename,
				ArchiveContentArray *archive)
{
	char *args[PG_CMD_MAX_ARG];
	int argsIndex = 0;

	args[argsIndex++] = (char *) pgPaths->pg_restore;

	args[argsIndex++] = "-f";
	args[argsIndex++] = (char *) listFilename;

	args[argsIndex++] = "-l";
	args[argsIndex++] = (char *) restoreFilename;

	args[argsIndex] = NULL;

	/*
	 * We do not want to call setsid() when running pg_dump.
	 */
	Program program = { 0 };

	(void) initialize_program(&program, args, false);
	program.processBuffer = &processBufferCallback;

	/* log the exact command line we're using */
	char command[BUFSIZE] = { 0 };
	(void) snprintf_program_command_line(&program, command, BUFSIZE);

	log_notice("%s", command);

	(void) execute_subprogram(&program);

	if (program.returnCode != 0)
	{
		log_error("Failed to run pg_restore: exit code %d", program.returnCode);

		return false;
	}

	if (!parse_archive_list(listFilename, archive))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * parse_archive_list parses a archive content list as obtained with the
 * pg_restore --list option.
 *
 * We are parsing the following format, plus a preamble that contains lines
 * that all start with a semi-colon, the comment separator for this format.
 *
 * ahprintf(AH, "%d; %u %u %s %s %s %s\n", te->dumpId,
 *          te->catalogId.tableoid, te->catalogId.oid,
 *          te->desc, sanitized_schema, sanitized_name,
 *          sanitized_owner);
 *
 */

struct ArchiveItemDescMapping
{
	ArchiveItemDesc desc;
	int len;
	char str[BUFSIZE];
};

/* remember to skip the \0 at the end of the static string here */
#define INSERT_MAPPING(d, s) { d, sizeof(s) - 1, s }

/*
 * List manually processed from describeDumpableObject in
 * postgres/src/bin/pg_dump/pg_dump_sort.c
 */
struct ArchiveItemDescMapping pgRestoreDescriptionArray[] = {
	INSERT_MAPPING(ARCHIVE_TAG_ACCESS_METHOD, "ACCESS METHOD"),
	INSERT_MAPPING(ARCHIVE_TAG_ACL, "ACL"),
	INSERT_MAPPING(ARCHIVE_TAG_AGGREGATE, "AGGREGATE"),
	INSERT_MAPPING(ARCHIVE_TAG_ATTRDEF, "ATTRDEF"),
	INSERT_MAPPING(ARCHIVE_TAG_BLOB_DATA, "BLOB DATA"),
	INSERT_MAPPING(ARCHIVE_TAG_BLOB, "BLOB"),
	INSERT_MAPPING(ARCHIVE_TAG_CAST, "CAST"),
	INSERT_MAPPING(ARCHIVE_TAG_CHECK_CONSTRAINT, "CHECK CONSTRAINT"),
	INSERT_MAPPING(ARCHIVE_TAG_COLLATION, "COLLATION"),
	INSERT_MAPPING(ARCHIVE_TAG_COMMENT, "COMMENT"),
	INSERT_MAPPING(ARCHIVE_TAG_CONSTRAINT, "CONSTRAINT"),
	INSERT_MAPPING(ARCHIVE_TAG_CONVERSION, "CONVERSION"),
	INSERT_MAPPING(ARCHIVE_TAG_DATABASE, "DATABASE"),
	INSERT_MAPPING(ARCHIVE_TAG_DEFAULT_ACL, "DEFAULT ACL"),
	INSERT_MAPPING(ARCHIVE_TAG_DEFAULT, "DEFAULT"),
	INSERT_MAPPING(ARCHIVE_TAG_DOMAIN, "DOMAIN"),
	INSERT_MAPPING(ARCHIVE_TAG_DUMMY_TYPE, "DUMMY TYPE"),
	INSERT_MAPPING(ARCHIVE_TAG_EVENT_TRIGGER, "EVENT TRIGGER"),
	INSERT_MAPPING(ARCHIVE_TAG_EXTENSION, "EXTENSION"),
	INSERT_MAPPING(ARCHIVE_TAG_FK_CONSTRAINT, "FK CONSTRAINT"),
	INSERT_MAPPING(ARCHIVE_TAG_FOREIGN_DATA_WRAPPER, "FOREIGN DATA WRAPPER"),
	INSERT_MAPPING(ARCHIVE_TAG_FOREIGN_SERVER, "FOREIGN SERVER"),
	INSERT_MAPPING(ARCHIVE_TAG_FOREIGN_TABLE, "FOREIGN TABLE"),
	INSERT_MAPPING(ARCHIVE_TAG_FUNCTION, "FUNCTION"),
	INSERT_MAPPING(ARCHIVE_TAG_INDEX_ATTACH, "INDEX ATTACH"),
	INSERT_MAPPING(ARCHIVE_TAG_INDEX, "INDEX"),
	INSERT_MAPPING(ARCHIVE_TAG_LANGUAGE, "LANGUAGE"),
	INSERT_MAPPING(ARCHIVE_TAG_LARGE_OBJECT, "LARGE OBJECT"),
	INSERT_MAPPING(ARCHIVE_TAG_MATERIALIZED_VIEW, "MATERIALIZED VIEW"),
	INSERT_MAPPING(ARCHIVE_TAG_OPERATOR_CLASS, "OPERATOR CLASS"),
	INSERT_MAPPING(ARCHIVE_TAG_OPERATOR_FAMILY, "OPERATOR FAMILY"),
	INSERT_MAPPING(ARCHIVE_TAG_OPERATOR, "OPERATOR"),
	INSERT_MAPPING(ARCHIVE_TAG_POLICY, "POLICY"),
	INSERT_MAPPING(ARCHIVE_TAG_PROCEDURAL_LANGUAGE, "PROCEDURAL LANGUAGE"),
	INSERT_MAPPING(ARCHIVE_TAG_PROCEDURE, "PROCEDURE"),
	INSERT_MAPPING(ARCHIVE_TAG_PUBLICATION_TABLES_IN_SCHEMA,
				   "PUBLICATION TABLES IN SCHEMA"),
	INSERT_MAPPING(ARCHIVE_TAG_PUBLICATION_TABLE, "PUBLICATION TABLE"),
	INSERT_MAPPING(ARCHIVE_TAG_PUBLICATION, "PUBLICATION"),
	INSERT_MAPPING(ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW, "REFRESH MATERIALIZED VIEW"),
	INSERT_MAPPING(ARCHIVE_TAG_ROW_SECURITY, "ROW SECURITY"),
	INSERT_MAPPING(ARCHIVE_TAG_RULE, "RULE"),
	INSERT_MAPPING(ARCHIVE_TAG_SCHEMA, "SCHEMA"),
	INSERT_MAPPING(ARCHIVE_TAG_SEQUENCE_OWNED_BY, "SEQUENCE OWNED BY"),
	INSERT_MAPPING(ARCHIVE_TAG_SEQUENCE_SET, "SEQUENCE SET"),
	INSERT_MAPPING(ARCHIVE_TAG_SEQUENCE, "SEQUENCE"),
	INSERT_MAPPING(ARCHIVE_TAG_SERVER, "SERVER"),
	INSERT_MAPPING(ARCHIVE_TAG_SHELL_TYPE, "SHELL TYPE"),
	INSERT_MAPPING(ARCHIVE_TAG_STATISTICS, "STATISTICS"),
	INSERT_MAPPING(ARCHIVE_TAG_SUBSCRIPTION, "SUBSCRIPTION"),
	INSERT_MAPPING(ARCHIVE_TAG_TABLE_ATTACH, "TABLE ATTACH"),
	INSERT_MAPPING(ARCHIVE_TAG_TABLE_DATA, "TABLE DATA"),
	INSERT_MAPPING(ARCHIVE_TAG_TABLE, "TABLE"),
	INSERT_MAPPING(ARCHIVE_TAG_TEXT_SEARCH_CONFIGURATION, "TEXT SEARCH CONFIGURATION"),
	INSERT_MAPPING(ARCHIVE_TAG_TEXT_SEARCH_DICTIONARY, "TEXT SEARCH DICTIONARY"),
	INSERT_MAPPING(ARCHIVE_TAG_TEXT_SEARCH_PARSER, "TEXT SEARCH PARSER"),
	INSERT_MAPPING(ARCHIVE_TAG_TEXT_SEARCH_TEMPLATE, "TEXT SEARCH TEMPLATE"),
	INSERT_MAPPING(ARCHIVE_TAG_TRANSFORM, "TRANSFORM"),
	INSERT_MAPPING(ARCHIVE_TAG_TRIGGER, "TRIGGER"),
	INSERT_MAPPING(ARCHIVE_TAG_TYPE, "TYPE"),
	INSERT_MAPPING(ARCHIVE_TAG_USER_MAPPING, "USER MAPPING"),
	INSERT_MAPPING(ARCHIVE_TAG_VIEW, "VIEW"),
	{ ARCHIVE_TAG_UNKNOWN, 0, "" }
};


/*
 * parse_archive_list implementation follows, see above for details/comments.
 */
bool
parse_archive_list(const char *filename, ArchiveContentArray *contents)
{
	char *buffer = NULL;
	long fileSize = 0L;

	if (!read_file(filename, &buffer, &fileSize))
	{
		/* errors have already been logged */
		return false;
	}

	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, buffer))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * If the file contains zero lines, we're done already, Also malloc(zero)
	 * leads to "corrupted size vs. prev_size" run-time errors.
	 */
	if (lbuf.count == 0)
	{
		return true;
	}

	contents->count = 0;
	contents->array =
		(ArchiveContentItem *) calloc(lbuf.count, sizeof(ArchiveContentItem));

	for (uint64_t lineNumber = 0; lineNumber < lbuf.count; lineNumber++)
	{
		ArchiveContentItem *item = &(contents->array[contents->count]);

		char *line = lbuf.lines[lineNumber];

		/* skip empty lines and lines that start with a semi-colon (comment) */
		if (line == NULL || *line == '\0' || *line == ';')
		{
			continue;
		}

		if (!parse_archive_list_entry(item, line))
		{
			log_error("Failed to parse line %lld of \"%s\", "
					  "see above for details",
					  (long long) lineNumber,
					  filename);
			return false;
		}

		/* use same format as file input */
		log_trace("parse_archive_list: %u; %u %u %s %s",
				  item->dumpId,
				  item->catalogOid,
				  item->objectOid,
				  item->description,
				  item->restoreListName);

		if (item->desc == ARCHIVE_TAG_UNKNOWN ||
			IS_EMPTY_STRING_BUFFER(item->description))
		{
			log_warn("Failed to parse desc \"%s\"", line);
		}

		++contents->count;
	}

	return true;
}


/*
 * parse_archive_list_entry parses a pg_restore archive TOC line such as the
 * following:
 *
 * 20; 2615 680978 SCHEMA - pgcopydb dim
 * 662; 1247 466596 DOMAIN public bıgınt postgres
 * 665; 1247 466598 TYPE public mpaa_rating postgres
 *
 * parse_archive_list_entry does not deal with empty lines or commented lines.
 */
bool
parse_archive_list_entry(ArchiveContentItem *item, const char *line)
{
	ArchiveToken token = { .ptr = (char *) line };

	/* 1. archive item dumpId */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_OID)
	{
		log_error("Failed to parse Archive TOC dumpId in: %s", line);
		return false;
	}

	item->dumpId = token.oid;

	/* 2. semicolon then space */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_SEMICOLON)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	/* 3. catalogOid */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_OID)
	{
		log_error("Failed to parse Archive TOC catalogOid in: %s", line);
		return false;
	}

	item->catalogOid = token.oid;

	/* 4. space */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	/* 5. objectOid */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_OID)
	{
		log_error("Failed to parse Archive TOC objectOid in: %s", line);
		return false;
	}

	item->objectOid = token.oid;

	/* 6. space */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	/* 7. desc */
	char *start = token.ptr;

	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_DESC)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	item->desc = token.desc;
	int itemDescLen = token.ptr - start + 1;

	if (itemDescLen == 0)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	item->description = (char *) calloc(token.ptr - start + 1, sizeof(char));

	if (item->description == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	strlcpy(item->description, start, token.ptr - start + 1);

	/* 8. space */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	/*
	 * 9. ACL and COMMENT tags are "composite"
	 *
	 * 4837; 0 0 ACL - SCHEMA public postgres
	 * 4838; 0 0 COMMENT - SCHEMA topology dim
	 * 4839; 0 0 COMMENT - EXTENSION intarray
	 * 4840; 0 0 COMMENT - EXTENSION postgis
	 */
	if (item->desc == ARCHIVE_TAG_ACL ||
		item->desc == ARCHIVE_TAG_COMMENT)
	{
		item->isCompositeTag = true;

		/* backwards compatibility */
		if (item->desc == ARCHIVE_TAG_ACL)
		{
			item->tagKind = ARCHIVE_TAG_KIND_ACL;
		}
		else if (item->desc == ARCHIVE_TAG_COMMENT)
		{
			item->tagKind = ARCHIVE_TAG_KIND_COMMENT;
		}

		/* ignore errors, that's stuff we don't support yet (no need to) */
		(void) parse_archive_acl_or_comment(token.ptr, item);
	}
	else
	{
		/* 10. restore list name */
		size_t len = strlen(token.ptr) + 1;
		item->restoreListName = (char *) calloc(len, sizeof(char));

		if (item->restoreListName == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(item->restoreListName, token.ptr, len);
	}

	return true;
}


/*
 * tokenize_archive_list_entry returns tokens from pg_restore catalog list
 * lines.
 */
bool
tokenize_archive_list_entry(ArchiveToken *token)
{
	char *line = token->ptr;

	if (line == NULL)
	{
		log_error("BUG: tokenize_archive_list_entry called with NULL line");
		return false;
	}

	if (*line == '\0')
	{
		token->type = ARCHIVE_TOKEN_EOL;
		return true;
	}

	if (*line == ';')
	{
		token->type = ARCHIVE_TOKEN_SEMICOLON;
		token->ptr = (char *) line + 1;

		return true;
	}

	if (*line == '-')
	{
		token->type = ARCHIVE_TOKEN_DASH;
		token->ptr = (char *) line + 1;

		return true;
	}

	if (*line == ' ')
	{
		char *ptr = line;

		/* advance ptr as long as *ptr is a space */
		for (; ptr != NULL && *ptr == ' '; ptr++)
		{ }

		token->type = ARCHIVE_TOKEN_SPACE;
		token->ptr = ptr;

		return true;
	}

	if (isdigit(*line))
	{
		char *ptr = line;

		/* advance ptr as long as *ptr is a digit */
		for (; ptr != NULL && isdigit(*ptr); ptr++)
		{ }

		if (ptr == NULL)
		{
			log_error("Failed to tokenize Archive Item line: %s", line);
			return false;
		}

		int len = ptr - line + 1;
		size_t size = len + 1;
		char *buf = (char *) calloc(size, sizeof(char));

		if (buf == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		strlcpy(buf, line, len);

		if (!stringToUInt32(buf, &(token->oid)))
		{
			log_error("Failed to parse OID \"%s\" from pg_restore --list",
					  buf);
			return false;
		}

		token->type = ARCHIVE_TOKEN_OID;
		token->ptr = ptr;

		return true;
	}

	/* is it an Archive Description then? */
	for (int i = 0; pgRestoreDescriptionArray[i].len != 0; i++)
	{
		if (strncmp(line,
					pgRestoreDescriptionArray[i].str,
					pgRestoreDescriptionArray[i].len) == 0)
		{
			token->type = ARCHIVE_TOKEN_DESC;
			token->desc = pgRestoreDescriptionArray[i].desc;
			token->ptr = (char *) line + pgRestoreDescriptionArray[i].len;

			return true;
		}
	}

	token->type = ARCHIVE_TOKEN_UNKNOWN;
	return true;
}


/*
 * parse_archive_acl_or_comment parses the ACL or COMMENT entry of the
 * pg_restore archive catalog TOC.
 *
 * 4837; 0 0 ACL - SCHEMA public postgres
 * 4838; 0 0 COMMENT - SCHEMA topology dim
 * 4839; 0 0 COMMENT - EXTENSION intarray
 * 4840; 0 0 COMMENT - EXTENSION postgis
 *
 * Here the - is for the namespace, which doesn't apply, and then the TAG is
 * composite: TYPE name; where it usually is just the object name.
 *
 * The ptr argument is positioned after the space following either the ACL or
 * COMMENT tag.
 */
bool
parse_archive_acl_or_comment(char *ptr, ArchiveContentItem *item)
{
	log_trace("parse_archive_acl_or_comment: \"%s\"", ptr);

	ArchiveToken token = { .ptr = ptr };

	/*
	 * At the moment we only support filtering ACLs and COMMENTS for SCHEMA and
	 * EXTENSION objects, see --skip-extensions. So first, we skip the
	 * namespace, which in our case would always be a dash.
	 */
	ArchiveTokenType list[] = {
		ARCHIVE_TOKEN_DASH,
		ARCHIVE_TOKEN_SPACE
	};

	int count = sizeof(list) / sizeof(list[0]);

	for (int i = 0; i < count; i++)
	{
		if (!tokenize_archive_list_entry(&token) || token.type != list[i])
		{
			log_trace("Unsupported ACL or COMMENT (namespace is not -): \"%s\"",
					  ptr);
			return false;
		}
	}

	/*
	 * Now parse the composite item description tag.
	 */
	if (!tokenize_archive_list_entry(&token) ||
		token.type != ARCHIVE_TOKEN_DESC)
	{
		log_error("Failed to parse Archive TOC comment or acl: %s", ptr);
		return false;
	}

	if (token.desc == ARCHIVE_TAG_SCHEMA)
	{
		/* skip the space after the SCHEMA tag */
		char *nsp_rol_name = token.ptr + 1;
		int len = strlen(nsp_rol_name);

		/* add 2 bytes for the prefix: "- " */
		int bytes = len + 1 + 2;

		item->restoreListName = (char *) calloc(bytes, sizeof(char));

		if (item->restoreListName == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		/* a schema pg_restore list name is "- nspname rolname" */
		sformat(item->restoreListName, bytes, "- %s", nsp_rol_name);
		item->tagType = ARCHIVE_TAG_TYPE_SCHEMA;
	}
	else if (token.desc == ARCHIVE_TAG_EXTENSION)
	{
		/*
		 * skip the space after the SCHEMA tag: use token.ptr + 1
		 *
		 * The extension name is following by a space, even though there is no
		 * owner to follow that space. We don't want that space at the end of
		 * the extension's name.
		 */
		char *extname = token.ptr + 1;
		char *space = strchr(extname, ' ');

		/* if the file has been pre-processed and trailing spaces removed... */
		if (space != NULL)
		{
			*space = '\0';
		}

		int len = strlen(extname);
		int bytes = len + 1;

		item->restoreListName = (char *) calloc(bytes, sizeof(char));

		if (item->restoreListName == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		/* an extension's pg_restore list name is just its name */
		sformat(item->restoreListName, bytes, "%s", extname);
		item->tagType = ARCHIVE_TAG_TYPE_EXTENSION;
	}
	else
	{
		log_debug("Failed to parse %s \"%s\": not supported yet",
				  item->description,
				  ptr);

		item->tagType = ARCHIVE_TAG_TYPE_OTHER;

		return false;
	}

	log_trace("parse_archive_acl_or_comment: %s [%s]",
			  item->description,
			  item->restoreListName);

	return true;
}
