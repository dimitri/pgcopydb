/*
 * src/bin/pgcopydb/pgcmd.h
 *   API for running PostgreSQL commands such as pg_dump and pg_restore.
 *
 */

#ifndef PGCMD_H
#define PGCMD_H

#include <limits.h>
#include <stdbool.h>
#include <string.h>

#include "postgres_fe.h"

#include "defaults.h"
#include "archive.h"
#include "file_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "schema.h"

#define PG_CMD_MAX_ARG 128
#define PG_VERSION_STRING_MAX 12

typedef struct PostgresPaths
{
	char psql[MAXPGPATH];
	char pg_config[MAXPGPATH];
	char pg_dump[MAXPGPATH];
	char pg_dumpall[MAXPGPATH];
	char pg_restore[MAXPGPATH];
	char vacuumdb[MAXPGPATH];
	char pg_version[PG_VERSION_STRING_MAX];
} PostgresPaths;

/* specify section of a dump: pre-data, post-data, data, schema */
typedef enum
{
	PG_DUMP_SECTION_ALL = 0,
	PG_DUMP_SECTION_SCHEMA,
	PG_DUMP_SECTION_PRE_DATA,
	PG_DUMP_SECTION_POST_DATA,
	PG_DUMP_SECTION_DATA,
	PG_DUMP_SECTION_ROLES       /* pg_dumpall --roles-only */
} PostgresDumpSection;


/*
 * Enumeration representing the different section options of
 * a Postgres restore operation.
 */
typedef enum
{
	PG_RESTORE_SECTION_PRE_DATA = 0,
	PG_RESTORE_SECTION_POST_DATA,
} PostgresRestoreSection;

/*
 * Convert PostgresDumpSection to string.
 */
static inline const char *
postgresRestoreSectionToString(PostgresRestoreSection section)
{
	switch (section)
	{
		case PG_RESTORE_SECTION_PRE_DATA:
		{
			return "pre-data";
		}

		case PG_RESTORE_SECTION_POST_DATA:
		{
			return "post-data";
		}

		default:
		{
			log_error("unknown PostgresRestoreSection value %d", section);
			return NULL;
		}
	}
}


typedef struct RestoreOptions
{
	bool dropIfExists;
	bool noOwner;
	bool noComments;
	bool noACL;
	bool noTableSpaces;
	int jobs;
	PostgresRestoreSection section;
} RestoreOptions;

bool psql_version(PostgresPaths *pgPaths);

void find_pg_commands(PostgresPaths *pgPaths);
void set_postgres_commands(PostgresPaths *pgPaths);
bool set_psql_from_PG_CONFIG(PostgresPaths *pgPaths);
bool set_psql_from_config_bindir(PostgresPaths *pgPaths, const char *pg_config);
bool set_psql_from_pg_config(PostgresPaths *pgPaths);

bool pg_dump_db(PostgresPaths *pgPaths,
				ConnStrings *connStrings,
				const char *snapshot,
				SourceFilters *filters,
				DatabaseCatalog *filtersDB,
				const char *filename);

bool pg_vacuumdb_analyze_only(PostgresPaths *pgPaths, ConnStrings *connStrings, int jobs);

bool pg_dumpall_roles(PostgresPaths *pgPaths,
					  ConnStrings *connStrings,
					  const char *filename,
					  bool noRolesPasswords);

bool pg_restore_roles(PostgresPaths *pgPaths,
					  const char *pguri,
					  const char *filename);

bool pg_copy_roles(PostgresPaths *pgPaths,
				   ConnStrings *connStrings,
				   const char *filename,
				   bool noRolesPasswords);

bool pg_restore_db(PostgresPaths *pgPaths,
				   ConnStrings *connStrings,
				   SourceFilters *filters,
				   const char *dumpFilename,
				   const char *listFilename,
				   RestoreOptions options);

bool pg_restore_list(PostgresPaths *pgPaths,
					 const char *restoreFilename,
					 const char *listFilename);

bool parse_archive_acl_or_comment(char *ptr, ArchiveContentItem *item);

#endif /* PGCMD_H */
