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
#include "file_utils.h"
#include "filtering.h"
#include "log.h"
#include "parsing_utils.h"
#include "pgsql.h"
#include "schema.h"

#define PG_CMD_MAX_ARG 128
#define PG_VERSION_STRING_MAX 12

/*
 * PGCOPYDB_DDL_EVENT_TRIGGER_SCHEMA, PGCOPYDB_DDL_EVENT_TRIGGER_FUNCTION and
 * PGCOPYDB_DDL_EVENT_TRIGGER_NAME name the schema, plpgsql function and
 * event trigger installed by `pgcopydb stream event-trigger`.
 *
 * These are source-only tooling and must never reach the target database:
 * dump_restore.c's copydb_write_restore_list_hook always skips the matching
 * SCHEMA, FUNCTION and EVENT TRIGGER archive entries by name, the same way
 * it always skips DATABASE entries, regardless of the user's own filters.
 *
 * All three need an explicit skip, for two different reasons:
 *
 *  - "pgcopydb" is already a reserved schema name that schema_list_schemas
 *    (sql_list_schemas) always excludes from pgcopydb's own catalog. If the
 *    SCHEMA entry itself were not also skipped here, pg_restore would still
 *    try to create it (pg_dump has no reason to omit it), but pgcopydb's
 *    restore-list preparation would fail to resolve its OID -- the catalog
 *    never learned about it. This is exactly the bug this feature hit
 *    during development.
 *  - the event trigger is a database-global object, not schema-owned, so
 *    putting the function in a dedicated "pgcopydb" schema does not by
 *    itself keep the trigger out of a dump -- pg_dump has no
 *    --exclude-event-trigger flag -- hence the name-based skip here
 *    instead of relying on schema exclusion.
 */
#define PGCOPYDB_DDL_EVENT_TRIGGER_SCHEMA "pgcopydb"
#define PGCOPYDB_DDL_EVENT_TRIGGER_FUNCTION "pgcopydb.emit_ddl_message"
#define PGCOPYDB_DDL_EVENT_TRIGGER_NAME "pgcopydb_emit_ddl_message"

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

typedef enum
{
	ARCHIVE_TAG_UNKNOWN = 0,
	ARCHIVE_TAG_ACCESS_METHOD,
	ARCHIVE_TAG_ACL,
	ARCHIVE_TAG_AGGREGATE,
	ARCHIVE_TAG_ATTRDEF,
	ARCHIVE_TAG_BLOB_DATA,
	ARCHIVE_TAG_BLOB,
	ARCHIVE_TAG_CAST,
	ARCHIVE_TAG_CHECK_CONSTRAINT,
	ARCHIVE_TAG_COLLATION,
	ARCHIVE_TAG_COLUMN,
	ARCHIVE_TAG_COMMENT,
	ARCHIVE_TAG_CONSTRAINT,
	ARCHIVE_TAG_CONVERSION,
	ARCHIVE_TAG_DATABASE,
	ARCHIVE_TAG_DEFAULT_ACL,
	ARCHIVE_TAG_DEFAULT,
	ARCHIVE_TAG_DOMAIN,
	ARCHIVE_TAG_DUMMY_TYPE,
	ARCHIVE_TAG_EVENT_TRIGGER,
	ARCHIVE_TAG_EXTENSION,
	ARCHIVE_TAG_FK_CONSTRAINT,
	ARCHIVE_TAG_FOREIGN_DATA_WRAPPER,
	ARCHIVE_TAG_FOREIGN_SERVER,
	ARCHIVE_TAG_FOREIGN_TABLE,
	ARCHIVE_TAG_FUNCTION,
	ARCHIVE_TAG_INDEX_ATTACH,
	ARCHIVE_TAG_INDEX,
	ARCHIVE_TAG_LANGUAGE,
	ARCHIVE_TAG_LARGE_OBJECT,
	ARCHIVE_TAG_MATERIALIZED_VIEW,
	ARCHIVE_TAG_OPERATOR_CLASS,
	ARCHIVE_TAG_OPERATOR_FAMILY,
	ARCHIVE_TAG_OPERATOR,
	ARCHIVE_TAG_POLICY,
	ARCHIVE_TAG_PROCEDURAL_LANGUAGE,
	ARCHIVE_TAG_PROCEDURE,
	ARCHIVE_TAG_PUBLICATION_TABLES_IN_SCHEMA,
	ARCHIVE_TAG_PUBLICATION_TABLE,
	ARCHIVE_TAG_PUBLICATION,
	ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW,
	ARCHIVE_TAG_ROW_SECURITY,
	ARCHIVE_TAG_RULE,
	ARCHIVE_TAG_SCHEMA,
	ARCHIVE_TAG_SEQUENCE_OWNED_BY,
	ARCHIVE_TAG_SEQUENCE_SET,
	ARCHIVE_TAG_SEQUENCE,
	ARCHIVE_TAG_SERVER,
	ARCHIVE_TAG_SHELL_TYPE,
	ARCHIVE_TAG_STATISTICS,
	ARCHIVE_TAG_SUBSCRIPTION,
	ARCHIVE_TAG_TABLE_ATTACH,
	ARCHIVE_TAG_TABLE_DATA,
	ARCHIVE_TAG_TABLE,
	ARCHIVE_TAG_TEXT_SEARCH_CONFIGURATION,
	ARCHIVE_TAG_TEXT_SEARCH_DICTIONARY,
	ARCHIVE_TAG_TEXT_SEARCH_PARSER,
	ARCHIVE_TAG_TEXT_SEARCH_TEMPLATE,
	ARCHIVE_TAG_TRANSFORM,
	ARCHIVE_TAG_TRIGGER,
	ARCHIVE_TAG_TYPE,
	ARCHIVE_TAG_USER_MAPPING,
	ARCHIVE_TAG_VIEW
} ArchiveItemDesc;

typedef enum
{
	ARCHIVE_TAG_KIND_UNKNOWN = 0,
	ARCHIVE_TAG_KIND_ACL,
	ARCHIVE_TAG_KIND_COMMENT
} ArchiveCompositeTagKind;


typedef enum
{
	ARCHIVE_TAG_TYPE_UNKNOWN = 0,
	ARCHIVE_TAG_TYPE_SCHEMA,
	ARCHIVE_TAG_TYPE_EXTENSION,
	ARCHIVE_TAG_TYPE_OTHER
} ArchiveCompositeTagType;


/*
 * Archive List tokenizer.
 */
typedef enum
{
	ARCHIVE_TOKEN_UNKNOWN = 0,
	ARCHIVE_TOKEN_SEMICOLON,
	ARCHIVE_TOKEN_SPACE,
	ARCHIVE_TOKEN_OID,
	ARCHIVE_TOKEN_DESC,
	ARCHIVE_TOKEN_DASH,
	ARCHIVE_TOKEN_EOL
} ArchiveTokenType;


typedef struct ArchiveToken
{
	char *ptr;
	ArchiveTokenType type;
	ArchiveItemDesc desc;

	/* we also parse/prepare some of the values */
	uint32_t oid;
} ArchiveToken;

/*
 * The Postgres pg_restore tool allows listing the contents of an archive. The
 * archive content is formatted the following way:
 *
 * ahprintf(AH, "%d; %u %u %s %s %s %s\n", te->dumpId,
 *          te->catalogId.tableoid, te->catalogId.oid,
 *          te->desc, sanitized_schema, sanitized_name,
 *          sanitized_owner);
 *
 * We need to parse the list of SQL objects to restore in the post-data step
 * and filter out the indexes and constraints that we already created in our
 * parallel step.
 *
 * We match the items we have restored already with the items in the archive
 * contents by their OID on the source database, so that's the most important
 * field we need.
 */
typedef struct ArchiveContentItem
{
	int dumpId;
	uint32_t catalogOid;
	uint32_t objectOid;

	ArchiveItemDesc desc;

	char *description;          /* malloc'ed area */
	char *restoreListName;      /* malloc'ed area */

	bool isCompositeTag;
	ArchiveCompositeTagKind tagKind;
	ArchiveCompositeTagType tagType;
} ArchiveContentItem;


typedef struct ArchiveContentArray
{
	int count;
	ArchiveContentItem *array;  /* malloc'ed area */
} ArchiveContentArray;

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

/* iterate over a file one line at a time */
typedef bool (ArchiveTOCFun)(void *context, ArchiveContentItem *item);

bool archive_iter_toc(const char *filename,
					  void *context,
					  ArchiveTOCFun *callback);

typedef struct ArchiveTOCIterator
{
	const char *filename;
	FileLinesIterator *fileIterator;
	ArchiveContentItem *item;
} ArchiveTOCIterator;

bool archive_iter_toc_init(ArchiveTOCIterator *iter);
bool archive_iter_toc_next(ArchiveTOCIterator *iter);
bool archive_iter_toc_finish(ArchiveTOCIterator *iter);

bool parse_archive_acl_or_comment(char *ptr, ArchiveContentItem *item);
bool parse_archive_acl_or_comment_relation(const char *nspname, int nspnameLen,
										   ArchiveToken *token,
										   ArchiveContentItem *item);
bool parse_archive_identifier(ArchiveToken *token, char *name, size_t size);

bool parse_archive_list_entry(ArchiveContentItem *item, const char *line);
bool tokenize_archive_list_entry(ArchiveToken *token);

#endif /* PGCMD_H */
