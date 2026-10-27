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

/*
 * ArchiveTocShape classifies how an entry's remainder (everything after
 * the TAG token) is laid out, so the parser knows how many fields to split
 * it into and what each one means. See tests/toc-coverage/ for real
 * captured examples of each shape.
 */
typedef enum
{
	ARCHIVE_SHAPE_UNKNOWN = 0,

	/*
	 * "<namespace> <name> <owner>" -- namespace is "-" for a global
	 * (non-schema-owned) object. Covers both: SCHEMA's own "- name owner"
	 * (name is the schema's own name) and schema-owned entries like
	 * TABLE, FUNCTION, SEQUENCE, VIEW, ... ("schema name owner").
	 */
	ARCHIVE_SHAPE_SIMPLE,

	/*
	 * "<namespace> <relname> <name> <owner>" -- an object that belongs to
	 * a relation (or, for the two PUBLICATION tags below, a publication):
	 * CONSTRAINT, FK_CONSTRAINT, TRIGGER, RULE, POLICY, DEFAULT (name is
	 * the column name here, not an object with its own identity),
	 * PUBLICATION_TABLE (relname is the publication name, name is the
	 * published table), PUBLICATION_TABLES_IN_SCHEMA (relname is the
	 * publication name, name is the published schema).
	 */
	ARCHIVE_SHAPE_RELATION,

	/*
	 * ACL or COMMENT: "<namespace> <objectType> ...", recursing into one
	 * of the shapes above (picked by objectType, not by the outer
	 * ACL/COMMENT tag) for the remainder after it.
	 */
	ARCHIVE_SHAPE_ACL_OR_COMMENT,

	/*
	 * Genuinely irregular one-off shapes, not worth generalizing: CAST
	 * ("CAST (domain AS type)"), USER_MAPPING ("USER MAPPING user SERVER
	 * server", repeating its own tag name), DEFAULT_ACL ("DEFAULT
	 * PRIVILEGES FOR TABLES"). The whole remainder up to the trailing
	 * owner (if any) is kept as-is in `name`.
	 */
	ARCHIVE_SHAPE_OPAQUE
} ArchiveTocShape;


/*
 * Naming note, for anyone cross-referencing with pg_dump's own source
 * (pg_backup_archiver.h's TocEntry, pg_dump.c's dumpACL/dumpCommentExtended):
 * pg_dump's te->desc is the category string ("ACL", "TABLE", "COMMENT", ...)
 * -- what WE call desc/description and the ArchiveItemDesc enum below --
 * while pg_dump's te->tag is the object's OWN name, which is what WE split
 * into relname/name below (pg_dump has no separate field for it, it's one
 * composite string). Our nspname and owner do match pg_dump's own naming
 * (nspname is dumpACL's own parameter name for the same thing; owner is
 * te->owner verbatim).
 *
 * What we call objectType (the nested keyword inside an ACL/COMMENT's tag,
 * e.g. TRIGGER or COLUMN) is named after backend/nodes/parsenodes.h's own
 * ObjectType enum and its objtype field in CommentStmt, SecLabelStmt and
 * GrantStmt -- the parse-tree representation of exactly the statements
 * (COMMENT ON, SECURITY LABEL, GRANT/REVOKE) these archive entries are the
 * already-executed result of. "shape" below has no such precedent -- it's
 * this parser's own invention, not something pg_dump or the backend already
 * names and we failed to reuse.
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

	/*
	 * Structured fields, split out of restoreListName according to
	 * ArchiveTocShape -- see archive_toc_parser.c. Empty ("") rather than
	 * NULL when not applicable to this entry's shape, so callers can
	 * always safely read them.
	 */
	ArchiveTocShape shape;

	bool hasNamespace;              /* false when the namespace field is "-" */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];   /* owning relation/publication, SHAPE_RELATION only */
	char name[PG_NAMEDATALEN];      /* the object's own name (or column name for DEFAULT) */
	char owner[PG_NAMEDATALEN];     /* role name; may be empty, never NULL */

	/*
	 * Set when shape == ARCHIVE_SHAPE_ACL_OR_COMMENT: objectType is the
	 * SQL object type this ACL/COMMENT is attached to (e.g.
	 * ARCHIVE_TAG_COLUMN, ARCHIVE_TAG_TRIGGER, ...; see the naming note
	 * above), and hasNamespace/nspname/relname/name/owner above describe
	 * its own remainder, parsed per its own shape, not the outer
	 * ACL/COMMENT tag.
	 */
	ArchiveItemDesc objectType;
	char *objectTypeText;        /* malloc'ed area; objectType's own raw
	                              * keyword text (e.g. "COLUMN", "TRIGGER"),
	                              * the same way description is the outer
	                              * tag's */

	/*
	 * Set when shape == ARCHIVE_SHAPE_ACL_OR_COMMENT: the full original
	 * remainder text, object type included, exactly as pg_restore --list
	 * would print it (e.g. "toc_fixture COLUMN parent.name postgres").
	 * Distinct from restoreListName on purpose: that field deliberately
	 * holds only the *relation's* own name for filtering (see
	 * finishRelationAclOrComment in archive_toc_parser.c), not a faithful
	 * reproduction of this entry's own line -- useListRemainder is the
	 * latter, for callers (e.g. --use-list) that need to write the line
	 * back out.
	 */
	char *useListRemainder;      /* malloc'ed area */

	/*
	 * Set when objectType (above) is FUNCTION, PROCEDURE or AGGREGATE:
	 * the raw text pg_dump wrote between name's own parentheses, and
	 * that same text split on its top-level commas (a nested pair from a
	 * type's own typmod, e.g. "numeric(10,2)", isn't a boundary) into one
	 * string per argument -- see archive_toc_parser.c's splitFunctionArgs.
	 * Each argument is kept verbatim, "[argmode] [argname] argtype" as
	 * pg_get_function_identity_arguments() printed it (what pg_dump's own
	 * dumpComment/dumpACL use): argtype is a full Postgres type-name
	 * expression, not a lexable token, so we go no further than this
	 * (see the file's own naming note). A zero-argument aggregate's
	 * literal "(*)" (format_function_arguments' own special case, not a
	 * real argument) is signatureArgsCount == 0, signatureArgsText == "*".
	 */
	char *signatureArgsText;     /* malloc'ed area */
	char **signatureArgs;        /* malloc'ed array of malloc'ed strings */
	int signatureArgsCount;

	/*
	 * Set when objectType (above) is OPERATOR: the left and right operand
	 * type, or absent (hasLeftArg/hasRightArg false, as pg_dump itself
	 * writes "NONE" for that side) for a prefix or postfix operator that
	 * doesn't have it.
	 */
	bool operatorHasLeftArg;
	char *operatorLeftArg;       /* malloc'ed area */
	bool operatorHasRightArg;
	char *operatorRightArg;      /* malloc'ed area */
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

/* archive_toc_parser.c: see the file header */
ArchiveTocShape archive_toc_shape_for_desc(ArchiveItemDesc desc);
bool archive_toc_parse_line(const char *line, ArchiveContentItem *item);

#endif /* PGCMD_H */
