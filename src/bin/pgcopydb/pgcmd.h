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
 * already-executed result of.
 */

/*
 * ArchiveFunctionSignature is set when objectType (ArchiveContentItem's
 * own field, below) is FUNCTION, PROCEDURE or AGGREGATE, OR when desc
 * itself is one of those three for a plain, non-ACL/COMMENT entry: the
 * raw text pg_dump wrote between name's own parentheses, and that same
 * text split on its top-level commas (a nested pair from a type's own
 * typmod, e.g. "numeric(10,2)", isn't a boundary) into one string per
 * argument -- see archive_toc_grammar.y's own `args`/`arglist`. Each
 * argument is kept verbatim, but the two cases differ in content, not
 * shape: the ACL/COMMENT case uses pg_get_function_identity_arguments()
 * ("[argmode] [argname] argtype", what dumpComment/dumpACL use), while
 * the plain-entry case uses format_function_signature()/
 * format_aggregate_signature() (bare "argtype" only, no argmode/argname
 * -- what the entry's own tag uses for overload resolution, pg_dump.c).
 * Either way, argtype is a full Postgres type-name expression, not a
 * lexable token, so we go no further than this (see the file's own
 * naming note). A zero-argument aggregate's literal "(*)"
 * (format_function_arguments' own special case, not a real argument)
 * is argsCount == 0, argsText == "*".
 */
typedef struct ArchiveFunctionSignature
{
	char *argsText;              /* malloc'ed area */
	char **args;                 /* malloc'ed array of malloc'ed strings */
	int argsCount;
} ArchiveFunctionSignature;

/*
 * ArchiveOperatorArgs is set when objectType is OPERATOR: the left and
 * right operand type, or absent (hasLeftArg/hasRightArg false, as
 * pg_dump itself writes "NONE" for that side) for a prefix or postfix
 * operator that doesn't have it. OPERATOR's own plain, non-ACL/COMMENT
 * entry has no equivalent: its tag is the bare operator symbol only
 * (dumpOpr's own ArchiveEntry, pg_dump.c), no operand types at all.
 */
typedef struct ArchiveOperatorArgs
{
	bool hasLeftArg;
	char *leftArg;                /* malloc'ed area */
	bool hasRightArg;
	char *rightArg;               /* malloc'ed area */
} ArchiveOperatorArgs;

/*
 * ArchiveCastTypes is set when desc == ARCHIVE_TAG_CAST: the two halves
 * of dumpCast's own "(sourcetype AS targettype)" (pg_dump.c) -- full
 * Postgres type-name expressions, same "not a lexable token" rule as
 * ArchiveFunctionSignature's own argsText.
 */
typedef struct ArchiveCastTypes
{
	char *sourceType;             /* malloc'ed area */
	char *targetType;             /* malloc'ed area */
} ArchiveCastTypes;

/*
 * ArchiveUserMapping is set when desc == ARCHIVE_TAG_USER_MAPPING: the
 * two halves of dumpUserMappings' own "USER MAPPING usename SERVER
 * servername" (pg_dump.c) -- plain identifiers, never escaped by
 * pg_dump (see archive_toc_lexer.l's own note on this format never
 * quoting the owner field either). USER_MAPPING has no ACL/COMMENT
 * form: neither GRANT nor COMMENT ON applies to a user mapping.
 */
typedef struct ArchiveUserMapping
{
	char user[PG_NAMEDATALEN];
	char server[PG_NAMEDATALEN];
} ArchiveUserMapping;

/*
 * ArchiveTransform is set when desc == ARCHIVE_TAG_TRANSFORM (either as
 * the plain entry, or as objectType for a COMMENT ON TRANSFORM --
 * dumpTransform does call dumpComment, pg_dump.c): the two halves of
 * pg_dump's own "FOR typename LANGUAGE langname". See
 * https://www.postgresql.org/docs/current/sql-createtransform.html --
 * a TRANSFORM tells a procedural language (PL/Python, PL/Perl, ...) how
 * to convert a given SQL type to and from that language's own native
 * representation. type is a full Postgres type-name expression, same
 * "go no further" rule as above; language is a plain identifier.
 */
typedef struct ArchiveTransform
{
	char *type;                   /* malloc'ed area */
	char language[PG_NAMEDATALEN];
} ArchiveTransform;

/*
 * ArchiveOpClassFamily is set when objectType is OPERATOR_CLASS or
 * OPERATOR_FAMILY (ACL/COMMENT only -- a plain top-level OPERATOR
 * CLASS/FAMILY entry is just the bare name, no "USING amname": that
 * text is only ever built for binary_upgrade_extension_member's own
 * tag, never for the ArchiveEntry itself, see dumpOpclass/dumpOpfamily
 * in pg_dump.c): the index access method named after "USING" in e.g.
 * "fixture_int_ops USING btree".
 */
typedef struct ArchiveOpClassFamily
{
	char accessMethod[PG_NAMEDATALEN];
} ArchiveOpClassFamily;


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
	 * Structured fields, split out of the entry's own remainder text by
	 * archive_toc_grammar.y's own productions. Empty ("") rather than NULL
	 * when not applicable to this entry's tag, so callers can always
	 * safely read them.
	 */
	bool hasNamespace;              /* false when the namespace field is "-" */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];   /* owning relation/publication, relation-shaped tags only */
	char name[PG_NAMEDATALEN];      /* the object's own name (or column name for DEFAULT) */
	char owner[PG_NAMEDATALEN];     /* role name; may be empty, never NULL */

	/*
	 * Set when isCompositeTag is true (an ACL or COMMENT entry):
	 * objectType is the SQL object type this ACL/COMMENT is attached to
	 * (e.g. ARCHIVE_TAG_COLUMN, ARCHIVE_TAG_TRIGGER, ...; see the naming
	 * note above), and hasNamespace/nspname/relname/name/owner above
	 * describe its own remainder, parsed per its own object type, not the
	 * outer ACL/COMMENT tag.
	 */
	ArchiveItemDesc objectType;
	char *objectTypeText;        /* malloc'ed area; objectType's own raw
	                              * keyword text (e.g. "COLUMN", "TRIGGER"),
	                              * the same way description is the outer
	                              * tag's */

	/*
	 * Set when isCompositeTag is true (an ACL or COMMENT entry): the full
	 * original remainder text, object type included, exactly as
	 * pg_restore --list would print it (e.g. "toc_fixture COLUMN
	 * parent.name postgres"). Distinct from restoreListName on purpose:
	 * that field deliberately holds only the *relation's* own name for
	 * filtering, not a faithful reproduction of this entry's own line --
	 * useListRemainder is the latter, for callers (e.g. --use-list) that
	 * need to write the line back out.
	 */
	char *useListRemainder;      /* malloc'ed area */

	/*
	 * The "overlapping fields": each of these applies both to a tag's
	 * own plain, top-level entry (desc) and (where pg_dump supports a
	 * COMMENT on it) its ACL/COMMENT-wrapped one (objectType) -- see
	 * archive_toc_grammar.y's own TOP_CAST/NESTED_CAST and similar
	 * pairs. name/restoreListName above keep their existing, narrower
	 * contract regardless; these are purely additional detail for the
	 * one object type each applies to, see each struct's own comment.
	 */
	ArchiveFunctionSignature signature;
	ArchiveOperatorArgs operatorArgs;
	ArchiveCastTypes cast;
	ArchiveUserMapping userMapping;
	ArchiveTransform transform;
	ArchiveOpClassFamily opClassFamily;

	/*
	 * Set when desc == ARCHIVE_TAG_DEFAULT_ACL: which of the 6 literal
	 * object-type words dumpDefaultACL's own switch can produce
	 * ("TABLES", "SEQUENCES", "FUNCTIONS", "TYPES", "SCHEMAS", "LARGE
	 * OBJECTS"), following "DEFAULT PRIVILEGES FOR" in its tag. This is
	 * itself a top-level tag distinct from the generic ACL one -- it has
	 * no further ACL/COMMENT-wrapped form, so unlike the structs above it
	 * stays a single flat field.
	 */
	char defaultAclObjectType[PG_NAMEDATALEN];
} ArchiveContentItem;

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
bool archive_toc_parse_line(const char *line, ArchiveContentItem *item);

#endif /* PGCMD_H */
