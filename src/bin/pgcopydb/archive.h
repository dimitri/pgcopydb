/*
 * src/bin/pgcopydb/archive.h
 *   API for running PostgreSQL commands such as pg_dump and pg_restore.
 *
 */

#ifndef ARCHIVE_H
#define ARCHIVE_H

#include <limits.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "defaults.h"

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


#endif /* ARCHIVE_H */
