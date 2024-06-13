/*
 * src/bin/pgcopydb/archive_iterator.c
 *   Implementations of a file iterator for reading new lıne seperated files
 */

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "file_iterator.h"
#include "file_utils.h"
#include "log.h"
#include "archive_iterator.h"
#include "archive.h"
#include "string_utils.h"

/*
 * A struct to hold the state of the archive iterator. This
 * iterator iterates an archive item-by-item.
 */
typedef struct ArchiveIterator
{
	FileIterator *file_iterator;
	ArchiveContentItem *item;
} ArchiveIterator;

/*
 * Create a new iterator from the file name
 */
ArchiveIterator *
archive_iterator_from(const char *filename)
{
	ArchiveIterator *iterator = (ArchiveIterator *) calloc(1, sizeof(ArchiveIterator));
	if (iterator == NULL)
	{
		log_error("Failed to allocate memory for ArchiveIterator");
		return NULL;
	}

	iterator->file_iterator = file_iterator_from(filename);
	if (iterator->file_iterator == NULL)
	{
		log_error("Failed to allocate memory for FileIterator");
		archive_iterator_destroy(iterator);
		return NULL;
	}

	iterator->item = calloc(1, sizeof(ArchiveContentItem));
	if (iterator->item == NULL)
	{
		log_error("Failed to allocate memory for ArchiveContentItem");
		archive_iterator_destroy(iterator);
		return NULL;
	}

	return iterator;
}


/*
 * Destroy the iterator and free resources
 */
void
archive_iterator_destroy(ArchiveIterator *iterator)
{
	if (iterator->file_iterator)
	{
		file_iterator_destroy(iterator->file_iterator);
	}
}


/* remember to skip the \0 at the end of the static string here */
#define INSERT_MAPPING(d, s) { d, sizeof(s) - 1, s }

/*
 * List manually processed from describeDumpableObject in
 * postgres/src/bin/pg_dump/pg_dump_sort.c
 */
static struct ArchiveItemDescMapping pgRestoreDescriptionArray[] = {
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

	/*
	 * MATERIALIZED VIEW DATA should come before MATERIALIZED VIEW, otherwise
	 * the strncmp will match the first part of the string and misidentify the
	 * MATERIALIZED VIEW DATA as MATERIALIZED VIEW.
	 */
	INSERT_MAPPING(ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW, "MATERIALIZED VIEW DATA"),
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
 * tokenize_archive_list_entry returns tokens from pg_restore catalog list
 * lines.
 */
static bool
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
static bool
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
static bool
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
 * Get the next item from the archive.
 *
 * The memory for the item is allocated by this function and will be freed by it too.
 */
bool
archive_iterator_next(ArchiveIterator *iterator, ArchiveContentItem **item)
{
	*item = NULL;
	char *line = NULL;
	bool should_skip = true;
	while (should_skip)
	{
		if (!file_iterator_next(iterator->file_iterator, &line))
		{
			log_error("Failed to read line from the archive file");
			return false;
		}
		if (line == NULL)
		{
			/* no more lines, stop */
			return true;
		}

		/* skip empty lines and lines that start with a semi-colon (comment) */
		should_skip = *line == '\0' || *line == ';';
	}

	/* prepare the item by resetting to zero */
	bzero(iterator->item, sizeof(ArchiveContentItem));
	*item = iterator->item;
	if (!parse_archive_list_entry(*item, line))
	{
		log_error("Failed to parse line %ld of \"%s\", "
				  "see above for details",
				  file_iterator_get_line_number(iterator->file_iterator),
				  file_iterator_get_file_name(iterator->file_iterator));
		return false;
	}

	return true;
}
