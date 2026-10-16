/*
 * src/bin/pgcopydb/archive_toc_parser.c
 *     Parses one pg_restore --list line into an ArchiveContentItem, using
 *     the flex-generated archive_toc_lexer.l for every TAG the format can
 *     contain -- both the outer dumpId/catalogOid/objectOid/TAG prefix and,
 *     for an ACL or COMMENT entry, the TAG2 nested inside its remainder --
 *     and a small shape table (below) for the irregular, tag-dependent
 *     rest of the line.
 *
 * This supersedes the old hand-rolled parser entirely (tokenize_archive_
 * list_entry, parse_archive_list_entry, parse_archive_acl_or_comment and
 * parse_archive_acl_or_comment_relation, formerly in pgcmd.c, now removed):
 * ACL/COMMENT composite-tag parsing is where the old declaration-order-
 * sensitive TAG matching mattered the most, since TAG2 is matched against
 * the exact same keyword set as the outer TAG, with the exact same
 * prefix-ambiguity hazards -- so it is also where replacing that matching
 * with a generated DFA pays off the most, not a part of the format worth
 * leaving on the old parser.
 */

#include <string.h>

#include "parson.h"

#include "pgcmd.h"
#include "archive_toc_lexer.h"
#include "log.h"
#include "string_utils.h"

/*
 * archiveTocShapeTable classifies every tag that can appear as the OUTER
 * (not ACL/COMMENT-wrapped) desc of a pg_restore --list line. ACL and
 * COMMENT themselves are not listed: they are handled before this table
 * is ever consulted (see archive_toc_parse_line). Tags that can only
 * appear as TAG2 inside an ACL/COMMENT entry (e.g. ARCHIVE_TAG_COLUMN)
 * are also not listed, for the same reason -- parseAclOrCommentRemainder
 * (below) owns that shape already.
 *
 * Verified against a real captured pg_restore --list fixture
 * (tests/toc-coverage/): every row marked (*) below. The rest are
 * reasoned from PrintTOCSummary's single uniform printf
 * ("%d; %u %u %s %s %s %s\n") plus pg_dump's own dumper source for that
 * tag, but not yet exercised against real output -- see the fixture's
 * header comment for why (needs a C-level handler, a genuine circular
 * type reference, or a reachable publisher this fixture doesn't set up).
 * Marking them UNKNOWN would silently degrade them to raw-text-only
 * (same as today), which is always safe; get one wrong here and the
 * field split is wrong, not a crash, so this list defaults to a
 * reasonable guess rather than UNKNOWN wherever the dumper's own
 * structure makes the guess high-confidence.
 */
typedef struct ArchiveTocShapeEntry
{
	ArchiveItemDesc desc;
	ArchiveTocShape shape;
} ArchiveTocShapeEntry;

static const ArchiveTocShapeEntry archiveTocShapeTable[] = {
	{ ARCHIVE_TAG_ACCESS_METHOD, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_AGGREGATE, ARCHIVE_SHAPE_SIMPLE },             /* (*) */
	{ ARCHIVE_TAG_ATTRDEF, ARCHIVE_SHAPE_RELATION },
	{ ARCHIVE_TAG_BLOB_DATA, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_BLOB, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_CAST, ARCHIVE_SHAPE_OPAQUE },                  /* (*) */
	{ ARCHIVE_TAG_CHECK_CONSTRAINT, ARCHIVE_SHAPE_RELATION },
	{ ARCHIVE_TAG_COLLATION, ARCHIVE_SHAPE_SIMPLE },             /* (*) */
	{ ARCHIVE_TAG_CONSTRAINT, ARCHIVE_SHAPE_RELATION },          /* (*) */
	{ ARCHIVE_TAG_CONVERSION, ARCHIVE_SHAPE_SIMPLE },            /* (*) */
	{ ARCHIVE_TAG_DATABASE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_DEFAULT_ACL, ARCHIVE_SHAPE_OPAQUE },           /* (*) */
	{ ARCHIVE_TAG_DEFAULT, ARCHIVE_SHAPE_RELATION },             /* (*) */
	{ ARCHIVE_TAG_DOMAIN, ARCHIVE_SHAPE_SIMPLE },                /* (*) */
	{ ARCHIVE_TAG_DUMMY_TYPE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_EVENT_TRIGGER, ARCHIVE_SHAPE_SIMPLE },         /* (*) */
	{ ARCHIVE_TAG_EXTENSION, ARCHIVE_SHAPE_SIMPLE },             /* (*) */
	{ ARCHIVE_TAG_FK_CONSTRAINT, ARCHIVE_SHAPE_RELATION },       /* (*) */
	{ ARCHIVE_TAG_FOREIGN_DATA_WRAPPER, ARCHIVE_SHAPE_SIMPLE },  /* (*) */
	{ ARCHIVE_TAG_FOREIGN_SERVER, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_FOREIGN_TABLE, ARCHIVE_SHAPE_SIMPLE },         /* (*) */
	{ ARCHIVE_TAG_FUNCTION, ARCHIVE_SHAPE_SIMPLE },              /* (*) */
	{ ARCHIVE_TAG_INDEX_ATTACH, ARCHIVE_SHAPE_RELATION },
	{ ARCHIVE_TAG_INDEX, ARCHIVE_SHAPE_SIMPLE },                 /* (*) */
	{ ARCHIVE_TAG_LANGUAGE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_LARGE_OBJECT, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_MATERIALIZED_VIEW, ARCHIVE_SHAPE_SIMPLE },     /* (*) */
	{ ARCHIVE_TAG_OPERATOR_CLASS, ARCHIVE_SHAPE_SIMPLE },        /* (*) */
	{ ARCHIVE_TAG_OPERATOR_FAMILY, ARCHIVE_SHAPE_SIMPLE },       /* (*) */
	{ ARCHIVE_TAG_OPERATOR, ARCHIVE_SHAPE_SIMPLE },              /* (*) */
	{ ARCHIVE_TAG_POLICY, ARCHIVE_SHAPE_RELATION },              /* (*) */
	{ ARCHIVE_TAG_PROCEDURAL_LANGUAGE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_PROCEDURE, ARCHIVE_SHAPE_SIMPLE },             /* (*) */
	{ ARCHIVE_TAG_PUBLICATION_TABLES_IN_SCHEMA, ARCHIVE_SHAPE_RELATION }, /* (*) */
	{ ARCHIVE_TAG_PUBLICATION_TABLE, ARCHIVE_SHAPE_RELATION },   /* (*) */
	{ ARCHIVE_TAG_PUBLICATION, ARCHIVE_SHAPE_SIMPLE },           /* (*) */
	{ ARCHIVE_TAG_REFRESH_MATERIALIZED_VIEW, ARCHIVE_SHAPE_SIMPLE }, /* (*) */
	{ ARCHIVE_TAG_ROW_SECURITY, ARCHIVE_SHAPE_SIMPLE },          /* (*) */
	{ ARCHIVE_TAG_RULE, ARCHIVE_SHAPE_RELATION },                /* (*) */
	{ ARCHIVE_TAG_SCHEMA, ARCHIVE_SHAPE_SIMPLE },                /* (*) */
	{ ARCHIVE_TAG_SEQUENCE_OWNED_BY, ARCHIVE_SHAPE_SIMPLE },     /* (*) */
	{ ARCHIVE_TAG_SEQUENCE_SET, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_SEQUENCE, ARCHIVE_SHAPE_SIMPLE },              /* (*) */
	{ ARCHIVE_TAG_SERVER, ARCHIVE_SHAPE_SIMPLE },                /* (*) */
	{ ARCHIVE_TAG_SHELL_TYPE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_STATISTICS, ARCHIVE_SHAPE_SIMPLE },            /* (*) */
	{ ARCHIVE_TAG_SUBSCRIPTION, ARCHIVE_SHAPE_SIMPLE },          /* (*) */
	{ ARCHIVE_TAG_TABLE_ATTACH, ARCHIVE_SHAPE_RELATION },
	{ ARCHIVE_TAG_TABLE_DATA, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_TABLE, ARCHIVE_SHAPE_SIMPLE },                 /* (*) */
	{ ARCHIVE_TAG_TEXT_SEARCH_CONFIGURATION, ARCHIVE_SHAPE_SIMPLE }, /* (*) */
	{ ARCHIVE_TAG_TEXT_SEARCH_DICTIONARY, ARCHIVE_SHAPE_SIMPLE }, /* (*) */
	{ ARCHIVE_TAG_TEXT_SEARCH_PARSER, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_TEXT_SEARCH_TEMPLATE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_TRANSFORM, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_TRIGGER, ARCHIVE_SHAPE_RELATION },              /* (*) */
	{ ARCHIVE_TAG_TYPE, ARCHIVE_SHAPE_SIMPLE },
	{ ARCHIVE_TAG_USER_MAPPING, ARCHIVE_SHAPE_OPAQUE },          /* (*) */
	{ ARCHIVE_TAG_VIEW, ARCHIVE_SHAPE_SIMPLE }                   /* (*) */
};

#define ARCHIVE_TOC_SHAPE_TABLE_COUNT \
	(sizeof(archiveTocShapeTable) / sizeof(archiveTocShapeTable[0]))


/*
 * archive_toc_shape_for_desc looks up the shape for a (non-ACL/COMMENT)
 * tag. Returns ARCHIVE_SHAPE_UNKNOWN for ARCHIVE_TAG_ACL/COMMENT
 * themselves (handled separately, never reach this table) and for any
 * tag this table has no entry for.
 */
ArchiveTocShape
archive_toc_shape_for_desc(ArchiveItemDesc desc)
{
	for (size_t i = 0; i < ARCHIVE_TOC_SHAPE_TABLE_COUNT; i++)
	{
		if (archiveTocShapeTable[i].desc == desc)
		{
			return archiveTocShapeTable[i].shape;
		}
	}

	return ARCHIVE_SHAPE_UNKNOWN;
}


/*
 * splitSimpleOrRelationRemainder splits a non-ACL/COMMENT entry's
 * remainder into namespace/relname/name/owner, given item->shape has
 * already been set to ARCHIVE_SHAPE_SIMPLE or ARCHIVE_SHAPE_RELATION.
 *
 * Both shapes share the same two outer boundaries: the namespace is
 * always the first space-delimited token ("-" for a global object), and
 * the owner is always the last one (possibly empty, seen as one trailing
 * space with nothing after it). What differs is the middle:
 *
 *  - SIMPLE and OPAQUE: the whole remaining text, verbatim, is `name` --
 *    it may contain embedded spaces (a FUNCTION/PROCEDURE/AGGREGATE/
 *    OPERATOR's argument list, "fixture_add(integer, integer)", a CAST's
 *    "CAST (domain AS type)", or USER_MAPPING's "USER MAPPING user SERVER
 *    server"), so this does NOT further split on space. The two shapes
 *    are positionally identical; OPAQUE only exists to flag that `name`
 *    is not a single object identifier for these tags.
 *  - RELATION: the middle is exactly two single-token fields, `relname`
 *    then `name` (DEFAULT's `name` is a column name, not an object with
 *    its own identity, but the position is the same).
 */
static bool
splitSimpleOrRelationRemainder(const char *remainder, ArchiveContentItem *item)
{
	char buffer[2 * PG_NAMEDATALEN + BUFSIZE] = { 0 };

	strlcpy(buffer, remainder, sizeof(buffer));

	/*
	 * A trailing space with nothing after it is PrintTOCSummary's own
	 * encoding of a NULL (unknown) owner -- sanitize_line always prints a
	 * space before the owner field, even an empty one. That's the only
	 * reliable signal that there is no owner at all: once this one
	 * trailing space is gone, the last space-delimited token in what's
	 * left could just as easily be the last word of a multi-word `name`
	 * (e.g. CAST's "CAST (domain AS type)"), not a real owner boundary.
	 */
	size_t bufferLen = strlen(buffer);
	bool hasOwner = !(bufferLen > 0 && buffer[bufferLen - 1] == ' ');

	if (!hasOwner && bufferLen > 0)
	{
		buffer[bufferLen - 1] = '\0';
	}

	char *p = buffer;

	/* namespace: "-" for global, or up to the next space */
	if (*p == '-' && (p[1] == ' ' || p[1] == '\0'))
	{
		item->hasNamespace = false;
		p += (p[1] == ' ') ? 2 : 1;
	}
	else
	{
		char *space = strchr(p, ' ');

		if (space == NULL)
		{
			log_debug("archive_toc_parser: no namespace boundary in \"%s\"",
					  remainder);
			return false;
		}

		size_t len = space - p;

		if (len >= sizeof(item->nspname))
		{
			log_debug("archive_toc_parser: namespace too long in \"%s\"",
					  remainder);
			return false;
		}

		strlcpy(item->nspname, p, len + 1);
		item->hasNamespace = true;
		p = space + 1;
	}

	/*
	 * owner: the last space-delimited token -- but only when hasOwner
	 * said there really is one; otherwise the marker space was already
	 * stripped above and there is no owner boundary left to look for.
	 */
	if (hasOwner)
	{
		char *lastSpace = strrchr(p, ' ');

		if (lastSpace == NULL)
		{
			log_debug("archive_toc_parser: no owner boundary in \"%s\"",
					  remainder);
			return false;
		}

		*lastSpace = '\0';
		strlcpy(item->owner, lastSpace + 1, sizeof(item->owner));
	}
	else
	{
		item->owner[0] = '\0';
	}

	if (item->shape == ARCHIVE_SHAPE_RELATION)
	{
		char *space = strchr(p, ' ');

		if (space == NULL)
		{
			log_debug("archive_toc_parser: no relation boundary in \"%s\"",
					  remainder);
			return false;
		}

		size_t len = space - p;

		if (len >= sizeof(item->relname))
		{
			log_debug("archive_toc_parser: relation name too long in \"%s\"",
					  remainder);
			return false;
		}

		strlcpy(item->relname, p, len + 1);
		p = space + 1;
	}

	strlcpy(item->name, p, sizeof(item->name));

	return true;
}


/*
 * parseQuotedIdentifier parses the identifier at *pp, as quoted by pg_dump
 * with fmtId(), copies its unquoted value to name, and advances *pp past
 * the identifier -- the only place in the whole TOC format where
 * identifiers are quoted at all (every other field, including the outer
 * namespace ACL/COMMENT entries carry, is plain, unquoted text):
 *
 *   column_acl_test     ->  column_acl_test
 *   "Foo.Bar"           ->  Foo.Bar
 *   "say ""hi"""        ->  say "hi"
 */
static bool
parseQuotedIdentifier(const char **pp, char *name, size_t size)
{
	const char *ptr = *pp;
	size_t len = 0;

	if (*ptr == '"')
	{
		/* skip the opening double-quote */
		++ptr;

		for (;;)
		{
			if (*ptr == '\0')
			{
				return false;
			}

			if (*ptr == '"')
			{
				/* a doubled double-quote is an escaped double-quote */
				if (*(ptr + 1) != '"')
				{
					/* skip the closing double-quote */
					++ptr;
					break;
				}

				++ptr;
			}

			if (len + 1 >= size)
			{
				return false;
			}

			name[len++] = *ptr++;
		}
	}
	else
	{
		while (*ptr != '\0' && *ptr != '.' && *ptr != ' ')
		{
			if (len + 1 >= size)
			{
				return false;
			}

			name[len++] = *ptr++;
		}
	}

	name[len] = '\0';
	*pp = ptr;

	return len > 0;
}


/*
 * buildRelationAclOrCommentRestoreListName finishes parsing an ACL/COMMENT
 * entry that belongs to a relation, once TAG2's own name(s) have been
 * parsed and q is left pointing right before the owner field (which may be
 * missing). It builds "nspname relname rolname" as item->restoreListName,
 * the restore list name of the *relation*, not of the ACL/COMMENT entry
 * itself, so that e.g. a COMMENT ON COLUMN is filtered out together with
 * the table it belongs to -- matching the only kind of name-based lookup
 * copydb_objectid_is_filtered_out performs for these OID-less entries.
 *
 * name, when not empty, is TAG2's own identifier (a constraint, trigger,
 * policy or rule name, or a column name) -- kept in item->name for callers
 * that want it, even though it plays no part in restoreListName.
 */
static bool
buildRelationAclOrCommentRestoreListName(ArchiveContentItem *item,
										 const char *nspname,
										 const char *relname,
										 const char *name,
										 const char *q)
{
	/* skip the space before the owner, which might be missing */
	if (*q == ' ')
	{
		++q;
	}
	else if (*q != '\0')
	{
		return false;
	}

	const char *owner = q;

	size_t bytes = strlen(nspname) + 1 + strlen(relname) + 1 + strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->restoreListName, bytes, "%s %s %s", nspname, relname, owner);

	strlcpy(item->relname, relname, sizeof(item->relname));
	strlcpy(item->name, name, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	return true;
}


/*
 * parseAclOrCommentRemainder parses the remainder of an ACL or COMMENT
 * entry, e.g.:
 *
 *   - SCHEMA public postgres
 *   - EXTENSION intarray
 *   public TABLE foo postgres
 *   public COLUMN "Foo Bar".id postgres
 *   public TRIGGER foo_trg ON foo postgres
 *
 * rawRemainder is positioned right at the outer namespace field (the
 * leading space after the outer ACL/COMMENT tag has already been
 * stripped by the caller). This outer namespace is plain, unquoted text,
 * like every top-level TOC field -- unlike the identifier(s) embedded in
 * TAG2's own remainder (a relation, column, constraint, trigger, policy
 * or rule name), which pg_dump builds with fmtId() and so can be
 * double-quoted (see parseQuotedIdentifier).
 *
 * TAG2 itself is recognized by re-entering the same flex lexer on the
 * substring starting at TAG2's own keyword text: by the time this runs,
 * every token already pulled from the outer line has been copied out
 * (rawRemainder is a heap copy, independent of the lexer's scan buffer),
 * so reusing the lexer here is safe.
 *
 * On success, populates item->restoreListName (the filtering-relevant
 * name -- the owning relation's for a relation-scoped TAG2, see
 * buildRelationAclOrCommentRestoreListName) and the structured
 * innerDesc/hasNamespace/nspname/relname/name/owner fields. On failure (a
 * TAG2 this function doesn't know how to split, or malformed input),
 * leaves item->restoreListName untouched, so the caller's raw-text
 * fallback (the whole, unsplit remainder) stands -- exactly as "we don't
 * support this one yet" has always been handled here.
 */
static bool
parseAclOrCommentRemainder(const char *rawRemainder, ArchiveContentItem *item)
{
	const char *p = rawRemainder;

	char nspname[PG_NAMEDATALEN] = { 0 };
	bool hasNamespace = true;

	if (*p == '-' && (p[1] == ' ' || p[1] == '\0'))
	{
		hasNamespace = false;
		p += (p[1] == ' ') ? 2 : 1;
	}
	else
	{
		const char *space = strchr(p, ' ');

		if (space == NULL)
		{
			return false;
		}

		size_t len = space - p;

		if (len >= sizeof(nspname))
		{
			return false;
		}

		strlcpy(nspname, p, len + 1);
		p = space + 1;
	}

	/* p now points right at TAG2's own keyword text */
	archive_toc_lex_line(p);

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_DESC)
	{
		return false;
	}

	ArchiveItemDesc tag2 = archive_toc_last_desc;

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
	{
		return false;
	}

	const char *tag2Remainder = archive_toc_text;

	if (*tag2Remainder == ' ')
	{
		tag2Remainder++;
	}

	item->innerDesc = tag2;
	item->hasNamespace = hasNamespace;
	strlcpy(item->nspname, nspname, sizeof(item->nspname));

	switch (tag2)
	{
		case ARCHIVE_TAG_SCHEMA:
		{
			/* "- nspname rolname"; the outer namespace is unused here */
			size_t bytes = strlen(tag2Remainder) + 3;

			item->restoreListName = (char *) calloc(bytes, sizeof(char));

			if (item->restoreListName == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			sformat(item->restoreListName, bytes, "- %s", tag2Remainder);
			item->tagType = ARCHIVE_TAG_TYPE_SCHEMA;

			return true;
		}

		case ARCHIVE_TAG_EXTENSION:
		{
			/*
			 * The extension name is followed by a space, even when there
			 * is no owner to follow that space: trim it so it doesn't end
			 * up as a trailing space on the extension's own name.
			 */
			char extname[PG_NAMEDATALEN] = { 0 };

			strlcpy(extname, tag2Remainder, sizeof(extname));

			size_t extLen = strlen(extname);

			if (extLen > 0 && extname[extLen - 1] == ' ')
			{
				extname[extLen - 1] = '\0';
			}

			item->restoreListName = strdup(extname);

			if (item->restoreListName == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			strlcpy(item->name, extname, sizeof(item->name));
			item->tagType = ARCHIVE_TAG_TYPE_EXTENSION;

			return true;
		}

		case ARCHIVE_TAG_TABLE:
		case ARCHIVE_TAG_VIEW:
		case ARCHIVE_TAG_MATERIALIZED_VIEW:
		case ARCHIVE_TAG_FOREIGN_TABLE:
		case ARCHIVE_TAG_INDEX:
		case ARCHIVE_TAG_SEQUENCE:
		{
			/* these are always schema-qualified; a "-" namespace is unexpected */
			if (!hasNamespace)
			{
				return false;
			}

			const char *q = tag2Remainder;
			char relname[PG_NAMEDATALEN] = { 0 };

			if (!parseQuotedIdentifier(&q, relname, sizeof(relname)))
			{
				return false;
			}

			return buildRelationAclOrCommentRestoreListName(item, nspname,
															relname, "", q);
		}

		case ARCHIVE_TAG_COLUMN:
		{
			if (!hasNamespace)
			{
				return false;
			}

			const char *q = tag2Remainder;
			char relname[PG_NAMEDATALEN] = { 0 };
			char colname[PG_NAMEDATALEN] = { 0 };

			if (!parseQuotedIdentifier(&q, relname, sizeof(relname)) ||
				*q != '.')
			{
				return false;
			}

			/* skip the dot */
			++q;

			if (!parseQuotedIdentifier(&q, colname, sizeof(colname)))
			{
				return false;
			}

			return buildRelationAclOrCommentRestoreListName(item, nspname,
															relname, colname, q);
		}

		case ARCHIVE_TAG_CONSTRAINT:
		case ARCHIVE_TAG_TRIGGER:
		case ARCHIVE_TAG_POLICY:
		case ARCHIVE_TAG_RULE:
		{
			if (!hasNamespace)
			{
				return false;
			}

			const char *q = tag2Remainder;
			char name[PG_NAMEDATALEN] = { 0 };
			char relname[PG_NAMEDATALEN] = { 0 };

			if (!parseQuotedIdentifier(&q, name, sizeof(name)) ||
				strncmp(q, " ON ", 4) != 0)
			{
				return false;
			}

			/* skip " ON " */
			q += 4;

			/* domain constraints are "CONSTRAINT name ON DOMAIN domain" */
			if (strncmp(q, "DOMAIN ", 7) == 0)
			{
				return false;
			}

			if (!parseQuotedIdentifier(&q, relname, sizeof(relname)))
			{
				return false;
			}

			return buildRelationAclOrCommentRestoreListName(item, nspname,
															relname, name, q);
		}

		default:
		{
			/*
			 * FUNCTION, TYPE, DOMAIN, ...: not split, the caller keeps the
			 * whole raw remainder as restoreListName, as it always has.
			 */
			return false;
		}
	}
}


/*
 * archive_toc_parse_line parses one pg_restore --list line (no trailing
 * newline needed) into item, using the flex lexer for the dumpId/
 * catalogOid/objectOid/TAG prefix and, for everything that isn't an
 * ACL/COMMENT entry, the shape table above for the remainder.
 *
 * ACL/COMMENT entries are parsed by parseAclOrCommentRemainder, above.
 */
bool
archive_toc_parse_line(const char *line, ArchiveContentItem *item)
{
	*item = (ArchiveContentItem) {
		0
	};

	archive_toc_lex_line(line);

	int token = archive_toc_lex();

	if (token != ARCHIVE_TOC_TOKEN_OID)
	{
		log_error("Failed to parse Archive TOC dumpId in: %s", line);
		return false;
	}

	item->dumpId = (int) archive_toc_last_oid;

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SEMICOLON ||
		archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC dumpId separator in: %s", line);
		return false;
	}

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_OID)
	{
		log_error("Failed to parse Archive TOC catalogOid in: %s", line);
		return false;
	}

	item->catalogOid = (uint32_t) archive_toc_last_oid;

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_OID)
	{
		log_error("Failed to parse Archive TOC objectOid in: %s", line);
		return false;
	}

	item->objectOid = (uint32_t) archive_toc_last_oid;

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE)
	{
		log_error("Failed to parse Archive TOC: %s", line);
		return false;
	}

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_DESC)
	{
		log_error("Failed to parse Archive TOC tag in: %s", line);
		return false;
	}

	item->desc = archive_toc_last_desc;

	size_t descLen = strlen(archive_toc_last_desc_text) + 1;

	item->description = (char *) calloc(descLen, sizeof(char));

	if (item->description == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	strlcpy(item->description, archive_toc_last_desc_text, descLen);

	/* the lexer switched to REMAINDER when it matched the tag */
	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
	{
		log_error("Failed to parse Archive TOC remainder in: %s", line);
		return false;
	}

	/*
	 * The REMAINDER rule starts matching right where the TAG keyword's own
	 * match ended, i.e. at the single space PrintTOCSummary always writes
	 * between the TAG and the rest of the line -- that space was never
	 * pulled off as its own SPACE token, because the lexer had already
	 * switched to the REMAINDER start condition by the time it would see
	 * it. Every consumer below (parseAclOrCommentRemainder and
	 * splitSimpleOrRelationRemainder) expects to start right at the
	 * namespace field instead, so skip that one leading space here, once,
	 * rather than in each of them.
	 */
	const char *remainder = archive_toc_text;

	if (*remainder == ' ')
	{
		remainder++;
	}

	size_t len = strlen(remainder) + 1;

	item->restoreListName = (char *) calloc(len, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	strlcpy(item->restoreListName, remainder, len);

	if (item->desc == ARCHIVE_TAG_ACL || item->desc == ARCHIVE_TAG_COMMENT)
	{
		item->isCompositeTag = true;
		item->shape = ARCHIVE_SHAPE_ACL_OR_COMMENT;
		item->tagKind = (item->desc == ARCHIVE_TAG_ACL)
						? ARCHIVE_TAG_KIND_ACL
						: ARCHIVE_TAG_KIND_COMMENT;

		/*
		 * parseAclOrCommentRemainder reassigns item->restoreListName to its
		 * own reconstructed pg_restore list name on success: free our
		 * placeholder copy first, it would otherwise leak. On failure it
		 * leaves restoreListName untouched, so our placeholder (the raw
		 * remainder, still useful for logging) is kept instead.
		 */
		char *rawRemainder = item->restoreListName;

		item->restoreListName = NULL;

		/* errors, if any, are not fatal: that's content we don't support yet */
		if (!parseAclOrCommentRemainder(rawRemainder, item))
		{
			item->restoreListName = rawRemainder;
		}
		else
		{
			free(rawRemainder);
		}

		return true;
	}

	item->shape = archive_toc_shape_for_desc(item->desc);

	if (item->shape == ARCHIVE_SHAPE_SIMPLE ||
		item->shape == ARCHIVE_SHAPE_RELATION ||
		item->shape == ARCHIVE_SHAPE_OPAQUE)
	{
		if (!splitSimpleOrRelationRemainder(item->restoreListName, item))
		{
			/*
			 * Not fatal: restoreListName (set above) is still valid and
			 * is what every existing caller already uses. Only this
			 * entry's new split fields are left at their zero value.
			 */
			log_debug("archive_toc_parser: could not split remainder "
					  "for %s: %s", item->description ? item->description :
					  "(unknown tag)", item->restoreListName);
		}
	}

	return true;
}
