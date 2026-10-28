/*
 * src/bin/pgcopydb/archive_toc_parser.c
 *     Parses one pg_restore --list line into an ArchiveContentItem, using
 *     the flex-generated archive_toc_lexer.l for every TAG the format can
 *     contain -- both the outer dumpId/catalogOid/objectOid/TAG prefix and,
 *     for an ACL or COMMENT entry, the object type nested inside its
 *     remainder -- and a small shape table (below) for the irregular,
 *     tag-dependent rest of the line.
 *
 * This supersedes the old hand-rolled parser entirely (tokenize_archive_
 * list_entry, parse_archive_list_entry, parse_archive_acl_or_comment and
 * parse_archive_acl_or_comment_relation, formerly in pgcmd.c, now removed):
 * ACL/COMMENT composite-tag parsing is where the old declaration-order-
 * sensitive TAG matching mattered the most, since the object type is
 * matched against the exact same keyword set as the outer TAG, with the
 * exact same prefix-ambiguity hazards -- so it is also where replacing
 * that matching with a generated DFA pays off the most, not a part of the
 * format worth leaving on the old parser.
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
 * appear as the object type inside an ACL/COMMENT entry (e.g.
 * ARCHIVE_TAG_COLUMN) are also not listed, for the same reason --
 * parseAclOrCommentRemainder (below) owns that shape already.
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
	{ ARCHIVE_TAG_TRANSFORM, ARCHIVE_SHAPE_OPAQUE },             /* (*) */
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
 * splitAclOrCommentOpaqueNameAndOwner splits the remainder of an
 * ACL/COMMENT entry whose nested object type is itself ARCHIVE_SHAPE_SIMPLE
 * (FUNCTION, PROCEDURE, AGGREGATE, OPERATOR, TYPE, DOMAIN, ...) into name
 * and owner, using the exact same trailing-space convention as
 * splitSimpleOrRelationRemainder's own SIMPLE/OPAQUE case (see its
 * comment): name may itself contain embedded spaces (e.g. a FUNCTION's own
 * argument list), so only the LAST space-delimited token, when there is
 * one at all, is ever taken as the owner boundary. The object type's own
 * identifier was already consumed by the caller (parseAclOrCommentRemainder)
 * before this is reached -- there is no separate namespace left to strip
 * here, unlike splitSimpleOrRelationRemainder.
 */
static bool
splitAclOrCommentOpaqueNameAndOwner(const char *remainder,
									ArchiveContentItem *item)
{
	char buffer[2 * PG_NAMEDATALEN + BUFSIZE] = { 0 };

	strlcpy(buffer, remainder, sizeof(buffer));

	size_t bufferLen = strlen(buffer);
	bool hasOwner = !(bufferLen > 0 && buffer[bufferLen - 1] == ' ');

	if (!hasOwner && bufferLen > 0)
	{
		buffer[bufferLen - 1] = '\0';
	}

	if (hasOwner)
	{
		char *lastSpace = strrchr(buffer, ' ');

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

	strlcpy(item->name, buffer, sizeof(item->name));

	return true;
}


/*
 * archiveTocAccumulateWords pulls ARCHIVE_TOC_TOKEN_WORD/SPACE tokens from
 * whichever CASTBODY/TRANSFORMBODY/USERMAPPINGBODY/DEFAULTACLBODY/
 * OPCLASSBODY scan is currently armed (see archive_toc_lexer.l's file
 * header) and concatenates them -- a single SPACE between two WORDs
 * becomes one literal space in the result, so a multi-word type name
 * (e.g. "double precision") comes back whole -- stopping at the first
 * token that is neither, which is always exactly the keyword (or EOF/
 * RPAREN) each of those five shapes uses as its own next boundary.
 * *stopTokenOut is that stopping token, for the caller to check against
 * the boundary it actually expected there (ARCHIVE_TOC_TOKEN_AS,
 * _LANGUAGE_KW, _SERVER_KW, _USING, _EOF, _RPAREN): this function only
 * tokenizes, same division as everywhere else in this file, it doesn't
 * know which boundary makes sense for the shape its caller has.
 *
 * Returns NULL (logging ALLOCATION_FAILED_ERROR) on OOM, same convention
 * as every other allocation in this file; otherwise always returns a
 * freshly malloc'ed string, the caller's to free, even when it turns out
 * empty (e.g. a shape that doesn't match what was expected at all).
 */
static char *
archiveTocAccumulateWords(int *stopTokenOut)
{
	char buffer[BUFSIZE] = { 0 };
	int token;

	for (;;)
	{
		token = archive_toc_lex();

		if (token == ARCHIVE_TOC_TOKEN_WORD)
		{
			strlcat(buffer, archive_toc_word, sizeof(buffer));
			continue;
		}

		if (token == ARCHIVE_TOC_TOKEN_SPACE)
		{
			size_t len = strlen(buffer);

			if (len > 0 && len + 1 < sizeof(buffer))
			{
				buffer[len] = ' ';
				buffer[len + 1] = '\0';
			}

			continue;
		}

		break;
	}

	*stopTokenOut = token;

	char *result = strdup(buffer);

	if (result == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}

	return result;
}


/*
 * splitCastTypes splits a CAST's own "(sourcetype AS targettype)" text
 * (parens included) into item->castSourceType/castTargetType, via the
 * lexer's own CASTBODY start condition (archive_toc_lexer.l) rather than
 * a hand-rolled " AS " strstr(). Used both for the plain top-level CAST
 * entry (applyTopLevelSignatureExtras, after stripping the repeated
 * "CAST " prefix pg_dump's own labelq includes, see dumpCast in
 * pg_dump.c) and for a COMMENT ON CAST's nested remainder
 * (parseAclOrCommentRemainder), which has no such prefix to strip
 * (dumpComment is called with castargs, not labelq).
 */
static bool
splitCastTypes(const char *parenText, ArchiveContentItem *item)
{
	archive_toc_lex_cast_body(parenText);

	int stopToken;
	char *source = archiveTocAccumulateWords(&stopToken);

	if (source == NULL)
	{
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_AS)
	{
		free(source);
		return false;
	}

	char *target = archiveTocAccumulateWords(&stopToken);

	if (target == NULL)
	{
		free(source);
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_RPAREN && stopToken != ARCHIVE_TOC_TOKEN_EOF)
	{
		free(source);
		free(target);
		return false;
	}

	item->castSourceType = source;
	item->castTargetType = target;

	return true;
}


/*
 * splitOperatorClassFamilyUsing narrows an OPERATOR CLASS/FAMILY ACL or
 * COMMENT entry's opaque item->name (already split from its owner by
 * splitAclOrCommentOpaqueNameAndOwner, e.g. "fixture_int_ops_class USING
 * btree") down to the bare object name, moving the access method name
 * into item->accessMethod, via the lexer's own OPCLASSBODY start
 * condition -- dumpOpclass/dumpOpfamily's own "%s USING %s" shape for an
 * ACL/COMMENT entry's remainder (pg_dump.c; the plain, non-ACL/COMMENT
 * entry has no such suffix at all, see pgcmd.h's own note on
 * accessMethod). Does nothing when the expected " USING " token doesn't
 * show up where expected.
 */
static void
splitOperatorClassFamilyUsing(ArchiveContentItem *item)
{
	archive_toc_lex_opclass_body(item->name);

	int stopToken;
	char *name = archiveTocAccumulateWords(&stopToken);

	if (name == NULL)
	{
		return;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_USING)
	{
		free(name);
		return;
	}

	char *amname = archiveTocAccumulateWords(&stopToken);

	if (amname == NULL)
	{
		free(name);
		return;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_EOF)
	{
		free(name);
		free(amname);
		return;
	}

	strlcpy(item->name, name, sizeof(item->name));
	strlcpy(item->accessMethod, amname, sizeof(item->accessMethod));
	free(name);
	free(amname);
}


/*
 * transformSplit splits a TRANSFORM's own "FOR typename LANGUAGE
 * langname" text into item->transformType/transformLanguage, via the
 * lexer's own TRANSFORMBODY start condition. Used both for the plain
 * top-level TRANSFORM entry (applyTopLevelSignatureExtras, after
 * stripping the repeated "TRANSFORM " prefix pg_dump's own labelq
 * includes, see dumpTransform in pg_dump.c -- what's left still starts
 * with "FOR ", same as the nested case below) and for a COMMENT ON
 * TRANSFORM's nested remainder (parseAclOrCommentRemainder), which has
 * no such prefix to strip (dumpComment is called with transformargs, not
 * labelq, but transformargs is itself already "FOR %s LANGUAGE %s").
 */
static bool
transformSplit(const char *text, ArchiveContentItem *item)
{
	archive_toc_lex_transform_body(text);

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_FOR)
	{
		return false;
	}

	int stopToken;
	char *type = archiveTocAccumulateWords(&stopToken);

	if (type == NULL)
	{
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_LANGUAGE_KW)
	{
		free(type);
		return false;
	}

	char *language = archiveTocAccumulateWords(&stopToken);

	if (language == NULL)
	{
		free(type);
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_EOF)
	{
		free(type);
		free(language);
		return false;
	}

	item->transformType = type;
	strlcpy(item->transformLanguage, language, sizeof(item->transformLanguage));
	free(language);

	return true;
}


/*
 * userMappingSplit splits a USER MAPPING's own "user SERVER server" text
 * into item->userMappingUser/userMappingServer, via the lexer's own
 * USERMAPPINGBODY start condition. A user mapping can't itself be
 * GRANTed on or COMMENTed on, so this is only ever reached for the plain
 * top-level entry (applyTopLevelSignatureExtras, after stripping the
 * repeated "USER MAPPING " prefix pg_dump's own labelq includes, see
 * dumpUserMappings in pg_dump.c).
 */
static bool
userMappingSplit(const char *text, ArchiveContentItem *item)
{
	archive_toc_lex_user_mapping_body(text);

	int stopToken;
	char *user = archiveTocAccumulateWords(&stopToken);

	if (user == NULL)
	{
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_SERVER_KW)
	{
		free(user);
		return false;
	}

	char *server = archiveTocAccumulateWords(&stopToken);

	if (server == NULL)
	{
		free(user);
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_EOF)
	{
		free(user);
		free(server);
		return false;
	}

	strlcpy(item->userMappingUser, user, sizeof(item->userMappingUser));
	strlcpy(item->userMappingServer, server, sizeof(item->userMappingServer));
	free(user);
	free(server);

	return true;
}


/*
 * defaultAclSplit splits a DEFAULT ACL's own "FOR objecttype" text into
 * item->defaultAclObjectType, via the lexer's own DEFAULTACLBODY start
 * condition. objecttype is always exactly one of 6 fixed literals
 * (TABLES/SEQUENCES/FUNCTIONS/TYPES/SCHEMAS/LARGE OBJECTS, dumpDefaultACL's
 * own switch, pg_dump.c) -- the last of which is itself two words, so
 * this still accumulates every WORD left after FOR rather than expecting
 * exactly one. A default ACL can't itself be GRANTed on or COMMENTed on,
 * so this is only ever reached for the plain top-level entry
 * (applyTopLevelSignatureExtras, after stripping the repeated "DEFAULT
 * PRIVILEGES " prefix pg_dump's own labelq includes).
 */
static bool
defaultAclSplit(const char *text, ArchiveContentItem *item)
{
	archive_toc_lex_default_acl_body(text);

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_FOR)
	{
		return false;
	}

	int stopToken;
	char *objectType = archiveTocAccumulateWords(&stopToken);

	if (objectType == NULL)
	{
		return false;
	}

	if (stopToken != ARCHIVE_TOC_TOKEN_EOF)
	{
		free(objectType);
		return false;
	}

	strlcpy(item->defaultAclObjectType, objectType,
			sizeof(item->defaultAclObjectType));
	free(objectType);

	return true;
}


/*
 * applyTopLevelSignatureExtras applies the Phase 4 "overlapping fields"
 * split (see pgcmd.h's own note just above castSourceType) to a plain,
 * non-ACL/COMMENT entry's already-opaque item->name, for the handful of
 * tags whose plain tag text has further regular structure of its own.
 * Called from archive_toc_parse_line right after splitSimpleOrRelation
 * Remainder succeeds; a no-op for every tag not listed below, and not
 * fatal when a listed tag's text doesn't match the expected shape (e.g.
 * a future pg_dump version changing the tag format) -- item->name is
 * left as the whole opaque text either way, exactly as it already was
 * before Phase 4.
 */
static void splitPlainFunctionSignature(ArchiveContentItem *item);

static void
applyTopLevelSignatureExtras(ArchiveContentItem *item)
{
	switch (item->desc)
	{
		case ARCHIVE_TAG_FUNCTION:
		case ARCHIVE_TAG_PROCEDURE:
		case ARCHIVE_TAG_AGGREGATE:
		{
			splitPlainFunctionSignature(item);
			break;
		}

		case ARCHIVE_TAG_CAST:
		{
			const char *prefix = "CAST ";

			if (strncmp(item->name, prefix, strlen(prefix)) == 0)
			{
				splitCastTypes(item->name + strlen(prefix), item);
			}

			break;
		}

		case ARCHIVE_TAG_USER_MAPPING:
		{
			const char *prefix = "USER MAPPING ";

			if (strncmp(item->name, prefix, strlen(prefix)) == 0)
			{
				userMappingSplit(item->name + strlen(prefix), item);
			}

			break;
		}

		case ARCHIVE_TAG_DEFAULT_ACL:
		{
			const char *prefix = "DEFAULT PRIVILEGES ";

			if (strncmp(item->name, prefix, strlen(prefix)) == 0)
			{
				defaultAclSplit(item->name + strlen(prefix), item);
			}

			break;
		}

		case ARCHIVE_TAG_TRANSFORM:
		{
			const char *prefix = "TRANSFORM ";

			if (strncmp(item->name, prefix, strlen(prefix)) == 0)
			{
				transformSplit(item->name + strlen(prefix), item);
			}

			break;
		}

		default:
		{
			break;
		}
	}
}


/*
 * quoteIdentForUseList re-applies pg_dump's own fmtId() double-quoting
 * (doubling any embedded quote) to an identifier the lexer's
 * OBJTYPEQIDENT state already unescaped into its true, plain value --
 * wasQuoted (see archive_toc_ident_quoted) says whether the original
 * actually needed it. item->name/relname/... always hold the plain,
 * unescaped value (that's the whole point of tokenizing it instead of
 * treating it as raw text); this is only for reconstructing
 * useListRemainder, which must round-trip the ORIGINAL spelling
 * byte-identically, quotes included.
 *
 * Returns NULL (logging ALLOCATION_FAILED_ERROR) on OOM, same convention
 * as every other allocation in this file.
 */
static char *
quoteIdentForUseList(const char *ident, bool wasQuoted)
{
	if (!wasQuoted)
	{
		char *copy = strdup(ident);

		if (copy == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
		}

		return copy;
	}

	size_t len = strlen(ident);
	size_t embeddedQuotes = 0;

	for (size_t i = 0; i < len; i++)
	{
		if (ident[i] == '"')
		{
			embeddedQuotes++;
		}
	}

	size_t bytes = len + embeddedQuotes + 2 + 1;
	char *quoted = (char *) calloc(bytes, sizeof(char));

	if (quoted == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return NULL;
	}

	char *p = quoted;
	*p++ = '"';

	for (size_t i = 0; i < len; i++)
	{
		if (ident[i] == '"')
		{
			*p++ = '"';
		}

		*p++ = ident[i];
	}

	*p++ = '"';
	*p = '\0';

	return quoted;
}


/*
 * finishRelationAclOrComment finishes parsing an ACL/COMMENT entry that
 * belongs to a relation, once the object type's own identifier(s) have
 * already been pulled as ARCHIVE_TOC_TOKEN_IDENT tokens: it pulls the one
 * token that's left, either SPACE (an owner follows) or EOF (no owner at
 * all), and builds "nspname relname rolname" as item->restoreListName --
 * the restore list name of the *relation*, not of the ACL/COMMENT entry
 * itself, so that e.g. a COMMENT ON COLUMN is filtered out together with
 * the table it belongs to, matching the only kind of name-based lookup
 * copydb_objectid_is_filtered_out performs for these OID-less entries.
 *
 * The owner, when present, is grabbed as one raw REMAINDER token (see
 * archive_toc_begin_remainder) rather than as an IDENT: unlike relname/
 * colname/name, it is never itself quoted or escaped by pg_dump.
 *
 * name, when not empty, is the object type's own identifier (a
 * constraint, trigger, policy or rule name, or a column name) -- kept in
 * item->name for callers that want it, even though it plays no part in
 * restoreListName.
 *
 * relnameQuoted/nameQuoted say whether relname/name were themselves
 * double-quoted in the original line (see archive_toc_ident_quoted),
 * needed to reconstruct useListRemainder's ORIGINAL spelling faithfully
 * -- restoreListName and item->relname/name themselves stay unescaped
 * either way.
 */
static bool
finishRelationAclOrComment(ArchiveContentItem *item, const char *nspname,
						   const char *relname, const char *name,
						   bool relnameQuoted, bool nameQuoted)
{
	char owner[PG_NAMEDATALEN] = { 0 };

	int next = archive_toc_lex();

	if (next == ARCHIVE_TOC_TOKEN_SPACE)
	{
		archive_toc_begin_remainder();

		if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
		{
			return false;
		}

		strlcpy(owner, archive_toc_text, sizeof(owner));
	}
	else if (next != ARCHIVE_TOC_TOKEN_EOF)
	{
		return false;
	}

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

	/*
	 * useListRemainder reproduces the full original line, object type
	 * included, unlike restoreListName above (see the field's own comment
	 * in pgcmd.h): the body differs by the object type's own shape --
	 *
	 *   name empty:                   "relname"        (TABLE, VIEW, ...)
	 *   objectType is COLUMN:         "relname.name"
	 *   objectType is CONSTRAINT/etc: "name ON relname"
	 *
	 * relname/name are re-quoted here (quoteIdentForUseList), not left as
	 * the plain, unescaped values item->relname/name themselves hold, so
	 * a quoted original (e.g. "Weird Table") round-trips byte-identically
	 * instead of losing its quotes.
	 */
	char *qRelname = quoteIdentForUseList(relname, relnameQuoted);
	char *qName = IS_EMPTY_STRING_BUFFER(name)
				  ? strdup("")
				  : quoteIdentForUseList(name, nameQuoted);

	if (qRelname == NULL || qName == NULL)
	{
		/* errors, if any, have already been logged */
		return false;
	}

	char body[2 * (2 * PG_NAMEDATALEN) + 8] = { 0 };

	if (IS_EMPTY_STRING_BUFFER(name))
	{
		strlcpy(body, qRelname, sizeof(body));
	}
	else if (item->objectType == ARCHIVE_TAG_COLUMN)
	{
		sformat(body, sizeof(body), "%s.%s", qRelname, qName);
	}
	else
	{
		sformat(body, sizeof(body), "%s ON %s", qName, qRelname);
	}

	size_t ulrBytes =
		strlen(nspname) + 1 + strlen(item->objectTypeText) + 1 +
		strlen(body) + 1 + strlen(owner) + 1;

	item->useListRemainder = (char *) calloc(ulrBytes, sizeof(char));

	if (item->useListRemainder == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->useListRemainder, ulrBytes, "%s %s %s %s",
			nspname, item->objectTypeText, body, owner);

	return true;
}


/*
 * splitFunctionArgs splits a FUNCTION/PROCEDURE/AGGREGATE's raw,
 * comma-separated argument-list text -- already extracted from between
 * its own balanced parentheses by the lexer's OBJTYPEARGS state, see
 * archive_toc_lexer.l -- into one string per argument, on TOP-LEVEL
 * commas only: a nested paren pair isn't a boundary. In practice, real
 * pg_dump output never puts one of those inside an ACL/COMMENT's own
 * argument list here -- pg_get_function_identity_arguments() (what
 * dumpComment/dumpACL actually use, see pg_dump.c) keeps argmode and
 * argname but drops default values outright, and a type's own typmod
 * (e.g. "numeric(10,2)") was never part of a function's argument types
 * to begin with -- but this still handles it defensively (verified with
 * a synthetic, hand-built line, not a captured fixture -- see
 * tests/toc-coverage/fixture.sql's own note on why) rather than assume
 * it can never happen.
 *
 * Each argument is kept exactly as pg_get_function_identity_arguments()
 * printed it, "[argmode] [argname] argtype" -- see this file's own
 * naming note: argtype is a full Postgres type-name expression, not a
 * lexable token, so we go no further than this.
 *
 * A zero-argument aggregate's literal "(*)" (format_function_arguments'
 * own special case for is_agg, not a real argument -- see pg_dump.c) is
 * recognized as zero arguments, not as one literal "*" argument.
 *
 * Not fatal on OOM: returns false, leaving item->signatureArgs/Count at
 * their zero value, the same "not supported, caller still has the full
 * text" convention every other split function in this file follows.
 */
static bool
splitFunctionArgs(const char *argsText, ArchiveContentItem *item)
{
	item->signatureArgs = NULL;
	item->signatureArgsCount = 0;

	if (IS_EMPTY_STRING_BUFFER(argsText) || streq(argsText, "*"))
	{
		return true;
	}

	int depth = 0;
	int count = 1;

	for (const char *p = argsText; *p != '\0'; p++)
	{
		if (*p == '(')
		{
			depth++;
		}
		else if (*p == ')')
		{
			depth--;
		}
		else if (*p == ',' && depth == 0)
		{
			count++;
		}
	}

	char **args = (char **) calloc(count, sizeof(char *));

	if (args == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	int argIndex = 0;
	const char *start = argsText;

	depth = 0;

	for (const char *p = argsText;; p++)
	{
		bool atBoundary = (*p == ',' && depth == 0) || *p == '\0';

		if (*p == '(')
		{
			depth++;
		}
		else if (*p == ')')
		{
			depth--;
		}

		if (atBoundary)
		{
			/* pg_dump always separates arguments with ", " */
			if (*start == ' ')
			{
				start++;
			}

			size_t len = p - start;

			char *arg = (char *) calloc(len + 1, sizeof(char));

			if (arg == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			memcpy(arg, start, len); /* IGNORE-BANNED */
			arg[len] = '\0';

			args[argIndex++] = arg;
			start = p + 1;
		}

		if (*p == '\0')
		{
			break;
		}
	}

	item->signatureArgs = args;
	item->signatureArgsCount = argIndex;

	return true;
}


/*
 * splitPlainFunctionSignature splits a top-level (non-ACL/COMMENT)
 * FUNCTION/PROCEDURE/AGGREGATE entry's own opaque item->name -- e.g.
 * "fixture_add(integer, integer)", already isolated by
 * splitSimpleOrRelationRemainder -- into the bare function name (still
 * in item->name, that field's existing contract) and its argument-list
 * text (item->signatureArgsText/signatureArgs, the same fields
 * finishFunctionLikeAclOrComment below already populates for the
 * ACL/COMMENT-wrapped case -- see pgcmd.h's own note on that pair for
 * how the two cases differ in content, not shape).
 *
 * Unlike the ACL/COMMENT case, this name is never fmtId()-quoted
 * (format_function_signature's own honor_quotes=false, "appropriate for
 * use in TOC tags" -- see pg_dump.c) even when it would need to be (e.g.
 * a name with an embedded space, see tests/toc-coverage/fixture.sql's
 * "Fixture Weird Fn"), so the only safe boundary between the name and
 * its argument list is the first '(' -- a function/aggregate name can't
 * itself contain a literal '(' -- not "first space", which Phase 1-3's
 * OBJTYPEBODY lexer state relies on for the (always quoted-when-needed)
 * ACL/COMMENT case.
 *
 * Does nothing (leaves item->name as the whole opaque text) when the
 * expected trailing ")" isn't found -- same "not supported yet"
 * convention as every other split function here.
 */
static void
splitPlainFunctionSignature(ArchiveContentItem *item)
{
	archive_toc_lex_function_signature(item->name);

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
	{
		return;
	}

	char name[PG_NAMEDATALEN] = { 0 };

	strlcpy(name, archive_toc_ident, sizeof(name));

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_ARGS)
	{
		return;
	}

	char argsText[BUFSIZE] = { 0 };

	strlcpy(argsText, archive_toc_args, sizeof(argsText));
	strlcpy(item->name, name, sizeof(item->name));

	item->signatureArgsText = strdup(argsText);

	if (item->signatureArgsText == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return;
	}

	if (!splitFunctionArgs(argsText, item))
	{
		log_debug("archive_toc_parser: could not split function args \"%s\"",
				  argsText);
	}
}


/*
 * finishFunctionLikeAclOrComment finishes parsing an ACL/COMMENT entry on
 * a FUNCTION, PROCEDURE or AGGREGATE, once its own name and the raw text
 * of its balanced-parentheses argument list have already been pulled by
 * the caller (parseAclOrCommentRemainder). Unlike finishRelationAclOrComment,
 * there is no separate owning relation here: the function/procedure/
 * aggregate is itself the object the ACL/COMMENT is attached to, so
 * restoreListName is built the same way ("nspname name owner"), just with
 * the bare function name standing in for a relation's.
 *
 * This is inert for filtering today: no filters catalog query populates
 * restore_list_name rows for these object types (only
 * list_collations.sql/list_source_sequences.sql do), same as the old
 * full-remainder fallback this replaces.
 *
 * funcnameQuoted says whether funcname was itself double-quoted in the
 * original line (see archive_toc_ident_quoted), needed to reconstruct
 * useListRemainder's original spelling faithfully -- item->name and
 * restoreListName stay unescaped either way. argsText needs no such
 * handling: OBJTYPEARGS never unescapes anything inside it in the first
 * place (see the lexer file header), so whatever quoting it already had
 * -- inside an argument's own name or type, say -- simply passed through
 * untouched.
 */
static bool
finishFunctionLikeAclOrComment(ArchiveContentItem *item, const char *nspname,
							   const char *funcname, const char *argsText,
							   bool funcnameQuoted)
{
	char owner[PG_NAMEDATALEN] = { 0 };

	int next = archive_toc_lex();

	if (next == ARCHIVE_TOC_TOKEN_SPACE)
	{
		archive_toc_begin_remainder();

		if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
		{
			return false;
		}

		strlcpy(owner, archive_toc_text, sizeof(owner));
	}
	else if (next != ARCHIVE_TOC_TOKEN_EOF)
	{
		return false;
	}

	strlcpy(item->name, funcname, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	item->signatureArgsText = strdup(argsText);

	if (item->signatureArgsText == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	if (!splitFunctionArgs(argsText, item))
	{
		/* not fatal: signatureArgsText above is still valid on its own */
		log_debug("archive_toc_parser: could not split function args \"%s\"",
				  argsText);
	}

	size_t bytes = strlen(nspname) + 1 + strlen(funcname) + 1 + strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->restoreListName, bytes, "%s %s %s", nspname, funcname, owner);

	/* useListRemainder reproduces the full original line, as ever --
	 * funcname re-quoted if it originally was (see this function's own
	 * comment) */
	char *qFuncname = quoteIdentForUseList(funcname, funcnameQuoted);

	if (qFuncname == NULL)
	{
		/* errors, if any, have already been logged */
		return false;
	}

	size_t ulrBytes = strlen(nspname) + 1 + strlen(item->objectTypeText) + 1 +
					  strlen(qFuncname) + 1 + strlen(argsText) + 2 +
					  strlen(owner) + 1;

	item->useListRemainder = (char *) calloc(ulrBytes, sizeof(char));

	if (item->useListRemainder == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->useListRemainder, ulrBytes, "%s %s %s(%s) %s",
			nspname, item->objectTypeText, qFuncname, argsText, owner);

	return true;
}


/*
 * finishOperatorAclOrComment finishes parsing an ACL/COMMENT entry on an
 * OPERATOR, once its own symbol and the raw text of its balanced-
 * parentheses operand-type pair have already been pulled by the caller.
 *
 * The operand-type pair is "leftarg-or-NONE, rightarg-or-NONE" (pg_dump's
 * own dumpOpr, pg_dump.c: NONE literally, for a prefix or postfix
 * operator missing that side) -- split on its one top-level comma into
 * operatorLeftArg/operatorRightArg, with operatorHasLeftArg/RightArg
 * false (and the arg field left empty) when that side is NONE.
 */
static bool
finishOperatorAclOrComment(ArchiveContentItem *item, const char *nspname,
						   const char *symbol, const char *argsText)
{
	char owner[PG_NAMEDATALEN] = { 0 };

	int next = archive_toc_lex();

	if (next == ARCHIVE_TOC_TOKEN_SPACE)
	{
		archive_toc_begin_remainder();

		if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
		{
			return false;
		}

		strlcpy(owner, archive_toc_text, sizeof(owner));
	}
	else if (next != ARCHIVE_TOC_TOKEN_EOF)
	{
		return false;
	}

	const char *comma = strchr(argsText, ',');

	if (comma == NULL)
	{
		log_debug("archive_toc_parser: no operand boundary in \"%s\"", argsText);
		return false;
	}

	char leftArg[PG_NAMEDATALEN] = { 0 };
	char rightArg[PG_NAMEDATALEN] = { 0 };

	size_t leftLen = comma - argsText;

	if (leftLen >= sizeof(leftArg))
	{
		return false;
	}

	strlcpy(leftArg, argsText, leftLen + 1);

	const char *rightStart = comma + 1;

	if (*rightStart == ' ')
	{
		rightStart++;
	}

	strlcpy(rightArg, rightStart, sizeof(rightArg));

	item->operatorHasLeftArg = !streq(leftArg, "NONE");
	item->operatorHasRightArg = !streq(rightArg, "NONE");

	if (item->operatorHasLeftArg)
	{
		item->operatorLeftArg = strdup(leftArg);

		if (item->operatorLeftArg == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}
	}

	if (item->operatorHasRightArg)
	{
		item->operatorRightArg = strdup(rightArg);

		if (item->operatorRightArg == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}
	}

	strlcpy(item->name, symbol, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	size_t bytes = strlen(nspname) + 1 + strlen(symbol) + 1 + strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->restoreListName, bytes, "%s %s %s", nspname, symbol, owner);

	/* useListRemainder reproduces the full original line, as ever */
	size_t ulrBytes = strlen(nspname) + 1 + strlen(item->objectTypeText) + 1 +
					  strlen(symbol) + 2 + strlen(argsText) + 2 +
					  strlen(owner) + 1;

	item->useListRemainder = (char *) calloc(ulrBytes, sizeof(char));

	if (item->useListRemainder == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->useListRemainder, ulrBytes, "%s %s %s (%s) %s",
			nspname, item->objectTypeText, symbol, argsText, owner);

	return true;
}


/*
 * pullOpaqueObjectTypeRemainder pulls the one REMAINDER token left after
 * an ACL/COMMENT entry's object type keyword (CAST, TRANSFORM, OPERATOR
 * CLASS/FAMILY, or the generic `default:` case's TYPE/DOMAIN/... --
 * none of these own an identifier of their own per archive_toc_object_
 * type_has_ident, so the lexer never switched to OBJTYPEBODY for them),
 * strips its single leading space, and splits it into item->name/owner
 * via splitAclOrCommentOpaqueNameAndOwner. *objTypeRemainderOut is set, on
 * success, to a freshly malloc'ed copy of the raw (unmodified) text --
 * the caller's to free -- for buildOpaqueAclOrCommentNames below to
 * reproduce verbatim in useListRemainder. A copy, not archive_toc_text
 * itself: a caller that goes on to re-arm the lexer on a further-
 * structured shape of its own (splitCastTypes, transformSplit,
 * splitOperatorClassFamilyUsing -- each re-enters the lexer on a
 * different text) would otherwise be left holding a dangling pointer
 * into a scan buffer that call just freed.
 */
static bool
pullOpaqueObjectTypeRemainder(ArchiveContentItem *item,
							  char **objTypeRemainderOut)
{
	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
	{
		return false;
	}

	const char *objTypeRemainder = archive_toc_text;

	if (*objTypeRemainder == ' ')
	{
		objTypeRemainder++;
	}

	if (!splitAclOrCommentOpaqueNameAndOwner(objTypeRemainder, item))
	{
		return false;
	}

	char *copy = strdup(objTypeRemainder);

	if (copy == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	*objTypeRemainderOut = copy;

	return true;
}


/*
 * buildOpaqueAclOrCommentNames builds item->restoreListName and
 * item->useListRemainder for an ACL/COMMENT entry whose object type's own
 * remainder has already been split into item->name/owner by
 * splitAclOrCommentOpaqueNameAndOwner -- optionally narrowed further by a
 * Phase 4 helper first (splitCastTypes, splitOperatorClassFamilyUsing,
 * transformSplit), the same way finishRelationAclOrComment's own
 * relname/name narrow restoreListName without losing the full original
 * text, which objTypeRemainder (unmodified raw text, not item->name)
 * still provides to useListRemainder below. Shared by the CAST, TRANSFORM,
 * OPERATOR CLASS/FAMILY cases and the generic `default:` case in
 * parseAclOrCommentRemainder, below.
 */
static bool
buildOpaqueAclOrCommentNames(ArchiveContentItem *item, bool hasNamespace,
							 const char *nspname, const char *objTypeRemainder)
{
	const char *rlNamespace = hasNamespace ? nspname : "-";

	/*
	 * restoreListName stays narrow and keyword-free, the same convention
	 * finishRelationAclOrComment uses ("nspname relname owner") -- even
	 * though no filters catalog currently populates restore_list_name
	 * rows for these object types (list_collations.sql/list_source_
	 * sequences.sql are the only two that do), so this is inert for
	 * filtering today, exactly as the old full-remainder fallback was.
	 */
	size_t bytes = strlen(rlNamespace) + 1 + strlen(item->name) + 1 +
				   strlen(item->owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->restoreListName, bytes, "%s %s %s",
			rlNamespace, item->name, item->owner);

	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	/* useListRemainder reproduces the full original line, as ever */
	size_t ulrBytes = strlen(rlNamespace) + 1 +
					  strlen(item->objectTypeText) + 1 +
					  strlen(objTypeRemainder) + 1;

	item->useListRemainder = (char *) calloc(ulrBytes, sizeof(char));

	if (item->useListRemainder == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	sformat(item->useListRemainder, ulrBytes, "%s %s %s",
			rlNamespace, item->objectTypeText, objTypeRemainder);

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
 * the object type's own remainder (a relation, column, constraint,
 * trigger, policy or rule name), which pg_dump builds with fmtId() and so
 * can be double-quoted; the lexer's OBJTYPEBODY/OBJTYPEQIDENT states
 * handle that unescaping (see archive_toc_lexer.l), this function only
 * consumes the resulting tokens.
 *
 * The object type itself is recognized by re-entering the same flex
 * lexer on the substring starting at its own keyword text: by the time
 * this runs, every token already pulled from the outer line has been
 * copied out (rawRemainder is a heap copy, independent of the lexer's
 * scan buffer), so reusing the lexer here is safe.
 *
 * On success, populates item->restoreListName (the filtering-relevant
 * name -- the owning relation's for a relation-scoped object type, see
 * finishRelationAclOrComment) and the structured objectType/hasNamespace/
 * nspname/relname/name/owner fields. On failure (an object type this
 * function doesn't know how to split, or malformed input), leaves
 * item->restoreListName untouched, so the caller's raw-text fallback (the
 * whole, unsplit remainder) stands -- exactly as "we don't support this
 * one yet" has always been handled here.
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

	/*
	 * p now points right at the object type's own keyword text.
	 * archive_toc_lex_object_type_line (rather than archive_toc_lex_line)
	 * arms OBJTYPEBODY tokenizing for whichever object type owns an
	 * identifier of its own.
	 */
	archive_toc_lex_object_type_line(p);

	if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_DESC)
	{
		return false;
	}

	ArchiveItemDesc objectType = archive_toc_last_desc;

	item->objectType = objectType;
	item->objectTypeText = strdup(archive_toc_last_desc_text);

	if (item->objectTypeText == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	item->hasNamespace = hasNamespace;
	strlcpy(item->nspname, nspname, sizeof(item->nspname));

	switch (objectType)
	{
		case ARCHIVE_TAG_SCHEMA:
		{
			/* "- nspname rolname"; the outer namespace is unused here */
			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
			{
				return false;
			}

			const char *objTypeRemainder = archive_toc_text;

			if (*objTypeRemainder == ' ')
			{
				objTypeRemainder++;
			}

			size_t bytes = strlen(objTypeRemainder) + 3;

			item->restoreListName = (char *) calloc(bytes, sizeof(char));

			if (item->restoreListName == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			sformat(item->restoreListName, bytes, "- %s", objTypeRemainder);
			item->tagType = ARCHIVE_TAG_TYPE_SCHEMA;

			size_t ulrBytes = strlen(item->objectTypeText) + strlen(objTypeRemainder) + 4;

			item->useListRemainder = (char *) calloc(ulrBytes, sizeof(char));

			if (item->useListRemainder == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			sformat(item->useListRemainder, ulrBytes, "- %s %s",
					item->objectTypeText, objTypeRemainder);

			return true;
		}

		case ARCHIVE_TAG_EXTENSION:
		{
			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_REMAINDER)
			{
				return false;
			}

			const char *objTypeRemainder = archive_toc_text;

			if (*objTypeRemainder == ' ')
			{
				objTypeRemainder++;
			}

			/*
			 * The extension name is followed by a space, even when there
			 * is no owner to follow that space: trim it so it doesn't end
			 * up as a trailing space on the extension's own name.
			 */
			char extname[PG_NAMEDATALEN] = { 0 };

			strlcpy(extname, objTypeRemainder, sizeof(extname));

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

			/*
			 * objTypeRemainder, not extname: pg_restore keeps the trailing
			 * space after the extension name even with no owner to
			 * follow it, and useListRemainder reproduces the line as-is.
			 */
			size_t ulrBytes = strlen(item->objectTypeText) + strlen(objTypeRemainder) + 4;

			item->useListRemainder = (char *) calloc(ulrBytes, sizeof(char));

			if (item->useListRemainder == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			sformat(item->useListRemainder, ulrBytes, "- %s %s",
					item->objectTypeText, objTypeRemainder);

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

			char relname[PG_NAMEDATALEN] = { 0 };

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(relname, archive_toc_ident, sizeof(relname));

			bool relnameQuoted = archive_toc_ident_quoted;

			return finishRelationAclOrComment(item, nspname, relname, "",
											  relnameQuoted, false);
		}

		case ARCHIVE_TAG_COLUMN:
		{
			if (!hasNamespace)
			{
				return false;
			}

			/*
			 * colname here is what pg_dump itself calls a sub-object name
			 * (subname/objsubid in dumpACL's own parameters, pg_dump.c): a
			 * column is just the one kind of sub-object this format ever
			 * puts in an ACL/COMMENT tag.
			 */
			char relname[PG_NAMEDATALEN] = { 0 };
			char colname[PG_NAMEDATALEN] = { 0 };

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(relname, archive_toc_ident, sizeof(relname));

			bool relnameQuoted = archive_toc_ident_quoted;

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_DOT ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(colname, archive_toc_ident, sizeof(colname));

			return finishRelationAclOrComment(item, nspname, relname, colname,
											  relnameQuoted,
											  archive_toc_ident_quoted);
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

			char name[PG_NAMEDATALEN] = { 0 };
			char relname[PG_NAMEDATALEN] = { 0 };

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(name, archive_toc_ident, sizeof(name));

			bool nameQuoted = archive_toc_ident_quoted;

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_ON ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE)
			{
				return false;
			}

			int next = archive_toc_lex();

			/* domain constraints are "CONSTRAINT name ON DOMAIN domain" */
			if (next == ARCHIVE_TOC_TOKEN_DOMAIN)
			{
				return false;
			}

			if (next != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(relname, archive_toc_ident, sizeof(relname));

			return finishRelationAclOrComment(item, nspname, relname, name,
											  archive_toc_ident_quoted,
											  nameQuoted);
		}

		case ARCHIVE_TAG_FUNCTION:
		case ARCHIVE_TAG_PROCEDURE:
		case ARCHIVE_TAG_AGGREGATE:
		{
			if (!hasNamespace)
			{
				return false;
			}

			char funcname[PG_NAMEDATALEN] = { 0 };

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(funcname, archive_toc_ident, sizeof(funcname));

			bool funcnameQuoted = archive_toc_ident_quoted;

			/* no space between the name and its own argument list */
			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_ARGS)
			{
				return false;
			}

			char argsText[BUFSIZE] = { 0 };
			strlcpy(argsText, archive_toc_args, sizeof(argsText));

			return finishFunctionLikeAclOrComment(item, nspname, funcname,
												  argsText, funcnameQuoted);
		}

		case ARCHIVE_TAG_OPERATOR:
		{
			if (!hasNamespace)
			{
				return false;
			}

			char symbol[PG_NAMEDATALEN] = { 0 };

			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_IDENT)
			{
				return false;
			}

			strlcpy(symbol, archive_toc_ident, sizeof(symbol));

			/* "op (leftarg-or-NONE, rightarg-or-NONE)", see pg_dump.c's
			 * dumpOpr: there IS a space before the parenthesized pair,
			 * unlike a function's own immediately-attached argument list */
			if (archive_toc_lex() != ARCHIVE_TOC_TOKEN_SPACE ||
				archive_toc_lex() != ARCHIVE_TOC_TOKEN_ARGS)
			{
				return false;
			}

			char argsText[BUFSIZE] = { 0 };
			strlcpy(argsText, archive_toc_args, sizeof(argsText));

			return finishOperatorAclOrComment(item, nspname, symbol, argsText);
		}

		case ARCHIVE_TAG_CAST:
		{
			/* COMMENT ON CAST only -- a CAST can't be GRANTed on */
			char *objTypeRemainder = NULL;

			if (!pullOpaqueObjectTypeRemainder(item, &objTypeRemainder))
			{
				return false;
			}

			splitCastTypes(item->name, item);

			bool ok = buildOpaqueAclOrCommentNames(item, hasNamespace, nspname,
												   objTypeRemainder);

			free(objTypeRemainder);

			return ok;
		}

		case ARCHIVE_TAG_TRANSFORM:
		{
			/* COMMENT ON TRANSFORM only -- same reason as CAST above */
			char *objTypeRemainder = NULL;

			if (!pullOpaqueObjectTypeRemainder(item, &objTypeRemainder))
			{
				return false;
			}

			transformSplit(item->name, item);

			bool ok = buildOpaqueAclOrCommentNames(item, hasNamespace, nspname,
												   objTypeRemainder);

			free(objTypeRemainder);

			return ok;
		}

		case ARCHIVE_TAG_OPERATOR_CLASS:
		case ARCHIVE_TAG_OPERATOR_FAMILY:
		{
			/*
			 * "name USING amname" (dumpOpclass/dumpOpfamily, pg_dump.c) --
			 * narrow item->name down to the bare object name and move the
			 * access method into item->accessMethod, same "overlapping
			 * fields" split as CAST/TRANSFORM above (see pgcmd.h).
			 */
			char *objTypeRemainder = NULL;

			if (!pullOpaqueObjectTypeRemainder(item, &objTypeRemainder))
			{
				return false;
			}

			splitOperatorClassFamilyUsing(item);

			bool ok = buildOpaqueAclOrCommentNames(item, hasNamespace, nspname,
												   objTypeRemainder);

			free(objTypeRemainder);

			return ok;
		}

		default:
		{
			/*
			 * TYPE, DOMAIN, LANGUAGE, ACCESS METHOD, ...: handle every
			 * remaining object type whose own top-level (non-ACL) shape is
			 * ARCHIVE_SHAPE_SIMPLE the same way splitSimpleOrRelationRemainder
			 * does for a top-level entry of the same tag -- their own name
			 * can contain embedded spaces, but the owner is still reliably
			 * the last space-delimited token. FUNCTION/PROCEDURE/AGGREGATE,
			 * OPERATOR, CAST, TRANSFORM, OPERATOR CLASS/FAMILY have their
			 * own dedicated cases above instead, since their own remainder
			 * has further regular structure worth splitting out. USER_
			 * MAPPING and DEFAULT_ACL genuinely never reach this function
			 * at all: they're each their own distinct outer tag (not ACL
			 * or COMMENT), handled by applyTopLevelSignatureExtras instead
			 * -- neither a user mapping nor a default ACL entry can itself
			 * be GRANTed on or COMMENTed on.
			 */
			if (archive_toc_shape_for_desc(objectType) != ARCHIVE_SHAPE_SIMPLE)
			{
				return false;
			}

			char *objTypeRemainder = NULL;

			if (!pullOpaqueObjectTypeRemainder(item, &objTypeRemainder))
			{
				return false;
			}

			bool ok = buildOpaqueAclOrCommentNames(item, hasNamespace, nspname,
												   objTypeRemainder);

			free(objTypeRemainder);

			return ok;
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
		else
		{
			applyTopLevelSignatureExtras(item);
		}
	}

	return true;
}
