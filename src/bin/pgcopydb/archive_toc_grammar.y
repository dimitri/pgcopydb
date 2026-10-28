%{
/*
 * src/bin/pgcopydb/archive_toc_grammar.y
 *     bison grammar for a whole pg_restore --list line, tokenized by
 *     archive_toc_lexer.l. Replaces the previous revision's hand-written
 *     recursive-descent dispatch (a shape table plus per-shape C
 *     functions in archive_toc_parser.c, splitting already-matched text
 *     with strchr()/strstr()): every production below writes directly
 *     into the ArchiveContentItem passed in as %parse-param, so there is
 *     no intermediate opaque string for any C code to re-split.
 *
 * The grammar mirrors the dispatch the previous archiveTocShapeTable and
 * parseAclOrCommentRemainder's switch encoded: a handful of distinct
 * "shapes" (SIMPLE's plain "namespace name owner", RELATION's
 * "namespace relname name owner", a function-like's own balanced
 * argument list, CAST's "(source AS target)", ...), each with its own
 * token (see archive_toc_lexer.l's TOP_ and NESTED_ token groups) rather
 * than a runtime switch on ArchiveItemDesc -- bison's own generated
 * parse table does that dispatch now, not a C function.
 *
 * Only one thing stays genuinely a C-level decision rather than a
 * grammar one: WHICH shape a given ArchiveItemDesc maps to is still a
 * lookup (archive_toc_tag_token, archive_toc_lexer.l), because dozens of
 * unrelated tags share the exact same shape (TYPE, DOMAIN, LANGUAGE, ...
 * all parse identically) -- that's not an ambiguity a grammar should
 * resolve, it's recognizing genuinely identical rules and not repeating
 * them 39 times over.
 */

#include <stdlib.h>
#include <string.h>

#include "parson.h"

#include "pgcmd.h"
#include "string_utils.h"
#include "log.h"
#include "archive_toc_lexer.h"

int archive_toc_lex(void);
void archive_toc_error(ArchiveContentItem *item, const char *msg);

static char *archiveTocJoinWords(char *acc, char *word);
static char *archiveTocConcat(char *acc, char *piece);
static char *archiveTocWrapParen(char *inner);
static char *archiveTocAppendArg(char *accText, char *arg);
static void archiveTocBuildSimpleNames(ArchiveContentItem *item,
										const char *name, const char *owner);
static void archiveTocBuildRelationNames(ArchiveContentItem *item,
										  const char *relname,
										  const char *name,
										  const char *owner);
static void archiveTocBuildOpaqueNames(ArchiveContentItem *item,
										const char *rlNamespace,
										const char *body,
										const char *owner);
static void archiveTocSetOperatorArg(bool *hasArg, char **arg, char *text);
static char *archiveTocSlice(const char *start, const char *end);
static void archiveTocBuildUseListRemainder(ArchiveContentItem *item,
											 const char *bodyStart,
											 const char *bodyEnd,
											 const char *owner);
static void archiveTocOverrideRestoreListName(ArchiveContentItem *item,
											   const char *bodyStart,
											   const char *bodyEnd,
											   const char *owner);
%}

/*
 * %code requires (unlike the plain %{ %} prologue above) is emitted into
 * BOTH archive_toc_grammar.c and the generated archive_toc_grammar.h --
 * needed here because ArchiveArgList is one of the %union's own member
 * types below, and that union itself has to be visible from the
 * generated header (archive_toc_lexer.l includes it, to set
 * archive_toc_lval).
 */
%code requires {
#include "pgcmd.h"

/*
 * A function/procedure/aggregate's own balanced-parentheses argument
 * list, built up one comma-separated typename at a time: items/count
 * hand off directly to item->signatureArgs/signatureArgsCount, text is
 * the full verbatim reconstruction for item->signatureArgsText.
 */
typedef struct ArchiveArgList
{
	char **items;
	int count;
	char *text;
} ArchiveArgList;
}

%define api.prefix {archive_toc_}
%define parse.error verbose
%parse-param { ArchiveContentItem *item }

%union
{
	uint64_t oid;
	char *str;
	const char *pos;

	struct
	{
		ArchiveItemDesc desc;
		char *text;
	} tag;

	struct
	{
		char *text;
		bool quoted;
		const char *end;
	} word;

	struct
	{
		char *text;
		const char *end;
	} span;

	ArchiveArgList arglist;
}

%token <oid> TOK_OID
%token TOK_SEMICOLON TOK_SPACE TOK_UNKNOWN TOK_EOF
%token TOK_DASH TOK_LPAREN TOK_RPAREN TOK_COMMA TOK_DOT
%token TOK_AS TOK_FOR TOK_LANGUAGE_KW TOK_SERVER_KW TOK_USING TOK_ON TOK_DOMAIN
%token TOK_NO_OWNER
%token <word> TOK_WORD TOK_LASTWORD
%token <tag> TOK_ACL TOK_COMMENT
%token <tag> TOK_TOP_SIMPLE TOK_TOP_RELATION TOK_TOP_FUNCLIKE TOK_TOP_OPERATOR
%token <tag> TOK_TOP_CAST TOK_TOP_TRANSFORM TOK_TOP_USER_MAPPING TOK_TOP_DEFAULT_ACL
%token <tag> TOK_NESTED_GENERIC TOK_NESTED_EXTENSION TOK_NESTED_RELATION_LIKE
%token <tag> TOK_NESTED_COLUMN TOK_NESTED_ON_RELATION TOK_NESTED_FUNCLIKE
%token <tag> TOK_NESTED_OPERATOR TOK_NESTED_CAST TOK_NESTED_TRANSFORM
%token <tag> TOK_NESTED_OPCLASS_FAMILY

%type <str> namespace_field typename paren_group paren_inner
%type <str> paren_inner_item owner_text
%type <arglist> arglist args
%type <pos> mark
%type <span> wordname

%start toc_line

%%

toc_line:
	TOK_OID TOK_SEMICOLON TOK_SPACE TOK_OID TOK_SPACE TOK_OID TOK_SPACE entry
		{
			item->dumpId = (int) $1;
			item->catalogOid = (uint32_t) $4;
			item->objectOid = (uint32_t) $6;
		}
	;

/*
 * owner_text is the one boundary this format encodes positionally, not
 * with its own keyword: an entry's owner is simply the last space-
 * delimited token on the line, or absent (a lone trailing space with
 * nothing after it). TOK_LASTWORD/TOK_NO_OWNER are flex's own $-anchored
 * twin of TOK_WORD and the ordinary "trailing space" rule -- see
 * archive_toc_lexer.l's file header -- so this production never has to
 * look ahead past the end of the line itself; by the time either token
 * is shifted, there is nothing else left to shift.
 */
owner_text:
	TOK_NO_OWNER                        { $$ = strdup(""); }
	| TOK_SPACE TOK_LASTWORD             { $$ = $2.text; }
	;

namespace_field:
	TOK_DASH                            { $$ = NULL; }
	| TOK_WORD                          { $$ = $1.text; }
	;

/*
 * mark is a zero-width mid-rule placeholder: wherever a production
 * needs the exact source text of a span it just finished matching
 * (rather than reassembling it from already-tokenized pieces, see
 * archive_toc_lexer.l's own file header on archive_toc_token_end), it
 * puts `mark` right after that span and reads this value back later in
 * the SAME production. Safe specifically because a mid-rule action like
 * this one reduces the instant its (empty) right-hand side is reachable
 * -- deterministically, this grammar has no ambiguity about whether to
 * reduce it -- so it runs before archive_toc_lex() is ever called again
 * for lookahead, and archive_toc_token_end still reflects the token
 * this `mark` was placed after, not some later one. An ordinary action
 * at the END of a rule has no such guarantee (bison may already have
 * fetched the next token's lookahead by then), which is exactly why
 * every other token value in this grammar flows through archive_toc_
 * lval (see archive_toc_lexer.l) instead of a plain global read lazily
 * -- this is the one deliberate, narrow exception, used only this way.
 */
mark:
	/* empty */                          { $$ = archive_toc_token_end; }
	;

/*
 * wordname accumulates a name that may run to several space-delimited
 * words (FUNCTION/PROCEDURE/AGGREGATE's own tag text is never fmtId()-
 * quoted, even with an embedded space, see tests/toc-coverage/
 * fixture.sql's "Fixture Weird Fn") with no further internal structure
 * -- unlike typename below, it never attaches a parenthesized group of
 * its own.
 */
wordname:
	TOK_WORD
		{
			$$.text = $1.text;
			$$.end = $1.end;
		}
	| wordname TOK_SPACE TOK_WORD
		{
			$$.text = archiveTocJoinWords($1.text, $3.text);
			$$.end = $3.end;
		}
	;

/*
 * typename is wordname's counterpart for an actual Postgres type-name
 * expression (a CAST's source/target, a TRANSFORM's own type, one
 * function argument's type): same multi-word accumulation, plus a
 * directly-attached (no separating space) parenthesized group for a
 * type's own typmod, e.g. "numeric(10,2)" -- recognized by ordinary
 * grammar recursion (paren_group below), not a hand-counted depth
 * variable.
 */
typename:
	TOK_WORD                            { $$ = $1.text; }
	| typename TOK_SPACE TOK_WORD        { $$ = archiveTocJoinWords($1, $3.text); }
	| typename paren_group               { $$ = archiveTocConcat($1, $2); }
	| typename TOK_DOT TOK_WORD
		{
			char *dotted = archiveTocConcat(strdup("."), $3.text);
			$$ = archiveTocConcat($1, dotted);
		}
	;

paren_group:
	TOK_LPAREN paren_inner TOK_RPAREN    { $$ = archiveTocWrapParen($2); }
	;

paren_inner:
	/* empty */                          { $$ = strdup(""); }
	| paren_inner paren_inner_item        { $$ = archiveTocConcat($1, $2); }
	;

paren_inner_item:
	TOK_WORD                            { $$ = $1.text; }
	| TOK_COMMA                          { $$ = strdup(","); }
	| TOK_SPACE                          { $$ = strdup(" "); }
	| paren_group                        { $$ = $1; }
	;

/*
 * arglist is a FUNCTION/PROCEDURE/AGGREGATE/OPERATOR's own balanced-
 * parentheses argument list: zero or more typenames, comma-and-space
 * separated (pg_dump always writes ", " between arguments). Each
 * argument is kept exactly as pg_get_function_identity_arguments() or
 * format_function_signature() printed it -- see pgcmd.h's own note on
 * signatureArgsText -- not lexed any further than typename already goes.
 */
arglist:
	/* empty */
		{
			$$.items = NULL;
			$$.count = 0;
			$$.text = strdup("");
		}
	| args                               { $$ = $1; }
	;

args:
	typename
		{
			$$.items = (char **) calloc(1, sizeof(char *));
			$$.items[0] = $1;
			$$.count = 1;
			$$.text = strdup($1);
		}
	| args TOK_COMMA TOK_SPACE typename
		{
			$$ = $1;
			$$.items = (char **) realloc($$.items,
										  ($$.count + 1) * sizeof(char *));
			$$.items[$$.count] = $4;
			$$.count++;
			$$.text = archiveTocAppendArg($$.text, $4);
		}
	;

entry:
	TOK_ACL namespace_field TOK_SPACE
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->isCompositeTag = true;
			item->shape = ARCHIVE_SHAPE_ACL_OR_COMMENT;
			item->tagKind = ARCHIVE_TAG_KIND_ACL;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archive_toc_begin_nested_tag();
		}
	  nested_entry
	| TOK_COMMENT namespace_field TOK_SPACE
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->isCompositeTag = true;
			item->shape = ARCHIVE_SHAPE_ACL_OR_COMMENT;
			item->tagKind = ARCHIVE_TAG_KIND_COMMENT;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archive_toc_begin_nested_tag();
		}
	  nested_entry

	| TOK_TOP_SIMPLE namespace_field TOK_SPACE wordname owner_text
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_SIMPLE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archiveTocBuildSimpleNames(item, $4.text, $5);
			free($4.text);
			free($5);
		}

	| TOK_TOP_RELATION namespace_field TOK_SPACE mark TOK_WORD TOK_SPACE wordname owner_text
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_RELATION;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			/*
			 * archiveTocBuildRelationNames's own restoreListName (the
			 * owning relation's name only, "nspname relname owner") is
			 * the right contract for a NESTED ACL/COMMENT entry (see
			 * finishRelationAclOrComment's old rationale -- filtering
			 * matches the RELATION, not the ACL/COMMENT itself), but
			 * NOT for this top-level entry, which has no separate
			 * "owning relation" at all: relname and name here are both
			 * just this ONE entry's own fields (e.g. DEFAULT's own
			 * column name), so restoreListName needs the full verbatim
			 * body (both fields), the same as every other top-level
			 * shape -- overridden right after, via the usual verbatim
			 * slice ($7.end, not a trailing `mark`, for the same
			 * reason as TOP_DEFAULT_ACL's own).
			 */
			archiveTocBuildRelationNames(item, $5.text, $7.text, $8);
			archiveTocOverrideRestoreListName(item, $4, $7.end, $8);

			free($5.text);
			free($7.text);
			free($8);
		}

	| TOK_TOP_FUNCLIKE namespace_field TOK_SPACE mark wordname TOK_LPAREN arglist TOK_RPAREN mark owner_text
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_SIMPLE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			bool isStar = ($7.count == 1 && streq($7.items[0], "*"));

			if (isStar)
			{
				free($7.items[0]);
				free($7.items);
				item->signatureArgs = NULL;
				item->signatureArgsCount = 0;
			}
			else
			{
				item->signatureArgs = $7.items;
				item->signatureArgsCount = $7.count;
			}

			item->signatureArgsText = $7.text;

			/*
			 * item->name stays narrowed to the bare function name
			 * ($5.text, matching the previous revision's own
			 * splitPlainFunctionSignature contract), but restoreListName
			 * keeps the FULL verbatim text, signature included --
			 * every other top-level shape's own raw-remainder-preserved
			 * behavior, overridden here since archiveTocBuildSimpleNames
			 * would otherwise build it from the already-narrowed name.
			 */
			archiveTocBuildSimpleNames(item, $5.text, $10);
			archiveTocOverrideRestoreListName(item, $4, $9, $10);

			free($5.text);
			free($10);
		}

	| TOK_TOP_OPERATOR namespace_field TOK_SPACE TOK_WORD owner_text
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_SIMPLE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archiveTocBuildSimpleNames(item, $4.text, $5);
			free($4.text);
			free($5);
		}

	| TOK_TOP_CAST namespace_field TOK_SPACE mark TOK_WORD TOK_SPACE TOK_LPAREN typename TOK_SPACE TOK_AS TOK_SPACE typename TOK_RPAREN mark owner_text
		{
			/*
			 * $5 (the repeated "CAST " tag text pg_dump's own labelq
			 * includes, see dumpCast in pg_dump.c) is a plain TOK_WORD,
			 * not wordname: unlike FUNCTION's own name, it is always
			 * exactly this one fixed word, never multi-word -- using
			 * wordname here anyway would make it greedily try to
			 * extend through the space before "(" as if it might be
			 * continuing a longer name, which LALR(1) can't walk back
			 * out of (it would need to see one more token, TOK_WORD vs
			 * TOK_LPAREN, before deciding whether to shift that
			 * space). The explicit TOK_SPACE around TOK_AS matters for
			 * the exact same reason: without it, after the source
			 * typename reduces, the only registered continuation for a
			 * TOK_SPACE lookahead is typename's OWN "more words" rule,
			 * so it shifts that space expecting another TOK_WORD and
			 * then chokes on the real TOK_AS that follows.
			 */
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_OPAQUE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			item->castSourceType = strdup($8);
			item->castTargetType = strdup($12);

			char *body = archiveTocSlice($4, $14);
			archiveTocBuildSimpleNames(item, body, $15);
			free(body);

			free($5.text);
			free($8);
			free($12);
			free($15);
		}

	| TOK_TOP_TRANSFORM namespace_field TOK_SPACE mark TOK_WORD TOK_SPACE TOK_FOR TOK_SPACE typename TOK_SPACE TOK_LANGUAGE_KW TOK_SPACE TOK_WORD mark owner_text
		{
			/* $5: same reasoning as TOK_TOP_CAST's own $5, above. */
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_OPAQUE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			item->transformType = strdup($9);
			strlcpy(item->transformLanguage, $13.text,
					sizeof(item->transformLanguage));

			char *body = archiveTocSlice($4, $14);
			archiveTocBuildSimpleNames(item, body, $15);
			free(body);

			free($5.text);
			free($9);
			free($13.text);
			free($15);
		}

	| TOK_TOP_USER_MAPPING namespace_field TOK_SPACE mark TOK_WORD TOK_SPACE TOK_WORD TOK_SPACE TOK_WORD TOK_SPACE TOK_SERVER_KW TOK_SPACE TOK_WORD mark owner_text
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_OPAQUE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			strlcpy(item->userMappingUser, $9.text,
					sizeof(item->userMappingUser));
			strlcpy(item->userMappingServer, $13.text,
					sizeof(item->userMappingServer));

			char *body = archiveTocSlice($4, $14);
			archiveTocBuildSimpleNames(item, body, $15);
			free(body);

			free($5.text);
			free($7.text);
			free($9.text);
			free($13.text);
			free($15);
		}

	| TOK_TOP_DEFAULT_ACL namespace_field TOK_SPACE mark TOK_WORD TOK_SPACE TOK_WORD TOK_SPACE TOK_FOR TOK_SPACE wordname owner_text
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_OPAQUE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			strlcpy(item->defaultAclObjectType, $11.text,
					sizeof(item->defaultAclObjectType));

			/*
			 * bodyEnd comes from wordname's own $11.end (set when its
			 * last TOK_WORD was lexed, see archive_toc_lexer.l), not a
			 * trailing `mark` here: wordname's own recursive rule and
			 * owner_text's "has an owner" rule both start with
			 * TOK_SPACE, so a `mark` placed between them would force
			 * bison to decide, with only the TOK_SPACE itself as
			 * lookahead, whether to reduce the (always-empty) mark or
			 * keep extending wordname -- a genuine shift/reduce
			 * conflict LALR(1) can't resolve locally (it would need to
			 * see one more token, TOK_WORD vs TOK_LASTWORD, to know).
			 * Reading the end position off wordname's own already-
			 * captured value sidesteps the conflict entirely.
			 */
			char *body = archiveTocSlice($4, $11.end);
			archiveTocBuildSimpleNames(item, body, $12);
			free(body);

			free($5.text);
			free($7.text);
			free($11.text);
			free($12);
		}
	;

/*
 * nested_entry parses the object type embedded inside an ACL/COMMENT
 * entry's remainder (see archive_toc_begin_nested_tag's own doc for how
 * the lexer re-enters tag-keyword matching to recognize it here), and
 * builds item->objectType/objectTypeText plus whichever shape-specific
 * fields that object type's own remainder has -- the nested counterpart
 * to `entry`'s own top-level productions above, one level narrower:
 * there is no namespace field of its own to parse (the ACL/COMMENT's own
 * one, already consumed, covers it).
 */
nested_entry:
	TOK_NESTED_RELATION_LIKE TOK_SPACE mark TOK_WORD mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $4.text, "", $6);
			archiveTocBuildUseListRemainder(item, $3, $5, $6);
			free($4.text);
			free($6);
		}

	| TOK_NESTED_COLUMN TOK_SPACE mark TOK_WORD TOK_DOT TOK_WORD mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $4.text, $6.text, $8);
			archiveTocBuildUseListRemainder(item, $3, $7, $8);
			free($4.text);
			free($6.text);
			free($8);
		}

	| TOK_NESTED_ON_RELATION TOK_SPACE mark TOK_WORD TOK_SPACE TOK_ON TOK_SPACE TOK_WORD mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $8.text, $4.text, $10);
			archiveTocBuildUseListRemainder(item, $3, $9, $10);
			free($4.text);
			free($8.text);
			free($10);
		}

	| TOK_NESTED_FUNCLIKE TOK_SPACE mark TOK_WORD TOK_LPAREN arglist TOK_RPAREN mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			bool isStar = ($6.count == 1 && streq($6.items[0], "*"));

			if (isStar)
			{
				free($6.items[0]);
				free($6.items);
				item->signatureArgs = NULL;
				item->signatureArgsCount = 0;
			}
			else
			{
				item->signatureArgs = $6.items;
				item->signatureArgsCount = $6.count;
			}

			item->signatureArgsText = $6.text;

			archiveTocBuildSimpleNames(item, $4.text, $9);
			archiveTocBuildUseListRemainder(item, $3, $8, $9);
			free($4.text);
			free($9);
		}

	| TOK_NESTED_OPERATOR TOK_SPACE mark TOK_WORD TOK_SPACE TOK_LPAREN typename TOK_COMMA TOK_SPACE typename TOK_RPAREN mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			archiveTocSetOperatorArg(&item->operatorHasLeftArg,
									  &item->operatorLeftArg, $7);
			archiveTocSetOperatorArg(&item->operatorHasRightArg,
									  &item->operatorRightArg, $10);

			archiveTocBuildSimpleNames(item, $4.text, $13);
			archiveTocBuildUseListRemainder(item, $3, $12, $13);
			free($4.text);
			free($13);
		}

	| TOK_NESTED_CAST TOK_SPACE mark TOK_LPAREN typename TOK_SPACE TOK_AS TOK_SPACE typename TOK_RPAREN mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			item->castSourceType = strdup($5);
			item->castTargetType = strdup($9);

			char *body = archiveTocSlice($3, $11);
			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   body, $12);
			archiveTocBuildUseListRemainder(item, $3, $11, $12);
			free(body);

			free($5);
			free($9);
			free($12);
		}

	| TOK_NESTED_TRANSFORM TOK_SPACE mark TOK_FOR TOK_SPACE typename TOK_SPACE TOK_LANGUAGE_KW TOK_SPACE TOK_WORD mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			item->transformType = strdup($6);
			strlcpy(item->transformLanguage, $10.text,
					sizeof(item->transformLanguage));

			char *body = archiveTocSlice($3, $11);
			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   body, $12);
			archiveTocBuildUseListRemainder(item, $3, $11, $12);
			free(body);

			free($6);
			free($10.text);
			free($12);
		}

	| TOK_NESTED_OPCLASS_FAMILY TOK_SPACE mark wordname TOK_SPACE TOK_USING TOK_SPACE TOK_WORD mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			strlcpy(item->accessMethod, $8.text, sizeof(item->accessMethod));

			/*
			 * item->name/restoreListName use the NARROWED name ($4.text,
			 * "USING amname" stripped), matching the previous
			 * revision's own splitOperatorClassFamilyUsing contract --
			 * only useListRemainder reproduces the FULL verbatim text,
			 * "USING amname" included.
			 */
			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   $4.text, $10);
			archiveTocBuildUseListRemainder(item, $3, $9, $10);

			free($4.text);
			free($8.text);
			free($10);
		}

	| TOK_NESTED_EXTENSION TOK_SPACE mark TOK_WORD mark owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			strlcpy(item->name, $4.text, sizeof(item->name));
			strlcpy(item->owner, $6, sizeof(item->owner));
			item->tagType = ARCHIVE_TAG_TYPE_EXTENSION;

			/*
			 * restoreListName keeps its own, narrower pre-grammar
			 * contract for EXTENSION specifically (bare name, no
			 * namespace/owner -- unlike every other object type's
			 * "nspname name owner", see archiveTocBuildSimpleNames):
			 * no filters catalog query relies on it being anything
			 * else, and this preserves that exactly.
			 */
			item->restoreListName = strdup($4.text);

			archiveTocBuildUseListRemainder(item, $3, $5, $6);

			free($4.text);
			free($6);
		}

	| TOK_NESTED_GENERIC TOK_SPACE mark wordname owner_text
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			/*
			 * bodyEnd is $4.end (wordname's own end position), not a
			 * trailing `mark` -- see TOP_DEFAULT_ACL's own comment on
			 * why that specific placement is a genuine LALR(1)
			 * shift/reduce conflict with wordname's own recursion.
			 */
			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   $4.text, $5);
			archiveTocBuildUseListRemainder(item, $3, $4.end, $5);
			free($4.text);
			free($5);
		}
	;

%%

/*
 * archive_toc_error is bison's own error hook: a shape this grammar
 * doesn't recognize (or genuinely malformed input) is not fatal here
 * either -- the same "we don't support this one yet, the caller's raw-
 * text fallback stands" convention the previous hand-written parser
 * followed throughout (e.g. a domain's own CONSTRAINT, excluded on
 * purpose: "CONSTRAINT name ON DOMAIN domain" is a real shape pg_dump
 * can produce that this grammar doesn't structure, the same as before).
 * archive_toc_parse_line (archive_toc_parser.c) checks archive_toc_
 * parse()'s own return value and falls back there, so this only logs.
 */
void
archive_toc_error(ArchiveContentItem *item, const char *msg)
{
	(void) item;
	log_debug("archive_toc_grammar: %s", msg);
}


/*
 * archiveTocSlice returns the exact source text between two `mark`
 * values (see that nonterminal's own comment) as a freshly malloc'ed,
 * NUL-terminated copy -- byte for byte, including whatever original
 * spacing, parentheses or punctuation a hand-rebuilt sformat() would
 * otherwise have to guess back from already-tokenized pieces.
 */
static char *
archiveTocSlice(const char *start, const char *end)
{
	size_t len = end - start;
	char *out = (char *) malloc(len + 1);

	if (out == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return NULL;
	}

	memcpy(out, start, len); /* IGNORE-BANNED */
	out[len] = '\0';

	return out;
}


/*
 * archiveTocJoinWords appends `word` to `acc` (NULL for the first word)
 * with exactly one space between them -- pg_dump's own convention for
 * every multi-word name or type expression this format can contain --
 * freeing both inputs and returning a freshly malloc'ed result, the
 * single-ownership chain every nonterminal built up this way relies on.
 */
static char *
archiveTocJoinWords(char *acc, char *word)
{
	if (acc == NULL)
	{
		return word;
	}

	size_t bytes = strlen(acc) + 1 + strlen(word) + 1;
	char *out = (char *) malloc(bytes);

	if (out == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(acc);
		free(word);
		return NULL;
	}

	sformat(out, bytes, "%s %s", acc, word);
	free(acc);
	free(word);

	return out;
}


/*
 * archiveTocConcat is archiveTocJoinWords without the inserted space --
 * for attaching a parenthesized group directly after a type name (no
 * space before "(" in e.g. "numeric(10,2)"), and for paren_inner's own
 * verbatim reconstruction of whatever sits between a balanced pair.
 */
static char *
archiveTocConcat(char *acc, char *piece)
{
	size_t bytes = strlen(acc) + strlen(piece) + 1;
	char *out = (char *) malloc(bytes);

	if (out == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(acc);
		free(piece);
		return NULL;
	}

	sformat(out, bytes, "%s%s", acc, piece);
	free(acc);
	free(piece);

	return out;
}


/*
 * archiveTocWrapParen wraps `inner` (already reconstructed verbatim by
 * paren_inner) in its own literal parentheses, freeing it.
 */
static char *
archiveTocWrapParen(char *inner)
{
	size_t bytes = strlen(inner) + 3;
	char *out = (char *) malloc(bytes);

	if (out == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(inner);
		return NULL;
	}

	sformat(out, bytes, "(%s)", inner);
	free(inner);

	return out;
}


/*
 * archiveTocAppendArg extends an argument list's own verbatim
 * reconstruction (item->signatureArgsText) with one more ", "-separated
 * argument -- `arg` is NOT freed here (unlike archiveTocConcat/
 * archiveTocJoinWords's inputs): it is also stored directly into the
 * arglist's own items[] array by the args rule that calls this, so it
 * outlives this one string.
 */
static char *
archiveTocAppendArg(char *accText, char *arg)
{
	size_t bytes = strlen(accText) + 2 + strlen(arg) + 1;
	char *out = (char *) malloc(bytes);

	if (out == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		free(accText);
		return NULL;
	}

	sformat(out, bytes, "%s, %s", accText, arg);
	free(accText);

	return out;
}


/*
 * archiveTocBuildSimpleNames and archiveTocBuildRelationNames set
 * item->name/relname/owner, item->restoreListName ("nspname name
 * owner", the filtering-relevant name) and item->tagType, for a SIMPLE-
 * or RELATION-shaped top-level entry or a nested ACL/COMMENT object type
 * whose own identifier has no further structure worth splitting (a
 * relation's own name, a function/operator's own name, ...).
 * archiveTocBuildOpaqueNames is their OPAQUE-shaped counterpart, used
 * when `body` is the WHOLE remaining text rather than already-split
 * fields (a nested CAST/TRANSFORM/OPERATOR CLASS or FAMILY's own
 * remainder, or anything else this grammar doesn't structure further) --
 * mirroring the previous revision's buildOpaqueAclOrCommentNames.
 */
static void
archiveTocBuildSimpleNames(ArchiveContentItem *item, const char *name,
						   const char *owner)
{
	strlcpy(item->name, name, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	const char *nspname = item->hasNamespace ? item->nspname : "-";
	size_t bytes = strlen(nspname) + 1 + strlen(name) + 1 + strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return;
	}

	sformat(item->restoreListName, bytes, "%s %s %s", nspname, name, owner);
}


static void
archiveTocBuildRelationNames(ArchiveContentItem *item, const char *relname,
							 const char *name, const char *owner)
{
	strlcpy(item->relname, relname, sizeof(item->relname));
	strlcpy(item->name, name, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	const char *nspname = item->hasNamespace ? item->nspname : "-";
	size_t bytes = strlen(nspname) + 1 + strlen(relname) + 1 +
				   strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}
	else
	{
		sformat(item->restoreListName, bytes, "%s %s %s",
				nspname, relname, owner);
	}
}


static void
archiveTocBuildOpaqueNames(ArchiveContentItem *item, const char *rlNamespace,
						  const char *body, const char *owner)
{
	strlcpy(item->name, body, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	size_t bytes = strlen(rlNamespace) + 1 + strlen(body) + 1 +
				   strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}
	else
	{
		sformat(item->restoreListName, bytes, "%s %s %s",
				rlNamespace, body, owner);
	}
}


/*
 * archiveTocBuildUseListRemainder builds item->useListRemainder for a
 * nested ACL/COMMENT object type: "nspname objectTypeText body owner",
 * body sliced verbatim between the two marks the calling production
 * placed right after its own TAG token and right before owner_text --
 * see archive_toc_lexer.l's own file header and the `mark` nonterminal's
 * comment for why this is a byte-exact copy of the original line, not a
 * reassembly from already-split pieces (a quoted identifier round-trips
 * with its original quoting this way, with no separate re-quoting step
 * needed, unlike the previous revision's own quoteIdentForUseList).
 * When owner is empty, the trailing "%s" contributes nothing but its
 * own leading space, matching pg_restore's own convention of always
 * writing that one separating space even for an empty owner field.
 */
static void
archiveTocBuildUseListRemainder(ArchiveContentItem *item,
								const char *bodyStart, const char *bodyEnd,
								const char *owner)
{
	const char *rlNamespace = item->hasNamespace ? item->nspname : "-";
	char *body = archiveTocSlice(bodyStart, bodyEnd);

	if (body == NULL)
	{
		return;
	}

	size_t bytes = strlen(rlNamespace) + 1 + strlen(item->objectTypeText) +
				   1 + strlen(body) + 1 + strlen(owner) + 1;

	item->useListRemainder = (char *) calloc(bytes, sizeof(char));

	if (item->useListRemainder == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}
	else
	{
		sformat(item->useListRemainder, bytes, "%s %s %s %s",
				rlNamespace, item->objectTypeText, body, owner);
	}

	free(body);
}


/*
 * archiveTocOverrideRestoreListName replaces item->restoreListName
 * (just set by archiveTocBuildSimpleNames/RelationNames from an
 * already-narrowed field) with "nspname body owner", body sliced
 * verbatim between two marks -- for the handful of top-level shapes
 * whose own item->name is deliberately narrower than their restore
 * ListName ought to be: FUNCTION/PROCEDURE/AGGREGATE's own bare name
 * (signature stripped, see splitPlainFunctionSignature's old contract)
 * and RELATION's own relname (the second field, name, stripped) --
 * restoreListName, unlike item->name, has no such narrowing contract
 * for a top-level entry: every other top-level shape's own raw-
 * remainder-preserved restoreListName already includes everything, and
 * these two should too.
 */
static void
archiveTocOverrideRestoreListName(ArchiveContentItem *item,
								  const char *bodyStart, const char *bodyEnd,
								  const char *owner)
{
	free(item->restoreListName);
	item->restoreListName = NULL;

	const char *nspname = item->hasNamespace ? item->nspname : "-";
	char *body = archiveTocSlice(bodyStart, bodyEnd);

	if (body == NULL)
	{
		return;
	}

	size_t bytes = strlen(nspname) + 1 + strlen(body) + 1 + strlen(owner) + 1;

	item->restoreListName = (char *) calloc(bytes, sizeof(char));

	if (item->restoreListName == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}
	else
	{
		sformat(item->restoreListName, bytes, "%s %s %s", nspname, body, owner);
	}

	free(body);
}


/*
 * archiveTocSetOperatorArg sets one side of an OPERATOR's own operand
 * types (operatorLeftArg/operatorRightArg) from its already-reconstructed
 * typename text, "NONE" literally (dumpOpr's own text for a prefix or
 * postfix operator missing that side, pg_dump.c) meaning absent rather
 * than a real type name.
 */
static void
archiveTocSetOperatorArg(bool *hasArg, char **arg, char *text)
{
	*hasArg = !streq(text, "NONE");

	if (*hasArg)
	{
		*arg = text;
	}
	else
	{
		free(text);
		*arg = NULL;
	}
}
