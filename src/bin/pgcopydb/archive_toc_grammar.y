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
	int cap;        /* allocated size of items[], see archiveTocArgListAppend */
	char *text;
} ArchiveArgList;

/*
 * Declared here, inside %code requires, rather than in the plain %{ %}
 * prologue above: bison's own generated archive_toc_grammar.c includes
 * archive_toc_grammar.h (which is where %code requires's own content
 * also goes) AFTER that prologue, so a prologue-level declaration
 * referencing ArchiveArgList would reference a type that doesn't exist
 * yet at that point in the generated file.
 */
static void archiveTocArgListAppend(ArchiveArgList *list, char *item);
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

/*
 * Token names follow scan.l/gram.y's own convention rather than a
 * uniform  prefix -- see archive_toc_lexer.l's own file header for
 * why: it makes a production read like the input text it matches.
 */
%token <oid> OID
%token UNKNOWN
%token AS FOR LANGUAGE_KW SERVER_KW USING ON DOMAIN
%token NO_OWNER
%token <word> WORD LASTWORD
%token <pos> SPACE
%token <tag> ACL COMMENT
%token <tag> TOP_SIMPLE TOP_RELATION TOP_FUNCLIKE TOP_OPERATOR
%token <tag> TOP_CAST TOP_TRANSFORM TOP_USER_MAPPING TOP_DEFAULT_ACL
%token <tag> NESTED_GENERIC NESTED_EXTENSION NESTED_RELATION_LIKE
%token <tag> NESTED_COLUMN NESTED_ON_RELATION NESTED_FUNCLIKE
%token <tag> NESTED_OPERATOR NESTED_CAST NESTED_TRANSFORM
%token <tag> NESTED_OPCLASS_FAMILY
%token <pos> '(' ')' ',' '.'

%type <str> nspname typename paren_group paren_inner
%type <str> paren_inner_item owner
%type <arglist> arglist args
%type <pos> mark relname_item relname name
%type <span> wordname

%start toc_line

%%

toc_line:
	OID ';' OID OID entry
		{
			/*
			 * No explicit SPACE tokens here: INITIAL silently skips
			 * interior whitespace in this fixed dumpId/catalogOid/
			 * objectOid/TAG prefix (see archive_toc_lexer.l's own file
			 * header) since nothing here is ambiguous without it --
			 * OID and ';' are already unmistakably distinct token
			 * types. SPACE only needs to be a real, visible token once
			 * inside a TAG's own remainder, where it can be a genuine
			 * field boundary (RELATION's relname/name split) or part
			 * of a multi-word value.
			 */
			item->dumpId = (int) $1;
			item->catalogOid = (uint32_t) $3;
			item->objectOid = (uint32_t) $4;
		}
	;

/*
 * owner is the one boundary this format encodes positionally, not
 * with its own keyword: an entry's owner is simply the last space-
 * delimited token on the line, or absent (a lone trailing space with
 * nothing after it). LASTWORD/NO_OWNER are flex's own $-anchored
 * twin of WORD and the ordinary "trailing space" rule -- see
 * archive_toc_lexer.l's file header -- so this production never has to
 * look ahead past the end of the line itself; by the time either token
 * is shifted, there is nothing else left to shift.
 */
owner:
	NO_OWNER                        { $$ = strdup(""); }
	| SPACE LASTWORD             { $$ = $2.text; }
	;

nspname:
	'-'                            { $$ = NULL; }
	| WORD                          { $$ = $1.text; }
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
	WORD
		{
			$$.text = $1.text;
			$$.end = $1.end;
		}
	| wordname SPACE WORD
		{
			$$.text = archiveTocJoinWords($1.text, $3.text);
			$$.end = $3.end;
		}
	| wordname '.' WORD
		{
			/*
			 * An opaque top-level (or NESTED_GENERIC-style) name has no
			 * escaping convention at all (see TOCBODY's own file-header
			 * comment on raw vs. quote-aware text), so a literal '.' in
			 * one -- same deal as a literal '"' -- is just another
			 * character, not the schema-qualification separator
			 * typename's own '.' rule means. Glued directly, no
			 * space, same as typename's.
			 */
			char *dotted = archiveTocConcat(strdup("."), $3.text);
			$$.text = archiveTocConcat($1.text, dotted);
			$$.end = $3.end;
		}
	;

/*
 * relname and name are for a field the previous revision kept
 * as raw, unsplit text (no fmtId() quoting, no escaping convention at
 * all -- see TOCBODY's own file-header comment): rather than enumerate
 * every WORD/DOT/COMMA/LPAREN/RPAREN combination such a field could
 * contain (a losing game -- CI's own "Sp1eCial .Char" schema name,
 * space then dot then more text, is exactly the kind of combination
 * wordname's own narrower, enumerated rules above don't cover), these
 * consume a run of ANY of those tokens generically and carry only the
 * END position of the last one consumed ($$ here is never the
 * reconstructed text itself): the caller slices the exact source bytes
 * out between an earlier `mark` and this value (archiveTocSlice), the
 * same verbatim-capture idea `mark` itself exists for, rather than
 * rebuilding the text from pieces that have already lost the original
 * formatting. relname stops at the first SPACE (for a field that is
 * itself only ever the first space-delimited token, e.g. RELATION's own
 * relname); name also consumes SPACE, for a field that is
 * whatever text remains up to the owner boundary.
 */
relname_item:
	WORD                            { $$ = $1.end; }
	| '.'                           { $$ = $1; }
	| ','                         { $$ = $1; }
	| '('                        { $$ = $1; }
	| ')'                        { $$ = $1; }
	;

relname:
	relname_item                      { $$ = $1; }
	| relname relname_item          { $$ = $2; }
	;

/*
 * name is relname's own counterpart allowing embedded SPACEs, built on
 * TOP of relname (never directly on relname_item + a bare SPACE
 * alternative): name's own first token has to be unambiguous without
 * any lookahead, which relname already guarantees (relname_item never
 * includes SPACE), so name can never itself START with a SPACE -- only
 * sandwich one between two real relname chunks. That distinction is
 * why TOP_RELATION's own explicit separator SPACE between relname and
 * name (a real token in the input, consumed as its own grammar symbol
 * there) doesn't collide with name's own grammar: if name could ALSO
 * start with a bare SPACE, the one physical separator character
 * between relname and name would be genuinely ambiguous between
 * satisfying that explicit SPACE symbol or being absorbed as name's
 * own leading (and semantically meaningless) one.
 */
name:
	relname                          { $$ = $1; }
	| name SPACE relname              { $$ = $3; }
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
	WORD                            { $$ = $1.text; }
	| typename SPACE WORD        { $$ = archiveTocJoinWords($1, $3.text); }
	| typename paren_group               { $$ = archiveTocConcat($1, $2); }
	| typename '.' WORD
		{
			char *dotted = archiveTocConcat(strdup("."), $3.text);
			$$ = archiveTocConcat($1, dotted);
		}
	;

paren_group:
	'(' paren_inner ')'    { $$ = archiveTocWrapParen($2); }
	;

paren_inner:
	/* empty */                          { $$ = strdup(""); }
	| paren_inner paren_inner_item        { $$ = archiveTocConcat($1, $2); }
	;

paren_inner_item:
	WORD                            { $$ = $1.text; }
	| ','                          { $$ = strdup(","); }
	| SPACE                          { $$ = strdup(" "); }
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
			$$.cap = 0;
			$$.text = strdup("");
		}
	| args                               { $$ = $1; }
	;

args:
	typename
		{
			$$.items = NULL;
			$$.count = 0;
			$$.cap = 0;
			archiveTocArgListAppend(&$$, $1);
			$$.text = strdup($1);
		}
	| args ',' SPACE typename
		{
			$$ = $1;
			archiveTocArgListAppend(&$$, $4);
			$$.text = archiveTocAppendArg($$.text, $4);
		}
	;

entry:
	ACL nspname SPACE
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
	| COMMENT nspname SPACE
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

	| TOP_SIMPLE nspname SPACE mark name owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->shape = ARCHIVE_SHAPE_SIMPLE;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			char *body = archiveTocSlice($4, $5);
			archiveTocBuildSimpleNames(item, body, $6);
			free(body);
			free($6);
		}

	| TOP_RELATION nspname SPACE mark relname SPACE name owner
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
			 * relname/name are each sliced verbatim (see relname's own
			 * comment) rather than reconstructed from tokens: either
			 * can contain arbitrary punctuation with no escaping
			 * convention at all (CI's own "Sp1eCial .Char" schema name
			 * -- a space, then a literal dot, then more text -- is
			 * exactly why enumerating WORD/DOT/SPACE combinations by
			 * hand doesn't scale here).
			 *
			 * archiveTocBuildRelationNames's own restoreListName (the
			 * owning relation's name only) is the right contract for a
			 * NESTED ACL/COMMENT entry (see finishRelationAclOrComment's
			 * old rationale -- filtering matches the RELATION, not the
			 * ACL/COMMENT itself), but NOT for this top-level entry,
			 * which has no separate "owning relation" at all: relname
			 * and name here are both just this ONE entry's own fields
			 * (e.g. DEFAULT's own column name), so restoreListName
			 * needs the full verbatim body (both fields), the same as
			 * every other top-level shape -- overridden right after,
			 * covering both fields at once.
			 */
			char *relname = archiveTocSlice($4, $5);
			char *name = archiveTocSlice($6, $7);

			archiveTocBuildRelationNames(item, relname, name, $8);
			archiveTocOverrideRestoreListName(item, $4, $7, $8);

			free(relname);
			free(name);
			free($8);
		}

	| TOP_FUNCLIKE nspname SPACE mark wordname '(' arglist ')' mark owner
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

	| TOP_OPERATOR nspname SPACE WORD owner
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

	| TOP_CAST nspname SPACE mark WORD SPACE '(' typename SPACE AS SPACE typename ')' mark owner
		{
			/*
			 * $5 (the repeated "CAST " tag text pg_dump's own labelq
			 * includes, see dumpCast in pg_dump.c) is a plain WORD,
			 * not wordname: unlike FUNCTION's own name, it is always
			 * exactly this one fixed word, never multi-word -- using
			 * wordname here anyway would make it greedily try to
			 * extend through the space before "(" as if it might be
			 * continuing a longer name, which LALR(1) can't walk back
			 * out of (it would need to see one more token, WORD vs
			 * '(', before deciding whether to shift that
			 * space). The explicit SPACE around AS matters for
			 * the exact same reason: without it, after the source
			 * typename reduces, the only registered continuation for a
			 * SPACE lookahead is typename's OWN "more words" rule,
			 * so it shifts that space expecting another WORD and
			 * then chokes on the real AS that follows.
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

	| TOP_TRANSFORM nspname SPACE mark WORD SPACE FOR SPACE typename SPACE LANGUAGE_KW SPACE WORD mark owner
		{
			/* $5: same reasoning as TOP_CAST's own $5, above. */
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

	| TOP_USER_MAPPING nspname SPACE mark WORD SPACE WORD SPACE WORD SPACE SERVER_KW SPACE WORD mark owner
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

	| TOP_DEFAULT_ACL nspname SPACE mark WORD SPACE WORD SPACE FOR SPACE wordname owner
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
			 * last WORD was lexed, see archive_toc_lexer.l), not a
			 * trailing `mark` here: wordname's own recursive rule and
			 * owner's "has an owner" rule both start with
			 * SPACE, so a `mark` placed between them would force
			 * bison to decide, with only the SPACE itself as
			 * lookahead, whether to reduce the (always-empty) mark or
			 * keep extending wordname -- a genuine shift/reduce
			 * conflict LALR(1) can't resolve locally (it would need to
			 * see one more token, WORD vs LASTWORD, to know).
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
	NESTED_RELATION_LIKE SPACE mark WORD mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $4.text, "", $6);
			archiveTocBuildUseListRemainder(item, $3, $5, $6);
			free($4.text);
			free($6);
		}

	| NESTED_COLUMN SPACE mark WORD '.' WORD mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $4.text, $6.text, $8);
			archiveTocBuildUseListRemainder(item, $3, $7, $8);
			free($4.text);
			free($6.text);
			free($8);
		}

	| NESTED_ON_RELATION SPACE mark WORD SPACE ON SPACE WORD mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $8.text, $4.text, $10);
			archiveTocBuildUseListRemainder(item, $3, $9, $10);
			free($4.text);
			free($8.text);
			free($10);
		}

	| NESTED_FUNCLIKE SPACE mark WORD '(' arglist ')' mark owner
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

	| NESTED_OPERATOR SPACE mark WORD SPACE '(' typename ',' SPACE typename ')' mark owner
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

	| NESTED_CAST SPACE mark '(' typename SPACE AS SPACE typename ')' mark owner
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

	| NESTED_TRANSFORM SPACE mark FOR SPACE typename SPACE LANGUAGE_KW SPACE WORD mark owner
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

	| NESTED_OPCLASS_FAMILY SPACE mark name SPACE USING SPACE WORD mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			strlcpy(item->accessMethod, $8.text, sizeof(item->accessMethod));

			/*
			 * item->name/restoreListName use the NARROWED name, sliced
			 * verbatim from before "USING amname" (matching the
			 * previous revision's own splitOperatorClassFamilyUsing
			 * contract) -- only useListRemainder reproduces the FULL
			 * verbatim text, "USING amname" included.
			 */
			char *opclassName = archiveTocSlice($3, $4);

			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   opclassName, $10);
			archiveTocBuildUseListRemainder(item, $3, $9, $10);

			free(opclassName);
			free($8.text);
			free($10);
		}

	| NESTED_EXTENSION SPACE mark WORD mark owner
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

	| NESTED_GENERIC SPACE mark name owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			/*
			 * bodyEnd is $4 (name's own end position), not a trailing
			 * `mark` -- see TOP_DEFAULT_ACL's own comment on why that
			 * specific placement would be a genuine LALR(1) shift/
			 * reduce conflict with name's own recursion.
			 */
			char *genericName = archiveTocSlice($3, $4);

			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   genericName, $5);
			archiveTocBuildUseListRemainder(item, $3, $4, $5);
			free(genericName);
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
 * archiveTocArgListAppend appends one more argument to list->items,
 * growing the underlying allocation 10 slots at a time (list->cap)
 * rather than realloc()ing on every single argument: real argument
 * lists are short, but a pathological one (hundreds of columns in a
 * wide function signature, say) would otherwise realloc() once per
 * argument.
 */
static void
archiveTocArgListAppend(ArchiveArgList *list, char *item)
{
	if (list->count == list->cap)
	{
		list->cap += 10;
		list->items = (char **) realloc(list->items,
										 list->cap * sizeof(char *));

		if (list->items == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return;
		}
	}

	list->items[list->count++] = item;
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
 * placed right after its own TAG token and right before owner --
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
