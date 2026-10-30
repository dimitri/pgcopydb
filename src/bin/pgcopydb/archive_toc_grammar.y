%{
/*
 * src/bin/pgcopydb/archive_toc_grammar.y
 *     bison grammar for a whole pg_restore --list line, tokenized by
 *     archive_toc_lexer.l: every production below writes directly into
 *     the ArchiveContentItem passed in as %parse-param, so there is no
 *     intermediate opaque string for any C code to re-split.
 *
 * A handful of distinct remainder shapes (SIMPLE's plain "namespace
 * name owner", RELATION's "namespace relname name owner", a
 * function-like's own balanced argument list, CAST's "(source AS
 * target)", ...), each with its own token (see archive_toc_lexer.l's
 * TOP_ and NESTED_ token groups), drive bison's own generated parse
 * table -- there is no runtime switch on ArchiveItemDesc anywhere in
 * this file.
 *
 * Only one thing stays genuinely a C-level decision rather than a
 * grammar one: WHICH of these shapes a given ArchiveItemDesc maps to
 * is still a lookup (archive_toc_tag_token, archive_toc_lexer.l),
 * because dozens of unrelated tags share the exact same shape (TYPE,
 * DOMAIN, LANGUAGE, ... all parse identically) -- that's not an
 * ambiguity a grammar should resolve, it's recognizing genuinely
 * identical rules and not repeating them 39 times over.
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

static char *archiveTocWrapParen(PQExpBuffer inner);
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
static void archiveTocSetOperatorArg(bool *hasArg, char **arg,
									  PQExpBuffer text);
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
 * hand off directly to item->signature.args/argsCount, text accumulates
 * the full verbatim reconstruction for item->signature.argsText.
 *
 * text is a PQExpBuffer, not a char *, for the same O(n^2) reason
 * wordname/typename thread one below: with N arguments, a plain char *
 * rebuilt via strdup+free on every single comma copies the whole
 * growing string again each time. The entry actions that consume a
 * finished ArchiveArgList (TOP_FUNCLIKE, NESTED_FUNCLIKE) finalize
 * text->data into item->signature.argsText's own malloc'ed char * and
 * destroy the buffer there.
 */
typedef struct ArchiveArgList
{
	char **items;
	int count;
	int cap;        /* allocated size of items[], see archiveTocArgListAppend */
	PQExpBuffer text;
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
		PQExpBuffer buf;
		const char *end;
	} span;

	PQExpBuffer buf;

	ArchiveArgList arglist;
}

/*
 * Token names follow scan.l/gram.y's own convention rather than a
 * uniform  prefix -- see archive_toc_lexer.l's own file header for
 * why: it makes a production read like the input text it matches.
 */
%token <oid> OID
%token UNKNOWN
%token AS LANGUAGE_KW SERVER_KW USING ON DOMAIN
%token CAST_KW TRANSFORM_KW USER_MAPPING_KW DEFAULT_ACL_KW
/*
 * FOR carries its own start-of-keyword position (set in archive_toc_
 * lexer.l, like SPACE's own .pos): NESTED_TRANSFORM's body starts
 * right at "FOR" itself with no intervening nonterminal to anchor a
 * `mark` on (unlike every other keyword here, always preceded by a
 * WORD/typename/name that already reduces its own end boundary) --
 * see NESTED_TRANSFORM's own comment below.
 */
%token <pos> FOR
%token NO_OWNER NO_NSPNAME
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

%type <str> nspname paren_group
%type <str> paren_inner_item owner
%type <buf> typename paren_inner castSourceType castTargetType
%type <word> objname
%type <arglist> arglist args
%type <pos> mark relname_item relname name
%type <span> wordname

%start toc_line

%%

toc_line:
	/* 277; 1255 16452 FUNCTION toc_fixture fixture_add(integer, integer) postgres */
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
	NO_NSPNAME                    { $$ = NULL; }
	| WORD                          { $$ = $1.text; }
	;

/*
 * objname is WORD's own alias for the quote-aware nested object
 * identifier (TABLE/VIEW/.../FUNCTION/PROCEDURE/AGGREGATE/OPERATOR's
 * own name, see archive_toc_lexer.l's TOCQBODY) -- a plain passthrough,
 * kept as its own nonterminal only so NESTED_RELATION_LIKE/NESTED_
 * COLUMN/NESTED_ON_RELATION/NESTED_FUNCLIKE/NESTED_OPERATOR's own
 * productions below read as "this field is the nested object's name",
 * not an unexplained bare WORD.
 */
objname:
	WORD                            { $$ = $1; }
	;

/*
 * castSourceType/castTargetType are typename's own aliases for a
 * CAST's two sides, (source AS target) -- same production, named for
 * which side of AS it is in TOP_CAST/NESTED_CAST below.
 */
castSourceType:
	typename                        { $$ = $1; }
	;

castTargetType:
	typename                        { $$ = $1; }
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
 *
 * $$.buf is the SAME PQExpBuffer threaded through every alternative
 * that extends an existing wordname, appended to directly rather than
 * rebuilt into a brand new, one-byte-longer allocation each time (what
 * the removed archiveTocJoinWords/archiveTocConcat helpers used to do):
 * PQExpBuffer's own amortized-doubling growth makes this whole
 * production O(n) in the total text length instead of O(n^2) in the
 * word count, the same reason arglist's own archiveTocArgListAppend
 * grows its array 10 slots at a time rather than realloc()ing once per
 * argument.
 */
wordname:
	WORD
		{
			$$.buf = createPQExpBuffer();
			appendPQExpBufferStr($$.buf, $1.text);
			free($1.text);
			$$.end = $1.end;
		}
	| wordname SPACE WORD
		{
			$$ = $1;
			appendPQExpBufferChar($$.buf, ' ');
			appendPQExpBufferStr($$.buf, $3.text);
			free($3.text);
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
			$$ = $1;
			appendPQExpBufferChar($$.buf, '.');
			appendPQExpBufferStr($$.buf, $3.text);
			free($3.text);
			$$.end = $3.end;
		}
	;

/*
 * relname and name are for a field kept as raw, unsplit text (no
 * fmtId() quoting, no escaping convention at all -- see TOCBODY's own
 * file-header comment): rather than enumerate
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
 * function argument's type): same multi-word accumulation (see
 * wordname's own comment on why $$ threads one PQExpBuffer through
 * every alternative instead of rebuilding a new allocation each time),
 * plus a directly-attached (no separating space) parenthesized group
 * for a type's own typmod, e.g. "numeric(10,2)" -- recognized by
 * ordinary grammar recursion (paren_group below), not a hand-counted
 * depth variable.
 */
typename:
	WORD
		{
			$$ = createPQExpBuffer();
			appendPQExpBufferStr($$, $1.text);
			free($1.text);
		}
	| typename SPACE WORD
		{
			$$ = $1;
			appendPQExpBufferChar($$, ' ');
			appendPQExpBufferStr($$, $3.text);
			free($3.text);
		}
	| typename paren_group
		{
			$$ = $1;
			appendPQExpBufferStr($$, $2);
			free($2);
		}
	| typename '.' WORD
		{
			$$ = $1;
			appendPQExpBufferChar($$, '.');
			appendPQExpBufferStr($$, $3.text);
			free($3.text);
		}
	;

paren_group:
	'(' paren_inner ')'    { $$ = archiveTocWrapParen($2); }
	;

/*
 * paren_inner reconstructs whatever sits between one balanced pair of
 * parentheses verbatim, the same "accumulate into one threaded
 * PQExpBuffer, not a fresh allocation per item" idea as wordname/
 * typename above -- a typmod rarely has more than a couple of items,
 * but there is no reason for this one to be the odd one out.
 */
paren_inner:
	/* empty */                          { $$ = createPQExpBuffer(); }
	| paren_inner paren_inner_item
		{
			$$ = $1;
			appendPQExpBufferStr($$, $2);
			free($2);
		}
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
 * ArchiveFunctionSignature's own argsText -- not lexed any further
 * than typename already goes.
 */
arglist:
	/* empty */
		{
			$$.items = NULL;
			$$.count = 0;
			$$.cap = 0;
			$$.text = createPQExpBuffer();
		}
	| args                               { $$ = $1; }
	;

args:
	typename
		{
			char *arg = strdup($1->data);
			destroyPQExpBuffer($1);

			$$.items = NULL;
			$$.count = 0;
			$$.cap = 0;
			archiveTocArgListAppend(&$$, arg);

			$$.text = createPQExpBuffer();
			appendPQExpBufferStr($$.text, arg);
		}
	| args ',' SPACE typename
		{
			char *arg = strdup($4->data);
			destroyPQExpBuffer($4);

			$$ = $1;
			archiveTocArgListAppend(&$$, arg);
			appendPQExpBufferStr($$.text, ", ");
			appendPQExpBufferStr($$.text, arg);
		}
	;

entry:
	/* 3580; 0 0 ACL - SCHEMA toc_fixture postgres */
	ACL nspname
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->isCompositeTag = true;
			item->tagKind = ARCHIVE_TAG_KIND_ACL;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archive_toc_begin_nested_tag();
		}
	  nested_entry
	/* 3579; 0 0 COMMENT - SCHEMA toc_fixture postgres */
	| COMMENT nspname
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->isCompositeTag = true;
			item->tagKind = ARCHIVE_TAG_KIND_COMMENT;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archive_toc_begin_nested_tag();
		}
	  nested_entry

	/*
	 * TOP_SIMPLE is a shape, not a keyword: archive_toc_tag_token's own
	 * default case, every top-level tag with no further structure of
	 * its own (TABLE, VIEW, DOMAIN, COLLATION, ... alongside SCHEMA
	 * below) reduces here, the same way TOP_RELATION/TOP_FUNCLIKE below
	 * group several keywords under one shape each -- item->description
	 * still carries the actual keyword text, off $1.text.
	 *
	 * 9; 2615 16401 SCHEMA - toc_fixture postgres
	 * 2210; 3456 16472 COLLATION toc_fixture fixture_collation postgres
	 */
	| TOP_SIMPLE nspname mark name owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			char *body = archiveTocSlice($3, $4);
			archiveTocBuildSimpleNames(item, body, $5);
			free(body);
			free($5);
		}

	/*
	 * TOP_RELATION is a shape too, grouping every top-level tag whose
	 * remainder is "owning relation, own name, owner" -- ATTRDEF/
	 * DEFAULT, CHECK CONSTRAINT, CONSTRAINT, FK CONSTRAINT, POLICY,
	 * PUBLICATION TABLE(S IN SCHEMA), RULE, TRIGGER (see archive_toc_tag_token's own switch) -- not
	 * just DEFAULT, despite the token's own name reading like one
	 * keyword among many others nearby that each got their own token
	 * (TOP_CAST, TOP_TRANSFORM, ...): those are per-keyword because
	 * each is the only tag with that exact shape; this one covers
	 * nine keywords that all share this one.
	 *
	 * 3396; 2604 16420 DEFAULT toc_fixture child id postgres
	 * 3404; 2606 16424 FK CONSTRAINT toc_fixture child child_parent_id_fkey postgres
	 * 3405; 2620 16431 TRIGGER toc_fixture parent parent_trg postgres
	 */
	| TOP_RELATION nspname mark relname SPACE name owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
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
			 * NESTED ACL/COMMENT entry (filtering matches the RELATION,
			 * not the ACL/COMMENT itself), but NOT for this top-level entry,
			 * which has no separate "owning relation" at all: relname
			 * and name here are both just this ONE entry's own fields
			 * (e.g. DEFAULT's own column name), so restoreListName
			 * needs the full verbatim body (both fields), the same as
			 * every other top-level shape -- overridden right after,
			 * covering both fields at once.
			 */
			char *relname = archiveTocSlice($3, $4);
			char *name = archiveTocSlice($5, $6);

			archiveTocBuildRelationNames(item, relname, name, $7);
			archiveTocOverrideRestoreListName(item, $3, $6, $7);

			free(relname);
			free(name);
			free($7);
		}

	/*
	 * TOP_FUNCLIKE is likewise FUNCTION/PROCEDURE/AGGREGATE sharing one
	 * shape (own name plus a parenthesized argument list):
	 *
	 * 277; 1255 16452 FUNCTION toc_fixture fixture_add(integer, integer) postgres
	 * 232; 1255 16453 PROCEDURE toc_fixture fixture_proc(integer) postgres
	 * 932; 1255 16454 AGGREGATE toc_fixture fixture_concat(text) postgres
	 */
	| TOP_FUNCLIKE nspname mark wordname '(' arglist ')' mark owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			bool isStar = ($6.count == 1 && streq($6.items[0], "*"));

			if (isStar)
			{
				free($6.items[0]);
				free($6.items);
				item->signature.args = NULL;
				item->signature.argsCount = 0;
			}
			else
			{
				item->signature.args = $6.items;
				item->signature.argsCount = $6.count;
			}

			item->signature.argsText = strdup($6.text->data);
			destroyPQExpBuffer($6.text);

			/*
			 * item->name stays narrowed to the bare function name
			 * ($4.text), but restoreListName keeps the FULL verbatim
			 * text, signature included --
			 * every other top-level shape's own raw-remainder-preserved
			 * behavior, overridden here since archiveTocBuildSimpleNames
			 * would otherwise build it from the already-narrowed name.
			 */
			archiveTocBuildSimpleNames(item, $4.buf->data, $9);
			archiveTocOverrideRestoreListName(item, $3, $8, $9);

			destroyPQExpBuffer($4.buf);
			free($9);
		}

	/* 1733; 2617 16459 OPERATOR toc_fixture === postgres */
	| TOP_OPERATOR nspname WORD owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			archiveTocBuildSimpleNames(item, $3.text, $4);
			free($3.text);
			free($4);
		}

	/* 3391; 2605 16471 CAST - CAST (toc_fixture.positive_int AS integer)  */
	| TOP_CAST nspname mark CAST_KW '(' castSourceType AS castTargetType ')' mark owner
		{
			/*
			 * CAST_KW matches the repeated "CAST " tag text pg_dump's
			 * own labelq includes (see dumpCast in pg_dump.c), baking
			 * in its own trailing separator space the same way AS/FOR/
			 * LANGUAGE_KW/SERVER_KW/USING/ON do (see archive_toc_
			 * lexer.l's TOCBODY) -- it is always exactly this one fixed
			 * word, never multi-word, so no explicit SPACE is needed
			 * around it here.
			 */
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			item->cast.sourceType = strdup($6->data);
			item->cast.targetType = strdup($8->data);

			char *body = archiveTocSlice($3, $10);
			archiveTocBuildSimpleNames(item, body, $11);
			free(body);

			destroyPQExpBuffer($6);
			destroyPQExpBuffer($8);
			free($11);
		}

	/* 3392; 3576 16582 TRANSFORM - TRANSFORM FOR integer LANGUAGE plpgsql  */
	| TOP_TRANSFORM nspname mark TRANSFORM_KW FOR typename LANGUAGE_KW WORD mark owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			item->transform.type = strdup($6->data);
			strlcpy(item->transform.language, $8.text,
					sizeof(item->transform.language));

			char *body = archiveTocSlice($3, $9);
			archiveTocBuildSimpleNames(item, body, $10);
			free(body);

			destroyPQExpBuffer($6);
			free($8.text);
			free($10);
		}

	/* 3602; 0 0 USER MAPPING - USER MAPPING postgres SERVER fixture_server postgres */
	| TOP_USER_MAPPING nspname mark USER_MAPPING_KW WORD SERVER_KW WORD mark owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			strlcpy(item->userMapping.user, $5.text,
					sizeof(item->userMapping.user));
			strlcpy(item->userMapping.server, $7.text,
					sizeof(item->userMapping.server));

			char *body = archiveTocSlice($3, $8);
			archiveTocBuildSimpleNames(item, body, $9);
			free(body);

			free($5.text);
			free($7.text);
			free($9);
		}

	/* 2151; 826 16584 DEFAULT ACL toc_fixture DEFAULT PRIVILEGES FOR TABLES postgres */
	| TOP_DEFAULT_ACL nspname mark DEFAULT_ACL_KW FOR wordname owner
		{
			item->desc = $1.desc;
			item->description = $1.text;
			item->hasNamespace = ($2 != NULL);

			if ($2 != NULL)
			{
				strlcpy(item->nspname, $2, sizeof(item->nspname));
			}

			strlcpy(item->defaultAclObjectType, $6.buf->data,
					sizeof(item->defaultAclObjectType));

			/*
			 * bodyEnd comes from wordname's own $6.end (set when its
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
			char *body = archiveTocSlice($3, $6.end);
			archiveTocBuildSimpleNames(item, body, $7);
			free(body);

			destroyPQExpBuffer($6.buf);
			free($7);
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
	/*
	 * NESTED_RELATION_LIKE groups TABLE/VIEW/MATERIALIZED VIEW/FOREIGN
	 * TABLE/INDEX/SEQUENCE: every object type whose own ACL/COMMENT
	 * remainder is just its bare name, no further structure --
	 * same "shape token, not a keyword token" idea as TOP_RELATION/
	 * TOP_FUNCLIKE above, one level narrower (see this comment's own
	 * header on nested_entry vs entry).
	 *
	 * 3605; 0 0 COMMENT toc_fixture TABLE parent postgres
	 * 3610; 0 0 COMMENT toc_fixture VIEW parent_view postgres
	 * 3611; 0 0 COMMENT toc_fixture SEQUENCE standalone_seq postgres
	 */
	NESTED_RELATION_LIKE mark objname mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $3.text, "", $5);
			archiveTocBuildUseListRemainder(item, $2, $4, $5);
			free($3.text);
			free($5);
		}

	/* 3606; 0 0 COMMENT toc_fixture COLUMN parent.name postgres */
	| NESTED_COLUMN mark objname '.' objname mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $3.text, $5.text, $7);
			archiveTocBuildUseListRemainder(item, $2, $6, $7);
			free($3.text);
			free($5.text);
			free($7);
		}

	/*
	 * NESTED_ON_RELATION groups CONSTRAINT/TRIGGER/POLICY/RULE: every
	 * object type whose ACL/COMMENT remainder names an owning relation
	 * after "ON", same shape-token idea as above.
	 *
	 * 3618; 0 0 COMMENT toc_fixture TRIGGER parent_trg ON parent postgres
	 * 3617; 0 0 COMMENT toc_fixture RULE child_rule ON child postgres
	 */
	| NESTED_ON_RELATION mark objname ON objname mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;
			archiveTocBuildRelationNames(item, $5.text, $3.text, $7);
			archiveTocBuildUseListRemainder(item, $2, $6, $7);
			free($3.text);
			free($5.text);
			free($7);
		}

	/*
	 * NESTED_FUNCLIKE: FUNCTION/PROCEDURE/AGGREGATE again, same shape
	 * token as TOP_FUNCLIKE above, one level narrower.
	 *
	 * 3589; 0 0 COMMENT toc_fixture FUNCTION fixture_add(a integer, b integer) postgres
	 * 3590; 0 0 COMMENT toc_fixture PROCEDURE fixture_proc(IN a integer) postgres
	 * 3592; 0 0 COMMENT toc_fixture AGGREGATE fixture_concat(text) postgres
	 */
	| NESTED_FUNCLIKE mark objname '(' arglist ')' mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			bool isStar = ($5.count == 1 && streq($5.items[0], "*"));

			if (isStar)
			{
				free($5.items[0]);
				free($5.items);
				item->signature.args = NULL;
				item->signature.argsCount = 0;
			}
			else
			{
				item->signature.args = $5.items;
				item->signature.argsCount = $5.count;
			}

			item->signature.argsText = strdup($5.text->data);
			destroyPQExpBuffer($5.text);

			archiveTocBuildSimpleNames(item, $3.text, $8);
			archiveTocBuildUseListRemainder(item, $2, $7, $8);
			free($3.text);
			free($8);
		}

	/* 3594; 0 0 COMMENT toc_fixture OPERATOR === (integer, integer) postgres */
	| NESTED_OPERATOR mark objname SPACE '(' typename ',' SPACE typename ')' mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			archiveTocSetOperatorArg(&item->operatorArgs.hasLeftArg,
									  &item->operatorArgs.leftArg, $6);
			archiveTocSetOperatorArg(&item->operatorArgs.hasRightArg,
									  &item->operatorArgs.rightArg, $9);

			archiveTocBuildSimpleNames(item, $3.text, $12);
			archiveTocBuildUseListRemainder(item, $2, $11, $12);
			free($3.text);
			free($12);
		}

	/* 3587; 0 0 COMMENT - CAST (toc_fixture.positive_int AS integer)  */
	| NESTED_CAST mark '(' castSourceType AS castTargetType ')' mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			item->cast.sourceType = strdup($4->data);
			item->cast.targetType = strdup($6->data);

			char *body = archiveTocSlice($2, $8);
			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   body, $9);
			archiveTocBuildUseListRemainder(item, $2, $8, $9);
			free(body);

			destroyPQExpBuffer($4);
			destroyPQExpBuffer($6);
			free($9);
		}

	/* 3583; 0 0 COMMENT - TRANSFORM FOR integer LANGUAGE plpgsql  */
	| NESTED_TRANSFORM FOR typename LANGUAGE_KW WORD mark owner
		{
			/*
			 * No leading SPACE/mark here, unlike every other nested
			 * production: FOR's own lexer pattern (archive_toc_lexer.l's
			 * TOCBODY) already consumes the one space separating the
			 * "TRANSFORM" tag from it, so there is no separate SPACE
			 * token left for the grammar to expect, and no WORD/typename
			 * reduction immediately before FOR to anchor a `mark` on
			 * (unlike TOP_TRANSFORM's own repeated tag WORD) -- FOR's
			 * own <pos> value (its start-of-keyword position, not
			 * archive_toc_token_end) is this production's body-start
			 * boundary instead.
			 */
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			item->transform.type = strdup($3->data);
			strlcpy(item->transform.language, $5.text,
					sizeof(item->transform.language));

			char *body = archiveTocSlice($2, $6);
			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   body, $7);
			archiveTocBuildUseListRemainder(item, $2, $6, $7);
			free(body);

			destroyPQExpBuffer($3);
			free($5.text);
			free($7);
		}

	/* 3596; 0 0 COMMENT toc_fixture OPERATOR FAMILY fixture_int_ops USING btree postgres */
	| NESTED_OPCLASS_FAMILY mark name USING WORD mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			strlcpy(item->opClassFamily.accessMethod, $5.text, sizeof(item->opClassFamily.accessMethod));

			/*
			 * item->name/restoreListName use the NARROWED name, sliced
			 * verbatim from before "USING amname" -- only
			 * useListRemainder reproduces the FULL verbatim text,
			 * "USING amname" included.
			 */
			char *opclassName = archiveTocSlice($2, $3);

			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   opclassName, $7);
			archiveTocBuildUseListRemainder(item, $2, $6, $7);

			free(opclassName);
			free($5.text);
			free($7);
		}

	/* 3582; 0 0 COMMENT - EXTENSION fixture_transform  */
	| NESTED_EXTENSION mark WORD mark owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			strlcpy(item->name, $3.text, sizeof(item->name));
			strlcpy(item->owner, $5, sizeof(item->owner));
			item->tagType = ARCHIVE_TAG_TYPE_EXTENSION;

			/*
			 * restoreListName keeps its own, narrower pre-grammar
			 * contract for EXTENSION specifically (bare name, no
			 * namespace/owner -- unlike every other object type's
			 * "nspname name owner", see archiveTocBuildSimpleNames):
			 * no filters catalog query relies on it being anything
			 * else, and this preserves that exactly.
			 */
			item->restoreListName = strdup($3.text);

			archiveTocBuildUseListRemainder(item, $2, $4, $5);

			free($3.text);
			free($5);
		}

	/*
	 * NESTED_GENERIC is archive_toc_tag_token's own nested default
	 * case, the same role TOP_SIMPLE plays at the top level: every
	 * object type nested inside an ACL/COMMENT with no further
	 * structure of its own (STATISTICS, DOMAIN, ... alongside
	 * COLLATION below).
	 *
	 * 3581; 0 0 COMMENT toc_fixture COLLATION fixture_collation postgres
	 * 3616; 0 0 COMMENT toc_fixture STATISTICS fixture_stats postgres
	 */
	| NESTED_GENERIC mark name owner
		{
			item->objectType = $1.desc;
			item->objectTypeText = $1.text;

			/*
			 * bodyEnd is $3 (name's own end position), not a trailing
			 * `mark` -- see TOP_DEFAULT_ACL's own comment on why that
			 * specific placement would be a genuine LALR(1) shift/
			 * reduce conflict with name's own recursion.
			 */
			char *genericName = archiveTocSlice($2, $3);

			archiveTocBuildOpaqueNames(item, item->hasNamespace ? item->nspname : "-",
									   genericName, $4);
			archiveTocBuildUseListRemainder(item, $2, $3, $4);
			free(genericName);
			free($4);
		}
	;

%%

/*
 * archive_toc_error is bison's own error hook: a shape this grammar
 * doesn't recognize (or genuinely malformed input) is not fatal here --
 * "we don't support this one yet, the caller's raw-text fallback
 * stands" (e.g. a domain's own CONSTRAINT, excluded on purpose:
 * "CONSTRAINT name ON DOMAIN domain" is a real shape pg_dump can
 * produce that this grammar doesn't structure). archive_toc_parse_line
 * (archive_toc_parser.c) checks archive_toc_parse()'s own return value
 * and falls back there, so this only logs.
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
 * archiveTocWrapParen wraps `inner` (already reconstructed verbatim by
 * paren_inner, as a PQExpBuffer -- see that nonterminal's own comment)
 * in its own literal parentheses, destroying it, and returns a freshly
 * malloc'ed flat copy: every nonterminal above passes plain char*
 * between productions, so this is where a threaded PQExpBuffer's
 * lifetime ends and ordinary single-ownership malloc'ed text resumes.
 */
static char *
archiveTocWrapParen(PQExpBuffer inner)
{
	PQExpBuffer buf = createPQExpBuffer();

	appendPQExpBufferChar(buf, '(');
	appendPQExpBufferStr(buf, inner->data);
	appendPQExpBufferChar(buf, ')');
	destroyPQExpBuffer(inner);

	if (PQExpBufferBroken(buf))
	{
		log_error(ALLOCATION_FAILED_ERROR);
		destroyPQExpBuffer(buf);
		return NULL;
	}

	char *out = strdup(buf->data);

	destroyPQExpBuffer(buf);

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
 * archiveTocBuildOpaqueNames is their counterpart for an irregular
 * shape, used when `body` is the WHOLE remaining text rather than
 * already-split fields (a nested CAST/TRANSFORM/OPERATOR CLASS or
 * FAMILY's own remainder, or anything else this grammar doesn't
 * structure further).
 */
/*
 * archiveTocFormatThree builds "a b c" (the fixed "nspname/rlNamespace
 * body owner" shape every one of item->restoreListName's own variants
 * below needs) into a freshly malloc'ed string, via PQExpBuffer rather
 * than each caller hand-computing its own byte count: the actual
 * separator convention (single space, three fields) only needs to be
 * right in one place this way.
 */
static char *
archiveTocFormatThree(const char *a, const char *b, const char *c)
{
	PQExpBuffer buf = createPQExpBuffer();

	appendPQExpBuffer(buf, "%s %s %s", a, b, c);

	if (PQExpBufferBroken(buf))
	{
		log_error(ALLOCATION_FAILED_ERROR);
		destroyPQExpBuffer(buf);
		return NULL;
	}

	char *out = strdup(buf->data);

	destroyPQExpBuffer(buf);

	return out;
}


static void
archiveTocBuildSimpleNames(ArchiveContentItem *item, const char *name,
						   const char *owner)
{
	strlcpy(item->name, name, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	const char *nspname = item->hasNamespace ? item->nspname : "-";

	item->restoreListName = archiveTocFormatThree(nspname, name, owner);
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

	item->restoreListName = archiveTocFormatThree(nspname, relname, owner);
}


static void
archiveTocBuildOpaqueNames(ArchiveContentItem *item, const char *rlNamespace,
						  const char *body, const char *owner)
{
	strlcpy(item->name, body, sizeof(item->name));
	strlcpy(item->owner, owner, sizeof(item->owner));
	item->tagType = ARCHIVE_TAG_TYPE_OTHER;

	item->restoreListName = archiveTocFormatThree(rlNamespace, body, owner);
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
 * needed). When owner is empty, the trailing "%s" contributes nothing but its
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

	PQExpBuffer buf = createPQExpBuffer();

	appendPQExpBuffer(buf, "%s %s %s %s",
					   rlNamespace, item->objectTypeText, body, owner);
	free(body);

	if (PQExpBufferBroken(buf))
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}
	else
	{
		item->useListRemainder = strdup(buf->data);
	}

	destroyPQExpBuffer(buf);
}


/*
 * archiveTocOverrideRestoreListName replaces item->restoreListName
 * (just set by archiveTocBuildSimpleNames/RelationNames from an
 * already-narrowed field) with "nspname body owner", body sliced
 * verbatim between two marks -- for the handful of top-level shapes
 * whose own item->name is deliberately narrower than their restore
 * ListName ought to be: FUNCTION/PROCEDURE/AGGREGATE's own bare name
 * (signature stripped) and RELATION's own relname (the second field,
 * name, stripped) --
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

	item->restoreListName = archiveTocFormatThree(nspname, body, owner);

	free(body);
}


/*
 * archiveTocSetOperatorArg sets one side of an OPERATOR's own operand
 * types (item->operatorArgs' own leftArg/rightArg) from its already-
 * reconstructed typename buffer, "NONE" literally (dumpOpr's own text
 * for a prefix or postfix operator missing that side, pg_dump.c)
 * meaning absent rather than a real type name. Always destroys `text`:
 * this is where typename's own threaded PQExpBuffer lifetime ends.
 */
static void
archiveTocSetOperatorArg(bool *hasArg, char **arg, PQExpBuffer text)
{
	*hasArg = !streq(text->data, "NONE");
	*arg = *hasArg ? strdup(text->data) : NULL;

	destroyPQExpBuffer(text);
}
